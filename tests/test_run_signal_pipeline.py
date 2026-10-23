from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS / "run_signal_pipeline.py"
SPEC = importlib.util.spec_from_file_location("run_signal_pipeline", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


ACCESSION = "0000015847-26-000051"
COMPACT = ACCESSION.replace("-", "")
INDEX_URL = f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/{ACCESSION}-index.html"
PRIMARY_URL = f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/buks-20261007.htm"
EXHIBIT_URL = f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/bnc-stockbuybackpressrelea.htm"
PRIMARY = """<!doctype html><html><body>
<h2>Item 5.02</h2>
<p>On October 21, 2026, the Board of Directors appointed Jane Doe and increased the board from 12 directors to 14 directors.</p>
<p>The appointment is effective immediately and remains subject to shareholder approval by October 31, 2026.</p>
<p>The appointment will help strengthen oversight of the company's long-term strategy.</p>
</body></html>"""


class FakeClient:
    def __init__(self, responses: dict[str, str]) -> None:
        self.responses = responses
        self.requests: list[str] = []

    def get_text(self, url: str):
        self.requests.append(url)
        return self.responses.get(url)


def detection() -> dict:
    return {
        "schema": "mypybite.sec-latest.v1",
        "recordId": f"{ACCESSION}:0000015847",
        "accessionNumber": ACCESSION,
        "cik": "0000015847",
        "companyName": "EXAMPLE CORP",
        "form": "8-K",
        "filedDate": "2026-10-21",
        "isAmendment": False,
        "filingIndexUrl": INDEX_URL,
        "filingTextUrl": f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/{ACCESSION}.txt",
    }


def metadata() -> dict:
    return {
        "schema": "mypybite.sec-filing-metadata.v1",
        "accessionNumber": ACCESSION,
        "acceptedAt": "2026-10-21T12:00:00-04:00",
        "items": ["5.02"],
    }


def roster() -> dict[str, dict]:
    return {
        "0000015847": {
            "cik": "0000015847",
            "legalName": "EXAMPLE CORP",
            "ticker": "EX",
            "cohort": "test",
            "relevance": ["governance"],
            "coverage": "domestic",
        }
    }


def client() -> FakeClient:
    index_html = (ROOT / "sample-filing-index.html").read_text(encoding="utf-8")
    return FakeClient(
        {
            INDEX_URL: index_html,
            PRIMARY_URL: PRIMARY,
            EXHIBIT_URL: "<html><body><p>The board said the appointment will help strengthen oversight.</p></body></html>",
        }
    )


class SignalPipelineTests(unittest.TestCase):
    def test_end_to_end_candidate_becomes_public_event(self) -> None:
        now = datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            state, feed, audit = MODULE.run_pipeline(
                [detection()],
                [metadata()],
                roster(),
                MODULE.blank_state(),
                MODULE.blank_public_feed(now),
                client(),
                Path(name),
                now,
            )
        self.assertEqual(audit["autoApprovedCount"], 1)
        self.assertEqual(audit["quarantinedCount"], 0)
        self.assertEqual(len(feed["events"]), 1)
        self.assertEqual(feed["events"][0]["quality"]["reviewMethod"], "automated")
        self.assertEqual(next(iter(state["candidates"].values()))["status"], "auto-approved")

    def test_processed_accession_is_not_fetched_twice(self) -> None:
        now = datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)
        fake = client()
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            state, feed, _ = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), MODULE.blank_state(),
                MODULE.blank_public_feed(now), fake, Path(name), now,
            )
            request_count = len(fake.requests)
            _, second_feed, audit = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), state, feed, fake, Path(name), now,
            )
        self.assertEqual(audit["pendingCandidateCount"], 0)
        self.assertEqual(len(fake.requests), request_count)
        self.assertEqual(len(second_feed["events"]), 1)

    def test_existing_public_event_marks_accession_processed(self) -> None:
        now = datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)
        fake = client()
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            state, feed, _ = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), MODULE.blank_state(),
                MODULE.blank_public_feed(now), fake, Path(name), now,
            )
            empty_state = MODULE.blank_state()
            request_count = len(fake.requests)
            _, _, audit = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), empty_state, feed, fake, Path(name), now,
            )
        self.assertEqual(audit["pendingCandidateCount"], 0)
        self.assertEqual(len(fake.requests), request_count)

    def test_merge_preserves_existing_events(self) -> None:
        now = datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            _, first_feed, _ = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), MODULE.blank_state(),
                MODULE.blank_public_feed(now), client(), Path(name), now,
            )
        merged = MODULE.merge_public_events(first_feed, [], now)
        self.assertEqual([item["eventId"] for item in merged["events"]], [item["eventId"] for item in first_feed["events"]])

    def test_collection_failure_quarantines_one_candidate(self) -> None:
        now = datetime(2026, 10, 23, 21, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            state, feed, audit = MODULE.run_pipeline(
                [detection()], [metadata()], roster(), MODULE.blank_state(),
                MODULE.blank_public_feed(now), FakeClient({}), Path(name), now,
            )
        record = next(iter(state["candidates"].values()))
        self.assertEqual(record["status"], "quarantined")
        self.assertEqual(record["stage"], "document-collection")
        self.assertEqual(audit["quarantinedCount"], 1)
        self.assertEqual(feed["events"], [])


if __name__ == "__main__":
    unittest.main()
