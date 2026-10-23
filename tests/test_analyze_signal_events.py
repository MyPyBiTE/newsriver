from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS / "analyze_signal_events.py"
SPEC = importlib.util.spec_from_file_location("analyze_signal_events", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def detection(accession: str, cik: str, form: str, name: str) -> dict:
    compact = accession.replace("-", "")
    return {
        "schema": "mypybite.sec-latest.v1",
        "recordId": f"{accession}:{cik}",
        "accessionNumber": accession,
        "cik": cik,
        "companyName": name,
        "form": form,
        "filedDate": "2026-10-22",
        "isAmendment": form.endswith("/A"),
        "filingIndexUrl": f"https://www.sec.gov/Archives/{compact}-index.htm",
        "filingTextUrl": f"https://www.sec.gov/Archives/{compact}.txt",
    }


def metadata(accession: str) -> dict:
    return {
        "schema": "mypybite.sec-filing-metadata.v1",
        "accessionNumber": accession,
        "acceptedAt": "2026-10-22T14:30:00-04:00",
        "items": ["5.02"],
    }


class GenericAnalysisQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.roster = {
            "0000070858": {
                "cik": "0000070858",
                "ticker": "BAC",
                "legalName": "Bank of America Corporation",
                "cohort": "financial-system",
                "relevance": ["systemic-bank"],
                "coverage": "domestic",
            },
            "0001050446": {
                "cik": "0001050446",
                "ticker": "MSTR",
                "legalName": "Strategy Inc",
                "cohort": "reflexive-financing",
                "relevance": ["crypto-treasury"],
                "coverage": "domestic",
            },
        }
        self.now = datetime(2026, 10, 23, 15, 0, tzinfo=timezone.utc)

    def test_company_agnostic_queue_joins_identity_and_metadata(self) -> None:
        bac_accession = "0000070858-26-000494"
        mstr_accession = "0001193125-26-423400"
        queue = MODULE.build_queue(
            [
                detection(bac_accession, "0000070858", "8-K", "BANK OF AMERICA"),
                detection(mstr_accession, "0001050446", "8-K", "Strategy Inc"),
            ],
            [metadata(bac_accession), metadata(mstr_accession)],
            self.roster,
            now=self.now,
        )
        self.assertEqual(queue["schema"], "mypybite.signal-analysis-queue.v1")
        self.assertFalse(queue["publicReleaseAllowed"])
        self.assertEqual(queue["stats"]["candidateCount"], 2)
        self.assertEqual(queue["stats"]["metadataVerifiedCount"], 2)
        self.assertEqual(
            {item["issuer"]["ticker"] for item in queue["candidates"]},
            {"BAC", "MSTR"},
        )
        self.assertTrue(
            all(item["analysis"]["engine"] == "generic-signal-analyzer" for item in queue["candidates"])
        )
        self.assertTrue(
            all(not item["analysis"]["publicReleaseAllowed"] for item in queue["candidates"])
        )

    def test_unwatched_issuer_is_skipped(self) -> None:
        queue = MODULE.build_queue(
            [detection("0000000001-26-000001", "0000000001", "8-K", "Other")],
            [],
            self.roster,
            now=self.now,
        )
        self.assertEqual(queue["stats"]["candidateCount"], 0)
        self.assertEqual(queue["stats"]["skippedUnwatchedCount"], 1)

    def test_missing_metadata_is_explicit_and_never_publishable(self) -> None:
        queue = MODULE.build_queue(
            [detection("0000070858-26-000494", "0000070858", "8-K", "BAC")],
            [],
            self.roster,
            now=self.now,
        )
        candidate = queue["candidates"][0]
        self.assertIsNone(candidate["filing"]["acceptedAt"])
        self.assertFalse(candidate["quality"]["acceptanceTimestampVerified"])
        self.assertFalse(candidate["analysis"]["publicReleaseAllowed"])

    def test_invalid_detection_fails_closed(self) -> None:
        invalid = detection("0000070858-26-000494", "0000070858", "8-K", "BAC")
        invalid["schema"] = "unknown"
        with self.assertRaisesRegex(MODULE.AnalysisQueueFailure, "detection schema"):
            MODULE.build_queue([invalid], [], self.roster, now=self.now)

    def test_queue_is_json_serializable(self) -> None:
        queue = MODULE.build_queue(
            [detection("0000070858-26-000494", "0000070858", "8-K", "BAC")],
            [metadata("0000070858-26-000494")],
            self.roster,
            now=self.now,
        )
        self.assertIn("signal-analysis-candidate.v1", json.dumps(queue))


if __name__ == "__main__":
    unittest.main()
