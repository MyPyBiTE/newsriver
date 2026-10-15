from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SCRIPT = SCRIPTS / "enrich_sec_metadata.py"
FIXTURE = ROOT / "tests" / "fixtures" / "filing-index.html"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location("enrich_sec_metadata", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeClient:
    def __init__(self, html: str) -> None:
        self.html = html
        self.urls: list[str] = []

    def get_text(self, url: str):
        self.urls.append(url)
        return self.html


class EnrichmentTests(unittest.TestCase):
    def test_parse_authoritative_summary(self):
        result = MODULE.parse_filing_summary(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(result["filingDate"], "2026-10-07")
        self.assertEqual(result["acceptedAt"], "2026-10-07T11:30:53-04:00")
        self.assertEqual(result["periodOfReport"], "2026-10-07")
        self.assertEqual(result["documentCount"], 12)
        self.assertEqual(len(result["items"]), 2)

    def test_enrichment_is_accession_keyed_and_idempotent(self):
        html = FIXTURE.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary_directory:
            temporary = Path(temporary_directory)
            intake_path = temporary / "intake.jsonl"
            output = temporary / "metadata"
            source_url = "https://www.sec.gov/example-index.html"
            base = {
                "accessionNumber": "0000015847-26-000051",
                "filedDate": "2026-10-07",
                "filingIndexUrl": source_url,
            }
            intake_path.write_text(
                json.dumps({**base, "cik": "0000015847"})
                + "\n"
                + json.dumps({**base, "cik": "0000999999"})
                + "\n",
                encoding="utf-8",
            )
            args = MODULE.build_parser().parse_args(
                [
                    "--input",
                    str(intake_path),
                    "--output-dir",
                    str(output),
                    "--max-filings",
                    "10",
                ]
            )
            first_client = FakeClient(html)
            self.assertEqual(MODULE.run(args, first_client), 0)
            self.assertEqual(first_client.urls, [source_url])
            second_client = FakeClient(html)
            self.assertEqual(MODULE.run(args, second_client), 0)
            self.assertEqual(second_client.urls, [])
            records = (output / "filings.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(records), 1)
            state = json.loads((output / "state.json").read_text(encoding="utf-8"))
            self.assertTrue(state["complete"])

    def test_filing_date_conflict_fails_closed(self):
        html = FIXTURE.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary_directory:
            temporary = Path(temporary_directory)
            intake_path = temporary / "intake.jsonl"
            output = temporary / "metadata"
            intake_path.write_text(
                json.dumps(
                    {
                        "accessionNumber": "0000015847-26-000051",
                        "filedDate": "2026-10-06",
                        "filingIndexUrl": "https://www.sec.gov/example-index.html",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            args = MODULE.build_parser().parse_args(
                ["--input", str(intake_path), "--output-dir", str(output)]
            )
            with self.assertRaisesRegex(MODULE.IntakeFailure, "failed for 1"):
                MODULE.run(args, FakeClient(html))
            audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
            self.assertEqual(audit["failedRecords"], 1)
            self.assertEqual(audit["newRecords"], 0)


if __name__ == "__main__":
    unittest.main()
