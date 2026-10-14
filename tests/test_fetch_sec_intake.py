from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "fetch_sec_intake.py"
FIXTURE = ROOT / "tests" / "fixtures" / "master.20250102.idx"
SPEC = importlib.util.spec_from_file_location("fetch_sec_intake", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class IntakeTests(unittest.TestCase):
    def test_parse_master_index_filters_forms(self):
        rows = list(MODULE.parse_master_index(FIXTURE.read_text(encoding="latin-1")))
        self.assertEqual([row.form for row in rows], ["4", "10-Q", "8-K"])
        self.assertEqual(rows[0].cik, "0000320193")

    def test_accession_and_urls(self):
        accession = MODULE.accession_from_filename(
            "edgar/data/320193/000032019325000001.txt"
        )
        self.assertEqual(accession, "0000320193-25-000001")
        self.assertEqual(
            MODULE.filing_index_url("0000320193", accession),
            "https://www.sec.gov/Archives/edgar/data/320193/000032019325000001/"
            "0000320193-25-000001-index.html",
        )

    def test_compact_sec_date_is_normalized(self):
        self.assertEqual(MODULE.normalize_filed_date("20250102"), "2025-01-02")
        self.assertEqual(MODULE.normalize_filed_date("2025-01-02"), "2025-01-02")

    def test_local_run_is_idempotent(self):
        # Keep test writes inside the checkout. Some managed Windows runners
        # aggressively scan the global temporary directory and can stall an
        # atomic file creation there.
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary_directory:
            parser = MODULE.build_parser()
            arguments = parser.parse_args(
                [
                    "--input-file",
                    str(FIXTURE),
                    "--output-dir",
                    temporary_directory,
                ]
            )
            self.assertEqual(MODULE.run(arguments), 0)
            self.assertEqual(MODULE.run(arguments), 0)

            output = Path(temporary_directory)
            lines = (output / "filings.jsonl").read_text(encoding="utf-8").splitlines()
            records = [json.loads(line) for line in lines]
            audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
            self.assertEqual(len(records), 3)
            self.assertEqual(audit["newRecords"], 0)
            self.assertEqual(audit["duplicateRecords"], 3)
            self.assertTrue(all(record["acceptedAt"] is None for record in records))

    def test_existing_record_conflict_fails(self):
        prior = {
            "recordId": "0000320193-25-000001:0000320193",
            "accessionNumber": "0000320193-25-000001",
            "cik": "0000320193",
            "companyName": "WRONG NAME",
            "form": "4",
            "filedDate": "2025-01-02",
            "filingTextUrl": "x",
        }
        incoming = {
            "recordId": "0000320193-25-000001:0000320193",
            "accessionNumber": "0000320193-25-000001",
            "cik": "0000320193",
            "companyName": "APPLE INC",
            "form": "4",
            "filedDate": "2025-01-02",
            "filingTextUrl": "y",
        }
        with self.assertRaises(MODULE.IntakeFailure):
            MODULE.merge_records({prior["recordId"]: prior}, [incoming])

    def test_one_accession_can_preserve_multiple_ciks(self):
        base = {
            "accessionNumber": "0001415889-25-000240",
            "companyName": "OSI SYSTEMS INC",
            "form": "4",
            "filedDate": "2025-01-02",
            "filingTextUrl": "https://example.test/a",
        }
        issuer = {
            **base,
            "recordId": "0001415889-25-000240:0001039065",
            "cik": "0001039065",
        }
        owner = {
            **base,
            "recordId": "0001415889-25-000240:0001047058",
            "cik": "0001047058",
            "companyName": "CHOPRA DEEPAK",
            "filingTextUrl": "https://example.test/b",
        }
        merged, added = MODULE.merge_records({}, [issuer, owner])
        self.assertEqual(added, 2)
        self.assertEqual(len(merged), 2)


if __name__ == "__main__":
    unittest.main()
