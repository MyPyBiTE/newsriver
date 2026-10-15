from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SCRIPT = SCRIPTS / "fetch_sec_latest.py"
FIXTURE = ROOT / "tests" / "fixtures" / "latest-filings.atom"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location("fetch_sec_latest", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LatestFeedTests(unittest.TestCase):
    def test_feed_filters_normalizes_and_preserves_related_ciks(self):
        records = MODULE.parse_feed(
            FIXTURE.read_text(encoding="latin-1"),
            "file://fixture",
            "2026-10-15T01:00:00Z",
        )
        self.assertEqual(len(records), 4)
        self.assertEqual([record["form"] for record in records], ["4", "4", "SC 13G", "8-K"])
        first_accession = [
            record for record in records
            if record["accessionNumber"] == "0001683168-26-007854"
        ]
        self.assertEqual(len(first_accession), 2)
        self.assertEqual(
            {record["cik"] for record in first_accession},
            {"0001496443", "0001506415"},
        )
        self.assertEqual(records[0]["role"], "Issuer")

    def test_local_run_is_idempotent_and_noop_does_not_rewrite_state(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary_directory:
            args = MODULE.build_parser().parse_args(
                [
                    "--input-file",
                    str(FIXTURE),
                    "--output-dir",
                    temporary_directory,
                ]
            )
            self.assertEqual(MODULE.run(args), 0)
            state_path = Path(temporary_directory) / "state.json"
            first_state = state_path.read_text(encoding="utf-8")
            self.assertEqual(MODULE.run(args), 0)
            self.assertEqual(state_path.read_text(encoding="utf-8"), first_state)
            records = [
                json.loads(line)
                for line in (Path(temporary_directory) / "filings.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(records), 4)

    def test_changed_official_entry_fails_closed(self):
        existing = {
            "a:1": {
                "recordId": "a:1",
                "accessionNumber": "a",
                "cik": "1",
                "companyName": "Original",
            }
        }
        incoming = {
            "recordId": "a:1",
            "accessionNumber": "a",
            "cik": "1",
            "companyName": "Changed",
        }
        with self.assertRaises(MODULE.IntakeFailure):
            MODULE.merge_records(existing, [incoming])


if __name__ == "__main__":
    unittest.main()
