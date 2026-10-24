from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "discover_leadership_identities", SCRIPTS / "discover_leadership_identities.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LeadershipDiscoveryTests(unittest.TestCase):
    def test_parses_officer_identity_and_role(self) -> None:
        xml_text = (ROOT / "tests" / "fixtures" / "form4-officer.xml").read_text(
            encoding="utf-8"
        )
        records = MODULE.parse_form4_officers(xml_text)
        self.assertEqual(records[0]["personCik"], "0001234567")
        self.assertEqual(records[0]["roles"], ["chief-executive"])
        self.assertEqual(records[0]["issuerCik"], "0000320193")

    def test_title_classifier_handles_abbreviations(self) -> None:
        self.assertEqual(MODULE.classify_title("Chairman & CEO"), {"chief-executive"})
        self.assertEqual(MODULE.classify_title("EVP and CFO"), {"chief-financial"})

    def test_latest_observation_wins_per_role(self) -> None:
        candidates = [
            {"role": "chief-executive", "lastObservedDate": "2026-09-01", "canonicalName": "Old", "personCik": "1"},
            {"role": "chief-executive", "lastObservedDate": "2026-10-01", "canonicalName": "New", "personCik": "2"},
        ]
        selected = MODULE.latest_per_role(candidates)
        self.assertEqual([item["canonicalName"] for item in selected], ["New"])

    def test_recent_submission_arrays_must_align(self) -> None:
        malformed = {"filings": {"recent": {"form": ["4"], "filingDate": []}}}
        with self.assertRaises(MODULE.IntakeFailure):
            list(MODULE.recent_filings(malformed))


if __name__ == "__main__":
    unittest.main()
