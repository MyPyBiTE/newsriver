from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "build_ticker_directory", SCRIPTS / "build_ticker_directory.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def payload() -> dict:
    return {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "aapl", "Nasdaq"],
            [789019, "Microsoft Corp.", "MSFT", "Nasdaq"],
            [19617, "JPMorgan Chase & Co.", "JPM", "NYSE"],
            [999, "Example OTC Company", "OTCX", "OTC"],
            [1000, "Example Cboe Fund", "FUND", "CBOE"],
        ],
    }


class TickerDirectoryTests(unittest.TestCase):
    def test_builds_compact_us_directory_and_excludes_otc_by_default(self):
        result = MODULE.build_directory(payload(), generated_at="2026-10-26T00:00:00Z")
        rows = {row[0]: row for row in result["data"]}
        self.assertEqual(result["schema"], MODULE.SCHEMA)
        self.assertEqual(result["coverage"]["securityCount"], 4)
        self.assertEqual(rows["AAPL"][2], "0000320193")
        self.assertEqual(rows["JPM"][3], "NYSE")
        self.assertNotIn("OTCX", rows)
        self.assertTrue(result["manualEntry"]["accepted"])
        self.assertEqual(result["canada"]["status"], "not-published")
        json.dumps(result)

    def test_otc_is_opt_in(self):
        result = MODULE.build_directory(payload(), include_otc=True)
        symbols = {row[0] for row in result["data"]}
        self.assertIn("OTCX", symbols)

    def test_rejects_missing_schema_fields(self):
        with self.assertRaisesRegex(ValueError, "missing fields"):
            MODULE.parse_sec_directory({"fields": ["ticker"], "data": []})

    def test_rejects_malformed_rows(self):
        broken = payload()
        broken["data"] = [[320193, "Apple Inc."]]
        with self.assertRaisesRegex(ValueError, "malformed"):
            MODULE.parse_sec_directory(broken)


if __name__ == "__main__":
    unittest.main()
