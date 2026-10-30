from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "build_fundamental_lookup_cache", SCRIPTS / "build_fundamental_lookup_cache.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def directory_payload() -> dict:
    return {
        "schema": "mypybite.ticker-directory.v1",
        "fields": ["symbol", "name", "cik", "exchange"],
        "data": [
            ["GOOG", "Alphabet Inc.", "0001652044", "Nasdaq"],
            ["GOOGL", "Alphabet Inc.", "0001652044", "Nasdaq"],
            ["AAPL", "Apple Inc.", "0000320193", "Nasdaq"],
            ["MSFT", "Microsoft Corp.", "0000789019", "Nasdaq"],
        ],
    }


class FundamentalLookupCacheTests(unittest.TestCase):
    def test_directory_deduplicates_share_classes_by_cik(self):
        companies = MODULE.directory_companies(directory_payload())
        self.assertEqual(len(companies), 3)
        self.assertEqual(companies["0001652044"]["aliases"], ["GOOG", "GOOGL"])

    def test_priority_ticker_is_first_then_uncached_entries(self):
        companies = MODULE.directory_companies(directory_payload())
        cache = {"0000320193": {"cik": "0000320193", "refreshedAt": "2026-01-01T00:00:00Z"}}
        selected = MODULE.choose_batch(companies, cache, 2, ["MSFT"])
        self.assertEqual([item["ticker"] for item in selected], ["MSFT", "GOOG"])

    def test_oldest_cached_entry_is_refreshed_after_full_coverage(self):
        companies = MODULE.directory_companies(directory_payload())
        cache = {
            "0000320193": {"cik": "0000320193", "refreshedAt": "2026-03-01T00:00:00Z"},
            "0000789019": {"cik": "0000789019", "refreshedAt": "2026-02-01T00:00:00Z"},
            "0001652044": {"cik": "0001652044", "refreshedAt": "2026-01-01T00:00:00Z"},
        }
        selected = MODULE.choose_batch(companies, cache, 1)
        self.assertEqual(selected[0]["ticker"], "GOOG")

    def test_large_batch_is_only_used_during_initial_fill(self):
        companies = MODULE.directory_companies(directory_payload())
        partial = {"0000320193": {"cik": "0000320193"}}
        self.assertEqual(
            MODULE.effective_batch_size(
                companies,
                partial,
                initial_batch_size=6000,
                refresh_batch_size=100,
                initial_fill_minimum=2,
            ),
            6000,
        )
        broad = {
            "0000320193": {"cik": "0000320193"},
            "0000789019": {"cik": "0000789019"},
        }
        self.assertEqual(
            MODULE.effective_batch_size(
                companies,
                broad,
                initial_batch_size=6000,
                refresh_batch_size=100,
                initial_fill_minimum=2,
            ),
            100,
        )

    def test_compact_company_never_carries_market_price(self):
        compact = MODULE.compact_company(
            {
                "ticker": "AAPL",
                "cik": "0000320193",
                "legalName": "Apple Inc.",
                "status": "available",
                "marketPrice": 999,
                "valuationInputs": {"freeCashFlow": 12, "netDebt": -4},
                "facts": {"sharesOutstanding": {"value": 100}, "assets": {"value": 200}},
            },
            refreshed_at="2026-10-27T00:00:00Z",
        )
        self.assertIsNone(compact["marketPrice"])
        self.assertEqual(compact["facts"]["sharesOutstanding"]["value"], 100)
        self.assertNotIn("assets", compact["facts"])

    def test_payload_reports_incremental_coverage(self):
        companies = MODULE.directory_companies(directory_payload())
        cache = {"0000320193": {"cik": "0000320193"}}
        payload = MODULE.build_payload(
            companies,
            cache,
            attempted=2,
            refreshed=1,
            failures=[{"ticker": "MSFT", "reason": "temporary"}],
            generated_at="2026-10-27T00:00:00Z",
        )
        self.assertEqual(payload["coverage"]["eligibleCiks"], 3)
        self.assertEqual(payload["coverage"]["cachedCiks"], 1)
        self.assertEqual(payload["coverage"]["remainingCiks"], 2)
        self.assertFalse(payload["marketPriceProvided"])


if __name__ == "__main__":
    unittest.main()
