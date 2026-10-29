from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "build_eod_price_cache", SCRIPTS / "build_eod_price_cache.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def approved_input() -> dict:
    return {
        "schema": "mypybite.approved-price-input.v1",
        "source": {
            "name": "Approved test source",
            "permission": "Public delayed display granted for MYPYBITE",
            "attribution": "Approved test source — delayed close",
            "url": "https://example.invalid/licence",
        },
        "quotes": [
            {
                "symbol": "aapl",
                "price": 211.1234567,
                "currency": "usd",
                "tradeDate": "2026-10-26",
            }
        ],
    }


class EodPriceCacheTests(unittest.TestCase):
    def test_normalizes_an_approved_delayed_quote(self):
        source, quotes = MODULE.normalize_input(approved_input())
        self.assertEqual(source["name"], "Approved test source")
        self.assertEqual(quotes["AAPL"]["price"], 211.123457)
        self.assertEqual(quotes["AAPL"]["currency"], "USD")
        self.assertEqual(quotes["AAPL"]["status"], "previous-close")

    def test_preserves_iex_reference_trade_labels(self):
        payload = approved_input()
        payload["source"]["scope"] = "IEX-only U.S. trades; not a consolidated close"
        payload["source"]["captureDate"] = "20261026"
        payload["quotes"][0].update(
            {"status": "iex-last-trade-t1", "venue": "IEX", "tradeSize": 100}
        )
        source, quotes = MODULE.normalize_input(payload)
        self.assertEqual(source["captureDate"], "20261026")
        self.assertIn("not a consolidated", source["scope"])
        self.assertEqual(quotes["AAPL"]["status"], "iex-last-trade-t1")
        self.assertEqual(quotes["AAPL"]["venue"], "IEX")
        self.assertEqual(quotes["AAPL"]["tradeSize"], 100)

    def test_rejects_pending_or_personal_use_permission(self):
        for permission in ("pending", "unknown", "personal use", "not granted"):
            payload = approved_input()
            payload["source"]["permission"] = permission
            with self.subTest(permission=permission), self.assertRaises(ValueError):
                MODULE.normalize_input(payload)

    def test_rejects_missing_or_nonpositive_prices(self):
        for value in (None, 0, -1, "not-a-price"):
            payload = approved_input()
            payload["quotes"][0]["price"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                MODULE.normalize_input(payload)

    def test_preserves_old_quotes_when_refresh_is_partial(self):
        source, fresh = MODULE.normalize_input(approved_input())
        existing = {
            "MSFT": {
                "symbol": "MSFT",
                "price": 400,
                "currency": "USD",
                "asOf": "2026-10-25",
                "status": "previous-close",
            }
        }
        payload = MODULE.build_payload(
            source,
            fresh,
            existing,
            generated_at="2026-10-27T00:00:00Z",
        )
        rows = {row["symbol"]: row for row in payload["prices"]}
        self.assertEqual(rows["AAPL"]["refreshState"], "fresh")
        self.assertEqual(rows["MSFT"]["refreshState"], "preserved")
        self.assertEqual(payload["coverage"]["preservedFromPriorRun"], 1)

    def test_does_not_accept_an_empty_quote_batch(self):
        payload = approved_input()
        payload["quotes"] = []
        with self.assertRaises(ValueError):
            MODULE.normalize_input(payload)


if __name__ == "__main__":
    unittest.main()
