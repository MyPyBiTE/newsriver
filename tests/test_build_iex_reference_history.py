"""Contract tests for the all-ticker IEX history accumulator; no network access."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import build_iex_reference_history as mod


def cache(quotes: list[dict]) -> str:
    return json.dumps({"schema": mod.INPUT_SCHEMA, "prices": quotes})


def quote(symbol: str, date: str, price: float, *, state: str = "fresh") -> dict:
    return {
        "symbol": symbol, "price": price, "currency": "USD",
        "asOf": date + "T20:55:00Z", "venue": "IEX",
        "status": "iex-last-trade-t1", "refreshState": state,
    }


class AllTickerHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.name", "Test"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.email", "test@example.org"], check=True)
        self.cache_file = self.repo / mod.RELATIVE_PRICE_FILE
        self.cache_file.parent.mkdir(parents=True)
        self.output = self.repo / "outputs" / "iex-reference-history-all.json"

    def snapshot(self, rows: list[dict]):
        self.cache_file.write_text(cache(rows), encoding="utf-8")
        subprocess.run(["git", "-C", str(self.repo), "add", mod.RELATIVE_PRICE_FILE], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "snapshot"], check=True)

    def test_bootstrap_multiple_tickers_and_preserved_date_not_duplicated(self):
        self.snapshot([quote("AAPL", "2026-11-02", 200), quote("TSLA", "2026-11-02", 400)])
        self.snapshot([quote("AAPL", "2026-11-03", 201), quote("TSLA", "2026-11-02", 400, state="preserved")])
        got = mod.build_all(self.repo, self.output)
        self.assertEqual(got["coverage"]["symbolCount"], 2)
        self.assertEqual(got["coverage"]["distinctUtcDates"], 2)
        self.assertEqual(got["coverage"]["observationCount"], 3)
        dates = got["dates"]
        self.assertEqual([(dates[i], p) for i, p in got["series"]["TSLA"]], [("2026-11-02", 400.0)])
        self.assertEqual([(dates[i], p) for i, p in got["series"]["AAPL"]],
                         [("2026-11-02", 200.0), ("2026-11-03", 201.0)])
        self.assertFalse(got["coverage"]["annualSharpeReady"])

    def test_incremental_new_date_and_skip_duplicate(self):
        self.snapshot([quote("AAPL", "2026-11-02", 200)])
        first = mod.build_all(self.repo, self.output)
        orig_bytes = self.output.read_bytes()
        mod.build_all(self.repo, self.output)
        self.assertEqual(orig_bytes, self.output.read_bytes())
        # No new Git commit: daily working-tree EOD is enough after bootstrap.
        self.cache_file.write_text(cache([quote("AAPL", "2026-11-03", 202), quote("MSFT", "2026-11-03", 500)]))
        new = mod.build_all(self.repo, self.output)
        self.assertEqual(new["coverage"]["symbolCount"], 2)
        self.assertEqual(new["coverage"]["observationCount"], 3)
        self.assertEqual(first["series"]["AAPL"][0][1], 200.0)
        self.assertEqual(new["series"]["AAPL"][1][1], 202.0)

    def test_invalid_latest_cache_does_not_overwrite_history(self):
        self.snapshot([quote("AAPL", "2026-11-02", 200)])
        mod.build_all(self.repo, self.output)
        before = self.output.read_bytes()
        self.cache_file.write_text(cache([quote("AAPL", "2026-11-03", float("nan"))]))
        with self.assertRaises(ValueError):
            mod.build_all(self.repo, self.output)
        self.assertEqual(before, self.output.read_bytes())

    def test_unknown_schema_does_not_overwrite_history(self):
        self.snapshot([quote("AAPL", "2026-11-02", 200)])
        mod.build_all(self.repo, self.output)
        before = self.output.read_bytes()
        self.output.write_text('{"schema":"other"}')
        with self.assertRaises(ValueError):
            mod.build_all(self.repo, self.output)
        self.assertNotEqual(before, self.output.read_bytes())
        self.assertEqual(self.output.read_text(), '{"schema":"other"}')


if __name__ == "__main__":
    unittest.main()
