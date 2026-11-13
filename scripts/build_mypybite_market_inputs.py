#!/usr/bin/env python3
"""Validate and combine independent MYPYBITE market-data observations.

This is a standalone, read-only input-preparation tool. It does not fetch third-
party services, change the SEC/DCF caches, or publish scores. Each input file is
an independently obtained JSON bundle with:

  {"schema":"mypybite.market-observations.v1", "provider":"Source name",
   "termsUrl":"https://...", "usageReviewed":true, "records":[...]}

Each record requires exact `cik`, `ticker`, `metric`, `asOf`, `sourceUrl`.
Supported metric-specific records:
  shortInterest: shortShares, freeFloatShares, basis="reported-short-interest"
  institutionalOwnership: institutionalShares, sharesOutstanding,
    basis="verified-unduplicated-common-shares"
  fiveYearVsSP500: startDate, endDate, stockAdjustedStart,
    stockAdjustedEnd, benchmarkAdjustedStart, benchmarkAdjustedEnd,
    benchmarkId="SP500_TOTAL_RETURN", adjustment="split-and-dividend-adjusted",
    stockSourceUrl, benchmarkSourceUrl

Only values with correct units, source URLs, and dates qualify. A human must
confirm third-party reuse/display terms before marking `usageReviewed` true.
This flag is not an automated license check or a claim of source accuracy.

Output: JSON with scorer-compatible fields keyed by exact CIK/ticker; a future
separate integration step can join it to the existing SEC calculator rows.

Examples:
  python scripts/build_mypybite_market_inputs.py --self-test
  python scripts/build_mypybite_market_inputs.py \\
      --source data/market/providers/provider1.json \\
      --source data/market/providers/provider2.json \\
      --output data/market/mypybite-market-inputs.json
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
from math import isfinite
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlparse
import unittest
from typing import Any

INPUT_SCHEMA = "mypybite.market-observations.v1"
OUTPUT_SCHEMA = "mypybite.market-inputs.v1"
METRICS = ("shortInterest", "institutionalOwnership", "fiveYearVsSP500")
CIK_RE = re.compile(r"^[0-9]{1,10}$")
TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")


def https_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc) and not parsed.username


def iso_day(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError("ISO reporting date required")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Noncanonical ISO date")
    return parsed


def number(value: Any, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError("Numeric share count or adjusted price required")
    v = float(value)
    if not isfinite(v) or (v <= 0 if positive else v < 0):
        raise ValueError("Nonfinite, negative or zero-required-positive value")
    return v


def identity(record: dict[str, Any]) -> tuple[str, str]:
    cik = str(record.get("cik", "")).strip()
    ticker = record.get("ticker")
    if not CIK_RE.fullmatch(cik) or not isinstance(ticker, str) or not TICKER_RE.fullmatch(ticker):
        raise ValueError("Exact CIK and uppercase ticker required; never use prefix matches")
    return cik.zfill(10), ticker


def validate_observation(record: dict[str, Any], today: date) -> dict[str, Any]:
    cik, ticker = identity(record)
    metric = record.get("metric")
    if metric not in METRICS:
        raise ValueError("Unsupported metric")
    day = iso_day(record.get("asOf"))
    if day > today:
        raise ValueError("Observation date is in the future")
    source = record.get("sourceUrl")
    if not https_url(source):
        raise ValueError("Evidence must have an HTTPS source URL")
    result = {"cik": cik, "ticker": ticker, "metric": metric,
              "asOf": day.isoformat(), "sourceUrl": source}

    if metric == "shortInterest":
        if record.get("basis") != "reported-short-interest":
            raise ValueError("Short-sale volume cannot substitute for short interest")
        if (today - day).days > 45:
            raise ValueError("Short-interest settlement observation too stale")
        shares = number(record.get("shortShares"))
        floating = number(record.get("freeFloatShares"), positive=True)
        pct = 100.0 * shares / floating
        if pct > 100:
            raise ValueError("Short interest above 100% needs manual review and scorer support")
        result.update(shortShares=shares, freeFloatShares=floating,
                      shortInterestPercent=pct)

    elif metric == "institutionalOwnership":
        if record.get("basis") != "verified-unduplicated-common-shares":
            raise ValueError("Unreviewed or duplicate 13F totals cannot be counted")
        if (today - day).days > 180:
            raise ValueError("Institutional-ownership data too stale")
        held = number(record.get("institutionalShares"))
        total = number(record.get("sharesOutstanding"), positive=True)
        pct = 100.0 * held / total
        if not 0 <= pct <= 100:
            raise ValueError("Ownership exceeds outstanding shares; investigate duplicates")
        result.update(institutionalShares=held, sharesOutstanding=total,
                      institutionalOwnershipPercent=pct)

    else:
        if record.get("adjustment") != "split-and-dividend-adjusted":
            raise ValueError("Five-year series must include splits and dividends")
        if record.get("benchmarkId") != "SP500_TOTAL_RETURN":
            raise ValueError("SPY ETF proxy is not the exact S&P 500 Total Return Index")
        start, end = iso_day(record.get("startDate")), iso_day(record.get("endDate"))
        if not 1790 <= (end - start).days <= 1860 or end != day:
            raise ValueError("Need same approximately five-year price interval")
        if (today - day).days > 14:
            raise ValueError("Five-year prices too stale")
        stock_start = number(record.get("stockAdjustedStart"), positive=True)
        stock_end = number(record.get("stockAdjustedEnd"), positive=True)
        bench_start = number(record.get("benchmarkAdjustedStart"), positive=True)
        bench_end = number(record.get("benchmarkAdjustedEnd"), positive=True)
        stock_source = record.get("stockSourceUrl")
        benchmark_source = record.get("benchmarkSourceUrl")
        if not https_url(stock_source) or not https_url(benchmark_source):
            raise ValueError("Both total-return price sources required")
        result.update(
            companyFiveYearTotalReturnPercent=100.0 * (stock_end / stock_start - 1.0),
            sp500FiveYearTotalReturnPercent=100.0 * (bench_end / bench_start - 1.0),
            fiveYearStartDate=start.isoformat(), fiveYearEndDate=end.isoformat(),
            fiveYearReturnBasis="total_return", fiveYearSourceUrl=stock_source,
            sp500FiveYearSourceUrl=benchmark_source,
            stockAdjustedStart=stock_start, stockAdjustedEnd=stock_end,
            benchmarkAdjustedStart=bench_start, benchmarkAdjustedEnd=bench_end,
        )
    return result


def build(bundles: list[dict[str, Any]], *, today: date) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    rejected: Counter[str] = Counter()
    for bundle in bundles:
        if not isinstance(bundle, dict) or bundle.get("schema") != INPUT_SCHEMA:
            raise ValueError("Wrong provider bundle schema")
        if not isinstance(bundle.get("provider"), str) or not bundle["provider"].strip():
            raise ValueError("Provider name required")
        if not https_url(bundle.get("termsUrl")):
            raise ValueError("Provider terms URL required")
        if bundle.get("usageReviewed") is not True:
            rejected["provider_terms_not_reviewed"] += len(bundle.get("records", []))
            continue
        records = bundle.get("records")
        if not isinstance(records, list):
            raise ValueError("Provider records must be a list")
        for item in records:
            try:
                if not isinstance(item, dict):
                    raise ValueError("Record is not a mapping")
                normalized = validate_observation(item, today)
            except (ValueError, TypeError, OverflowError):
                rejected["invalid_or_stale_record"] += 1
                continue
            normalized["provider"] = bundle["provider"]
            normalized["termsUrl"] = bundle["termsUrl"]
            key = (normalized["cik"], normalized["ticker"], normalized["metric"])
            grouped[key].append(normalized)

    by_company: dict[tuple[str, str], dict[str, Any]] = {}
    for (cik, ticker, metric), candidates in sorted(grouped.items()):
        most_recent_date = max(item["asOf"] for item in candidates)
        newest = [item for item in candidates if item["asOf"] == most_recent_date]
        field = ("shortInterestPercent" if metric == "shortInterest" else
                 "institutionalOwnershipPercent" if metric == "institutionalOwnership" else
                 "companyFiveYearTotalReturnPercent")
        # Do not average incompatible or conflicting sources. Different reporting
        # dates are genuine time changes; for an identical date, a conflict is a halt.
        if len(newest) > 1:
            values = [item[field] for item in newest]
            if max(values) - min(values) > 0.25:
                rejected["same_date_source_conflict"] += 1
                continue
            if metric == "fiveYearVsSP500":
                if len({(x["fiveYearStartDate"], x["fiveYearEndDate"]) for x in newest}) != 1:
                    rejected["same_date_source_conflict"] += 1
                    continue
                benchmark = [item["sp500FiveYearTotalReturnPercent"] for item in newest]
                if max(benchmark) - min(benchmark) > 0.25:
                    rejected["same_date_source_conflict"] += 1
                    continue
        chosen = sorted(newest, key=lambda item: (item["provider"], item["sourceUrl"]))[0]
        company = by_company.setdefault((cik, ticker), {
            "cik": cik, "ticker": ticker, "fields": {}, "evidence": {}})
        keys = ({"shortInterestPercent", "shortInterestAsOf", "shortInterestSourceUrl"}
                if metric == "shortInterest" else
                {"institutionalOwnershipPercent", "institutionalOwnershipAsOf", "institutionalOwnershipSourceUrl"}
                if metric == "institutionalOwnership" else
                {"companyFiveYearTotalReturnPercent", "sp500FiveYearTotalReturnPercent",
                 "fiveYearStartDate", "fiveYearEndDate", "fiveYearReturnBasis",
                 "fiveYearSourceUrl", "sp500FiveYearSourceUrl"})
        for key in keys:
            if key in chosen:
                company["fields"][key] = chosen[key]
        prefix = metric if metric != "fiveYearVsSP500" else "fiveYear"
        if metric in ("shortInterest", "institutionalOwnership"):
            company["fields"][prefix + "AsOf"] = chosen["asOf"]
            company["fields"][prefix + "SourceUrl"] = chosen["sourceUrl"]
        company["evidence"][metric] = chosen

    counts = Counter(metric for company in by_company.values() for metric in company["evidence"])
    return {"schema": OUTPUT_SCHEMA, "asOf": today.isoformat(),
            "coverage": {"companiesWithInputs": len(by_company),
                         "byMetric": {name: counts[name] for name in METRICS},
                         "rejected": dict(rejected)},
            "data": sorted(by_company.values(), key=lambda item: (item["ticker"], item["cik"]))}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".market-", suffix=".tmp", delete=False) as fh:
            temp = fh.name
            json.dump(value, fh, allow_nan=False, separators=(",", ":"), ensure_ascii=False)
            fh.write("\n")
        os.replace(temp, path)
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)


class ContractTests(unittest.TestCase):
    TODAY = date(2026, 11, 12)
    BASE = {"cik": "320193", "ticker": "AAPL", "asOf": "2026-10-30",
            "sourceUrl": "https://example.org/observation"}

    def bundle(self, *items: dict[str, Any], provider: str = "provider") -> dict[str, Any]:
        return {"schema": INPUT_SCHEMA, "provider": provider,
                "termsUrl": "https://example.org/terms", "usageReviewed": True,
                "records": list(items)}

    def test_sources_can_combine_three_independent_metrics(self) -> None:
        short = dict(self.BASE, metric="shortInterest", basis="reported-short-interest",
                     shortShares=3_000, freeFloatShares=100_000)
        owner = dict(self.BASE, metric="institutionalOwnership",
                     basis="verified-unduplicated-common-shares",
                     institutionalShares=70_000, sharesOutstanding=100_000)
        five = dict(self.BASE, metric="fiveYearVsSP500", asOf="2026-11-10",
                    startDate="2021-11-10", endDate="2026-11-10",
                    stockAdjustedStart=100, stockAdjustedEnd=220,
                    benchmarkAdjustedStart=100, benchmarkAdjustedEnd=180,
                    benchmarkId="SP500_TOTAL_RETURN", adjustment="split-and-dividend-adjusted",
                    stockSourceUrl="https://example.org/stock",
                    benchmarkSourceUrl="https://example.org/index")
        out = build([self.bundle(short), self.bundle(owner), self.bundle(five)], today=self.TODAY)
        self.assertEqual(out["coverage"]["byMetric"],
                         {"shortInterest": 1, "institutionalOwnership": 1, "fiveYearVsSP500": 1})
        self.assertAlmostEqual(out["data"][0]["fields"]["shortInterestPercent"], 3)
        self.assertAlmostEqual(out["data"][0]["fields"]["institutionalOwnershipPercent"], 70)
        self.assertAlmostEqual(out["data"][0]["fields"]["companyFiveYearTotalReturnPercent"], 120)

    def test_daily_short_sale_volume_is_rejected(self) -> None:
        bad = dict(self.BASE, metric="shortInterest", basis="daily-short-sale-volume",
                   shortShares=20, freeFloatShares=100)
        out = build([self.bundle(bad)], today=self.TODAY)
        self.assertEqual(out["coverage"]["companiesWithInputs"], 0)

    def test_13f_double_counting_risk_is_rejected(self) -> None:
        bad = dict(self.BASE, metric="institutionalOwnership",
                   basis="unreviewed-13f-sum", institutionalShares=150,
                   sharesOutstanding=100)
        out = build([self.bundle(bad)], today=self.TODAY)
        self.assertEqual(out["coverage"]["byMetric"]["institutionalOwnership"], 0)

    def test_conflict_leaves_metric_pending(self) -> None:
        one = dict(self.BASE, metric="shortInterest", basis="reported-short-interest",
                   shortShares=2, freeFloatShares=100)
        two = dict(one, shortShares=9)
        out = build([self.bundle(one, provider="A"), self.bundle(two, provider="B")], today=self.TODAY)
        self.assertEqual(out["coverage"]["byMetric"]["shortInterest"], 0)
        self.assertEqual(out["coverage"]["rejected"]["same_date_source_conflict"], 1)

    def test_split_only_prices_are_rejected(self) -> None:
        bad = dict(self.BASE, metric="fiveYearVsSP500", asOf="2026-11-10",
                   startDate="2021-11-10", endDate="2026-11-10",
                   stockAdjustedStart=100, stockAdjustedEnd=200,
                   benchmarkAdjustedStart=100, benchmarkAdjustedEnd=150,
                   benchmarkId="SP500_TOTAL_RETURN", adjustment="split-only",
                   stockSourceUrl="https://example.org/stock",
                   benchmarkSourceUrl="https://example.org/index")
        out = build([self.bundle(bad)], today=self.TODAY)
        self.assertEqual(out["coverage"]["byMetric"]["fiveYearVsSP500"], 0)

    def test_exact_ticker_not_prefix(self) -> None:
        short = dict(self.BASE, ticker="APP", metric="shortInterest",
                     basis="reported-short-interest", shortShares=3, freeFloatShares=100)
        out = build([self.bundle(short)], today=self.TODAY)
        self.assertEqual(out["data"][0]["ticker"], "APP")

    def test_provider_requires_usage_review(self) -> None:
        short = dict(self.BASE, metric="shortInterest", basis="reported-short-interest",
                     shortShares=3, freeFloatShares=100)
        b = self.bundle(short)
        b["usageReviewed"] = False
        out = build([b], today=self.TODAY)
        self.assertEqual(out["coverage"]["companiesWithInputs"], 0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", action="append", type=Path, default=[],
                    help="Independent reviewed provider JSON bundle; repeat per provider")
    ap.add_argument("--output", type=Path,
                    default=Path("data/market/mypybite-market-inputs.json"))
    ap.add_argument("--as-of", default=date.today().isoformat(),
                    help="Reference date for observation staleness, YYYY-MM-DD")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(ContractTests))
        return 0 if result.wasSuccessful() else 1
    if not args.source:
        ap.error("At least one --source is required; no synthetic data published")
    today = iso_day(args.as_of)
    bundles = [json.loads(path.read_text(encoding="utf-8")) for path in args.source]
    payload = build(bundles, today=today)
    if not payload["data"]:
        raise SystemExit("No verified market observations: refusing to write empty feed")
    atomic_json(args.output, payload)
    print(json.dumps(payload["coverage"], indent=2))
    print(f"Wrote {args.output}; this is not yet connected to the 125-point scorecard")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
