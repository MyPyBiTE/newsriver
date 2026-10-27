#!/usr/bin/env python3
"""Incrementally build a compact SEC-fundamentals cache for ticker lookup.

This file is intentionally separate from the editorial fundamental snapshot.
It keeps only the filing facts needed by the calculator, never supplies market
prices, and preserves previously checked entries when an upstream request fails.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import build_fundamental_snapshot as fundamentals
from fetch_sec_intake import REQUEST_INTERVAL_SECONDS, IntakeFailure, SecClient, atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.fundamental-lookup-cache.v1"
DEFAULT_DIRECTORY = ROOT / "outputs" / "zero-cost-ticker-directory-development.json"
DEFAULT_SEED = ROOT / "data" / "sec" / "fundamentals" / "fundamentals.json"
DEFAULT_OUTPUT = ROOT / "data" / "sec" / "fundamentals" / "ticker-lookup.json"
KEEP_FACTS = (
    "sharesOutstanding",
    "operatingCashFlow",
    "capitalExpenditures",
    "cash",
    "debt",
    "currentDebt",
    "noncurrentDebt",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def directory_companies(payload: dict) -> dict[str, dict]:
    fields = payload.get("fields")
    rows = payload.get("data")
    if payload.get("schema") != "mypybite.ticker-directory.v1":
        raise ValueError("Ticker directory schema is not supported")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise ValueError("Ticker directory must contain fields and data arrays")
    positions = {str(field): index for index, field in enumerate(fields)}
    required = {"symbol", "name", "cik", "exchange"}
    missing = required.difference(positions)
    if missing:
        raise ValueError(f"Ticker directory is missing fields: {sorted(missing)}")

    companies: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < len(fields):
            raise ValueError("Ticker directory contains a malformed row")
        cik = str(row[positions["cik"]] or "").strip()
        symbol = str(row[positions["symbol"]] or "").strip().upper()
        name = str(row[positions["name"]] or "").strip()
        exchange = str(row[positions["exchange"]] or "").strip()
        if not (cik and symbol and name and exchange):
            continue
        company = companies.setdefault(
            cik,
            {"cik": cik, "legalName": name, "aliases": [], "exchanges": []},
        )
        if symbol not in company["aliases"]:
            company["aliases"].append(symbol)
        if exchange not in company["exchanges"]:
            company["exchanges"].append(exchange)

    for company in companies.values():
        company["aliases"].sort(key=lambda value: ("-" in value, len(value), value))
        company["exchanges"].sort()
        company["ticker"] = company["aliases"][0]
    return companies


def compact_company(company: dict, *, refreshed_at: str | None = None) -> dict:
    facts = company.get("facts") if isinstance(company.get("facts"), dict) else {}
    valuation = (
        company.get("valuationInputs")
        if isinstance(company.get("valuationInputs"), dict)
        else {}
    )
    return {
        "ticker": company.get("ticker"),
        "aliases": list(company.get("aliases") or ([company.get("ticker")] if company.get("ticker") else [])),
        "cik": str(company.get("cik") or ""),
        "legalName": company.get("legalName"),
        "exchanges": list(company.get("exchanges") or []),
        "status": company.get("status") or "insufficient",
        "latestPeriodEnd": company.get("latestPeriodEnd"),
        "refreshedAt": refreshed_at or company.get("refreshedAt") or utc_now_iso(),
        "valuationInputs": {
            "freeCashFlow": valuation.get("freeCashFlow"),
            "totalDebt": valuation.get("totalDebt"),
            "netDebt": valuation.get("netDebt"),
            "unit": valuation.get("unit"),
            "shareCountBasis": valuation.get("shareCountBasis"),
        },
        "facts": {key: facts.get(key) for key in KEEP_FACTS if facts.get(key) is not None},
        "marketPrice": None,
    }


def load_existing(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    payload = read_json(path)
    if payload.get("schema") != SCHEMA or not isinstance(payload.get("companies"), list):
        raise ValueError("Existing lookup cache has an unsupported schema")
    return {
        str(company.get("cik")): company
        for company in payload["companies"]
        if isinstance(company, dict) and company.get("cik")
    }


def add_seed(cache: dict[str, dict], seed_path: Path | None) -> None:
    if not seed_path or not seed_path.exists():
        return
    payload = read_json(seed_path)
    for company in payload.get("companies", []):
        cik = str(company.get("cik") or "")
        if cik and cik not in cache:
            cache[cik] = compact_company(
                company, refreshed_at=payload.get("generatedAt") or utc_now_iso()
            )


def choose_batch(
    directory: dict[str, dict],
    cache: dict[str, dict],
    batch_size: int,
    priority_tickers: list[str] | None = None,
) -> list[dict]:
    if batch_size < 1:
        return []
    by_ticker = {
        alias: company
        for company in directory.values()
        for alias in company.get("aliases", [])
    }
    chosen: list[dict] = []
    seen: set[str] = set()
    for ticker in priority_tickers or []:
        company = by_ticker.get(str(ticker).strip().upper())
        if company and company["cik"] not in seen:
            chosen.append(company)
            seen.add(company["cik"])

    uncached = [
        company
        for company in directory.values()
        if company["cik"] not in cache and company["cik"] not in seen
    ]
    uncached.sort(key=lambda company: (company["ticker"], company["cik"]))
    for company in uncached:
        if len(chosen) >= batch_size:
            return chosen
        chosen.append(company)
        seen.add(company["cik"])

    stale = [
        company
        for company in directory.values()
        if company["cik"] in cache and company["cik"] not in seen
    ]
    stale.sort(
        key=lambda company: (
            str(cache[company["cik"]].get("refreshedAt") or ""),
            company["ticker"],
        )
    )
    for company in stale:
        if len(chosen) >= batch_size:
            break
        chosen.append(company)
    return chosen


def build_payload(
    directory: dict[str, dict],
    cache: dict[str, dict],
    *,
    attempted: int,
    refreshed: int,
    failures: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict:
    eligible = set(directory)
    companies = [cache[cik] for cik in sorted(cache) if cik in eligible]
    return {
        "schema": SCHEMA,
        "generatedAt": generated_at or utc_now_iso(),
        "source": "SEC EDGAR Companyfacts API collected by GitHub Actions",
        "marketPriceProvided": False,
        "companies": companies,
        "coverage": {
            "eligibleCiks": len(eligible),
            "cachedCiks": len(companies),
            "remainingCiks": max(0, len(eligible) - len(companies)),
            "attemptedThisRun": attempted,
            "refreshedThisRun": refreshed,
            "failures": failures,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--seed-snapshot", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    args = parser.parse_args()
    if not args.user_agent:
        raise SystemExit("SEC_USER_AGENT is required")

    try:
        directory = directory_companies(read_json(args.directory))
        cache = load_existing(args.output)
        add_seed(cache, args.seed_snapshot)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Fundamental lookup cache setup failed: {exc}") from exc

    selected = choose_batch(directory, cache, args.batch_size, args.ticker)
    client = SecClient(args.user_agent, REQUEST_INTERVAL_SECONDS)
    failures: list[dict[str, str]] = []
    refreshed = 0
    refreshed_at = utc_now_iso()
    for index, company in enumerate(selected, start=1):
        cik = company["cik"]
        url = f"{fundamentals.SEC_COMPANYFACTS}/CIK{cik}.json"
        try:
            text = client.get_text(url, missing_statuses=frozenset({404}))
            if text is None:
                raise IntakeFailure("companyfacts-not-found")
            analyzed = fundamentals.analyze_company(
                {
                    "ticker": company["ticker"],
                    "cik": cik,
                    "legalName": company["legalName"],
                    "cohort": None,
                },
                json.loads(text),
            )
            analyzed["aliases"] = company["aliases"]
            analyzed["exchanges"] = company["exchanges"]
            cache[cik] = compact_company(analyzed, refreshed_at=refreshed_at)
            refreshed += 1
        except (IntakeFailure, json.JSONDecodeError, ValueError) as exc:
            failures.append({"cik": cik, "ticker": company["ticker"], "reason": str(exc)})
        print(f"[{index}/{len(selected)}] {company['ticker']}", flush=True)

    payload = build_payload(
        directory,
        cache,
        attempted=len(selected),
        refreshed=refreshed,
        failures=failures,
        generated_at=refreshed_at,
    )
    if not payload["companies"]:
        raise SystemExit("Refusing to write an empty fundamental lookup cache")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        args.output,
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n",
    )
    print(
        f"Wrote {args.output} with {payload['coverage']['cachedCiks']} cached CIKs; "
        f"{refreshed} refreshed and {len(failures)} failed this run"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
