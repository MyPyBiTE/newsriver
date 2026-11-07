#!/usr/bin/env python3
"""Accumulate unadjusted IEX reference trades for ALL available U.S. tickers.

Consumes the existing MYPYBITE daily price cache and (on initial bootstrap) its
Git history. Never fetches a market-data service and never changes the EOD cache.
One compact JSON file stores every ticker, with date indexes to avoid repeating
ISO dates thousands of times. Missing ticker-days remain missing, never zeroed.

IMPORTANT: IEX-only last trades are not consolidated market closes, and these
prices are not adjusted for dividends or splits. This output is exploratory;
do not award Sharpe points or label its returns as total returns.

Examples:
  python scripts/build_iex_reference_history.py --all
  python scripts/build_iex_reference_history.py --all --backfill
  python scripts/build_iex_reference_history.py --symbol TSLA
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELATIVE_PRICE_FILE = "data/market/eod-prices.json"
INPUT_SCHEMA = "mypybite.eod-price-cache.v1"
ALL_SCHEMA = "mypybite.iex-reference-history-all.v1"
LEGACY_SCHEMA = "mypybite.iex-reference-history-poc.v1"

SOURCE = {
    "name": "IEX historical TOPS-derived reference trades in MYPYBITE EOD cache",
    "scope": "IEX-only last observed trade on each UTC trading date, not consolidated closes",
    "attribution": "Data provided for free by IEX. Subject to IEX Historical Data Terms of Use.",
    "terms": "https://www.iex.io/legal/hist-data-terms",
}
NOTE = (
    "Exploratory price history: no dividend/split adjustment, session close verification, "
    "or annual performance validation. Do not award Sharpe points from this file."
)


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode:
        raise ValueError(f"Git command failed: {result.stderr.strip()}")
    return result.stdout


def validate_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 32 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for c in symbol):
        raise ValueError(f"Invalid symbol: {value!r}")
    return symbol


def parse_cache(raw: str) -> dict[str, dict[str, tuple[str, float]]]:
    """Read one EOD snapshot in one pass. Key by ticker and UTC date.

    Per-source-dated observations avoid fabricating a new day when an EOD quote
    was merely preserved from a previous run.
    """
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("schema") != INPUT_SCHEMA or not isinstance(payload.get("prices"), list):
        raise ValueError("Unexpected EOD price-cache schema; no history written")
    result: dict[str, dict[str, tuple[str, float]]] = {}
    for row in payload["prices"]:
        if not isinstance(row, dict) or row.get("venue") != "IEX" or row.get("status") != "iex-last-trade-t1":
            continue
        symbol = validate_symbol(row.get("symbol"))
        if row.get("currency") != "USD":
            continue
        try:
            price = float(row["price"])
            timestamp = str(row["asOf"])
            as_of = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if as_of.tzinfo is None or not (math.isfinite(price) and price > 0):
                raise ValueError("Non-positive, non-finite or timezone-naive quote")
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise ValueError(f"Invalid IEX quote for {symbol}: {exc}") from exc
        dt = as_of.astimezone(timezone.utc)
        date = dt.date().isoformat()
        stamp = dt.isoformat().replace("+00:00", "Z")
        per_symbol = result.setdefault(symbol, {})
        earlier = per_symbol.get(date)
        if earlier is None or stamp >= earlier[0]:
            per_symbol[date] = (stamp, round(price, 6))
    return result


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=".iex-all-history-", suffix=".tmp", delete=False,
        ) as f:
            temp = f.name
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)


def read_previous_all(path: Path) -> dict[str, dict[str, tuple[str, float]]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema") != ALL_SCHEMA:
        raise ValueError("Existing all-ticker history has a different schema; refusing to overwrite")
    dates = payload.get("dates")
    series = payload.get("series")
    if not isinstance(dates, list) or not isinstance(series, dict):
        raise ValueError("Malformed existing history: no dates/series")
    if len(dates) != len(set(dates)) or dates != sorted(dates):
        raise ValueError("Existing history has unsorted or duplicate dates")
    for day in dates:
        if not isinstance(day, str) or datetime.fromisoformat(day).date().isoformat() != day:
            raise ValueError("Invalid history date")
    result: dict[str, dict[str, tuple[str, float]]] = {}
    for raw_symbol, pairs in series.items():
        symbol = validate_symbol(raw_symbol)
        if not isinstance(pairs, list):
            raise ValueError(f"Malformed series for {symbol}")
        rows: dict[str, tuple[str, float]] = {}
        for pair in pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError(f"Malformed observation for {symbol}")
            index, price = pair
            if type(index) is not int or not 0 <= index < len(dates):
                raise ValueError(f"Invalid day index for {symbol}")
            if isinstance(price, bool) or not isinstance(price, (int, float)) or not (math.isfinite(price) and price > 0):
                raise ValueError(f"Invalid saved price for {symbol}")
            day = dates[index]
            if day in rows:
                raise ValueError(f"Duplicate observation for {symbol} on {day}")
            # Exact timestamps were intentionally excluded from compact output;
            # the date acts as a stable key for incremental accumulation.
            rows[day] = (day, float(price))
        result[symbol] = rows
    return result


def merge_snapshot(target: dict, incoming: dict, *, allow_replace: bool) -> None:
    for symbol, dated in incoming.items():
        destination = target.setdefault(symbol, {})
        for day, record in dated.items():
            if day not in destination or allow_replace:
                destination[day] = record


def gather_git_bootstrap(repo: Path) -> dict[str, dict[str, tuple[str, float]]]:
    revisions = [h for h in git(repo, "log", "--format=%H", "--", RELATIVE_PRICE_FILE).splitlines() if h]
    gathered: dict[str, dict[str, tuple[str, float]]] = {}
    # Revisions are newest first. Iterate oldest first, letting newer commits
    # supersede older records on the same date.
    for h in reversed(revisions):
        snapshot = parse_cache(git(repo, "show", f"{h}:{RELATIVE_PRICE_FILE}"))
        merge_snapshot(gathered, snapshot, allow_replace=True)
    return gathered


def build_all(repo: Path, output: Path, *, backfill: bool = False) -> dict:
    known = read_previous_all(output)
    if not output.exists() or backfill:
        # Bootstrap only once by default; later runs use the saved cumulative
        # file plus the latest EOD cache, rather than rescanning all Git history.
        old = gather_git_bootstrap(repo)
        merge_snapshot(known, old, allow_replace=False)
    latest = repo / RELATIVE_PRICE_FILE
    if not latest.is_file():
        raise ValueError(f"Missing EOD cache at {latest}")
    merge_snapshot(known, parse_cache(latest.read_text(encoding="utf-8")), allow_replace=False)
    known = {s: records for s, records in known.items() if records}
    if not known:
        raise ValueError("No eligible IEX records found; output left unchanged")

    dates = sorted({date for records in known.values() for date in records})
    index = {date: i for i, date in enumerate(dates)}
    series = {
        symbol: [[index[date], known[symbol][date][1]] for date in sorted(known[symbol])]
        for symbol in sorted(known)
    }
    count = sum(len(pairs) for pairs in series.values())
    payload = {
        "schema": ALL_SCHEMA,
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": SOURCE,
        "basis": "unadjusted-iex-last-trade",
        "dates": dates,
        "series": series,
        "coverage": {
            "symbolCount": len(series),
            "distinctUtcDates": len(dates),
            "observationCount": count,
            "annualSharpeReady": False,
        },
        "note": NOTE,
    }
    # Avoid writing identical series each scheduled run, including duplicate-day
    # retries, so the bot does not create noise commits.
    if output.exists():
        previous = json.loads(output.read_text(encoding="utf-8"))
        if previous.get("dates") == dates and previous.get("series") == series:
            return previous
    atomic_write(output, json.dumps(payload, separators=(",", ":"), ensure_ascii=True) + "\n")
    return payload


def build_single(repo: Path, symbol: str, output: Path) -> dict:
    """Preserve the previous single-ticker command for existing local tests."""
    by_day: dict[str, tuple[str, float]] = {}
    if output.exists():
        prior = json.loads(output.read_text(encoding="utf-8"))
        if prior.get("schema") != LEGACY_SCHEMA or prior.get("symbol") != symbol:
            raise ValueError("Existing single-ticker history format differs; not overwriting")
        for record in prior.get("observations", []):
            if isinstance(record, dict) and record.get("dateUtc") and record.get("price"):
                by_day[record["dateUtc"]] = (record["asOf"], float(record["price"]))
    from_git = gather_git_bootstrap(repo)
    for date, quote in from_git.get(symbol, {}).items():
        by_day.setdefault(date, quote)
    latest = repo / RELATIVE_PRICE_FILE
    if latest.exists():
        snapshot = parse_cache(latest.read_text(encoding="utf-8"))
        for date, quote in snapshot.get(symbol, {}).items():
            by_day.setdefault(date, quote)
    if not by_day:
        raise ValueError(f"No eligible IEX records for {symbol}")
    observations = [
        {"dateUtc": day, "asOf": stamp, "price": price, "venue": "IEX", "status": "iex-last-trade-t1"}
        for day, (stamp, price) in sorted(by_day.items())
    ]
    payload = {
        "schema": LEGACY_SCHEMA, "symbol": symbol, "source": SOURCE,
        "basis": "unadjusted-iex-last-trade", "observations": observations,
        "coverage": {"distinctUtcDates": len(observations), "annualSharpeReady": False},
        "note": NOTE,
    }
    atomic_write(output, json.dumps(payload, indent=2) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--all", action="store_true", help="Collect every eligible ticker into one compact JSON file")
    modes.add_argument("--symbol", help="Legacy single-ticker mode (e.g. TSLA)")
    parser.add_argument("--backfill", action="store_true", help="For --all, review Git history again")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.backfill and not args.all:
        parser.error("--backfill requires --all")
    if args.all:
        output = args.output or args.repo / "outputs" / "iex-reference-history-all.json"
    else:
        symbol = validate_symbol(args.symbol or "TSLA")
        output = args.output or args.repo / "outputs" / f"iex-reference-history-{symbol}.json"
    try:
        if args.all:
            payload = build_all(args.repo, output, backfill=args.backfill)
            coverage = payload["coverage"]
            print(
                f"IEX history: {coverage['symbolCount']:,} tickers, "
                f"{coverage['distinctUtcDates']} UTC dates, "
                f"{coverage['observationCount']:,} observations saved to {output}"
            )
        else:
            payload = build_single(args.repo, symbol, output)
            print(f"{symbol}: {payload['coverage']['distinctUtcDates']} UTC dates saved to {output}")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"History not written: {exc}\n")
    print("Unadjusted IEX-only history: Sharpe scoring remains pending.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
