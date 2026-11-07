#!/usr/bin/env python3
"""Accumulate IEX-only reference trades from the existing MYPYBITE EOD Git history.

Proof-of-concept only. No network calls, no IEX downloads, no modification to
existing quote caches or Zoho. The last reported IEX trade is NOT the official
market close, and these prices are NOT adjusted for splits or dividends. Do not
feed this output to a total-return Sharpe calculator or award research points.

Example:
    python build_iex_reference_history.py --symbol TSLA
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
OUTPUT_SCHEMA = "mypybite.iex-reference-history-poc.v1"


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8"
    )
    if result.returncode:
        raise ValueError(f"Git command failed: {result.stderr.strip()}")
    return result.stdout


def read_source(raw: str, *, symbol: str, revision: str) -> dict | None:
    payload = json.loads(raw)
    if payload.get("schema") != INPUT_SCHEMA or not isinstance(payload.get("prices"), list):
        raise ValueError("Price cache format changed; stopping without writing")
    for row in payload["prices"]:
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            continue
        if row.get("venue") != "IEX" or row.get("status") != "iex-last-trade-t1":
            continue
        try:
            price = float(row["price"])
            timestamp = str(row["asOf"])
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if dt.tzinfo is None or not (math.isfinite(price) and price > 0):
                raise ValueError("Invalid price or timestamp")
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError(f"Invalid IEX record for {symbol}: {exc}") from exc
        return {
            "dateUtc": dt.astimezone(timezone.utc).date().isoformat(),
            "asOf": dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "price": round(price, 6),
            "venue": "IEX",
            "status": "iex-last-trade-t1",
            "revision": revision,
        }
    return None


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".iex-history-", suffix=".tmp", delete=False
        ) as out:
            name = out.name
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def run(repo: Path, symbol: str, output: Path) -> dict:
    # Keep earlier recorded dates even if old Git commits are subsequently pruned.
    by_date: dict[str, dict] = {}
    if output.exists():
        previous = json.loads(output.read_text(encoding="utf-8"))
        if previous.get("schema") != OUTPUT_SCHEMA or previous.get("symbol") != symbol:
            raise ValueError("Existing output is not this symbol's POC history; refusing to overwrite")
        for item in previous.get("observations", []):
            if isinstance(item, dict) and item.get("dateUtc"):
                by_date[item["dateUtc"]] = item

    history = [s for s in git(repo, "log", "--format=%H", "--", RELATIVE_PRICE_FILE).splitlines() if s]
    # Oldest to newest, so the newest Git revision wins if multiple versions have one date.
    for revision in reversed(history):
        row = read_source(
            git(repo, "show", f"{revision}:{RELATIVE_PRICE_FILE}"),
            symbol=symbol,
            revision=revision[:12],
        )
        if row is not None:
            by_date[row["dateUtc"]] = row
    current = repo / RELATIVE_PRICE_FILE
    if current.exists():
        row = read_source(current.read_text(encoding="utf-8"), symbol=symbol, revision="working-tree")
        if row is not None:
            older = by_date.get(row["dateUtc"])
            if older is None or row["asOf"] >= older["asOf"]:
                by_date[row["dateUtc"]] = row

    rows = [by_date[key] for key in sorted(by_date)]
    if not rows:
        raise ValueError(f"No IEX reference records found for {symbol}; no file written")
    payload = {
        "schema": OUTPUT_SCHEMA,
        "symbol": symbol,
        "source": {
            "name": "IEX historical TOPS-derived reference trades saved in MYPYBITE EOD cache",
            "scope": "IEX-only last observed trade per recorded day, not consolidated market close",
            "attribution": "Data provided for free by IEX. Subject to IEX Historical Data Terms of Use.",
            "terms": "https://www.iex.io/legal/hist-data-terms",
        },
        "basis": "unadjusted-iex-last-trade",
        "observations": rows,
        "coverage": {"distinctUtcDates": len(rows), "annualSharpeReady": False},
        "note": "Exploratory price history only: no split/dividend adjustment, no verified regular-session closing price. Do not score Sharpe with it.",
    }
    atomic_write(output, json.dumps(payload, indent=2) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--symbol", default="TSLA")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    symbol = args.symbol.strip().upper()
    if not symbol or len(symbol) > 32 or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for ch in symbol):
        parser.error("Invalid ticker")
    output = args.output or args.repo / "outputs" / f"iex-reference-history-{symbol}.json"
    try:
        result = run(args.repo, symbol, output)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"History not written: {exc}\n")
    print(f"{symbol}: {result['coverage']['distinctUtcDates']} distinct UTC dates saved to {output}")
    print("Indicative IEX trade history only; Sharpe scoring remains pending.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
