#!/usr/bin/env python3
"""Validate and publish a provider-neutral delayed closing-price cache.

The input is produced by a separately approved market-data adapter. This
script deliberately does not scrape a website or call an undocumented API.
It validates source/permission metadata, normalizes quote rows, merges them
with the last valid cache, and writes atomically so a failed refresh cannot
erase previously working prices.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from fetch_sec_intake import atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.eod-price-cache.v1"
INPUT_SCHEMA = "mypybite.approved-price-input.v1"
DEFAULT_OUTPUT = ROOT / "data" / "market" / "eod-prices.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 32 or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for character in symbol
    ):
        raise ValueError(f"Invalid ticker symbol: {value!r}")
    return symbol


def validate_source(source: object) -> dict:
    if not isinstance(source, dict):
        raise ValueError("Input must include source metadata")
    required = ("name", "permission", "attribution")
    cleaned = {key: str(source.get(key) or "").strip() for key in required}
    if any(not cleaned[key] for key in required):
        raise ValueError("Source name, permission and attribution are required")
    if cleaned["permission"].lower() in {
        "unknown",
        "pending",
        "personal use",
        "personal-use-only",
        "not granted",
    }:
        raise ValueError("Source permission does not authorize public display")
    cleaned["url"] = str(source.get("url") or "").strip() or None
    for key in ("scope", "captureDate"):
        value = str(source.get(key) or "").strip()
        if value:
            cleaned[key] = value
    return cleaned


def normalize_input(payload: dict) -> tuple[dict, dict[str, dict]]:
    if payload.get("schema") != INPUT_SCHEMA:
        raise ValueError("Price input schema is not supported")
    source = validate_source(payload.get("source"))
    rows = payload.get("quotes")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Price input must contain at least one quote")

    quotes: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Price input contains a malformed quote")
        symbol = clean_symbol(row.get("symbol"))
        try:
            price = float(row.get("price"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid price for {symbol}") from exc
        if not (price > 0 and price < 100_000_000):
            raise ValueError(f"Price for {symbol} must be a positive finite amount")
        as_of = str(row.get("asOf") or row.get("tradeDate") or "").strip()
        if len(as_of) < 10:
            raise ValueError(f"Quote date is required for {symbol}")
        try:
            datetime.fromisoformat(as_of[:10])
        except ValueError as exc:
            raise ValueError(f"Invalid quote date for {symbol}: {as_of}") from exc
        currency = str(row.get("currency") or "").strip().upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError(f"Three-letter currency is required for {symbol}")
        quotes[symbol] = {
            "symbol": symbol,
            "price": round(price, 6),
            "currency": currency,
            "asOf": as_of,
            "status": str(row.get("status") or "previous-close").strip(),
        }
        for key in ("venue", "tradeSize"):
            if row.get(key) is not None:
                quotes[symbol][key] = row[key]
    return source, quotes


def load_existing(path: Path) -> tuple[dict | None, dict[str, dict]]:
    if not path.exists():
        return None, {}
    payload = read_json(path)
    if payload.get("schema") != SCHEMA or not isinstance(payload.get("prices"), list):
        raise ValueError("Existing price cache has an unsupported schema")
    prices = {
        clean_symbol(row.get("symbol")): row
        for row in payload["prices"]
        if isinstance(row, dict) and row.get("symbol")
    }
    return payload.get("source"), prices


def build_payload(
    source: dict,
    fresh: dict[str, dict],
    existing: dict[str, dict] | None = None,
    *,
    generated_at: str | None = None,
) -> dict:
    merged = dict(existing or {})
    merged.update(fresh)
    refreshed = set(fresh)
    prices = []
    for symbol in sorted(merged):
        row = dict(merged[symbol])
        row["symbol"] = symbol
        row["refreshState"] = "fresh" if symbol in refreshed else "preserved"
        prices.append(row)
    return {
        "schema": SCHEMA,
        "generatedAt": generated_at or utc_now_iso(),
        "source": source,
        "prices": prices,
        "coverage": {
            "priceCount": len(prices),
            "refreshedThisRun": len(fresh),
            "preservedFromPriorRun": len(prices) - len(fresh),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    try:
        source, fresh = normalize_input(read_json(args.input))
        _old_source, existing = load_existing(args.output)
        payload = build_payload(source, fresh, existing)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"EOD price cache was not changed: {exc}") from exc

    atomic_write_text(args.output, json.dumps(payload, indent=2) + "\n")
    print(
        f"Published {len(fresh)} refreshed prices; "
        f"{payload['coverage']['preservedFromPriorRun']} prior prices preserved."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
