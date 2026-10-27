#!/usr/bin/env python3
"""Build a compact, zero-cost U.S. ticker directory from the official SEC file.

The directory resolves company/security names, tickers, CIKs and exchanges. It
does not provide a market price and does not imply that every SEC registrant has
complete standardized financial facts.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fetch_sec_intake import REQUEST_INTERVAL_SECONDS, IntakeFailure, SecClient, atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.ticker-directory.v1"
SEC_DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
DEFAULT_EXCHANGES = ("Nasdaq", "NYSE", "CBOE", "TXSE")
FIELD_NAMES = (
    "symbol",
    "name",
    "cik",
    "exchange",
    "country",
    "fundamentalCoverage",
    "coverageLabel",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_cik(value: object) -> str:
    try:
        return f"{int(value):010d}"
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid SEC CIK: {value!r}") from exc


def parse_sec_directory(payload: dict) -> list[dict[str, str]]:
    fields = payload.get("fields")
    rows = payload.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise ValueError("SEC ticker directory must contain fields and data arrays")
    required = {"cik", "name", "ticker", "exchange"}
    missing = required.difference(str(item) for item in fields)
    if missing:
        raise ValueError(f"SEC ticker directory is missing fields: {sorted(missing)}")
    index = {str(name): position for position, name in enumerate(fields)}
    parsed: list[dict[str, str]] = []
    for position, row in enumerate(rows, start=1):
        if not isinstance(row, list) or len(row) < len(fields):
            raise ValueError(f"SEC ticker directory row {position} is malformed")
        symbol = normalize_symbol(row[index["ticker"]])
        name = str(row[index["name"]] or "").strip()
        exchange = str(row[index["exchange"]] or "").strip()
        if not symbol or not name or not exchange:
            continue
        parsed.append(
            {
                "symbol": symbol,
                "name": name,
                "cik": normalize_cik(row[index["cik"]]),
                "exchange": exchange,
            }
        )
    return parsed


def build_directory(
    sec_payload: dict,
    *,
    generated_at: str | None = None,
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES,
    include_otc: bool = False,
) -> dict:
    allowed = set(exchanges)
    if include_otc:
        allowed.add("OTC")
    all_rows = parse_sec_directory(sec_payload)
    selected = [row for row in all_rows if row["exchange"] in allowed]
    selected.sort(key=lambda row: (row["symbol"], row["exchange"], row["name"], row["cik"]))

    compact_rows = [
        [
            row["symbol"],
            row["name"],
            row["cik"],
            row["exchange"],
            "US",
            "sec-companyfacts",
            "SEC filings can be checked automatically",
        ]
        for row in selected
    ]
    exchange_counts = {
        exchange: sum(row["exchange"] == exchange for row in selected)
        for exchange in sorted(allowed)
    }
    return {
        "schema": SCHEMA,
        "generatedAt": generated_at or utc_now_iso(),
        "source": {
            "name": "U.S. Securities and Exchange Commission",
            "url": SEC_DIRECTORY_URL,
            "description": "Official company ticker, legal name, CIK and exchange associations.",
        },
        "fields": list(FIELD_NAMES),
        "data": compact_rows,
        "coverage": {
            "country": "US",
            "includedExchanges": sorted(allowed),
            "exchangeCounts": exchange_counts,
            "securityCount": len(compact_rows),
            "indexNote": "S&P 500 and Dow Jones companies are included through their listed exchange; index membership is not asserted by this file.",
            "priceNote": "No market price is supplied. Price remains a reader-entered value under the zero-cost design.",
            "factsNote": "An SEC match allows filing facts to be checked, but a specific standardized fact may still be missing.",
        },
        "manualEntry": {
            "accepted": True,
            "label": "Ticker not found — continue with manual inputs",
            "preserveReaderValues": True,
        },
        "canada": {
            "status": "not-published",
            "label": "TSX directory source permission still required",
            "reason": "The official TMX downloadable issuer workbook carries an explicit no-copy/no-distribution/no-modification notice. MYPYBITE will not republish it without permission or another permitted source.",
        },
    }


def load_payload(path: Path | None, client: SecClient) -> dict:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    text = client.get_text(SEC_DIRECTORY_URL)
    if text is None:
        raise IntakeFailure("SEC ticker directory was not returned")
    return json.loads(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Optional downloaded SEC directory JSON")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs" / "zero-cost-ticker-directory-development.json",
    )
    parser.add_argument("--include-otc", action="store_true")
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    args = parser.parse_args()

    if not args.input and not args.user_agent:
        raise SystemExit("SEC_USER_AGENT is required when downloading the SEC directory")
    client = SecClient(args.user_agent or "local-file", REQUEST_INTERVAL_SECONDS)
    try:
        payload = load_payload(args.input, client)
        directory = build_directory(payload, include_otc=args.include_otc)
    except (IntakeFailure, json.JSONDecodeError, OSError, ValueError) as exc:
        raise SystemExit(f"Ticker directory build failed: {exc}") from exc

    minimum = 1000 if args.input is None else 1
    if directory["coverage"]["securityCount"] < minimum:
        raise SystemExit(
            f"Refusing to write an unexpectedly small ticker directory "
            f"({directory['coverage']['securityCount']} rows)"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(args.output, json.dumps(directory, separators=(",", ":"), ensure_ascii=False) + "\n")
    print(
        f"Wrote {args.output} with {directory['coverage']['securityCount']} "
        "U.S. exchange-listed securities"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
