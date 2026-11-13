#!/usr/bin/env python3
"""Collect official FINRA short-position SHARES for MYPYBITE research.

This is an isolated, opt-in collector. It NEVER creates a score or modifies the
working financial cache, GitHub Actions, DCF, Sharpe, or Zoho. It needs a FINRA
OAuth credential or a FINRA bearer token supplied through environment variables.
The public credential is for permitted research, NOT assumed suitable for a
commercial, redistributed MYPYBITE feed.

FINRA provides short position shares. Percent of free float requires a SECOND,
verified source for *free-float shares of the same security and date*; until
then the 10-point short-interest category must stay PENDING.

Use:
  python scripts/fetch_finra_short_interest.py --self-test
  python scripts/fetch_finra_short_interest.py --symbol MSFT
  python scripts/fetch_finra_short_interest.py --offline-input sample-finra.json

Optional environment variables:
  FINRA_BEARER_TOKEN, or FINRA_CLIENT_ID and FINRA_CLIENT_SECRET.

Output defaults to data/market/RESEARCH-ONLY-finra-short-shares.json; do not
commit it to public GitHub or use it to assert a licensed publication right.
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "outputs" / "zero-cost-ticker-directory-development.json"
OUTPUT = ROOT / "data" / "market" / "RESEARCH-ONLY-finra-short-shares.json"
DATA_ENDPOINT = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
PARTITIONS_ENDPOINT = "https://api.finra.org/partitions/group/otcMarket/name/consolidatedShortInterest"
TOKEN_ENDPOINT = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
TERMS_URL = "https://developer.finra.org/docs#getting_started-introduction"
SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")


def request_json(url: str, *, bearer: str | None = None,
                 data: dict[str, Any] | None = None,
                 basic_auth: str | None = None) -> tuple[Any, dict[str, str]]:
    headers = {"Accept": "application/json", "User-Agent": "MYPYBITE-research/0.1"}
    body = None
    if bearer:
        headers["Authorization"] = "Bearer " + bearer
    if basic_auth:
        headers["Authorization"] = "Basic " + basic_auth
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=body, headers=headers,
                  method="POST" if (body is not None or basic_auth) else "GET")
    try:
        with urlopen(req, timeout=40) as resp:
            payload = json.loads(resp.read())
            response_headers = {k.lower(): v for k, v in resp.headers.items()}
            return payload, response_headers
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        # Avoid echoing authorization values or secret-bearing request objects.
        status = exc.code if isinstance(exc, HTTPError) else type(exc).__name__
        raise RuntimeError(f"FINRA request failed ({status}); check access/date/permissions") from None


def token_from_environment() -> str:
    token = os.environ.get("FINRA_BEARER_TOKEN", "").strip()
    if token:
        return token
    client_id = os.environ.get("FINRA_CLIENT_ID", "").strip()
    client_secret = os.environ.get("FINRA_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("FINRA credentials unavailable. Supply FINRA_CLIENT_ID and "
                           "FINRA_CLIENT_SECRET as environment variables (never in GitHub code), "
                           "or FINRA_BEARER_TOKEN. Use --self-test without credentials.")
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
    payload, _ = request_json(TOKEN_ENDPOINT, basic_auth=encoded)
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise RuntimeError("FINRA did not issue an OAuth access token")
    return token


def parse_partitions(payload: Any, *, today: date) -> str:
    if not isinstance(payload, dict) or payload.get("partitionFields") != ["settlementDate"]:
        raise ValueError("Unexpected FINRA partition structure (expected settlementDate)")
    dates: list[date] = []
    for item in payload.get("availablePartitions", []):
        if not isinstance(item, dict):
            continue
        values = item.get("partitions")
        if isinstance(values, list) and len(values) == 1 and isinstance(values[0], str):
            try:
                observed = date.fromisoformat(values[0])
                if observed.isoformat() == values[0] and observed <= today:
                    dates.append(observed)
            except ValueError:
                continue
    if not dates:
        raise ValueError("FINRA returned no usable published settlement date")
    newest = max(dates)
    if (today - newest).days > 45:
        raise ValueError("Latest FINRA settlement is older than 45 days; not usable for scoring")
    return newest.isoformat()


def sec_directory(path: Path) -> dict[str, str | None]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "mypybite.ticker-directory.v1":
        raise ValueError("Wrong SEC ticker directory schema")
    fields, rows = payload.get("fields"), payload.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise ValueError("Malformed ticker directory")
    cols = {name: fields.index(name) for name in ("symbol", "cik") if name in fields}
    if len(cols) != 2:
        raise ValueError("Ticker directory lacks symbol or CIK")
    all_ids: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if not isinstance(row, list) or len(row) != len(fields):
            continue
        symbol = row[cols["symbol"]]
        cik = row[cols["cik"]]
        if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
            continue
        if not isinstance(cik, (str, int)) or not str(cik).isdigit():
            continue
        all_ids[symbol].add(str(cik).zfill(10))
    # Never force an ambiguous ticker to a single CIK.
    return {symbol: (next(iter(ciks)) if len(ciks) == 1 else None)
            for symbol, ciks in all_ids.items()}


def fetch_rows(*, bearer: str, settlement_date: str, symbol: str | None = None,
               page_limit: int = 2000, max_pages: int = 100,
               fetcher=request_json) -> list[dict[str, Any]]:
    if not (1 <= page_limit <= 5000 and max_pages >= 1):
        raise ValueError("Invalid FINRA page limits")
    date.fromisoformat(settlement_date)
    filters = [{"compareType": "equal", "fieldName": "settlementDate",
                "fieldValue": settlement_date}]
    if symbol:
        if not SYMBOL.fullmatch(symbol):
            raise ValueError("Ticker must be uppercase and exact")
        filters.append({"compareType": "equal", "fieldName": "symbolCode",
                        "fieldValue": symbol})
    all_rows: list[dict[str, Any]] = []
    offset = 0
    total: int | None = None
    for _ in range(max_pages):
        rows, headers = fetcher(DATA_ENDPOINT, bearer=bearer,
                                data={"limit": page_limit, "offset": offset,
                                      "compareFilters": filters})
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("FINRA returned non-tabular data")
        declared = headers.get("record-total")
        if declared is not None:
            try:
                total = int(declared)
            except ValueError:
                raise ValueError("Invalid FINRA record-total header") from None
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        if total is not None and offset >= total:
            break
        if len(rows) < page_limit and total is None:
            break
    else:
        raise RuntimeError("FINRA result exceeded page cap; refusing partial dataset")
    if total is not None and len(all_rows) != total:
        raise RuntimeError(f"FINRA paging incomplete ({len(all_rows)} of {total})")
    return all_rows


def build_research_payload(rows: list[dict[str, Any]], directory: dict[str, str | None],
                           *, settlement_date: str) -> dict[str, Any]:
    rejected: Counter[str] = Counter()
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if not isinstance(row, dict) or row.get("settlementDate") != settlement_date:
            rejected["wrong_date_or_shape"] += 1
            continue
        symbol = row.get("symbolCode")
        shares = row.get("currentShortPositionQuantity")
        if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
            rejected["invalid_symbol"] += 1
            continue
        if type(shares) is not int or shares < 0:
            rejected["invalid_short_shares"] += 1
            continue
        cik = directory.get(symbol)
        if not cik:
            rejected["no_unique_SEC_CIK"] += 1
            continue
        by_symbol[symbol].append({
            "ticker": symbol, "cik": cik, "asOf": settlement_date,
            "shortShares": shares, "basis": "reported-short-interest",
            "sourceUrl": DATA_ENDPOINT,
            "exchangeClass": row.get("marketClassCode"),
            "issueName": row.get("issueName"),
            "stockSplitFlag": row.get("stockSplitFlag"),
            "revisionFlag": row.get("revisionFlag"),
        })
    # Multiple rows for a symbol may represent different security classes;
    # never combine or select one without confirming the actual instrument.
    records = []
    for symbol, matches in sorted(by_symbol.items()):
        if len(matches) != 1:
            rejected["ambiguous_FINRA_symbol"] += len(matches)
            continue
        records.append(matches[0])
    return {
        "schema": "mypybite.finra-short-shares.RESEARCH-ONLY.v1",
        "provider": "FINRA consolidatedShortInterest",
        "sourceUrl": DATA_ENDPOINT,
        "termsUrl": TERMS_URL,
        "usageReviewed": False,
        "publishable": False,
        "settlementDate": settlement_date,
        "retrievedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "note": "No free-float denominator or commercial reuse rights verified. "
                "DO NOT treat these share counts as percent short interest or scoring inputs.",
        "coverage": {"inputRows": len(rows), "uniqueMatchedSecurities": len(records),
                     "rejected": dict(rejected)},
        "records": records,
    }


def atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         prefix=".finra-", suffix=".tmp", delete=False) as f:
            temporary = f.name
            json.dump(payload, f, allow_nan=False, ensure_ascii=False, separators=(",", ":"))
            f.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


class Tests(unittest.TestCase):
    def sample(self, symbol: str = "MSFT", count: int = 100) -> dict[str, Any]:
        return {"settlementDate": "2026-10-30", "symbolCode": symbol,
                "currentShortPositionQuantity": count,
                "issueName": "Microsoft Corporation", "marketClassCode": "NNM",
                "revisionFlag": None, "stockSplitFlag": None}

    def test_sec_directory_exact_match(self) -> None:
        import tempfile as _temp
        with _temp.TemporaryDirectory() as d:
            f = Path(d) / "directory.json"
            f.write_text(json.dumps({"schema": "mypybite.ticker-directory.v1",
                                     "fields": ["symbol", "cik"],
                                     "data": [["MSFT", "789019"], ["APP", "11"],
                                              ["APP", "22"], ["AAPL", "320193"]]}))
            mapping = sec_directory(f)
            self.assertEqual(mapping["MSFT"], "0000789019")
            self.assertIsNone(mapping["APP"])
            self.assertEqual(mapping["AAPL"], "0000320193")

    def test_latest_partition_not_future_or_stale(self) -> None:
        p = {"partitionFields": ["settlementDate"], "availablePartitions": [
            {"partitions": ["2026-10-15"]}, {"partitions": ["2026-10-30"]},
            {"partitions": ["2026-11-30"]}]}
        self.assertEqual(parse_partitions(p, today=date(2026, 11, 12)), "2026-10-30")
        with self.assertRaises(ValueError):
            parse_partitions(p, today=date(2027, 1, 31))

    def test_paged_api_and_header(self) -> None:
        calls = []
        def fake(url, *, bearer, data):
            calls.append(data["offset"])
            rows = [self.sample(count=i) for i in [10, 20, 30]]
            offset = data["offset"]
            return rows[offset:offset + 2], {"record-total": "3"}
        rows = fetch_rows(bearer="fake-token", settlement_date="2026-10-30",
                          page_limit=2, fetcher=fake)
        self.assertEqual(calls, [0, 2])
        self.assertEqual(len(rows), 3)

    def test_not_scored_no_float(self) -> None:
        payload = build_research_payload([self.sample()], {"MSFT": "0000789019"},
                                         settlement_date="2026-10-30")
        self.assertFalse(payload["publishable"])
        self.assertFalse(payload["usageReviewed"])
        self.assertNotIn("shortInterestPercent", payload["records"][0])
        self.assertEqual(payload["records"][0]["shortShares"], 100)

    def test_ambiguous_symbol_rejected(self) -> None:
        payload = build_research_payload([self.sample(), self.sample(count=200)],
                                         {"MSFT": "0000789019"}, settlement_date="2026-10-30")
        self.assertEqual(payload["coverage"]["uniqueMatchedSecurities"], 0)
        self.assertEqual(payload["coverage"]["rejected"]["ambiguous_FINRA_symbol"], 2)

    def test_symbol_missing_from_directory(self) -> None:
        payload = build_research_payload([self.sample(symbol="XXX")], {},
                                         settlement_date="2026-10-30")
        self.assertEqual(payload["coverage"]["rejected"]["no_unique_SEC_CIK"], 1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--offline-input", type=Path, help="FINRA JSON response array, for local tests")
    p.add_argument("--symbol", type=str, help="One exact symbol, e.g., MSFT; not prefix search")
    p.add_argument("--settlement-date", type=str, help="FINRA published reporting date, YYYY-MM-DD")
    p.add_argument("--directory", type=Path, default=DIRECTORY)
    p.add_argument("--output", type=Path, default=OUTPUT)
    p.add_argument("--max-pages", type=int, default=100)
    args = p.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    directory = sec_directory(args.directory)
    if args.symbol:
        args.symbol = args.symbol.strip().upper()
        if not SYMBOL.fullmatch(args.symbol):
            p.error("Invalid exact ticker")
    if args.offline_input:
        rows = json.loads(args.offline_input.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            p.error("Offline FINRA payload must be a JSON array of records")
        if not args.settlement_date:
            p.error("--offline-input requires --settlement-date")
        settlement = args.settlement_date
    else:
        bearer = token_from_environment()
        if args.settlement_date:
            settlement = date.fromisoformat(args.settlement_date).isoformat()
        else:
            partitions, _ = request_json(PARTITIONS_ENDPOINT, bearer=bearer)
            settlement = parse_partitions(partitions, today=date.today())
        rows = fetch_rows(bearer=bearer, settlement_date=settlement,
                          symbol=args.symbol, max_pages=args.max_pages)
    payload = build_research_payload(rows, directory, settlement_date=settlement)
    atomic_write(args.output, payload)
    print("FINRA research-only short-share coverage:", json.dumps(payload["coverage"], sort_keys=True))
    print("Saved:", args.output)
    print("No scores added. Free-float shares and publication rights remain unverified.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
