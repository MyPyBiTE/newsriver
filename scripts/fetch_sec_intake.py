#!/usr/bin/env python3

"""Collect a point-in-time manifest of relevant SEC EDGAR filings.

This first-stage collector intentionally stores filing identities and source links
only.  Form-specific interpretation belongs in later, separately tested parsers.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zlib

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator


SCHEMA = "mypybite.sec-intake.v1"
SEC_ARCHIVES = "https://www.sec.gov/Archives"
DEFAULT_OUTPUT_DIR = Path("data/sec/intake")
DEFAULT_LOOKBACK_DAYS = 7
REQUEST_INTERVAL_SECONDS = 0.25  # Four requests/second, below the SEC ceiling.
MAX_ATTEMPTS = 4

TARGET_FORMS = frozenset(
    {
        "3",
        "3/A",
        "4",
        "4/A",
        "5",
        "5/A",
        "8-K",
        "8-K/A",
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "SC 13D",
        "SC 13D/A",
        "SC 13G",
        "SC 13G/A",
    }
)

ACCESSION_DIGITS = re.compile(r"^(?P<a>\d{10})(?P<b>\d{2})(?P<c>\d{6})$")


class IntakeFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class IndexRow:
    cik: str
    company_name: str
    form: str
    filed_date: str
    filename: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def normalize_form(value: str) -> str:
    return " ".join(value.strip().upper().split())


def normalize_filed_date(value: str) -> str:
    raw = value.strip()
    if re.fullmatch(r"\d{8}", raw):
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise IntakeFailure(f"Invalid filing date: {value!r}") from exc


def normalize_filename(value: str) -> str:
    result = value.strip().lstrip("/")
    if not result.startswith("edgar/data/") or not result.endswith(".txt"):
        raise IntakeFailure(f"Unexpected EDGAR filename: {value!r}")
    if ".." in result.split("/"):
        raise IntakeFailure(f"Unsafe EDGAR filename: {value!r}")
    return result


def accession_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    if re.fullmatch(r"\d{10}-\d{2}-\d{6}", stem):
        return stem
    match = ACCESSION_DIGITS.fullmatch(stem)
    if match:
        return f"{match.group('a')}-{match.group('b')}-{match.group('c')}"
    raise IntakeFailure(f"Cannot derive accession number from {filename!r}")


def filing_index_url(cik: str, accession: str) -> str:
    compact = accession.replace("-", "")
    return f"{SEC_ARCHIVES}/edgar/data/{int(cik)}/{compact}/{accession}-index.html"


def parse_master_index(text: str) -> Iterator[IndexRow]:
    in_records = False
    saw_header = False
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip("\r\n")
        if not in_records:
            if line.strip() in {
                "CIK|Company Name|Form Type|Date Filed|Filename",
                "CIK|Company Name|Form Type|Date Filed|File Name",
            }:
                saw_header = True
            elif saw_header and line.startswith("---"):
                in_records = True
            continue

        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != 5:
            raise IntakeFailure(
                f"Malformed master index record at line {line_number}: {line!r}"
            )
        cik, company_name, form, filed_date, filename = (part.strip() for part in parts)
        normalized_form = normalize_form(form)
        if normalized_form not in TARGET_FORMS:
            continue
        if not cik.isdigit() or not company_name:
            raise IntakeFailure(f"Invalid issuer record at line {line_number}")
        yield IndexRow(
            cik=cik.zfill(10),
            company_name=" ".join(company_name.split()),
            form=normalized_form,
            filed_date=normalize_filed_date(filed_date),
            filename=normalize_filename(filename),
        )

    if not saw_header or not in_records:
        raise IntakeFailure("The response is not a recognized EDGAR master index.")


def index_url(day: date) -> str:
    quarter = ((day.month - 1) // 3) + 1
    return (
        f"{SEC_ARCHIVES}/edgar/daily-index/{day.year}/QTR{quarter}/"
        f"master.{day:%Y%m%d}.idx"
    )


class SecClient:
    def __init__(self, user_agent: str, interval_seconds: float) -> None:
        if "@" not in user_agent or len(user_agent.strip()) < 8:
            raise IntakeFailure(
                "SEC_USER_AGENT must identify the operator and include a contact email."
            )
        self.user_agent = user_agent.strip()
        self.interval_seconds = max(interval_seconds, REQUEST_INTERVAL_SECONDS)
        self._last_request_at = 0.0

    def get_text(self, url: str) -> str | None:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.interval_seconds:
                time.sleep(self.interval_seconds - elapsed)
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept-Encoding": "gzip, deflate",
                    "Accept": "text/plain, application/json;q=0.9, */*;q=0.1",
                },
            )
            try:
                self._last_request_at = time.monotonic()
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = response.read()
                    content_encoding = str(response.headers.get("Content-Encoding") or "").lower()
                    if content_encoding == "gzip":
                        payload = gzip.decompress(payload)
                    elif content_encoding == "deflate":
                        payload = zlib.decompress(payload)
                    return payload.decode("latin-1")
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return None
                if exc.code not in {429, 500, 502, 503, 504} or attempt == MAX_ATTEMPTS:
                    raise IntakeFailure(f"SEC request failed ({exc.code}): {url}") from exc
            except urllib.error.URLError as exc:
                if attempt == MAX_ATTEMPTS:
                    raise IntakeFailure(f"SEC request failed: {url}: {exc.reason}") from exc
            time.sleep(2 ** (attempt - 1))
        raise AssertionError("unreachable")


def record_from_row(row: IndexRow, source_url: str, discovered_at: str) -> dict[str, object]:
    accession = accession_from_filename(row.filename)
    return {
        "schema": SCHEMA,
        "recordId": f"{accession}:{row.cik}",
        "accessionNumber": accession,
        "cik": row.cik,
        "companyName": row.company_name,
        "form": row.form,
        "filedDate": row.filed_date,
        "acceptedAt": None,
        "isAmendment": row.form.endswith("/A"),
        "filingTextUrl": f"{SEC_ARCHIVES}/{row.filename}",
        "filingIndexUrl": filing_index_url(row.cik, accession),
        "sourceIndexUrl": source_url,
        "discoveredAt": discovered_at,
    }


IMMUTABLE_FIELDS = (
    "recordId",
    "accessionNumber",
    "cik",
    "companyName",
    "form",
    "filedDate",
    "filingTextUrl",
)


def load_existing(path: Path) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    if not path.exists():
        return records
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntakeFailure(f"Invalid JSONL at {path}:{line_number}") from exc
        record_id = str(record.get("recordId") or "")
        if not record_id or record_id in records:
            raise IntakeFailure(f"Missing or duplicate recordId at {path}:{line_number}")
        records[record_id] = record
    return records


def merge_records(
    existing: dict[str, dict[str, object]], incoming: Iterable[dict[str, object]]
) -> tuple[dict[str, dict[str, object]], int]:
    merged = dict(existing)
    added = 0
    for record in incoming:
        record_id = str(record["recordId"])
        prior = merged.get(record_id)
        if prior is not None:
            mismatches = [field for field in IMMUTABLE_FIELDS if prior.get(field) != record.get(field)]
            if mismatches:
                raise IntakeFailure(
                    f"Immutable filing conflict for {record_id}: {', '.join(mismatches)}"
                )
            continue
        merged[record_id] = record
        added += 1
    return merged, added


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def write_json(path: Path, value: object) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def write_jsonl(path: Path, records: Iterable[dict[str, object]]) -> None:
    ordered = sorted(
        records,
        key=lambda item: (
            str(item.get("filedDate") or ""),
            str(item.get("accessionNumber") or ""),
            str(item.get("cik") or ""),
        ),
    )
    content = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in ordered
    )
    atomic_write_text(path, content)


def candidate_dates(end_date: date, lookback_days: int) -> Iterator[date]:
    for offset in range(lookback_days):
        yield end_date - timedelta(days=offset)


def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    filings_path = output_dir / "filings.jsonl"
    state_path = output_dir / "state.json"
    audit_path = output_dir / "audit.json"
    now = iso_utc(utc_now())

    existing = load_existing(filings_path)
    incoming: list[dict[str, object]] = []
    checked: list[dict[str, object]] = []

    if args.input_file:
        source_url = f"file://{Path(args.input_file).resolve()}"
        text = Path(args.input_file).read_text(encoding="latin-1")
        rows = list(parse_master_index(text))
        incoming.extend(record_from_row(row, source_url, now) for row in rows)
        checked.append({"source": source_url, "status": "ok", "targetRecords": len(rows)})
    else:
        client = SecClient(args.user_agent, args.request_interval)
        for day in candidate_dates(args.end_date, args.lookback_days):
            source_url = index_url(day)
            text = client.get_text(source_url)
            if text is None:
                checked.append({"date": day.isoformat(), "source": source_url, "status": "not_found"})
                continue
            rows = list(parse_master_index(text))
            incoming.extend(record_from_row(row, source_url, now) for row in rows)
            checked.append(
                {
                    "date": day.isoformat(),
                    "source": source_url,
                    "status": "ok",
                    "targetRecords": len(rows),
                }
            )

    successful_sources = sum(item["status"] == "ok" for item in checked)
    if successful_sources == 0:
        raise IntakeFailure("No SEC master indexes were available in the requested range.")

    merged, added = merge_records(existing, incoming)
    duplicate_count = len(incoming) - added
    write_jsonl(filings_path, merged.values())
    state = {
        "schema": SCHEMA,
        "updatedAt": now,
        "recordCount": len(merged),
        "latestFiledDate": max(
            (str(record["filedDate"]) for record in merged.values()), default=None
        ),
        "checkedSources": checked,
    }
    audit = {
        "schema": SCHEMA,
        "generatedAt": now,
        "existingRecords": len(existing),
        "incomingRecords": len(incoming),
        "newRecords": added,
        "duplicateRecords": duplicate_count,
        "finalRecords": len(merged),
        "uniqueFilings": len(
            {str(record["accessionNumber"]) for record in merged.values()}
        ),
        "targetForms": sorted(TARGET_FORMS),
        "acceptanceTimestampCoverage": 0,
        "notes": [
            "acceptedAt is intentionally null in intake v1 and will be enriched from authoritative filing metadata in the next component."
        ],
    }
    write_json(state_path, state)
    write_json(audit_path, audit)
    print(
        f"SEC intake complete: {added} new, {duplicate_count} already known, "
        f"{len(merged)} total records."
    )
    return 0


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        default=utc_now().date(),
        help="Newest daily index date to check (default: today UTC).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Number of calendar dates to reconcile.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("SEC_USER_AGENT", ""),
        help="Declared SEC user agent; normally supplied through SEC_USER_AGENT.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=REQUEST_INTERVAL_SECONDS,
        help="Seconds between SEC requests; values below 0.25 are raised to 0.25.",
    )
    parser.add_argument(
        "--input-file",
        help="Parse a local master index fixture instead of using the network.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.lookback_days < 1 or args.lookback_days > 31:
        parser.error("--lookback-days must be between 1 and 31")
    try:
        return run(args)
    except (IntakeFailure, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
