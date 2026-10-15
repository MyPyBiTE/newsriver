#!/usr/bin/env python3
"""Enrich SEC intake accessions from authoritative EDGAR filing pages."""

from __future__ import annotations

import argparse
import json
import os
import sys

from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fetch_sec_intake import (
    IntakeFailure,
    REQUEST_INTERVAL_SECONDS,
    SecClient,
    atomic_write_text,
    iso_utc,
    utc_now,
)


SCHEMA = "mypybite.sec-filing-metadata.v1"
DEFAULT_INPUT = Path("data/sec/intake/filings.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/sec/metadata")
DEFAULT_MAX_FILINGS = 250
try:
    SEC_TIMEZONE = ZoneInfo("America/New_York")
except ZoneInfoNotFoundError:
    # Minimal Windows Python installations may not ship the IANA database.
    # GitHub's Linux runner uses the authoritative IANA zone. This fallback
    # follows the post-2007 US rule and keeps offline tests dependency-free.
    SEC_TIMEZONE = None


def sec_localize(value: datetime) -> datetime:
    if SEC_TIMEZONE is not None:
        return value.replace(tzinfo=SEC_TIMEZONE)
    march_first = datetime(value.year, 3, 1)
    first_sunday_march = 1 + ((6 - march_first.weekday()) % 7)
    dst_start = datetime(value.year, 3, first_sunday_march + 7, 2)
    november_first = datetime(value.year, 11, 1)
    first_sunday_november = 1 + ((6 - november_first.weekday()) % 7)
    dst_end = datetime(value.year, 11, first_sunday_november, 2)
    offset = timedelta(hours=-4 if dst_start <= value < dst_end else -5)
    return value.replace(tzinfo=timezone(offset))


class FilingSummaryParser(HTMLParser):
    """Extract EDGAR's labelled filing summary without parsing presentation."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._active: str | None = None
        self._buffer: list[str] = []
        self._pending_label: str | None = None
        self.values: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        classes = set(dict(attrs).get("class", "").split())
        if tag == "div" and "infoHead" in classes:
            self._active = "label"
            self._buffer = []
        elif tag == "div" and "info" in classes:
            self._active = "value"
            self._buffer = []
        elif tag == "br" and self._active == "value":
            self._buffer.append("\n")

    def handle_data(self, data: str) -> None:
        if self._active:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "div" or not self._active:
            return
        text = "\n".join(
            normalized
            for part in "".join(self._buffer).splitlines()
            if (normalized := " ".join(part.split()))
        )
        if self._active == "label":
            self._pending_label = text
        elif self._pending_label and text:
            self.values[self._pending_label] = text
            self._pending_label = None
        self._active = None
        self._buffer = []


def parse_filing_summary(html: str) -> dict[str, object]:
    parser = FilingSummaryParser()
    parser.feed(html)
    values = parser.values
    required = ("Filing Date", "Accepted")
    missing = [name for name in required if not values.get(name)]
    if missing:
        raise IntakeFailure(
            "EDGAR filing page is missing required metadata: " + ", ".join(missing)
        )
    try:
        accepted_local = sec_localize(
            datetime.strptime(str(values["Accepted"]), "%Y-%m-%d %H:%M:%S")
        )
    except ValueError as exc:
        raise IntakeFailure(
            f"Invalid EDGAR acceptance timestamp: {values['Accepted']!r}"
        ) from exc
    try:
        datetime.strptime(str(values["Filing Date"]), "%Y-%m-%d")
    except ValueError as exc:
        raise IntakeFailure(f"Invalid EDGAR filing date: {values['Filing Date']!r}") from exc

    period = values.get("Period of Report")
    if period:
        try:
            datetime.strptime(str(period), "%Y-%m-%d")
        except ValueError as exc:
            raise IntakeFailure(f"Invalid EDGAR period of report: {period!r}") from exc

    document_count: int | None = None
    if values.get("Documents"):
        try:
            document_count = int(str(values["Documents"]))
        except ValueError as exc:
            raise IntakeFailure(
                f"Invalid EDGAR document count: {values['Documents']!r}"
            ) from exc

    items = [item.strip() for item in str(values.get("Items") or "").split("\n")]
    return {
        "filingDate": str(values["Filing Date"]),
        "acceptedAt": accepted_local.isoformat(),
        "acceptedAtSecDisplay": str(values["Accepted"]),
        "periodOfReport": str(period) if period else None,
        "documentCount": document_count,
        "items": [item for item in items if item],
    }


def read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise IntakeFailure(f"Required input does not exist: {path}")
    records: list[dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntakeFailure(f"Invalid JSON at {path}:{line_number}") from exc
        if not isinstance(value, dict):
            raise IntakeFailure(f"Expected object at {path}:{line_number}")
        records.append(value)
    return records


def unique_accessions(records: Iterable[dict[str, object]]) -> dict[str, dict[str, object]]:
    accessions: dict[str, dict[str, object]] = {}
    for record in records:
        accession = str(record.get("accessionNumber") or "")
        source_url = str(record.get("filingIndexUrl") or "")
        if not accession or not source_url:
            raise IntakeFailure("Intake record lacks accessionNumber or filingIndexUrl.")
        existing = accessions.get(accession)
        if existing and str(existing.get("filingIndexUrl")) != source_url:
            # Ownership filings may be indexed against more than one related CIK.
            # Prefer a URL that is already present; every candidate remains official SEC data.
            continue
        accessions[accession] = record
    return accessions


def existing_metadata(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    records = read_jsonl(path)
    result: dict[str, dict[str, object]] = {}
    for record in records:
        accession = str(record.get("accessionNumber") or "")
        if not accession or accession in result:
            raise IntakeFailure("Metadata ledger contains a missing or duplicate accession.")
        result[accession] = record
    return result


def write_json(path: Path, value: object) -> None:
    atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def write_metadata(path: Path, records: Iterable[dict[str, object]]) -> None:
    ordered = sorted(records, key=lambda record: str(record["accessionNumber"]))
    content = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in ordered
    )
    atomic_write_text(path, content)


def run(args: argparse.Namespace, client: SecClient | None = None) -> int:
    intake = read_jsonl(Path(args.input))
    accessions = unique_accessions(intake)
    output_dir = Path(args.output_dir)
    metadata_path = output_dir / "filings.jsonl"
    state_path = output_dir / "state.json"
    audit_path = output_dir / "audit.json"
    existing = existing_metadata(metadata_path)
    pending = [
        record
        for accession, record in accessions.items()
        if accession not in existing
    ]
    pending.sort(
        key=lambda record: (
            str(record.get("filedDate") or ""),
            str(record.get("accessionNumber") or ""),
        ),
        reverse=True,
    )
    selected = pending[: args.max_filings]
    if client is None:
        client = SecClient(args.user_agent, args.request_interval)

    enriched_at = iso_utc(utc_now())
    additions: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    for intake_record in selected:
        accession = str(intake_record["accessionNumber"])
        source_url = str(intake_record["filingIndexUrl"])
        try:
            html = client.get_text(source_url)
            if html is None:
                raise IntakeFailure("EDGAR filing page was not found.")
            summary = parse_filing_summary(html)
            if summary["filingDate"] != intake_record.get("filedDate"):
                raise IntakeFailure(
                    "Filing date conflicts with the immutable daily-index record."
                )
            additions.append(
                {
                    "schema": SCHEMA,
                    "accessionNumber": accession,
                    **summary,
                    "sourceUrl": source_url,
                    "enrichedAt": enriched_at,
                }
            )
        except IntakeFailure as exc:
            errors.append(
                {"accessionNumber": accession, "sourceUrl": source_url, "error": str(exc)}
            )

    merged = {**existing}
    for record in additions:
        accession = str(record["accessionNumber"])
        if accession in merged:
            raise IntakeFailure(f"Refusing to replace metadata for {accession}.")
        merged[accession] = record

    write_metadata(metadata_path, merged.values())
    remaining = max(0, len(accessions) - len(merged))
    write_json(
        state_path,
        {
            "schema": SCHEMA,
            "updatedAt": enriched_at,
            "intakeAccessions": len(accessions),
            "metadataRecords": len(merged),
            "remainingAccessions": remaining,
            "batchLimit": args.max_filings,
            "complete": remaining == 0,
        },
    )
    write_json(
        audit_path,
        {
            "schema": SCHEMA,
            "generatedAt": enriched_at,
            "existingRecords": len(existing),
            "selectedRecords": len(selected),
            "newRecords": len(additions),
            "failedRecords": len(errors),
            "finalRecords": len(merged),
            "remainingAccessions": remaining,
            "acceptanceTimestampCoverage": (
                len(merged) / len(accessions) if accessions else 1.0
            ),
            "errors": errors,
        },
    )
    if errors:
        raise IntakeFailure(
            f"Metadata enrichment failed for {len(errors)} of {len(selected)} selected filings."
        )
    print(
        f"SEC metadata enrichment complete: {len(additions)} new, "
        f"{len(merged)} total, {remaining} remaining."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-filings", type=int, default=DEFAULT_MAX_FILINGS)
    parser.add_argument(
        "--user-agent", default=os.environ.get("SEC_USER_AGENT", "")
    )
    parser.add_argument(
        "--request-interval", type=float, default=REQUEST_INTERVAL_SECONDS
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.max_filings < 1 or args.max_filings > 1000:
        parser.error("--max-filings must be between 1 and 1000")
    try:
        return run(args)
    except (IntakeFailure, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

