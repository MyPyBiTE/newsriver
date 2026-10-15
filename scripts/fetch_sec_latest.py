#!/usr/bin/env python3

"""Collect target filings from the SEC Latest Filings Atom feed.

This is the low-latency detection layer.  The nightly master-index collector
remains the canonical completeness layer and is intentionally independent.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import urllib.parse
import xml.etree.ElementTree as ET

from pathlib import Path
from typing import Iterable

from fetch_sec_intake import (
    IntakeFailure,
    SecClient,
    TARGET_FORMS,
    accession_from_filename,
    atomic_write_text,
    iso_utc,
    normalize_filed_date,
    normalize_form,
    utc_now,
    write_json,
)


SCHEMA = "mypybite.sec-latest.v1"
SEC_LATEST = "https://www.sec.gov/cgi-bin/browse-edgar"
DEFAULT_OUTPUT_DIR = Path("data/sec/detections")
DEFAULT_PAGES = 5
PAGE_SIZE = 100
ATOM = {"atom": "http://www.w3.org/2005/Atom"}
FORM_ALIASES = {
    "SCHEDULE 13D": "SC 13D",
    "SCHEDULE 13D/A": "SC 13D/A",
    "SCHEDULE 13G": "SC 13G",
    "SCHEDULE 13G/A": "SC 13G/A",
}
CIK_PATTERN = re.compile(r"\((\d{10})\)")
SUMMARY_ACCESSION = re.compile(r"AccNo:\s*</b>\s*([0-9-]+)", re.IGNORECASE)
SUMMARY_FILED = re.compile(r"Filed:\s*</b>\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE)
ROLE_PATTERN = re.compile(r"\(([^()]*)\)\s*$")


def latest_feed_url(start: int = 0) -> str:
    query = urllib.parse.urlencode(
        {
            "action": "getcurrent",
            "output": "atom",
            "count": PAGE_SIZE,
            "start": start,
        }
    )
    return f"{SEC_LATEST}?{query}"


def canonical_feed_form(value: str) -> str:
    normalized = normalize_form(value)
    return FORM_ALIASES.get(normalized, normalized)


def child_text(entry: ET.Element, name: str) -> str:
    child = entry.find(f"atom:{name}", ATOM)
    return (child.text or "").strip() if child is not None else ""


def entry_link(entry: ET.Element) -> str:
    for link in entry.findall("atom:link", ATOM):
        if link.attrib.get("rel", "alternate") == "alternate":
            return str(link.attrib.get("href") or "").strip()
    return ""


def parse_feed(xml_text: str, source_url: str, observed_at: str) -> list[dict[str, object]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise IntakeFailure(f"Invalid SEC Atom feed: {source_url}") from exc
    if root.tag != "{http://www.w3.org/2005/Atom}feed":
        raise IntakeFailure(f"Unexpected SEC Atom document: {source_url}")
    entries = root.findall("atom:entry", ATOM)
    if not entries:
        raise IntakeFailure(f"SEC Atom feed contained no entries: {source_url}")

    records: list[dict[str, object]] = []
    for entry in entries:
        category = entry.find("atom:category", ATOM)
        raw_form = str(category.attrib.get("term") or "") if category is not None else ""
        form = canonical_feed_form(raw_form)
        if form not in TARGET_FORMS:
            continue

        title = child_text(entry, "title")
        cik_matches = CIK_PATTERN.findall(title)
        if not cik_matches:
            raise IntakeFailure(f"Target SEC feed entry lacks a CIK: {title!r}")
        cik = cik_matches[-1].zfill(10)
        prefix = f"{raw_form} - "
        company_tail = title[len(prefix) :] if title.startswith(prefix) else title
        company_name = CIK_PATTERN.split(company_tail, maxsplit=1)[0].strip()
        if not company_name:
            raise IntakeFailure(f"Target SEC feed entry lacks a company name: {title!r}")
        role_match = ROLE_PATTERN.search(title)
        role = role_match.group(1).strip() if role_match else None

        summary = html.unescape(child_text(entry, "summary"))
        accession_match = SUMMARY_ACCESSION.search(summary)
        filed_match = SUMMARY_FILED.search(summary)
        filing_index_url = entry_link(entry)
        if not accession_match or not filed_match or not filing_index_url:
            raise IntakeFailure(f"Target SEC feed entry is incomplete: {title!r}")
        accession = accession_match.group(1)
        if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
            raise IntakeFailure(f"Invalid accession in SEC feed: {accession!r}")
        filed_date = normalize_filed_date(filed_match.group(1))
        compact = accession.replace("-", "")
        filing_text_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{compact}/{accession}.txt"
        )
        # Validate the derived accession path using the same strict parser used
        # by the nightly master-index collector.
        if accession_from_filename(f"edgar/data/{int(cik)}/{compact}.txt") != accession:
            raise AssertionError("unreachable accession mismatch")
        records.append(
            {
                "schema": SCHEMA,
                "recordId": f"{accession}:{cik}",
                "accessionNumber": accession,
                "cik": cik,
                "companyName": " ".join(company_name.split()),
                "role": role,
                "form": form,
                "filedDate": filed_date,
                "isAmendment": form.endswith("/A"),
                "filingTextUrl": filing_text_url,
                "filingIndexUrl": filing_index_url,
                "feedUpdatedAt": child_text(entry, "updated") or None,
                "detectedAt": observed_at,
                "sourceFeedUrl": source_url,
            }
        )
    return records


IMMUTABLE_FIELDS = (
    "recordId",
    "accessionNumber",
    "cik",
    "companyName",
    "role",
    "form",
    "filedDate",
    "filingTextUrl",
    "filingIndexUrl",
    "feedUpdatedAt",
)


def load_existing(path: Path) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    if not path.exists():
        return records
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
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
            mismatches = [
                field for field in IMMUTABLE_FIELDS if prior.get(field) != record.get(field)
            ]
            if mismatches:
                raise IntakeFailure(
                    f"Immutable feed conflict for {record_id}: {', '.join(mismatches)}"
                )
            continue
        merged[record_id] = record
        added += 1
    return merged, added


def write_jsonl(path: Path, records: Iterable[dict[str, object]]) -> None:
    ordered = sorted(
        records,
        key=lambda item: (
            str(item.get("filedDate") or ""),
            str(item.get("feedUpdatedAt") or ""),
            str(item.get("accessionNumber") or ""),
            str(item.get("cik") or ""),
        ),
    )
    content = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in ordered
    )
    atomic_write_text(path, content)


def run(args: argparse.Namespace, client: SecClient | None = None) -> int:
    output_dir = Path(args.output_dir)
    filings_path = output_dir / "filings.jsonl"
    state_path = output_dir / "state.json"
    audit_path = output_dir / "audit.json"
    observed_at = iso_utc(utc_now())
    existing = load_existing(filings_path)
    incoming: list[dict[str, object]] = []
    checked_sources: list[str] = []

    if args.input_file:
        source_url = f"file://{Path(args.input_file).resolve()}"
        incoming.extend(
            parse_feed(
                Path(args.input_file).read_text(encoding="latin-1"),
                source_url,
                observed_at,
            )
        )
        checked_sources.append(source_url)
    else:
        if client is None:
            client = SecClient(args.user_agent, args.request_interval)
        for page in range(args.pages):
            source_url = latest_feed_url(page * PAGE_SIZE)
            xml_text = client.get_text(source_url, frozenset())
            if xml_text is None:
                raise IntakeFailure(f"SEC Latest Filings feed was unavailable: {source_url}")
            incoming.extend(parse_feed(xml_text, source_url, observed_at))
            checked_sources.append(source_url)

    merged, added = merge_records(existing, incoming)
    duplicate_count = len(incoming) - added
    if added == 0 and filings_path.exists() and state_path.exists() and audit_path.exists():
        print(
            f"SEC latest check complete: 0 new, {duplicate_count} already known, "
            f"{len(merged)} total detections."
        )
        return 0

    write_jsonl(filings_path, merged.values())
    write_json(
        state_path,
        {
            "schema": SCHEMA,
            "updatedAt": observed_at,
            "recordCount": len(merged),
            "uniqueFilings": len(
                {str(record["accessionNumber"]) for record in merged.values()}
            ),
            "latestFeedUpdatedAt": max(
                (str(record.get("feedUpdatedAt") or "") for record in merged.values()),
                default=None,
            ),
            "pagesChecked": len(checked_sources),
            "checkedSources": checked_sources,
        },
    )
    write_json(
        audit_path,
        {
            "schema": SCHEMA,
            "generatedAt": observed_at,
            "existingRecords": len(existing),
            "incomingRecords": len(incoming),
            "newRecords": added,
            "duplicateRecords": duplicate_count,
            "finalRecords": len(merged),
            "targetForms": sorted(TARGET_FORMS),
            "notes": [
                "This feed is the low-latency detector; nightly master-index reconciliation remains authoritative.",
                "feedUpdatedAt is not treated as the authoritative acceptance timestamp.",
            ],
        },
    )
    print(
        f"SEC latest check complete: {added} new, {duplicate_count} already known, "
        f"{len(merged)} total detections."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--pages", type=int, default=DEFAULT_PAGES)
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--input-file", help="Parse a local Atom fixture instead of the network.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.pages < 1 or args.pages > 20:
        parser.error("--pages must be between 1 and 20")
    try:
        return run(args)
    except (IntakeFailure, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
