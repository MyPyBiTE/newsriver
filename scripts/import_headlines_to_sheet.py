#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Incoming"

HEADERS = [
    "story_id", "source", "source_url", "canonical_url", "source_story_id",
    "published_at", "scraped_headline", "editor_headline", "candidate_synopsis",
    "editor_synopsis", "status", "cluster_id", "duplicate_of", "ticker_eligible",
    "newsriver_eligible", "auto_eligible", "go_live_at", "expires_at", "keep_until",
    "priority", "pin_until", "access_type", "free_alternative_url",
    "access_checked_at", "rights_note", "editor_notes", "source_trust_tier",
    "risk_class", "auto_candidate", "auto_block_reason", "validation_state",
    "selection_mode", "correction_of", "retraction_note", "imported_at",
    "last_updated_at",
]

MACHINE_REFRESH_FIELDS = [
    "source", "source_url", "canonical_url", "source_story_id",
    "scraped_headline", "cluster_id", "validation_state",
]


class ImportFailure(RuntimeError):
    pass


def utc_now_sheet() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def to_sheet_datetime(value: Any) -> str:
    if not value:
        return ""
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return text


def stable_story_id(item: dict[str, Any]) -> str:
    existing = str(item.get("canonical_id") or "").strip()
    if existing:
        return existing

    url = str(item.get("canonical_url") or item.get("url") or "").strip()
    if not url:
        raise ImportFailure("A headline item has no canonical_id or URL.")

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return f"u:{digest}"


def load_input(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ImportFailure(f"Input file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ImportFailure(f"Malformed JSON in {path}: {exc}") from exc

    items = payload.get("items")
    if not isinstance(items, list):
        raise ImportFailure(f"{path} does not contain an items list.")

    return [item for item in items if isinstance(item, dict)]


def load_credentials():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise ImportFailure("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ImportFailure(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
        ) from exc

    return service_account.Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )


def make_new_row(item: dict[str, Any], now: str) -> list[Any]:
    story_id = stable_story_id(item)
    url = str(item.get("url") or "").strip()
    canonical_url = str(item.get("canonical_url") or url).strip()

    values: dict[str, Any] = {
        "story_id": story_id,
        "source": str(item.get("source") or "").strip(),
        "source_url": url or canonical_url,
        "canonical_url": canonical_url or url,
        "source_story_id": story_id,
        "published_at": to_sheet_datetime(item.get("published_utc")),
        "scraped_headline": str(item.get("title") or "").strip(),
        "editor_headline": "",
        "candidate_synopsis": "",
        "editor_synopsis": "",
        "status": "Review",
        "cluster_id": str(item.get("cluster_id") or "").strip(),
        "duplicate_of": "",
        "ticker_eligible": False,
        "newsriver_eligible": False,
        "auto_eligible": False,
        "go_live_at": "",
        "expires_at": "",
        "keep_until": "",
        "priority": "",
        "pin_until": "",
        "access_type": "UNKNOWN",
        "free_alternative_url": "",
        "access_checked_at": "",
        "rights_note": "",
        "editor_notes": "",
        "source_trust_tier": "",
        "risk_class": "",
        "auto_candidate": False,
        "auto_block_reason": "",
        "validation_state": "VALID",
        "selection_mode": "MANUAL",
        "correction_of": "",
        "retraction_note": "",
        "imported_at": now,
        "last_updated_at": now,
    }

    return [values[header] for header in HEADERS]


def merge_existing_row(
    current: list[Any],
    candidate: list[Any],
    now: str,
) -> tuple[list[Any], list[str]]:
    merged = list(current)
    changed_fields: list[str] = []
    index = {name: position for position, name in enumerate(HEADERS)}

    for field in MACHINE_REFRESH_FIELDS:
        position = index[field]
        if str(merged[position]) != str(candidate[position]):
            merged[position] = candidate[position]
            changed_fields.append(field)

    if changed_fields:
        merged[index["last_updated_at"]] = now

    return merged, changed_fields


def pad_row(row: list[Any]) -> list[Any]:
    return list(row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(row))


def import_items(
    spreadsheet_id: str,
    items: list[dict[str, Any]],
    limit: int,
    dry_run: bool,
) -> None:
    credentials = load_credentials()
    service = build(
        "sheets",
        "v4",
        credentials=credentials,
        cache_discovery=False,
    )
    values_api = service.spreadsheets().values()

    result = values_api.get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A1:AJ",
    ).execute()

    values = result.get("values", [])
    if not values:
        raise ImportFailure(f"{SHEET_NAME} has no header row.")

    actual_headers = pad_row(values[0])
    if actual_headers != HEADERS:
        mismatches = [
            f"{position + 1}: expected {expected!r}, found {actual!r}"
            for position, (expected, actual) in enumerate(
                zip(HEADERS, actual_headers)
            )
            if expected != actual
        ]
        raise ImportFailure(
            "Incoming header mismatch:\n" + "\n".join(mismatches)
        )

    existing_rows = [pad_row(row) for row in values[1:]]
    row_by_story_id: dict[str, tuple[int, list[Any]]] = {}

    for sheet_row_number, row in enumerate(existing_rows, start=2):
        story_id = str(row[0]).strip()
        if story_id:
            row_by_story_id[story_id] = (sheet_row_number, row)

    now = utc_now_sheet()
    additions: list[list[Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged = 0
    skipped_duplicate_input = 0
    seen_input_ids: set[str] = set()

    selected = items[:limit] if limit > 0 else items

    for item in selected:
        candidate = make_new_row(item, now)
        story_id = str(candidate[0])

        if story_id in seen_input_ids:
            skipped_duplicate_input += 1
            continue
        seen_input_ids.add(story_id)

        existing = row_by_story_id.get(story_id)
        if existing is None:
            additions.append(candidate)
            continue

        row_number, current = existing
        merged, changed_fields = merge_existing_row(current, candidate, now)

        if changed_fields:
            print(
                "REFRESH "
                f"story_id={story_id} "
                f"fields={','.join(changed_fields)}"
            )
            updates.append(
                {
                    "range": f"{SHEET_NAME}!A{row_number}:AJ{row_number}",
                    "values": [merged],
                }
            )
        else:
            unchanged += 1

    print(
        "PLAN "
        f"input={len(selected)} "
        f"add={len(additions)} "
        f"update={len(updates)} "
        f"unchanged={unchanged} "
        f"input_duplicates={skipped_duplicate_input}"
    )

    if dry_run:
        print("DRY RUN: no Google Sheet values were changed.")
        return

    writes = list(updates)

    if additions:
        # Google Sheets append() treats preformatted checkbox/dropdown rows as
        # occupied and can place records around row 1001. Instead, place new
        # records in the first rows whose story_id cell (column A) is blank.
        blank_story_rows = [
            row_number
            for row_number, row in enumerate(existing_rows, start=2)
            if not str(row[0]).strip()
        ]

        next_new_row = len(existing_rows) + 2
        while len(blank_story_rows) < len(additions):
            blank_story_rows.append(next_new_row)
            next_new_row += 1

        for row_number, row_values in zip(blank_story_rows, additions):
            writes.append(
                {
                    "range": f"{SHEET_NAME}!A{row_number}:AJ{row_number}",
                    "values": [row_values],
                }
            )

    if writes:
        values_api.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": writes,
            },
        ).execute()

    print(
        "DONE "
        f"added={len(additions)} "
        f"updated={len(updates)} "
        f"unchanged={unchanged}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import duplicate-safe V1 headline records into "
            "the MYPYBITE V2 Incoming sheet."
        )
    )
    parser.add_argument(
        "--input",
        default="headlines.json",
        help="Path to the existing V1 headlines JSON.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=40,
        help="Maximum number of newest items to consider; 0 means all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and calculate changes without writing to Sheets.",
    )
    return parser.parse_args()


def main() -> int:
    spreadsheet_id = os.environ.get("MYPYBITE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        print("FAIL: MYPYBITE_SHEET_ID is missing.", file=sys.stderr)
        return 1

    args = parse_args()

    try:
        items = load_input(Path(args.input))
        import_items(
            spreadsheet_id=spreadsheet_id,
            items=items,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    except (ImportFailure, HttpError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
