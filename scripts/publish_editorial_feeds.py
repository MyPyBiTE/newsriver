#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
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

PRIVATE_FIELDS = {
    "candidate_synopsis", "editor_synopsis", "editor_notes", "rights_note",
    "source_trust_tier", "risk_class", "auto_candidate", "auto_block_reason",
    "validation_state", "retraction_note", "imported_at", "last_updated_at",
    "source_story_id", "duplicate_of", "auto_eligible", "access_checked_at",
}


class PublishFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class Candidate:
    source_row: int
    story_id: str
    correction_of: str
    public_item: dict[str, Any]
    ticker_eligible: bool
    newsriver_eligible: bool
    pinned: bool
    priority: int
    published_sort: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def word_count(value: str) -> int:
    return len(value.split()) if value else 0


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return clean_text(value).lower() in {"true", "yes", "y", "1", "checked"}


def parse_priority(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    try:
        number = int(float(text))
    except ValueError as exc:
        raise PublishFailure(f"Invalid priority value: {text!r}") from exc
    if not 0 <= number <= 100:
        raise PublishFailure(f"Priority must be from 0 to 100, found {number}.")
    return number


def parse_datetime(value: Any, default_tz: ZoneInfo, *, assume_utc: bool = False) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    parsed: datetime | None = None
    for candidate in (
        normalized,
        normalized.replace(" ", "T", 1),
    ):
        try:
            parsed = datetime.fromisoformat(candidate)
            break
        except ValueError:
            pass

    if parsed is None:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                pass

    if parsed is None:
        raise PublishFailure(f"Invalid date/time value: {text!r}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc if assume_utc else default_tz)
    return parsed.astimezone(timezone.utc)


def safe_url(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return text


def pad_row(row: list[Any]) -> list[Any]:
    return list(row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(row))


def row_dict(row: list[Any]) -> dict[str, Any]:
    return dict(zip(HEADERS, pad_row(row)))


def load_credentials():
    from google.oauth2 import service_account

    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise PublishFailure("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PublishFailure("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def read_incoming(spreadsheet_id: str) -> list[list[Any]]:
    from googleapiclient.discovery import build

    credentials = load_credentials()
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A1:AJ",
        valueRenderOption="FORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    values = result.get("values", [])
    if not values:
        raise PublishFailure(f"{SHEET_NAME} has no header row.")

    actual = pad_row(values[0])
    if actual != HEADERS:
        mismatches = [
            f"column {i + 1}: expected {expected!r}, found {found!r}"
            for i, (expected, found) in enumerate(zip(HEADERS, actual))
            if expected != found
        ]
        raise PublishFailure("Incoming header mismatch:\n" + "\n".join(mismatches))
    return [pad_row(row) for row in values[1:]]


def is_active(row: dict[str, Any], now: datetime, editor_tz: ZoneInfo) -> tuple[bool, str]:
    status = clean_text(row["status"]).lower()
    if status not in {"approved", "scheduled"}:
        return False, f"status={status or 'blank'}"

    if clean_text(row["validation_state"]).upper() != "VALID":
        return False, "validation_state is not VALID"

    if clean_text(row["selection_mode"]).upper() not in {"", "MANUAL"}:
        return False, "selection_mode is not MANUAL"

    if clean_text(row["duplicate_of"]):
        return False, "duplicate_of is populated"

    if not parse_bool(row["ticker_eligible"]) and not parse_bool(row["newsriver_eligible"]):
        return False, "no public channel is enabled"

    go_live = parse_datetime(row["go_live_at"], editor_tz)
    if status == "scheduled" and go_live is None:
        return False, "Scheduled row has no go_live_at"
    if go_live is not None and now < go_live:
        return False, "go_live_at is in the future"

    expires = parse_datetime(row["expires_at"], editor_tz)
    keep_until = parse_datetime(row["keep_until"], editor_tz)
    if expires is not None and now >= expires:
        if keep_until is None or now >= keep_until:
            return False, "expired"

    return True, "active"


def build_candidate(row: dict[str, Any], row_number: int, now: datetime, editor_tz: ZoneInfo) -> Candidate:
    story_id = clean_text(row["story_id"])
    if not story_id:
        raise PublishFailure(f"Row {row_number}: active row has no story_id.")

    headline = clean_text(row["editor_headline"]) or clean_text(row["scraped_headline"])
    if not headline:
        raise PublishFailure(f"Row {row_number} ({story_id}): active row has no headline.")

    synopsis = clean_text(row["editor_synopsis"]) or clean_text(row["candidate_synopsis"])
    if not synopsis:
        raise PublishFailure(f"Row {row_number} ({story_id}): active row has no synopsis.")
    if word_count(synopsis) > 25:
        raise PublishFailure(
            f"Row {row_number} ({story_id}): synopsis has {word_count(synopsis)} words; maximum is 25."
        )

    source = clean_text(row["source"])
    if not source:
        raise PublishFailure(f"Row {row_number} ({story_id}): active row has no source.")

    visitor_url = (
        safe_url(row["free_alternative_url"])
        or safe_url(row["canonical_url"])
        or safe_url(row["source_url"])
    )
    if visitor_url is None:
        raise PublishFailure(f"Row {row_number} ({story_id}): no valid http/https visitor URL.")

    published_at = parse_datetime(row["published_at"], editor_tz, assume_utc=True)
    priority = parse_priority(row["priority"])
    pin_until = parse_datetime(row["pin_until"], editor_tz)
    pinned = pin_until is not None and now < pin_until

    access = clean_text(row["access_type"]).upper() or "UNKNOWN"
    paywall_map = {"FREE": "none", "METERED": "metered", "PAYWALL": "paywall", "UNKNOWN": "unknown"}
    if access not in paywall_map:
        raise PublishFailure(f"Row {row_number} ({story_id}): invalid access_type {access!r}.")

    ticker_eligible = parse_bool(row["ticker_eligible"])
    newsriver_eligible = parse_bool(row["newsriver_eligible"])

    public_item = {
        "id": story_id,
        "category": "GENERAL",
        "headline": headline,
        "summaryShort": synopsis,
        "canonicalUrl": visitor_url,
        "sourceName": source,
        "publishedAt": iso_z(published_at) if published_at else None,
        "status": "approved",
        "priority": priority,
        "urgency": "normal",
        "paywall": paywall_map[access],
        "clusterId": clean_text(row["cluster_id"]) or None,
        "relayEnabled": ticker_eligible,
        "newsriverEnabled": newsriver_eligible,
        "imageUrl": None,
        "fixture": False,
    }

    leaked = PRIVATE_FIELDS.intersection(public_item)
    if leaked:
        raise PublishFailure(f"Internal error: private fields selected for export: {sorted(leaked)}")

    return Candidate(
        source_row=row_number,
        story_id=story_id,
        correction_of=clean_text(row["correction_of"]),
        public_item=public_item,
        ticker_eligible=ticker_eligible,
        newsriver_eligible=newsriver_eligible,
        pinned=pinned,
        priority=priority,
        published_sort=published_at or datetime(1970, 1, 1, tzinfo=timezone.utc),
    )


def candidate_sort_key(candidate: Candidate) -> tuple[int, int, float, str]:
    return (
        1 if candidate.pinned else 0,
        candidate.priority,
        candidate.published_sort.timestamp(),
        candidate.story_id,
    )


def validate_feed(feed: dict[str, Any], *, channel: str) -> None:
    if set(feed) != {"schemaVersion", "generatedAt", "selectionMode", "items"}:
        raise PublishFailure(f"{channel}: invalid top-level public contract.")
    if feed["schemaVersion"] != 1 or feed["selectionMode"] != "manual":
        raise PublishFailure(f"{channel}: invalid schemaVersion or selectionMode.")
    if not isinstance(feed["items"], list):
        raise PublishFailure(f"{channel}: items must be a list.")

    seen: set[str] = set()
    for item in feed["items"]:
        required = {
            "id", "category", "headline", "summaryShort", "canonicalUrl", "sourceName",
            "publishedAt", "status", "priority", "urgency", "paywall", "clusterId",
            "relayEnabled", "newsriverEnabled", "imageUrl", "fixture",
        }
        if set(item) != required:
            raise PublishFailure(f"{channel}: item {item.get('id')!r} has an invalid public field set.")
        if item["id"] in seen:
            raise PublishFailure(f"{channel}: duplicate public id {item['id']!r}.")
        seen.add(item["id"])
        if item["status"] != "approved" or item["fixture"] is not False:
            raise PublishFailure(f"{channel}: item {item['id']!r} is not production-approved.")
        if not item["summaryShort"] or word_count(item["summaryShort"]) > 25:
            raise PublishFailure(f"{channel}: invalid synopsis for {item['id']!r}.")
        if safe_url(item["canonicalUrl"]) is None:
            raise PublishFailure(f"{channel}: invalid URL for {item['id']!r}.")
        if channel == "breaking" and item["relayEnabled"] is not True:
            raise PublishFailure(f"breaking: non-relay item {item['id']!r} included.")
        if channel == "newsriver" and item["newsriverEnabled"] is not True:
            raise PublishFailure(f"newsriver: disabled item {item['id']!r} included.")
        if PRIVATE_FIELDS.intersection(item):
            raise PublishFailure(f"{channel}: private field leaked for {item['id']!r}.")


def make_feeds(rows: list[list[Any]], now: datetime, editor_tz: ZoneInfo) -> tuple[dict[str, Any], dict[str, Any], dict[str, int]]:
    candidates: list[Candidate] = []
    blocked_ids: set[str] = set()
    skipped = 0

    for row_number, raw_row in enumerate(rows, start=2):
        row = row_dict(raw_row)
        story_id = clean_text(row["story_id"])
        status = clean_text(row["status"]).lower()
        if story_id and status in {"hold", "rejected", "retracted"}:
            blocked_ids.add(story_id)

        active, reason = is_active(row, now, editor_tz)
        if not active:
            if story_id:
                print(f"SKIP row={row_number} story_id={story_id} reason={reason}")
            skipped += 1
            continue
        candidates.append(build_candidate(row, row_number, now, editor_tz))

    correction_targets = {candidate.correction_of for candidate in candidates if candidate.correction_of}
    blocked_ids.update(correction_targets)
    selected = [candidate for candidate in candidates if candidate.story_id not in blocked_ids]
    selected.sort(key=candidate_sort_key, reverse=True)

    generated_at = iso_z(now)
    breaking_items = [candidate.public_item for candidate in selected if candidate.ticker_eligible]
    newsriver_items = [candidate.public_item for candidate in selected if candidate.newsriver_eligible]

    breaking = {
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "selectionMode": "manual",
        "items": breaking_items,
    }
    newsriver = {
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "selectionMode": "manual",
        "items": newsriver_items,
    }
    validate_feed(breaking, channel="breaking")
    validate_feed(newsriver, channel="newsriver")

    counts = {
        "rows": len(rows),
        "active_candidates": len(candidates),
        "blocked_or_replaced": len(candidates) - len(selected),
        "breaking": len(breaking_items),
        "newsriver": len(newsriver_items),
        "skipped": skipped,
    }
    return breaking, newsriver, counts


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish manually approved MYPYBITE editorial feeds.")
    parser.add_argument("--output-dir", default=".", help="Directory containing breaking.json and newsriver.json.")
    parser.add_argument("--publish", action="store_true", help="Write validated files. Without this flag, run as dry-run.")
    parser.add_argument("--allow-empty", action="store_true", help="Permit an intentional publish where both feeds contain zero items.")
    parser.add_argument("--now", help="Testing override in ISO-8601 form. Omit in production.")
    return parser.parse_args()


def main() -> int:
    spreadsheet_id = os.environ.get("MYPYBITE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        print("FAIL: MYPYBITE_SHEET_ID is missing.", file=sys.stderr)
        return 1

    timezone_name = os.environ.get("MYPYBITE_EDITOR_TIMEZONE", "America/Toronto").strip()
    try:
        editor_tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        print(f"FAIL: unknown MYPYBITE_EDITOR_TIMEZONE {timezone_name!r}.", file=sys.stderr)
        return 1

    args = parse_args()
    try:
        now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else utc_now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)

        rows = read_incoming(spreadsheet_id)
        breaking, newsriver, counts = make_feeds(rows, now, editor_tz)
        print("PLAN " + " ".join(f"{key}={value}" for key, value in counts.items()))

        if not args.publish:
            print("DRY RUN: Google Sheets was read, but public JSON files were not changed.")
            print(json.dumps({"breaking": breaking, "newsriver": newsriver}, ensure_ascii=False, indent=2))
            return 0

        if not args.allow_empty and not breaking["items"] and not newsriver["items"]:
            raise PublishFailure(
                "Refusing to replace the public feeds with two empty item lists. Run dry-run, correct the Sheet, and publish again."
            )

        output_dir = Path(args.output_dir)
        atomic_write_json(output_dir / "breaking.json", breaking)
        atomic_write_json(output_dir / "newsriver.json", newsriver)
        print(f"PUBLISHED breaking.json={counts['breaking']} newsriver.json={counts['newsriver']}")
        return 0
    except Exception as exc:
        # Google API exceptions are imported lazily so pure rule tests need no Google packages.
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
