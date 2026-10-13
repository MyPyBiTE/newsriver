#!/usr/bin/env python3

from __future__ import annotations

import json
import os

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CACHE_SHEET = "Substack Cache"
CACHE_HEADERS = [
    "publication",
    "headline",
    "url",
    "published_at",
    "priority",
    "source_feed",
]
OUTPUT_PATH = Path("substack.json")
OUTPUT_COUNT = 7
SHEET_TIMEZONE = ZoneInfo("America/Toronto")
SHEETS_EPOCH = datetime(1899, 12, 30)


class FeedFailure(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def canonical_url(value: str) -> str:
    value = (value or "").strip()
    try:
        parts = urlsplit(value)
    except ValueError:
        return ""
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return ""
    host = parts.netloc.lower().removeprefix("www.")
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https", host, path, "", ""))


def parse_sheet_datetime(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        local_value = (SHEETS_EPOCH + timedelta(days=float(value))).replace(
            tzinfo=SHEET_TIMEZONE
        )
        return local_value.astimezone(timezone.utc)

    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHEET_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def parse_priority(value: Any, fallback: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def credentials_from_environment():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise FeedFailure("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise FeedFailure("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def sheets_service():
    return build(
        "sheets",
        "v4",
        credentials=credentials_from_environment(),
        cache_discovery=False,
    )


def load_cache(service, spreadsheet_id: str) -> list[dict[str, Any]]:
    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{CACHE_SHEET}'!A1:F",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="SERIAL_NUMBER",
        )
        .execute()
    )
    values = result.get("values", [])
    if not values:
        raise FeedFailure(f"{CACHE_SHEET} is empty.")

    actual_headers = list(values[0][: len(CACHE_HEADERS)])
    if actual_headers != CACHE_HEADERS:
        raise FeedFailure(
            f"{CACHE_SHEET} headers do not match. Expected {CACHE_HEADERS!r}; "
            f"found {actual_headers!r}."
        )

    posts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row[: len(CACHE_HEADERS)]) + [""] * max(
            0, len(CACHE_HEADERS) - len(raw_row)
        )
        publication = str(row[0] or "").strip()
        headline = str(row[1] or "").strip()
        url = canonical_url(str(row[2] or ""))
        published = parse_sheet_datetime(row[3])
        source_feed = canonical_url(str(row[5] or ""))

        if not publication or not headline or not url or not published or not source_feed:
            print(f"SKIP: invalid cache row {row_number}")
            continue
        if url in seen:
            continue
        seen.add(url)
        posts.append(
            {
                "source": publication,
                "headline": headline,
                "url": url,
                "canonicalUrl": url,
                "publishedAt": iso_utc(published),
                "category": "Editorial",
                "_timestamp": published.timestamp(),
                "_priority": parse_priority(row[4], row_number),
                "_source_feed": source_feed,
            }
        )

    posts.sort(
        key=lambda item: (
            -float(item["_timestamp"]),
            int(item["_priority"]),
            item["canonicalUrl"],
        )
    )
    return posts


def write_payload(posts: list[dict[str, Any]]) -> None:
    if len(posts) < OUTPUT_COUNT:
        raise FeedFailure(
            "Refusing to replace the last valid feed: expected at least "
            f"{OUTPUT_COUNT} cached posts, found {len(posts)}."
        )

    selected = posts[:OUTPUT_COUNT]
    clean_items = [
        {key: value for key, value in item.items() if not key.startswith("_")}
        for item in selected
    ]
    payload = {
        "schema": "mypybite.substack-feed.v1",
        "generatedAt": iso_utc(utc_now()),
        "sourceCount": len({item["_source_feed"] for item in posts}),
        "latest": clean_items[0],
        "items": clean_items,
    }

    temporary = OUTPUT_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(OUTPUT_PATH)


def main() -> int:
    spreadsheet_id = os.environ.get("MYPYBITE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        print("FAIL: MYPYBITE_SHEET_ID is missing.")
        return 1

    try:
        posts = load_cache(sheets_service(), spreadsheet_id)
        write_payload(posts)
        print(
            f"DONE: published the seven newest posts from {len(posts)} valid cached rows."
        )
        return 0
    except (FeedFailure, HttpError, OSError) as exc:
        print(f"FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
