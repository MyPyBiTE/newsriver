#!/usr/bin/env python3

from __future__ import annotations

import html
import json
import math
import os
import urllib.request
import xml.etree.ElementTree as ET

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Substack Sources"
HEADERS = [
    "publication",
    "homepage_url",
    "feed_url",
    "enabled",
    "priority",
    "last_checked",
    "status",
]
OUTPUT_PATH = Path("substack.json")
OUTPUT_COUNT = 7
MAX_POSTS_PER_SOURCE = 25
MAX_WORKERS = 8


class FeedFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class Source:
    row_number: int
    publication: str
    homepage_url: str
    feed_url: str
    priority: int


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def sheet_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "enabled"}


def parse_priority(value: Any, fallback: int) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return fallback


def parse_date(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None

    try:
        result = parsedate_to_datetime(value)
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        pass

    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except ValueError:
        return None


def element_text(element: ET.Element, names: list[str]) -> str:
    for name in names:
        found = element.find(name)
        if found is not None and found.text:
            return html.unescape(found.text.strip())
    return ""


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


def load_sources(service, spreadsheet_id: str) -> list[Source]:
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{SHEET_NAME}'!A1:G",
    ).execute()
    values = result.get("values", [])
    if not values:
        raise FeedFailure(f"{SHEET_NAME} is empty.")

    actual_headers = list(values[0][: len(HEADERS)])
    if actual_headers != HEADERS:
        raise FeedFailure(
            f"{SHEET_NAME} headers do not match. Expected {HEADERS!r}; "
            f"found {actual_headers!r}."
        )

    sources: list[Source] = []
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(raw_row))
        publication = str(row[0]).strip()
        homepage_url = str(row[1]).strip()
        feed_url = str(row[2]).strip()
        if not publication and not feed_url:
            continue
        if not truthy(row[3]):
            continue
        if not publication or not canonical_url(homepage_url) or not canonical_url(feed_url):
            raise FeedFailure(f"Invalid enabled source on sheet row {row_number}.")
        sources.append(
            Source(
                row_number=row_number,
                publication=publication,
                homepage_url=homepage_url,
                feed_url=feed_url,
                priority=parse_priority(row[4], row_number),
            )
        )

    if not sources:
        raise FeedFailure("No enabled Substack sources were found.")
    return sources


def parse_rss(source: Source, root: ET.Element) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = element_text(item, ["title"])
        link = element_text(item, ["link", "guid"])
        published = parse_date(
            element_text(
                item,
                ["pubDate", "{http://purl.org/dc/elements/1.1/}date"],
            )
        )
        url = canonical_url(link)
        if title and url and published:
            posts.append(
                {
                    "source": source.publication,
                    "headline": title,
                    "url": url,
                    "canonicalUrl": url,
                    "publishedAt": iso_utc(published),
                    "category": "Editorial",
                    "_timestamp": published.timestamp(),
                    "_priority": source.priority,
                }
            )
    return posts


def parse_atom(source: Source, root: ET.Element) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    namespace = "{http://www.w3.org/2005/Atom}"
    for entry in root.findall(f".//{namespace}entry"):
        title = element_text(entry, [f"{namespace}title"])
        link = ""
        for link_element in entry.findall(f"{namespace}link"):
            href = link_element.attrib.get("href", "")
            rel = link_element.attrib.get("rel", "alternate")
            if href and rel == "alternate":
                link = href
                break
        published = parse_date(
            element_text(entry, [f"{namespace}published", f"{namespace}updated"])
        )
        url = canonical_url(link)
        if title and url and published:
            posts.append(
                {
                    "source": source.publication,
                    "headline": title,
                    "url": url,
                    "canonicalUrl": url,
                    "publishedAt": iso_utc(published),
                    "category": "Editorial",
                    "_timestamp": published.timestamp(),
                    "_priority": source.priority,
                }
            )
    return posts


def fetch_source(source: Source) -> list[dict[str, Any]]:
    request = urllib.request.Request(
        source.feed_url,
        headers={
            "User-Agent": "MYPYBITE-Substack-Reader/2.0 (+https://www.mypybite.com)",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        },
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        raw = response.read()
    root = ET.fromstring(raw)
    posts = parse_rss(source, root) or parse_atom(source, root)
    posts.sort(key=lambda item: item["_timestamp"], reverse=True)
    return posts[:MAX_POSTS_PER_SOURCE]


def update_source_statuses(
    service,
    spreadsheet_id: str,
    checked_at: datetime,
    statuses: dict[int, str],
) -> None:
    data = [
        {
            "range": f"'{SHEET_NAME}'!F{row_number}:G{row_number}",
            "values": [[sheet_timestamp(checked_at), status]],
        }
        for row_number, status in sorted(statuses.items())
    ]
    if data:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()


def select_newest(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    posts.sort(
        key=lambda item: (
            -float(item["_timestamp"]),
            int(item["_priority"]),
            item["canonicalUrl"],
        )
    )
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in posts:
        key = item["canonicalUrl"]
        if key in seen:
            continue
        seen.add(key)
        clean_item = {key: value for key, value in item.items() if not key.startswith("_")}
        selected.append(clean_item)
        if len(selected) == OUTPUT_COUNT:
            break
    return selected


def write_payload(
    selected: list[dict[str, Any]],
    checked_at: datetime,
    total_sources: int,
    successful_sources: int,
) -> None:
    if len(selected) != OUTPUT_COUNT:
        raise FeedFailure(
            f"Refusing to replace the last valid feed: expected {OUTPUT_COUNT} posts, "
            f"found {len(selected)}."
        )

    payload = {
        "schema": "mypybite.substack-feed.v1",
        "generatedAt": iso_utc(checked_at),
        "sourceCount": total_sources,
        "successfulSources": successful_sources,
        "latest": selected[0],
        "items": selected,
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

    checked_at = utc_now()
    try:
        service = sheets_service()
        sources = load_sources(service, spreadsheet_id)

        posts: list[dict[str, Any]] = []
        statuses: dict[int, str] = {}
        successful_sources = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_source, source): source for source in sources}
            for future in as_completed(futures):
                source = futures[future]
                try:
                    source_posts = future.result()
                    if not source_posts:
                        raise FeedFailure("feed contained no dated posts")
                    posts.extend(source_posts)
                    successful_sources += 1
                    statuses[source.row_number] = f"ok ({len(source_posts)} posts)"
                    print(f"OK: {source.publication}: {len(source_posts)} posts")
                except Exception as exc:
                    message = " ".join(str(exc).split())[:160]
                    statuses[source.row_number] = f"error: {message}"
                    print(f"SKIP: {source.publication}: {message}")

        update_source_statuses(service, spreadsheet_id, checked_at, statuses)

        minimum_successes = math.ceil(len(sources) * 0.5)
        if successful_sources < minimum_successes:
            raise FeedFailure(
                "Refusing to replace the last valid feed: only "
                f"{successful_sources}/{len(sources)} sources succeeded; "
                f"minimum is {minimum_successes}."
            )

        selected = select_newest(posts)
        write_payload(selected, checked_at, len(sources), successful_sources)
        print(
            f"DONE: published {len(selected)} newest posts from "
            f"{successful_sources}/{len(sources)} sources."
        )
        return 0
    except (FeedFailure, HttpError, OSError, ET.ParseError) as exc:
        print(f"FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
