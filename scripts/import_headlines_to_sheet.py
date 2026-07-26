#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Shadow-test tab. Do not point this branch at the live Incoming tab yet.
SHEET_NAME = "Incoming - Auto Test"

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

BASE_REFRESH_FIELDS = [
    "source", "source_url", "canonical_url", "source_story_id",
    "published_at", "scraped_headline", "cluster_id",
]

AUTO_REFRESH_FIELDS = [
    "candidate_synopsis", "status", "ticker_eligible", "newsriver_eligible",
    "auto_eligible", "priority", "source_trust_tier", "risk_class",
    "auto_candidate", "auto_block_reason", "validation_state", "selection_mode",
]

PROTECTED_STATUSES = {"hold", "rejected", "retracted", "scheduled"}

MAX_AGE_HOURS = 72
MASS_CASUALTY_MIN = 10

AGGREGATOR_PATTERN = re.compile(
    r"news\.google|google\s+news|news\.yahoo|apple\.news|bing\.com/news|"
    r"msn\.com/en-|flipboard\.com|drudgereport\.com|newsnow\.co\.uk|feedly\.com",
    re.IGNORECASE,
)
SECTION_ONLY_PATTERN = re.compile(
    r"/(news|world|business|markets|sports|technology|tech|culture|"
    r"entertainment|opinion|politics|video|videos|live|frontpage|homepage|"
    r"home|today)/*$",
    re.IGNORECASE,
)
TRACKING_PARAM_PATTERN = re.compile(
    r"^(utm_|fbclid$|gclid$|mc_(cid|eid)$|ref$|cmpid$|source$|scid$)",
    re.IGNORECASE,
)

RE_OBIT = re.compile(
    r"\b(dies|dead|death|passes\s+away|passed\s+away|obituary|"
    r"in\s+memoriam|dead\s+at\s+\d{2,3})\b",
    re.IGNORECASE,
)
RE_URGENT = re.compile(
    r"\b(attack|attacks|attacked|strike|air[-\s]?strike|missile|drone|"
    r"explosion|blast|shelling|shooting|casualties?|dead|killed|evacuate|"
    r"evacuation|emergency|alert|earthquake|hurricane|wildfire|flood|"
    r"tsunami|tornado|kidnapping|hostage)\b",
    re.IGNORECASE,
)
RE_GEO = re.compile(
    r"\b(russia|ukraine|poland|nato|iran|israel|gaza|taiwan|china|"
    r"north\s?korea|war|invasion|ceasefire)\b",
    re.IGNORECASE,
)
RE_BREAKING_HINT = re.compile(
    r"\b(breaking|developing|just\s+in|alert)\b",
    re.IGNORECASE,
)
RE_TIER1 = re.compile(
    r"\b(associated press|ap news|reuters|bbc|bloomberg|financial times|"
    r"washington post|wall street journal|the guardian|nyt|new york times|"
    r"cbc|ctv news|global news|the canadian press)\b",
    re.IGNORECASE,
)
RE_BUSINESS = re.compile(
    r"\b(markets?|stocks?|equit(?:y|ies)|bonds?|treasur(?:y|ies)|yields?|"
    r"inflation|cpi|ppi|retail\s+sales|gdp|bank\s+of\s+canada|"
    r"federal\s+reserve|earnings|guidance|revenue|profit|crypto|bitcoin|"
    r"btc|ethereum|eth|tsx|tsxv|cad)\b",
    re.IGNORECASE,
)
RE_SEXY = re.compile(
    r"\b(celebrity|dating|romance|relationship|affair|breakup|split|divorce|"
    r"wedding|engaged|kiss|kissing|bikini|lingerie|topless|nude|naked|"
    r"steamy|sexy|sex|hookup|onlyfans|red\s+carpet|wardrobe|kardashian|"
    r"jenner|tiktok\s+star|reality\s+star)\b",
    re.IGNORECASE,
)
RE_UNVERIFIED = re.compile(
    r"\b(unconfirmed|rumou?r|hoax|reportedly\s+dead|allegedly\s+dead|"
    r"social\s+media\s+claims?|sources?\s+claim)\b",
    re.IGNORECASE,
)
RE_DEATH_TOLL = re.compile(
    r"\b(?:at\s+least|more\s+than|over|around|nearly|about|"
    r"approx(?:\.|imately)?)?\s*(\d{1,4})\s*(?:people\s*)?"
    r"(?:dead|killed|die|dies|death|fatalities?)\b",
    re.IGNORECASE,
)
RE_CATA_EVENT = re.compile(
    r"\b(fire|hotel\s+fire|building\s+fire|explosion|blast|crash|collision|"
    r"pile-?up|derail(?:ment)?|train|bus|plane|aircraft|helicopter|ship|"
    r"boat|ferry|capsiz(?:e|ed)|stampede|collapse|landslide|avalanche|"
    r"earthquake|flood|wildfire|tornado|hurricane|shooting|gunman|"
    r"mass\s+shooting|bomb(?:ing)?|missile|air\s*strike|strike)\b",
    re.IGNORECASE,
)
HOT_TICKERS = {
    "AAPL", "IBIT", "BTC", "PLTR", "NVDA", "AMZN", "ENVX", "BYON",
    "MNKD", "META", "CIBC", "RY", "C", "GLD",
}


class ImportFailure(RuntimeError):
    pass


def utc_now_sheet() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def strip_html(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return clean_text(text)


def limit_words(value: Any, maximum: int = 25) -> str:
    words = strip_html(value).split()
    return " ".join(words[:maximum])


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


def parse_item_datetime(item: dict[str, Any]) -> datetime | None:
    raw = (
        item.get("published_utc")
        or item.get("published")
        or item.get("pubDate")
        or item.get("date")
        or item.get("timestamp")
        or item.get("time")
    )
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def normalize_url(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            return ""
        kept_query = [
            (key, val)
            for key, val in parse_qsl(parsed.query, keep_blank_values=True)
            if not TRACKING_PARAM_PATTERN.search(key)
        ]
        kept_query.sort()
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            query=urlencode(kept_query, doseq=True),
            fragment="",
        )
        return urlunparse(normalized).rstrip("/")
    except ValueError:
        return ""


def is_likely_article(item: dict[str, Any], url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url)
        path = parsed.path or "/"
        source_and_url = f"{item.get('source', '')} {url}"
        if AGGREGATOR_PATTERN.search(source_and_url):
            return False
        if path in {"", "/"}:
            return False
        if SECTION_ONLY_PATTERN.search(path):
            return False
        parts = [part for part in path.split("/") if part]
        if len(parts) <= 1 and not re.search(r"\d{4}", path):
            return False
        if not re.search(r"[a-z]{3,}", path, re.IGNORECASE):
            return False
        return True
    except ValueError:
        return False


def stable_story_id(item: dict[str, Any]) -> str:
    existing = clean_text(item.get("canonical_id"))
    if existing:
        return existing

    url = normalize_url(item.get("canonical_url") or item.get("url"))
    if not url:
        raise ImportFailure("A headline item has no usable canonical_id or URL.")

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return f"u:{digest}"


def catastrophic_death_toll(title: str) -> int:
    if not RE_CATA_EVENT.search(title):
        return 0
    match = RE_DEATH_TOLL.search(title)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return 0


def has_hot_ticker(title: str) -> bool:
    return any(symbol in HOT_TICKERS for symbol in re.findall(r"\$([A-Z]{1,5})\b", title))


def candidate_synopsis(item: dict[str, Any], headline: str) -> str:
    raw = (
        item.get("summary")
        or item.get("description")
        or item.get("excerpt")
        or item.get("synopsis")
        or item.get("summaryShort")
        or headline
    )
    return limit_words(raw, 25)


def classify_item(item: dict[str, Any], now_utc: datetime) -> dict[str, Any]:
    headline = clean_text(item.get("title") or item.get("headline"))
    source = clean_text(item.get("source"))
    url = normalize_url(item.get("canonical_url") or item.get("url"))
    published = parse_item_datetime(item)

    reasons: list[str] = []
    if not headline or len(headline) < 8:
        reasons.append("Headline is missing or too short")
    if not source:
        reasons.append("Source is missing")
    if not url:
        reasons.append("URL is invalid")
    elif not is_likely_article(item, url):
        reasons.append("URL does not appear to be a direct article")
    if published is None:
        reasons.append("Publication time is missing or invalid")
    else:
        age_hours = (now_utc - published).total_seconds() / 3600
        if age_hours > MAX_AGE_HOURS:
            reasons.append(f"Story is older than {MAX_AGE_HOURS} hours")
        elif age_hours < -1:
            reasons.append("Publication time is in the future")

    unverified_sensitive = bool(RE_UNVERIFIED.search(headline)) and bool(
        RE_OBIT.search(headline) or RE_URGENT.search(headline)
    )
    if unverified_sensitive:
        reasons.append("Sensitive claim appears unverified")

    toll = catastrophic_death_toll(headline)
    is_obit = bool(RE_OBIT.search(headline))
    is_urgent = bool(RE_URGENT.search(headline))
    is_geo = bool(RE_GEO.search(headline))
    is_breaking_hint = bool(RE_BREAKING_HINT.search(headline))
    is_breaking = is_breaking_hint or is_obit or toll >= MASS_CASUALTY_MIN
    is_sexy = bool(RE_SEXY.search(headline))
    is_tier1 = bool(RE_TIER1.search(source))
    is_business = bool(RE_BUSINESS.search(headline))

    priority = 40
    if published is not None:
        age_hours = max(0.0, (now_utc - published).total_seconds() / 3600)
        if age_hours <= 4:
            priority += 15
        elif age_hours <= 12:
            priority += 10
        elif age_hours <= 24:
            priority += 5
    if is_breaking_hint:
        priority += 18
    if is_obit:
        priority += 20
    if toll >= MASS_CASUALTY_MIN:
        priority += 22
    if is_urgent:
        priority += 15
    if is_geo:
        priority += 10
    if is_tier1:
        priority += 5
    if is_business:
        priority += 6
    if is_sexy:
        priority += 10
    if has_hot_ticker(headline):
        priority += 6
    priority = max(0, min(priority, 100))

    if is_obit or is_urgent or toll >= MASS_CASUALTY_MIN:
        risk_class = "SENSITIVE"
    elif is_sexy or is_geo:
        risk_class = "MEDIUM_RISK"
    else:
        risk_class = "LOW_RISK"

    if is_tier1:
        source_trust_tier = "TIER_1"
    elif source:
        source_trust_tier = "TIER_2"
    else:
        source_trust_tier = "TIER_3"

    auto_candidate = not reasons
    ticker_eligible = auto_candidate and (is_breaking or is_urgent or is_geo)
    newsriver_eligible = auto_candidate

    return {
        "status": "Approved" if auto_candidate else "Review",
        "ticker_eligible": ticker_eligible,
        "newsriver_eligible": newsriver_eligible,
        "auto_eligible": auto_candidate,
        "priority": priority,
        "source_trust_tier": source_trust_tier,
        "risk_class": risk_class,
        "auto_candidate": auto_candidate,
        "auto_block_reason": "; ".join(reasons),
        "validation_state": "VALID" if auto_candidate else "REVIEW",
        "selection_mode": "AUTO_FALLBACK" if auto_candidate else "MANUAL",
    }


def load_input(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ImportFailure(f"Input file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ImportFailure(f"Malformed JSON in {path}: {exc}") from exc

    if isinstance(payload, list):
        items = payload
    else:
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


def make_new_row(item: dict[str, Any], now: str, now_utc: datetime) -> list[Any]:
    story_id = stable_story_id(item)
    source_url = normalize_url(item.get("url") or item.get("canonical_url"))
    canonical_url = normalize_url(item.get("canonical_url") or item.get("url"))
    headline = clean_text(item.get("title") or item.get("headline"))
    decision = classify_item(item, now_utc)

    values: dict[str, Any] = {
        "story_id": story_id,
        "source": clean_text(item.get("source")),
        "source_url": source_url or canonical_url,
        "canonical_url": canonical_url or source_url,
        "source_story_id": story_id,
        "published_at": to_sheet_datetime(
            item.get("published_utc")
            or item.get("published")
            or item.get("pubDate")
            or item.get("date")
            or item.get("timestamp")
            or item.get("time")
        ),
        "scraped_headline": headline,
        "editor_headline": "",
        "candidate_synopsis": candidate_synopsis(item, headline),
        "editor_synopsis": "",
        "status": decision["status"],
        "cluster_id": clean_text(item.get("cluster_id")),
        "duplicate_of": "",
        "ticker_eligible": decision["ticker_eligible"],
        "newsriver_eligible": decision["newsriver_eligible"],
        "auto_eligible": decision["auto_eligible"],
        "go_live_at": "",
        "expires_at": "",
        "keep_until": "",
        "priority": decision["priority"],
        "pin_until": "",
        "access_type": clean_text(item.get("access_type")).upper() or "UNKNOWN",
        "free_alternative_url": "",
        "access_checked_at": "",
        "rights_note": "",
        "editor_notes": "",
        "source_trust_tier": decision["source_trust_tier"],
        "risk_class": decision["risk_class"],
        "auto_candidate": decision["auto_candidate"],
        "auto_block_reason": decision["auto_block_reason"],
        "validation_state": decision["validation_state"],
        "selection_mode": decision["selection_mode"],
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
    *,
    reclassify_existing: bool,
) -> tuple[list[Any], list[str]]:
    merged = list(current)
    changed_fields: list[str] = []
    index = {name: position for position, name in enumerate(HEADERS)}

    refresh_fields = list(BASE_REFRESH_FIELDS)

    current_status = clean_text(current[index["status"]]).lower()
    has_editor_override = bool(
        clean_text(current[index["editor_headline"]])
        or clean_text(current[index["editor_synopsis"]])
        or clean_text(current[index["editor_notes"]])
    )
    may_reclassify = (
        reclassify_existing
        and current_status not in PROTECTED_STATUSES
        and not has_editor_override
    )
    if may_reclassify:
        refresh_fields.extend(AUTO_REFRESH_FIELDS)

    for field in refresh_fields:
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
    reclassify_existing: bool,
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
        range=f"'{SHEET_NAME}'!A1:AJ",
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
            f"{SHEET_NAME} header mismatch:\n" + "\n".join(mismatches)
        )

    existing_rows = [pad_row(row) for row in values[1:]]
    row_by_story_id: dict[str, tuple[int, list[Any]]] = {}

    for sheet_row_number, row in enumerate(existing_rows, start=2):
        story_id = clean_text(row[0])
        if story_id:
            row_by_story_id[story_id] = (sheet_row_number, row)

    now_utc = datetime.now(timezone.utc)
    now = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    additions: list[list[Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged = 0
    skipped_duplicate_input = 0
    skipped_invalid_input = 0
    seen_input_ids: set[str] = set()

    selected = items[:limit] if limit > 0 else items

    for item in selected:
        try:
            candidate = make_new_row(item, now, now_utc)
        except ImportFailure as exc:
            skipped_invalid_input += 1
            print(f"SKIP invalid_input reason={exc}", file=sys.stderr)
            continue

        story_id = clean_text(candidate[0])

        if story_id in seen_input_ids:
            skipped_duplicate_input += 1
            continue
        seen_input_ids.add(story_id)

        existing = row_by_story_id.get(story_id)
        if existing is None:
            additions.append(candidate)
            continue

        row_number, current = existing
        merged, changed_fields = merge_existing_row(
            current,
            candidate,
            now,
            reclassify_existing=reclassify_existing,
        )

        if changed_fields:
            print(
                "REFRESH "
                f"story_id={story_id} "
                f"fields={','.join(changed_fields)}"
            )
            updates.append(
                {
                    "range": f"'{SHEET_NAME}'!A{row_number}:AJ{row_number}",
                    "values": [merged],
                }
            )
        else:
            unchanged += 1

    print(
        "PLAN "
        f"sheet={SHEET_NAME!r} "
        f"input={len(selected)} "
        f"add={len(additions)} "
        f"update={len(updates)} "
        f"unchanged={unchanged} "
        f"input_duplicates={skipped_duplicate_input} "
        f"invalid_input={skipped_invalid_input} "
        f"reclassify_existing={reclassify_existing}"
    )

    if dry_run:
        print("DRY RUN: no Google Sheet values were changed.")
        return

    writes = list(updates)

    if additions:
        # Preformatted checkbox/dropdown rows can appear occupied to append().
        # Write into the first rows whose story_id cell is blank instead.
        blank_story_rows = [
            row_number
            for row_number, row in enumerate(existing_rows, start=2)
            if not clean_text(row[0])
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
        f"unchanged={unchanged} "
        f"invalid_input={skipped_invalid_input}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import duplicate-safe V1 headline records into the MYPYBITE V2 "
            "shadow-test editorial sheet and classify new stories automatically."
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
    parser.add_argument(
        "--reclassify-existing",
        action="store_true",
        help=(
            "Recalculate automation-owned fields for existing Review/Approved "
            "rows that have no editor overrides. Protected statuses remain untouched."
        ),
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
            reclassify_existing=args.reclassify_existing,
        )
    except (ImportFailure, HttpError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
