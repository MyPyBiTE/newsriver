from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]

FEEDS = {
    "breaking": ROOT / "data" / "v2" / "breaking.json",
    "newsriver": ROOT / "data" / "v2" / "newsriver.json",
}

TOP_LEVEL_FIELDS = {
    "schema_version",
    "feed_id",
    "generated_at",
    "last_human_publish_at",
    "selection_mode",
    "stale_after_minutes",
    "count",
    "items",
}

STORY_FIELDS = {
    "story_id",
    "headline",
    "synopsis",
    "source",
    "url",
    "published_at",
    "category",
    "access_type",
    "selection_mode",
}

SELECTION_MODES = {
    "manual",
    "scheduled",
    "auto_fallback",
}

ACCESS_TYPES = {
    "FREE",
    "METERED",
    "PAYWALL",
    "UNKNOWN",
}

FORBIDDEN_PRIVATE_FIELDS = {
    "editor_notes",
    "rights_note",
    "risk_class",
    "source_trust_tier",
    "auto_candidate",
    "auto_block_reason",
    "validation_state",
    "duplicate_of",
    "cluster_id",
    "source_story_id",
    "access_checked_at",
    "imported_at",
    "last_updated_at",
    "correction_of",
    "retraction_note",
}


class ValidationError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def parse_iso8601(value: Any, label: str) -> None:
    require(
        isinstance(value, str) and value.strip(),
        f"{label} must be a non-empty string",
    )

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(
            f"{label} must be ISO-8601: {value}"
        ) from exc


def validate_url(value: Any, label: str) -> None:
    require(
        isinstance(value, str) and value.strip(),
        f"{label} must be a non-empty string",
    )

    parsed = urlparse(value)

    require(
        parsed.scheme in {"http", "https"} and bool(parsed.netloc),
        f"{label} must be an http(s) URL",
    )


def word_count(value: str) -> int:
    return len(value.split())


def scan_for_private_fields(
    value: Any,
    path: str = "root",
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            require(
                key not in FORBIDDEN_PRIVATE_FIELDS,
                f"Private field leaked at {path}.{key}",
            )
            scan_for_private_fields(child, f"{path}.{key}")

    elif isinstance(value, list):
        for index, child in enumerate(value):
            scan_for_private_fields(
                child,
                f"{path}[{index}]",
            )


def validate_story(
    story: Any,
    label: str,
) -> None:
    require(
        isinstance(story, dict),
        f"{label} must be an object",
    )

    keys = set(story)

    require(
        keys == STORY_FIELDS,
        (
            f"{label} fields differ from the public contract: "
            f"{sorted(keys ^ STORY_FIELDS)}"
        ),
    )

    for field in (
        "story_id",
        "headline",
        "synopsis",
        "source",
        "category",
    ):
        require(
            isinstance(story[field], str)
            and story[field].strip(),
            f"{label}.{field} is required",
        )

    require(
        word_count(story["synopsis"]) <= 25,
        f"{label}.synopsis exceeds 25 words",
    )

    validate_url(
        story["url"],
        f"{label}.url",
    )

    parse_iso8601(
        story["published_at"],
        f"{label}.published_at",
    )

    require(
        story["access_type"] in ACCESS_TYPES,
        f"{label}.access_type is invalid",
    )

    require(
        story["selection_mode"] in SELECTION_MODES,
        f"{label}.selection_mode is invalid",
    )


def validate_feed(
    kind: str,
    path: Path,
) -> None:
    require(
        path.exists(),
        f"Missing file: {path}",
    )

    try:
        feed = json.loads(
            path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"{path}: malformed JSON: {exc}"
        ) from exc

    require(
        isinstance(feed, dict),
        f"{path}: top level must be an object",
    )

    scan_for_private_fields(feed)

    keys = set(feed)

    require(
        keys == TOP_LEVEL_FIELDS,
        (
            f"{path}: top-level fields differ from contract: "
            f"{sorted(keys ^ TOP_LEVEL_FIELDS)}"
        ),
    )

    require(
        feed["schema_version"] == 1,
        f"{path}: schema_version must be 1",
    )

    require(
        isinstance(feed["feed_id"], str)
        and feed["feed_id"].strip(),
        f"{path}: feed_id is required",
    )

    parse_iso8601(
        feed["generated_at"],
        f"{path}.generated_at",
    )

    parse_iso8601(
        feed["last_human_publish_at"],
        f"{path}.last_human_publish_at",
    )

    require(
        feed["selection_mode"] in SELECTION_MODES,
        f"{path}: selection_mode is invalid",
    )

    require(
        isinstance(feed["stale_after_minutes"], int)
        and feed["stale_after_minutes"] > 0,
        f"{path}: stale_after_minutes must be positive",
    )

    require(
        isinstance(feed["count"], int)
        and feed["count"] >= 0,
        f"{path}: count must be non-negative",
    )

    require(
        isinstance(feed["items"], list),
        f"{path}: items must be a list",
    )

    require(
        feed["count"] == len(feed["items"]),
        f"{path}: count does not match items length",
    )

    if kind == "breaking":
        require(
            len(feed["items"]) <= 6,
            f"{path}: breaking feed may contain at most 6 items",
        )

    seen_ids: set[str] = set()

    for index, story in enumerate(feed["items"]):
        label = f"{path}.items[{index}]"

        validate_story(story, label)

        require(
            story["story_id"] not in seen_ids,
            (
                f"{path}: duplicate story_id "
                f"{story['story_id']}"
            ),
        )

        seen_ids.add(story["story_id"])

    print(
        f"PASS {kind}: "
        f"{path.relative_to(ROOT)} "
        f"({len(feed['items'])} items)"
    )


def main() -> int:
    try:
        for kind, path in FEEDS.items():
            validate_feed(kind, path)

    except ValidationError as exc:
        print(
            f"FAIL: {exc}",
            file=sys.stderr,
        )
        return 1

    print(
        "PASS: all V2 public JSON files satisfy "
        "the visitor-safe contract"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
