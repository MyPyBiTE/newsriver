#!/usr/bin/env python3
"""Publish the browser-safe MYPYBITE Signal feed from verified SEC ledgers."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from build_signal_preview import PUBLIC_FILING_LIMIT, build_payload


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.signal-feed.v1"


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def choose_path(primary: Path, fallback: Path) -> Path:
    return primary if primary.exists() else fallback


def latest_value(rows: list[dict], key: str) -> str | None:
    values = [str(row.get(key) or "") for row in rows]
    return max((value for value in values if value), default=None)


def build_feed(
    root: Path = ROOT,
    detections_path: Path | None = None,
    metadata_path: Path | None = None,
    state_path: Path | None = None,
    metadata_state_path: Path | None = None,
    now: datetime | None = None,
) -> dict:
    detections_path = detections_path or choose_path(
        root / "data" / "sec" / "detections" / "filings.jsonl",
        root / "live-latest-output" / "filings.jsonl",
    )
    metadata_path = metadata_path or choose_path(
        root / "data" / "sec" / "metadata" / "filings.jsonl",
        root / "live-latest-metadata" / "filings.jsonl",
    )
    state_path = state_path or choose_path(
        root / "data" / "sec" / "detections" / "state.json",
        detections_path.parent / "state.json",
    )
    metadata_state_path = metadata_state_path or metadata_path.parent / "state.json"

    payload = build_payload(root, detections_path, metadata_path)
    research_generated_at = payload.get("generatedAt")
    state = read_json(state_path)
    metadata_state = read_json(metadata_state_path)

    source_observed_at = max(
        (
            str(value)
            for value in (
                state.get("updatedAt"),
                metadata_state.get("updatedAt"),
                research_generated_at,
            )
            if value
        ),
        default=None,
    )

    generated_at = (
        iso_utc(now)
        if now is not None
        else source_observed_at or iso_utc(datetime.now(timezone.utc))
    )

    filings = payload.get("filings", [])

    payload["schema"] = SCHEMA
    payload["generatedAt"] = generated_at
    payload["researchGeneratedAt"] = research_generated_at
    payload["freshness"] = {
        "status": "current",
        "feedGeneratedAt": generated_at,
        "sourceObservedAt": source_observed_at,
        "latestFiledDate": latest_value(filings, "filedDate"),
        "latestAcceptedAt": latest_value(filings, "acceptedAt"),
        "recommendedRefreshSeconds": 300,
        "warningAfterMinutes": 4320,
        "source": "SEC EDGAR latest-filings detection with nightly reconciliation",
    }

    return payload


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--detections", type=Path)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--metadata-state", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    root = args.root.resolve()
    output = args.output or root / "signal-feed.json"

    feed = build_feed(
        root,
        args.detections,
        args.metadata,
        args.state,
        args.metadata_state,
    )

    if not feed.get("filings"):
        raise SystemExit("Refusing to publish an empty Signal feed.")

    atomic_write_json(output, feed)

    print(
        f"Wrote {output} ({len(feed['filings'])} filings, "
        f"{len(feed.get('issuers', []))} issuers; {feed['generatedAt']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
