#!/usr/bin/env python3
"""Build a company-agnostic queue for Signal event analysis.

Step 1A deliberately stops before interpretation. It joins immutable SEC
detections to authoritative filing metadata and the watched-issuer roster,
then emits explicit, non-public candidates for the later W5H engine.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
DETECTION_SCHEMA = "mypybite.sec-latest.v1"
METADATA_SCHEMA = "mypybite.sec-filing-metadata.v1"
ROSTER_SCHEMA = "mypybite.management-roster.v1"
CANDIDATE_SCHEMA = "mypybite.signal-analysis-candidate.v1"
QUEUE_SCHEMA = "mypybite.signal-analysis-queue.v1"
ACCESSION_PATTERN = re.compile(r"^\d{10}-\d{2}-\d{6}$")
CIK_PATTERN = re.compile(r"^\d{10}$")


class AnalysisQueueFailure(ValueError):
    """Raised when authoritative inputs cannot produce a safe queue."""


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AnalysisQueueFailure(f"Invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AnalysisQueueFailure(f"Expected a JSON object: {path}")
    return value


def load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AnalysisQueueFailure(f"Unable to read JSONL: {path}") from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise AnalysisQueueFailure(f"Invalid JSONL at {path}:{line_number}") from exc
        if not isinstance(record, dict):
            raise AnalysisQueueFailure(f"Expected object at {path}:{line_number}")
        records.append(record)
    return records


def load_roster(path: Path) -> dict[str, dict]:
    roster = load_json(path)
    if roster.get("schema") != ROSTER_SCHEMA:
        raise AnalysisQueueFailure("Unsupported management roster schema.")
    issuers = roster.get("issuers")
    if not isinstance(issuers, list):
        raise AnalysisQueueFailure("Management roster lacks issuers.")
    indexed: dict[str, dict] = {}
    for issuer in issuers:
        cik = str(issuer.get("cik") or "")
        if not CIK_PATTERN.fullmatch(cik):
            raise AnalysisQueueFailure(f"Invalid roster CIK: {cik!r}")
        if cik in indexed:
            raise AnalysisQueueFailure(f"Duplicate roster CIK: {cik}")
        indexed[cik] = issuer
    return indexed


def index_metadata(records: Iterable[dict]) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for record in records:
        if record.get("schema") != METADATA_SCHEMA:
            raise AnalysisQueueFailure("Unsupported SEC metadata schema.")
        accession = str(record.get("accessionNumber") or "")
        if not ACCESSION_PATTERN.fullmatch(accession):
            raise AnalysisQueueFailure(f"Invalid metadata accession: {accession!r}")
        existing = indexed.get(accession)
        if existing is not None and existing != record:
            raise AnalysisQueueFailure(f"Conflicting metadata for accession {accession}")
        indexed[accession] = record
    return indexed


def validate_detection(record: dict) -> None:
    if record.get("schema") != DETECTION_SCHEMA:
        raise AnalysisQueueFailure("Unsupported SEC detection schema.")
    accession = str(record.get("accessionNumber") or "")
    cik = str(record.get("cik") or "")
    if not ACCESSION_PATTERN.fullmatch(accession):
        raise AnalysisQueueFailure(f"Invalid detection accession: {accession!r}")
    if not CIK_PATTERN.fullmatch(cik):
        raise AnalysisQueueFailure(f"Invalid detection CIK: {cik!r}")
    for field in ("companyName", "form", "filedDate", "filingIndexUrl", "filingTextUrl"):
        if not str(record.get(field) or "").strip():
            raise AnalysisQueueFailure(
                f"Detection {accession}:{cik} lacks required field {field}."
            )


def build_candidate(
    detection: dict, metadata: dict | None, roster_issuer: dict, queued_at: str
) -> dict:
    validate_detection(detection)
    accession = str(detection["accessionNumber"])
    cik = str(detection["cik"])
    accepted_at = metadata.get("acceptedAt") if metadata else None
    items = metadata.get("items", []) if metadata else []
    if not isinstance(items, list):
        raise AnalysisQueueFailure(f"Metadata items must be a list: {accession}")
    return {
        "schema": CANDIDATE_SCHEMA,
        "candidateId": f"sec:{accession}:{cik}",
        "queuedAt": queued_at,
        "issuer": {
            "cik": cik,
            "legalName": str(roster_issuer["legalName"]),
            "ticker": str(roster_issuer["ticker"]),
        },
        "filing": {
            "accessionNumber": accession,
            "form": str(detection["form"]),
            "isAmendment": bool(detection.get("isAmendment")),
            "filedDate": str(detection["filedDate"]),
            "acceptedAt": accepted_at,
            "items": items,
            "indexUrl": str(detection["filingIndexUrl"]),
            "textUrl": str(detection["filingTextUrl"]),
        },
        "coverage": {
            "cohort": roster_issuer.get("cohort"),
            "relevance": list(roster_issuer.get("relevance") or []),
            "coverage": roster_issuer.get("coverage"),
        },
        "analysis": {
            "status": "queued",
            "engine": "generic-signal-analyzer",
            "requiredDocuments": ["filing-index", "primary-document", "exhibits"],
            "publicReleaseAllowed": False,
        },
        "quality": {
            "issuerIdentityVerified": True,
            "primarySourceReady": True,
            "acceptanceTimestampVerified": bool(accepted_at),
            "watchedIssuer": True,
        },
    }


def build_queue(
    detections: Iterable[dict],
    metadata_records: Iterable[dict],
    roster: dict[str, dict],
    now: datetime | None = None,
) -> dict:
    queued_at = iso_utc(now or datetime.now(timezone.utc))
    metadata = index_metadata(metadata_records)
    candidates: dict[str, dict] = {}
    detection_count = 0
    skipped_unwatched = 0
    for detection in detections:
        detection_count += 1
        validate_detection(detection)
        cik = str(detection["cik"])
        issuer = roster.get(cik)
        if issuer is None:
            skipped_unwatched += 1
            continue
        candidate = build_candidate(
            detection,
            metadata.get(str(detection["accessionNumber"])),
            issuer,
            queued_at,
        )
        candidate_id = candidate["candidateId"]
        existing = candidates.get(candidate_id)
        if existing is not None and existing != candidate:
            raise AnalysisQueueFailure(f"Conflicting detection for {candidate_id}")
        candidates[candidate_id] = candidate

    ordered = sorted(
        candidates.values(),
        key=lambda item: (
            str(item["filing"]["acceptedAt"] or item["filing"]["filedDate"]),
            item["candidateId"],
        ),
        reverse=True,
    )
    return {
        "schema": QUEUE_SCHEMA,
        "generatedAt": queued_at,
        "mode": "analysis-queue",
        "publicReleaseAllowed": False,
        "stats": {
            "detectionCount": detection_count,
            "candidateCount": len(ordered),
            "skippedUnwatchedCount": skipped_unwatched,
            "metadataVerifiedCount": sum(
                item["quality"]["acceptanceTimestampVerified"] for item in ordered
            ),
        },
        "candidates": ordered,
    }


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--detections",
        type=Path,
        default=ROOT / "data" / "sec" / "detections" / "filings.jsonl",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=ROOT / "data" / "sec" / "metadata" / "filings.jsonl",
    )
    parser.add_argument(
        "--roster", type=Path, default=ROOT / "config" / "management-roster.json"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "private" / "signal-analysis-queue.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    queue = build_queue(
        load_jsonl(args.detections),
        load_jsonl(args.metadata),
        load_roster(args.roster),
    )
    atomic_write(args.output, json.dumps(queue, ensure_ascii=False, indent=2) + "\n")
    print(
        f"Signal analysis queue written: {args.output} "
        f"({queue['stats']['candidateCount']} watched candidates; publication disabled)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
