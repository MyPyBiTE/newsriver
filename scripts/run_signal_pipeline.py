#!/usr/bin/env python3
"""Run the complete automated MYPYBITE Signal SEC analysis pipeline.

Each unseen watched accession moves through queueing, official-document
collection, evidence extraction, W5H assembly, materiality analysis and the
automated publication gate. Raw documents remain temporary. Approved events
are merged into the public feed; uncertain events are recorded as quarantined.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from analyze_signal_events import build_queue, load_jsonl, load_roster
from analyze_signal_materiality import MaterialityAnalysisFailure, analyze_materiality
from assemble_signal_w5h import W5HAssemblyFailure, assemble_w5h
from collect_signal_documents import (
    DocumentCollectionFailure,
    SecClient,
    collect_candidate,
)
from extract_signal_evidence import EvidenceExtractionFailure, extract_bundle
from fetch_sec_intake import REQUEST_INTERVAL_SECONDS
from gate_signal_publication import PublicationGateFailure, gate_analysis
from validate_signal_event import validate_event


ROOT = Path(__file__).resolve().parents[1]
STATE_SCHEMA = "mypybite.signal-pipeline-state.v1"
PUBLIC_SCHEMA = "mypybite.signal-events-public.v1"
PIPELINE_VERSION = 1


class SignalPipelineFailure(ValueError):
    """Raised when pipeline state or an existing public feed is unsafe."""


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def blank_state() -> dict:
    return {
        "schema": STATE_SCHEMA,
        "pipelineVersion": PIPELINE_VERSION,
        "updatedAt": None,
        "candidates": {},
    }


def load_state(path: Path) -> dict:
    if not path.exists():
        return blank_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SignalPipelineFailure(f"Invalid Signal pipeline state: {path}") from exc
    if (
        state.get("schema") != STATE_SCHEMA
        or state.get("pipelineVersion") != PIPELINE_VERSION
        or not isinstance(
        state.get("candidates"), dict
        )
    ):
        raise SignalPipelineFailure("Unsupported Signal pipeline state schema.")
    return state


def blank_public_feed(now: datetime) -> dict:
    return {
        "schema": PUBLIC_SCHEMA,
        "generatedAt": iso_utc(now),
        "releaseStatus": "ready-for-publication",
        "publicReleaseAllowed": True,
        "editorialPolicy": {
            "factsAndInferenceSeparated": True,
            "w5hComplete": True,
            "primaryEvidenceRequired": True,
            "automatedEvidenceGate": True,
            "humanReviewIsException": True,
        },
        "stats": {},
        "events": [],
    }


def load_public_feed(path: Path, now: datetime) -> dict:
    if not path.exists():
        return blank_public_feed(now)
    try:
        feed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SignalPipelineFailure(f"Invalid public Signal feed: {path}") from exc
    if feed.get("schema") != PUBLIC_SCHEMA or not isinstance(feed.get("events"), list):
        raise SignalPipelineFailure("Unsupported public Signal feed schema.")
    for event in feed["events"]:
        validate_event(event)
    return feed


def event_date(event: dict) -> str:
    for status in ("occurred", "effective", "filed", "accepted", "deadline"):
        for item in event["w5h"]["when"]:
            if item["status"] == status:
                return str(item["value"])
    return str(event["generatedAt"])


def candidate_ids_in_feed(feed: dict) -> set[str]:
    identifiers: set[str] = set()
    for event in feed["events"]:
        accession = next(
            (
                source.get("accessionNumber")
                for source in event.get("sources", [])
                if source.get("accessionNumber")
            ),
            None,
        )
        cik = event.get("issuer", {}).get("cik")
        if accession and cik:
            identifiers.add(f"sec:{accession}:{cik}")
    return identifiers


def merge_public_events(feed: dict, new_events: list[dict], now: datetime) -> dict:
    merged = {event["eventId"]: event for event in feed["events"]}
    for event in new_events:
        validate_event(event)
        existing = merged.get(event["eventId"])
        if existing is not None and existing != event:
            raise SignalPipelineFailure(f"Conflicting public eventId: {event['eventId']}")
        merged[event["eventId"]] = event
    events = sorted(
        merged.values(), key=lambda item: (event_date(item), item["eventId"]), reverse=True
    )
    materiality = {name: 0 for name in ("critical", "important", "notable", "routine")}
    for event in events:
        materiality[event["classification"]["materiality"]] += 1
    return {
        "schema": PUBLIC_SCHEMA,
        "generatedAt": iso_utc(now),
        "releaseStatus": "ready-for-publication",
        "publicReleaseAllowed": True,
        "editorialPolicy": {
            "factsAndInferenceSeparated": True,
            "w5hComplete": True,
            "primaryEvidenceRequired": True,
            "automatedEvidenceGate": True,
            "humanReviewIsException": True,
        },
        "stats": {
            "eventCount": len(events),
            "issuerCount": len({event["issuer"]["cik"] for event in events}),
            "criticalCount": materiality["critical"],
            "importantCount": materiality["important"],
            "notableCount": materiality["notable"],
            "routineCount": materiality["routine"],
            "automatedCount": sum(
                event["quality"].get("reviewMethod") == "automated" for event in events
            ),
        },
        "events": events,
    }


def quarantine_record(candidate: dict, stage: str, reason: str, completed_at: str) -> dict:
    return {
        "pipelineVersion": PIPELINE_VERSION,
        "status": "quarantined",
        "accessionNumber": candidate["filing"]["accessionNumber"],
        "cik": candidate["issuer"]["cik"],
        "completedAt": completed_at,
        "stage": stage,
        "reasons": [reason],
        "eventId": None,
    }


def run_pipeline(
    detections: list[dict],
    metadata: list[dict],
    roster: dict[str, dict],
    state: dict,
    public_feed: dict,
    client,
    work_dir: Path,
    now: datetime | None = None,
) -> tuple[dict, dict, dict]:
    current = now or datetime.now(timezone.utc)
    timestamp = iso_utc(current)
    queue = build_queue(detections, metadata, roster, current)
    already_public = candidate_ids_in_feed(public_feed)
    pending = [
        candidate
        for candidate in queue["candidates"]
        if candidate["candidateId"] not in already_public
        and (
            candidate["candidateId"] not in state["candidates"]
            or state["candidates"][candidate["candidateId"]].get("pipelineVersion", 0)
            < PIPELINE_VERSION
        )
    ]
    next_state = json.loads(json.dumps(state))
    approved_events: list[dict] = []
    run_records: list[dict] = []

    for candidate in pending:
        candidate_id = candidate["candidateId"]
        try:
            collect_candidate(candidate, work_dir, client, current)
        except DocumentCollectionFailure as exc:
            record = quarantine_record(
                candidate, "document-collection", str(exc), timestamp
            )
        else:
            bundle_path = work_dir / (
                f"{candidate['filing']['accessionNumber']}.{candidate['issuer']['cik']}"
            ) / "manifest.json"
            try:
                evidence = extract_bundle(bundle_path, now=current)
                brief = assemble_w5h(evidence, now=current)
                analysis = analyze_materiality(brief, now=current)
                decision = gate_analysis(analysis, now=current)
            except (
                EvidenceExtractionFailure,
                W5HAssemblyFailure,
                MaterialityAnalysisFailure,
                PublicationGateFailure,
            ) as exc:
                record = quarantine_record(candidate, "analysis", str(exc), timestamp)
            else:
                if decision["decision"] == "auto-approved":
                    event = decision["event"]
                    approved_events.append(event)
                    record = {
                        "pipelineVersion": PIPELINE_VERSION,
                        "status": "auto-approved",
                        "accessionNumber": candidate["filing"]["accessionNumber"],
                        "cik": candidate["issuer"]["cik"],
                        "completedAt": timestamp,
                        "stage": "publication-gate",
                        "reasons": [],
                        "eventId": event["eventId"],
                    }
                else:
                    record = {
                        "pipelineVersion": PIPELINE_VERSION,
                        "status": "quarantined",
                        "accessionNumber": candidate["filing"]["accessionNumber"],
                        "cik": candidate["issuer"]["cik"],
                        "completedAt": timestamp,
                        "stage": "publication-gate",
                        "reasons": decision["quarantineReasons"],
                        "eventId": None,
                    }
        next_state["candidates"][candidate_id] = record
        run_records.append({"candidateId": candidate_id, **record})

    next_feed = merge_public_events(public_feed, approved_events, current)
    if pending:
        next_state["updatedAt"] = timestamp
    audit = {
        "schema": "mypybite.signal-pipeline-audit.v1",
        "runAt": timestamp,
        "watchedCandidateCount": queue["stats"]["candidateCount"],
        "pendingCandidateCount": len(pending),
        "autoApprovedCount": sum(item["status"] == "auto-approved" for item in run_records),
        "quarantinedCount": sum(item["status"] == "quarantined" for item in run_records),
        "records": run_records,
    }
    return next_state, next_feed, audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--detections", type=Path, default=ROOT / "data" / "sec" / "detections" / "filings.jsonl")
    parser.add_argument("--metadata", type=Path, default=ROOT / "data" / "sec" / "metadata" / "filings.jsonl")
    parser.add_argument("--roster", type=Path, default=ROOT / "config" / "management-roster.json")
    parser.add_argument("--state", type=Path, default=ROOT / "data" / "sec" / "signal-pipeline" / "state.json")
    parser.add_argument("--audit", type=Path, default=ROOT / "data" / "sec" / "signal-pipeline" / "audit.json")
    parser.add_argument("--public-feed", type=Path, default=ROOT / "public" / "signal-events.json")
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    parser.add_argument("--request-interval", type=float, default=REQUEST_INTERVAL_SECONDS)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    now = datetime.now(timezone.utc)
    state = load_state(args.state)
    feed = load_public_feed(args.public_feed, now)
    client = SecClient(args.user_agent, args.request_interval)
    with tempfile.TemporaryDirectory(prefix="mypybite-signal-") as directory:
        next_state, next_feed, audit = run_pipeline(
            load_jsonl(args.detections),
            load_jsonl(args.metadata),
            load_roster(args.roster),
            state,
            feed,
            client,
            Path(directory),
            now,
        )
    if audit["pendingCandidateCount"]:
        atomic_write_json(args.state, next_state)
        atomic_write_json(args.public_feed, next_feed)
        atomic_write_json(args.audit, audit)
    print(
        "Signal pipeline complete: "
        f"{audit['pendingCandidateCount']} new candidates, "
        f"{audit['autoApprovedCount']} auto-approved, "
        f"{audit['quarantinedCount']} quarantined."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
