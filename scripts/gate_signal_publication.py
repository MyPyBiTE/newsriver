#!/usr/bin/env python3
"""Automatically approve complete Signal analyses or quarantine them safely.

The gate converts a private materiality analysis into a schema-valid Signal
event only when every required source, W5H, classification, confidence and
language check passes. Failed analyses remain private with explicit reasons.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from validate_signal_event import (
    REQUIRED_AUTOMATED_CHECKS,
    SignalValidationFailure,
    validate_event,
)


ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_SCHEMA = "mypybite.signal-materiality-analysis.v1"
DECISION_SCHEMA = "mypybite.signal-publication-decision.v1"
PROHIBITED = re.compile(
    r"\b(?:a current report flags?|potentially material event|between periodic reports|"
    r"bullish|bearish|buy|sell|hold recommendation|price target)\b",
    re.IGNORECASE,
)
ACTION_BOILERPLATE = re.compile(
    r"(?:if the form is filed by|instruction\s+\d|pursuant to and in accordance "
    r"with the securities exchange act|if an emerging growth company|inline xbrl|"
    r"instance document|compiled based on applicable requirements|"
    r"we believe that adjusting)",
    re.IGNORECASE,
)
SPECIFIC_ACTION = re.compile(
    r"(?:"
    r"\b(?:appointed|elected|resigned|terminated)\b.{0,180}\b(?:officer|director|chief|board|committee)\b|"
    r"\b(?:officer|director|chief|board|committee)\b.{0,180}\b(?:appointed|elected|resigned|terminated)\b|"
    r"\b(?:declared|approved|authorized|increased|reduced|announced)\b.{0,180}\b(?:dividend|repurchase|buyback|board|directors?)\b|"
    r"\b(?:issued|redeemed|offered|borrowed|entered into|amended)\b.{0,180}\b(?:shares?|stock|notes?|debt|facility|agreement|financing)\b|"
    r"\b(?:acquired|sold|divested|completed|terminated|entered into)\b.{0,180}\b(?:acquisition|merger|sale|transaction|agreement|business|assets?)\b|"
    r"\b(?:restated|identified|remediated)\b.{0,180}\b(?:financial statements?|material weakness|internal controls?)\b"
    r")",
    re.IGNORECASE | re.DOTALL,
)
FULL_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),\s*(\d{4})\b",
    re.IGNORECASE,
)


class PublicationGateFailure(ValueError):
    """Raised when a publication decision cannot be evaluated safely."""


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def compact(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rsplit(" ", 1)[0].rstrip(" ,;:") + "…"


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def load_analysis(path: Path) -> tuple[dict, bytes]:
    try:
        payload = path.read_bytes()
        analysis = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublicationGateFailure(f"Invalid materiality analysis: {path}") from exc
    validate_analysis(analysis)
    return analysis, payload


def validate_analysis(analysis: dict) -> None:
    if analysis.get("schema") != ANALYSIS_SCHEMA:
        raise PublicationGateFailure("Unsupported Signal materiality analysis schema.")
    if analysis.get("publicReleaseAllowed") is not False:
        raise PublicationGateFailure("Publication gate accepts private analyses only.")
    quality = analysis.get("quality", {})
    if quality.get("semanticAnalysisComplete") is not True:
        raise PublicationGateFailure("Semantic analysis is incomplete.")
    if quality.get("publicationGatePassed") is not False:
        raise PublicationGateFailure("Analysis claims a publication decision before gating.")
    if not isinstance(analysis.get("evidence"), list) or not analysis["evidence"]:
        raise PublicationGateFailure("Analysis lacks source evidence.")


def walk_evidence_ids(value) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "evidenceIds" and isinstance(child, list):
                found.update(str(item) for item in child)
            else:
                found.update(walk_evidence_ids(child))
    elif isinstance(value, list):
        for child in value:
            found.update(walk_evidence_ids(child))
    return found


def public_text(analysis: dict) -> str:
    values = [
        analysis["headlineCandidate"],
        analysis["synopsisCandidate"],
        analysis["readerMeaning"]["plainLanguageSummary"],
        analysis["readerMeaning"]["whyItMatters"],
    ]
    values.extend(item["text"] for item in analysis["implications"])
    return " ".join(values)


def action_is_temporally_relevant(analysis: dict, action_text: str) -> bool:
    accepted = str(analysis.get("filing", {}).get("acceptedAt") or "")[:10]
    try:
        filed = date.fromisoformat(accepted)
    except ValueError:
        return False
    mentioned: list[date] = []
    for month, day, year in FULL_DATE.findall(action_text):
        try:
            mentioned.append(
                datetime.strptime(f"{month} {day}, {year}", "%B %d, %Y").date()
            )
        except ValueError:
            continue
    if not mentioned:
        return True
    return any(-30 <= (item - filed).days <= 730 for item in mentioned)


def evaluate_checks(analysis: dict) -> dict[str, tuple[bool, str]]:
    evidence = analysis["evidence"]
    known_ids = {item.get("evidenceId") for item in evidence}
    referenced_ids = walk_evidence_ids(
        {"w5h": analysis["w5h"], "implications": analysis["implications"]}
    )
    sources_official = all(
        urlparse(str(item.get("source", {}).get("url", ""))).scheme == "https"
        and urlparse(str(item.get("source", {}).get("url", ""))).hostname
        in {"sec.gov", "www.sec.gov"}
        and re.fullmatch(r"[a-f0-9]{64}", str(item.get("source", {}).get("sha256", "")))
        for item in evidence
    )
    w5h = analysis["w5h"]
    missing = [
        key
        for key in ("who", "what", "when", "where", "why", "how")
        if key not in w5h
    ]
    w5h_core = (
        not missing
        and bool(w5h["who"])
        and w5h["what"].get("status") == "disclosed"
        and bool(w5h["when"])
        and w5h["how"].get("status") == "disclosed"
    )
    classification = analysis["classification"]
    category_ok = (
        classification["category"] != "other-disclosed-event"
        and analysis["quality"].get("classificationRuleMatched") is True
    )
    if classification["materiality"] in {"critical", "important"}:
        category_ok = category_ok and analysis["quality"].get("materialityRuleMatched") is True
    confidence_ok = classification["confidence"] != "low"
    if classification["materiality"] in {"critical", "important"}:
        confidence_ok = classification["confidence"] == "high"
    quantities_ok = all(
        item.get("type") != "quantity"
        or re.fullmatch(r"[a-f0-9]{64}", str(item.get("source", {}).get("sha256", "")))
        for item in evidence
    )
    language_ok = PROHIBITED.search(public_text(analysis)) is None
    action_ok = (
        len(str(w5h.get("what", {}).get("text", "")).strip()) >= 20
        and PROHIBITED.search(str(w5h.get("what", {}).get("text", ""))) is None
    )
    action_text = str(w5h.get("what", {}).get("text", ""))
    supported_filing = str(analysis.get("filing", {}).get("form", "")).upper() in {
        "8-K",
        "8-K/A",
    }
    action_specific = (
        ACTION_BOILERPLATE.search(action_text) is None
        and SPECIFIC_ACTION.search(action_text) is not None
    )
    temporal_relevance = action_is_temporally_relevant(analysis, action_text)
    return {
        "official-sec-sources": (sources_official, "Every cited passage must come from a hashed official SEC document."),
        "evidence-references": (bool(referenced_ids) and referenced_ids.issubset(known_ids), "Every W5H and consequence claim must cite known evidence."),
        "concrete-action": (action_ok, "The event must name a concrete disclosed action without filing boilerplate."),
        "w5h-contract": (w5h_core, "Who, What, When and How must be source-supported; undisclosed Why or Where must be explicit."),
        "classification-rules": (category_ok, "The category and high-materiality decision must match a defined rule."),
        "confidence-threshold": (confidence_ok, "Important and Critical events require high confidence; other events require at least moderate confidence."),
        "quantities-source-linked": (quantities_ok, "Every extracted quantity must retain a verified source hash."),
        "no-trading-language": (language_ok, "Reader text must not contain hype, trade instructions, or price predictions."),
        "supported-filing": (supported_filing, "Automatic publication currently supports Form 8-K and 8-K/A only; other forms require a form-specific reader."),
        "action-specificity": (action_specific, "The selected passage must state a concrete corporate action and must not be filing instructions or regulatory boilerplate."),
        "temporal-relevance": (temporal_relevance, "An explicitly dated action must be current enough to represent a development in the newly filed statement."),
    }


def source_id(source: dict) -> str:
    return f"src-{int(source['sequence']):03d}-{source['sha256'][:10]}"


def build_sources(analysis: dict) -> list[dict]:
    sources: dict[str, dict] = {}
    for item in analysis["evidence"]:
        source = item["source"]
        identifier = source_id(source)
        if identifier in sources:
            continue
        role = source["role"]
        sources[identifier] = {
            "sourceId": identifier,
            "sourceType": "sec-exhibit" if role == "exhibit" else "sec-filing",
            "title": f"{analysis['issuer']['legalName']} {source['documentType']} {role.replace('-', ' ')}",
            "url": source["url"],
            "publishedAt": analysis["filing"].get("acceptedAt"),
            "primary": True,
            "accessionNumber": analysis["filing"].get("accessionNumber"),
            "form": analysis["filing"].get("form"),
        }
    return list(sources.values())


def build_evidence(analysis: dict) -> list[dict]:
    return [
        {
            "evidenceId": item["evidenceId"],
            "sourceId": source_id(item["source"]),
            "locator": f"{item['source']['filename']} block {item['locator']['block']} ({item['locator']['tag']})",
            "fact": item["quote"],
            "quotedText": compact(item["quote"], 500),
        }
        for item in analysis["evidence"]
    ]


def event_when(items: list[dict]) -> list[dict]:
    status_map = {"disclosed": "occurred", "conditional": "conditional"}
    return [
        {
            "label": f"Disclosed {item['status']} date",
            "value": item["value"],
            "status": status_map.get(item["status"], item["status"]),
            "evidenceIds": item["evidenceIds"],
        }
        for item in items
    ]


def normalize_due_at(value: str | None) -> str | None:
    if value and "T" in value:
        return value
    return None


def build_event(analysis: dict, passed_checks: list[str], decided_at: datetime) -> dict:
    accession = str(analysis["filing"]["accessionNumber"])
    fingerprint = sha256(analysis["w5h"]["what"]["text"].encode("utf-8"))[:12]
    unknowns = [item["text"] for item in analysis["unknowns"]]
    if not unknowns:
        unknowns = [
            "The filing does not establish the eventual business, financial, or market outcome of the disclosed action."
        ]
    classification = analysis["classification"]
    event = {
        "schema": "mypybite.signal-event.v1",
        "eventId": f"sec:{accession}:{fingerprint}",
        "generatedAt": iso_utc(decided_at),
        "status": "publishable",
        "issuer": analysis["issuer"],
        "headline": compact(analysis["headlineCandidate"], 160),
        "synopsis": compact(analysis["synopsisCandidate"], 420),
        "w5h": {
            "who": [
                {"name": item["text"], "role": item["role"], "evidenceIds": item["evidenceIds"]}
                for item in analysis["w5h"]["who"]
            ],
            "what": {key: analysis["w5h"]["what"][key] for key in ("text", "evidenceIds")},
            "when": event_when(analysis["w5h"]["when"]),
            "where": [{key: analysis["w5h"]["where"][key] for key in ("text", "evidenceIds")}],
            "why": analysis["w5h"]["why"],
            "how": {key: analysis["w5h"]["how"][key] for key in ("text", "evidenceIds")},
        },
        "implications": analysis["implications"],
        "unknowns": unknowns,
        "watchNext": [
            {
                "checkpoint": item["checkpoint"],
                "dueAt": normalize_due_at(item["dueAt"]),
                "evidenceToSeek": item["evidenceToSeek"],
            }
            for item in analysis["watchNext"]
        ],
        "classification": {
            "category": classification["category"],
            "direction": classification["direction"],
            "materiality": classification["materiality"],
            "confidence": classification["confidence"],
            "structuralOddity": classification["structuralOddity"],
        },
        "sources": build_sources(analysis),
        "evidence": build_evidence(analysis),
        "consolidation": {
            "groupKey": f"{analysis['issuer']['cik']}:{accession}:{classification['category']}",
            "isPrimaryEvent": True,
            "relatedEventIds": [],
            "rationale": "This record consolidates the source-linked action, dates, conditions, consequences, and follow-up checkpoints extracted from one SEC filing bundle.",
        },
        "quality": {
            "w5hComplete": True,
            "evidenceComplete": True,
            "quantitiesVerified": True,
            "humanReviewRequired": False,
            "humanReviewed": False,
            "reviewMethod": "automated",
            "automatedReviewPassed": True,
            "automatedReviewChecks": passed_checks,
            "quarantineReasons": [],
            "reviewNotes": [
                f"Automated evidence gate passed {len(passed_checks)} required checks at {iso_utc(decided_at)}."
            ],
        },
    }
    validate_event(event)
    return event


def gate_analysis(
    analysis: dict,
    analysis_payload: bytes | None = None,
    now: datetime | None = None,
) -> dict:
    validate_analysis(analysis)
    decided_at = now or datetime.now(timezone.utc)
    checks = evaluate_checks(analysis)
    passed = sorted(name for name, (ok, _) in checks.items() if ok)
    failures = [detail for _, (ok, detail) in checks.items() if not ok]
    event = None
    if not failures:
        try:
            event = build_event(analysis, passed, decided_at)
        except SignalValidationFailure as exc:
            failures.append(
                "Generated event failed permanent validation: "
                + str(exc).replace("\n", " ")
            )
    decision = "auto-approved" if event is not None else "quarantined"
    payload = analysis_payload or json.dumps(
        analysis, ensure_ascii=False, sort_keys=True
    ).encode("utf-8")
    return {
        "schema": DECISION_SCHEMA,
        "decisionId": f"decision:{analysis['analysisId']}",
        "analysisId": analysis["analysisId"],
        "decidedAt": iso_utc(decided_at),
        "decision": decision,
        "publicReleaseAllowed": decision == "auto-approved",
        "sourceAnalysis": {"sha256": sha256(payload)},
        "checks": [
            {"name": name, "passed": ok, "requirement": detail}
            for name, (ok, detail) in checks.items()
        ],
        "quarantineReasons": failures,
        "event": event,
    }


def gate_file(
    analysis_path: Path,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict:
    analysis, payload = load_analysis(analysis_path)
    decision = gate_analysis(analysis, payload, now)
    destination = output_path or analysis_path.with_name("publication-decision.json")
    atomic_write_json(destination, decision)
    event_path = analysis_path.with_name("auto-approved-event.json")
    if decision["event"] is not None:
        atomic_write_json(event_path, decision["event"])
    elif event_path.exists():
        event_path.unlink()
    return decision


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("analyses", nargs="*", type=Path)
    parser.add_argument(
        "--documents-dir", type=Path, default=ROOT / "private" / "signal-documents"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    paths = args.analyses or sorted(args.documents_dir.glob("*/materiality-analysis.json"))
    if not paths:
        raise PublicationGateFailure("No private materiality analyses found.")
    decisions = [gate_file(path) for path in paths]
    approved = sum(item["decision"] == "auto-approved" for item in decisions)
    print(f"Publication gate complete: {approved} auto-approved, {len(decisions) - approved} quarantined.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
