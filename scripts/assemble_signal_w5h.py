#!/usr/bin/env python3
"""Assemble a private W5H fact brief from verified Signal evidence packets.

The assembler is company-agnostic and conservative. It selects exact source
passages for Who, What, When, Where, Why and How, plainly labels undisclosed
dimensions, and never grants publication permission. Classification,
implications and the automated publication gate remain later stages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_SCHEMA = "mypybite.signal-evidence-packet.v1"
W5H_SCHEMA = "mypybite.signal-w5h-draft.v1"

MONTHS = (
    r"January|February|March|April|May|June|July|August|September|October|"
    r"November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|"
    r"Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\."
)
DATE_PATTERN = re.compile(
    rf"\b(?:{MONTHS})\s+\d{{1,2}}(?:,\s*\d{{4}})?\b|\b\d{{4}}-\d{{2}}-\d{{2}}\b",
    re.IGNORECASE,
)
ACTION_VERB = re.compile(
    r"\b(?:appointed|elected|resigned|declared|approved|authorized|acquired|"
    r"sold|issued|entered|terminated|completed|increased|reduced|recommended|"
    r"filed|amended|appoints|elects|declares|approves|authorizes|acquires|"
    r"issues|enters|terminates|completes|increases|reduces|recommends|amends)\b",
    re.IGNORECASE,
)
WHERE_TERMS = re.compile(
    r"\b(?:board(?: of directors)?|committee|common stock|preferred stock|"
    r"shares?|security|securities|facility|subsidiary|segment|division|"
    r"jurisdiction|agreement|contract|certificate|exchange|market)\b",
    re.IGNORECASE,
)
MECHANISM_TERMS = re.compile(
    r"\b(?:subject to|provided that|unless|if|through|via|using|by |"
    r"with the proceeds|using the proceeds|condition|approval|effective|"
    r"payable|funded|financed|amended)\b",
    re.IGNORECASE,
)


class W5HAssemblyFailure(ValueError):
    """Raised when a verified evidence packet cannot support a private brief."""


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def load_packet(path: Path) -> tuple[dict, bytes]:
    try:
        payload = path.read_bytes()
        packet = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise W5HAssemblyFailure(f"Invalid Signal evidence packet: {path}") from exc
    validate_packet(packet)
    return packet, payload


def validate_packet(packet: dict) -> None:
    if packet.get("schema") != EVIDENCE_SCHEMA:
        raise W5HAssemblyFailure("Unsupported Signal evidence packet schema.")
    if packet.get("publicReleaseAllowed") is not False:
        raise W5HAssemblyFailure("W5H assembly accepts private evidence only.")
    verification = packet.get("verification", {})
    required = {
        "manifestPrivate",
        "allDocumentHashesVerified",
        "primaryTextPresent",
        "sourceLinksPreserved",
    }
    if not required.issubset(verification) or not all(
        verification.get(item) is True for item in required
    ):
        raise W5HAssemblyFailure("Evidence packet has not passed source verification.")
    if not isinstance(packet.get("evidence"), list):
        raise W5HAssemblyFailure("Evidence packet lacks an evidence list.")
    identifiers = [item.get("evidenceId") for item in packet["evidence"]]
    if any(not identifier for identifier in identifiers) or len(identifiers) != len(
        set(identifiers)
    ):
        raise W5HAssemblyFailure("Evidence identifiers must be present and unique.")


def grouped_passages(packet: dict) -> list[dict]:
    grouped: dict[tuple, dict] = {}
    for item in packet["evidence"]:
        source = item.get("source", {})
        locator = item.get("locator", {})
        key = (
            source.get("filename"),
            locator.get("block"),
            item.get("quote"),
        )
        if not all(key):
            raise W5HAssemblyFailure("Evidence passage lacks source, locator, or quote.")
        passage = grouped.setdefault(
            key,
            {
                "text": item["quote"],
                "source": source,
                "locator": locator,
                "types": [],
                "evidenceIds": [],
            },
        )
        if item["type"] not in passage["types"]:
            passage["types"].append(item["type"])
        passage["evidenceIds"].append(item["evidenceId"])
    return list(grouped.values())


def first_sentence(text: str) -> str:
    match = re.match(r"(.+?[.!?])(?:\s|$)", text)
    return match.group(1).strip() if match else text.strip()


def compact(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    shortened = text[: limit - 1].rsplit(" ", 1)[0].rstrip(" ,;:")
    return shortened + "…"


def choose_what(passages: list[dict]) -> dict | None:
    action_passages = [item for item in passages if "action" in item["types"]]
    if not action_passages:
        return None
    return max(
        action_passages,
        key=lambda item: (
            "quantity" in item["types"],
            "condition" in item["types"],
            "date" in item["types"],
            len(item["text"]),
        ),
    )


def actor_phrase(text: str) -> str | None:
    sentence = first_sentence(text)
    sentence = re.sub(
        rf"^On\s+(?:{MONTHS})\s+\d{{1,2}}(?:,\s*\d{{4}})?,?\s+",
        "",
        sentence,
        flags=re.IGNORECASE,
    )
    match = ACTION_VERB.search(sentence)
    if not match:
        return None
    actor = sentence[: match.start()].strip(" ,:;-.")
    actor = re.sub(r"^(?:the\s+)?company\s+", "The company ", actor, flags=re.IGNORECASE)
    if actor.casefold() in {"we", "us", "our", "it", "the registrant", "registrant"}:
        return None
    if 2 <= len(actor) <= 120 and not actor.lower().startswith(("this ", "that ")):
        return actor
    return None


def build_who(packet: dict, passages: list[dict], what: dict | None) -> list[dict]:
    supporting = (what or (passages[0] if passages else None))
    evidence_ids = supporting["evidenceIds"] if supporting else []
    parties = [
        {
            "text": packet["issuer"]["legalName"],
            "role": "SEC registrant and verified issuer",
            "status": "verified-context",
            "evidenceIds": evidence_ids,
        }
    ]
    seen = {packet["issuer"]["legalName"].casefold()}
    for passage in [item for item in passages if "action" in item["types"]]:
        actor = actor_phrase(passage["text"])
        if actor and actor.casefold() not in seen:
            seen.add(actor.casefold())
            parties.append(
                {
                    "text": actor,
                    "role": "Actor phrase stated in the filing",
                    "status": "disclosed",
                    "evidenceIds": passage["evidenceIds"],
                }
            )
        if len(parties) >= 5:
            break
    return parties


def normalize_date(value: str) -> str:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    cleaned = value.replace("Sept.", "Sep.")
    for form in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(cleaned, form).date().isoformat()
        except ValueError:
            continue
    return value


def date_status(text: str, position: int = 0) -> str:
    lowered = text.casefold()
    prefix = lowered[max(0, position - 40) : position]
    if re.search(r"\bpayable\s+(?:on\s+)?$", prefix):
        return "scheduled"
    if re.search(r"\b(?:record|approval|deadline)\s+(?:date\s+)?(?:on\s+)?$", prefix):
        return "deadline"
    if "accepted" in lowered:
        return "accepted"
    if "filed" in lowered or "filing" in lowered:
        return "filed"
    if "effective" in lowered:
        return "effective"
    if any(term in lowered for term in ("deadline", "by ", "at or prior to", "until")):
        return "deadline"
    if any(term in lowered for term in ("meeting", "will be held", "scheduled")):
        return "scheduled"
    if "subject to" in lowered or "conditional" in lowered:
        return "conditional"
    return "disclosed"


def build_when(passages: list[dict]) -> list[dict]:
    dates: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for passage in [item for item in passages if "date" in item["types"]]:
        for match in DATE_PATTERN.finditer(passage["text"]):
            raw = match.group(0)
            value = normalize_date(raw)
            key = (value, date_status(passage["text"], match.start()))
            if key in seen:
                continue
            seen.add(key)
            dates.append(
                {
                    "text": raw,
                    "value": value,
                    "status": key[1],
                    "evidenceIds": passage["evidenceIds"],
                }
            )
    return dates[:12]


def claim_from_passage(passage: dict | None, missing_text: str) -> dict:
    if passage is None:
        return {"status": "not-disclosed", "text": missing_text, "evidenceIds": []}
    return {
        "status": "disclosed",
        "text": passage["text"],
        "evidenceIds": passage["evidenceIds"],
    }


def first_matching(passages: list[dict], predicate) -> dict | None:
    return next((item for item in passages if predicate(item)), None)


def nearby_passages(
    passages: list[dict], what: dict | None, radius: int = 3
) -> list[dict]:
    """Keep context tied to the selected action instead of the whole filing."""
    if what is None:
        return []
    source_hash = what["source"]["sha256"]
    block = int(what["locator"]["block"])
    return [
        item
        for item in passages
        if item["source"]["sha256"] == source_hash
        and abs(int(item["locator"]["block"]) - block) <= radius
    ]


def evidence_map(packet: dict, used_ids: set[str]) -> list[dict]:
    return [
        {
            "evidenceId": item["evidenceId"],
            "type": item["type"],
            "source": item["source"],
            "locator": item["locator"],
            "quote": item["quote"],
        }
        for item in packet["evidence"]
        if item["evidenceId"] in used_ids
    ]


def assemble_w5h(
    packet: dict,
    packet_payload: bytes | None = None,
    now: datetime | None = None,
) -> dict:
    validate_packet(packet)
    passages = grouped_passages(packet)
    what_passage = choose_what(passages)
    related_passages = nearby_passages(passages, what_passage)
    action_passages = [what_passage] if what_passage else []
    who = build_who(packet, action_passages, what_passage)
    where_passage = (
        what_passage
        if what_passage and WHERE_TERMS.search(what_passage["text"])
        else first_matching(
            related_passages,
            lambda item: WHERE_TERMS.search(item["text"]) is not None
            and ("action" in item["types"] or "quantity" in item["types"]),
        )
    )
    why_passage = first_matching(
        related_passages, lambda item: "stated-reason" in item["types"]
    )
    how_passage = (
        what_passage
        if what_passage
        and (
            "condition" in what_passage["types"]
            or MECHANISM_TERMS.search(what_passage["text"]) is not None
        )
        else first_matching(
            related_passages,
            lambda item: item is not what_passage
            and (
                "condition" in item["types"]
                or MECHANISM_TERMS.search(item["text"]) is not None
            ),
        )
    )
    date_passages = list(action_passages)
    if how_passage is not None and how_passage not in date_passages:
        date_passages.append(how_passage)
    when = build_when(date_passages)

    what = claim_from_passage(
        what_passage, "The filing does not provide a machine-identifiable action."
    )
    where = claim_from_passage(
        where_passage,
        "The filing does not identify a narrower affected security, business unit, geography, contract, or governance body.",
    )
    why = claim_from_passage(
        why_passage, "The company did not disclose why in the captured documents."
    )
    how = claim_from_passage(
        how_passage,
        "The filing does not disclose a machine-identifiable mechanism or condition.",
    )

    dimension_status = {
        "who": "disclosed" if who else "not-disclosed",
        "what": what["status"],
        "when": "disclosed" if when else "not-disclosed",
        "where": where["status"],
        "why": why["status"],
        "how": how["status"],
    }
    missing = [key for key, value in dimension_status.items() if value == "not-disclosed"]
    unknowns = [
        {
            "dimension": key,
            "text": {
                "who": "Additional directly involved people or counterparties are not identified by the current extraction.",
                "what": what["text"],
                "when": "No event, effective, scheduled, or deadline date was identified.",
                "where": where["text"],
                "why": why["text"],
                "how": how["text"],
            }[key],
        }
        for key in missing
    ]

    selected_passages = [item for item in (what_passage, how_passage, why_passage) if item]
    synopsis_parts: list[str] = []
    for item in selected_passages:
        sentence = first_sentence(item["text"])
        if sentence not in synopsis_parts:
            synopsis_parts.append(sentence)
    issuer_name = packet["issuer"]["legalName"]
    action_text = first_sentence(what_passage["text"]) if what_passage else "Action not identified"
    action_text = re.split(
        r",\s*(?:payable|effective|subject to|with the proceeds)\b",
        action_text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    headline = action_text if issuer_name.casefold() in action_text.casefold() else f"{issuer_name}: {action_text}"

    used_ids: set[str] = set()
    for party in who:
        used_ids.update(party["evidenceIds"])
    for item in when:
        used_ids.update(item["evidenceIds"])
    for claim in (what, where, why, how):
        used_ids.update(claim["evidenceIds"])

    packet_bytes = packet_payload or (
        json.dumps(packet, ensure_ascii=False, sort_keys=True).encode("utf-8")
    )
    return {
        "schema": W5H_SCHEMA,
        "briefId": f"w5h:{packet['candidateId']}",
        "packetId": packet["packetId"],
        "assembledAt": iso_utc(now or datetime.now(timezone.utc)),
        "status": "private-draft",
        "publicReleaseAllowed": False,
        "issuer": packet["issuer"],
        "filing": packet["filing"],
        "sourcePacket": {"sha256": sha256(packet_bytes)},
        "headlineCandidate": compact(headline, 200),
        "synopsisCandidate": compact(" ".join(synopsis_parts), 800),
        "w5h": {
            "who": who,
            "what": what,
            "when": when,
            "where": where,
            "why": why,
            "how": how,
        },
        "unknowns": unknowns,
        "quality": {
            "w5hFieldsPresent": True,
            "dimensionsSupported": [key for key in dimension_status if key not in missing],
            "dimensionsNotDisclosed": missing,
            "usesExactSourceLanguage": True,
            "requiresSemanticAnalysis": True,
            "publicationGatePassed": False,
        },
        "evidence": evidence_map(packet, used_ids),
    }


def assemble_file(
    packet_path: Path,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict:
    packet, payload = load_packet(packet_path)
    brief = assemble_w5h(packet, payload, now)
    atomic_write_json(output_path or packet_path.with_name("w5h-draft.json"), brief)
    return brief


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "packets",
        nargs="*",
        type=Path,
        help="Private evidence packet paths. Defaults to all collected bundles.",
    )
    parser.add_argument(
        "--documents-dir", type=Path, default=ROOT / "private" / "signal-documents"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    packets = args.packets or sorted(args.documents_dir.glob("*/evidence.json"))
    if not packets:
        raise W5HAssemblyFailure("No private Signal evidence packets found.")
    briefs = [assemble_file(path) for path in packets]
    supported = sum(len(item["quality"]["dimensionsSupported"]) for item in briefs)
    print(
        f"Assembled {len(briefs)} private W5H drafts with "
        f"{supported} source-supported dimensions."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
