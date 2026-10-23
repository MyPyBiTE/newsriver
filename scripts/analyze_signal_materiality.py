#!/usr/bin/env python3
"""Classify private Signal W5H drafts and explain measured consequences.

This stage applies issuer-neutral disclosure rules to a source-linked W5H draft.
It assigns category, materiality, direction and confidence, then creates sober
reader meaning and follow-up checkpoints. It never predicts price, recommends a
trade, or grants permission to publish.
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
W5H_SCHEMA = "mypybite.signal-w5h-draft.v1"
ANALYSIS_SCHEMA = "mypybite.signal-materiality-analysis.v1"

CATEGORY_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("liquidity-and-solvency", ("bankruptcy", "going concern", "insolvency", "default", "liquidity shortfall")),
    ("accounting-and-disclosure-controls", ("restatement", "material weakness", "internal control", "auditor", "accounting error")),
    ("mergers-acquisitions-and-divestitures", ("merger", "acquisition", "acquired", "divestiture", "sold substantially", "tender offer")),
    ("legal-and-regulatory", ("lawsuit", "litigation", "subpoena", "investigation", "settlement", "regulatory", "consent decree")),
    ("ownership-and-activism", ("schedule 13d", "beneficial ownership", "activist", "proxy contest", "control person")),
    ("insider-transaction", ("form 4", "insider transaction", "reporting person", "non-derivative security")),
    ("capital-allocation", ("dividend", "repurchase", "buyback", "distribution", "capital return", "redeemed", "redemption")),
    ("leadership-and-governance", ("appointed", "elected", "resigned", "board of directors", "committee", "chief executive", "chief financial")),
    ("compensation", ("compensation", "salary", "bonus", "equity award", "severance")),
    ("financing-and-dilution", ("offering", "new shares", "preferred shares", "convertible", "credit facility", "borrowed", "financing", "dilution")),
    ("operations-and-outlook", ("guidance", "forecast", "revenue", "operating results", "production", "contract award")),
]

MATERIALITY_RULES: list[tuple[str, int, str]] = [
    (r"\b(?:bankruptcy|insolvency|going concern)\b", 100, "survival or going-concern disclosure"),
    (r"\b(?:default|restatement|material weakness)\b", 75, "financial reliability or solvency disclosure"),
    (r"\b(?:merger|tender offer|change of control)\b", 75, "control or transformative transaction"),
    (r"\b(?:chief executive|chief financial officer)\b.*\b(?:appointed|resigned|terminated)\b|\b(?:appointed|resigned|terminated)\b.*\b(?:chief executive|chief financial officer)\b", 70, "core executive leadership change"),
    (r"\b(?:offering|convertible|credit facility|financing|dilution)\b", 55, "financing or dilution mechanics"),
    (r"\b(?:repurchase|buyback|distribution)\b", 50, "capital-allocation change"),
    (r"\bdividend\b", 40, "dividend declaration"),
    (r"\b(?:board of directors|the board|committee)\b.*\b(?:appointed|elected|resigned|increased|reduced)\b|\b(?:appointed|elected|resigned|increased|reduced)\b.*\b(?:board of directors|the board|committee)\b", 45, "board or governance change"),
    (r"\b(?:lawsuit|investigation|settlement|subpoena|consent decree)\b", 45, "legal or regulatory exposure"),
    (r"\b(?:acquired|acquisition|divestiture)\b", 45, "acquisition or divestiture"),
    (r"\b(?:guidance|forecast)\b", 40, "forward operating disclosure"),
]

ADVERSE_TERMS = re.compile(
    r"\b(?:bankruptcy|insolvency|default|restatement|material weakness|terminated|"
    r"resigned|declined|reduced|loss|investigation|lawsuit|subpoena)\b",
    re.IGNORECASE,
)
CONSTRUCTIVE_TERMS = re.compile(
    r"\b(?:repaid|resolved)\b",
    re.IGNORECASE,
)
PROHIBITED_BOILERPLATE = re.compile(
    r"\b(?:a current report flags?|potentially material event|between periodic reports)\b",
    re.IGNORECASE,
)

CATEGORY_MEANING = {
        "leadership-and-governance": "The disclosure changes who holds authority, oversight responsibility, or a governance seat. The filing establishes the personnel action but does not establish its effect on strategy or results.",
    "capital-allocation": "The disclosure changes or confirms how company resources are distributed or retained. The practical effect depends on the amount, conditions, timing, and whether the action recurs.",
    "financing-and-dilution": "The disclosure changes the company's funding or capital structure. Readers should distinguish immediate cash raised from the possible future cost, repayment obligation, or increase in claims on the company.",
    "liquidity-and-solvency": "The disclosure bears directly on the company's ability to meet obligations or continue operating. Confirm available liquidity, deadlines, waivers, and creditor remedies.",
    "ownership-and-activism": "The disclosure changes or clarifies a holder's ownership position or potential influence. Ownership alone does not establish control; agreements, voting power, and stated intent matter.",
    "mergers-acquisitions-and-divestitures": "The disclosure concerns a transaction that may change the assets, liabilities, control, or operating scope of the company. Closing conditions and financing determine whether the announced structure becomes effective.",
    "legal-and-regulatory": "The disclosure identifies a legal or regulatory process that may create cost, operating limits, or uncertainty. Allegations and preliminary proceedings are not final outcomes.",
    "accounting-and-disclosure-controls": "The disclosure bears on the reliability of reported financial information or the controls used to produce it. Corrected filings, quantified effects, and remediation evidence are the next tests.",
    "insider-transaction": "The disclosure records a transaction by a reporting person. The transaction code, ownership form, price, and any trading plan determine what can safely be inferred.",
    "compensation": "The disclosure changes or describes pay, incentives, or separation terms. The economic significance depends on amount, performance conditions, vesting, and forfeiture provisions.",
    "operations-and-outlook": "The disclosure changes or clarifies operating expectations, capacity, demand, or performance. Later reported results are needed to test whether the stated development affects cash generation.",
    "other-disclosed-event": "The filing records a concrete corporate action, but the captured facts do not yet place it in a narrower analytical category. The primary document should control any further interpretation.",
}

CATEGORY_WATCH = {
    "leadership-and-governance": "A later filing confirming the appointment, departure, committee role, effective date, or resulting governance change.",
    "capital-allocation": "The payment, authorization usage, subsequent declaration, or cash-flow disclosure confirming the economic cost.",
    "financing-and-dilution": "Final financing terms, proceeds, share count, conversion or exercise activity, repayment schedule, and use of funds.",
    "liquidity-and-solvency": "Updated cash, borrowing availability, covenant compliance, waivers, maturities, and creditor actions.",
    "ownership-and-activism": "Amended ownership filings, agreements, nominations, voting activity, purchases, sales, or changes in stated intent.",
    "mergers-acquisitions-and-divestitures": "Financing, regulatory and shareholder approvals, closing conditions, amendments, termination rights, and completion notice.",
    "legal-and-regulatory": "A court order, agency action, settlement, quantified exposure, deadline, or dismissal rather than commentary about the dispute.",
    "accounting-and-disclosure-controls": "An amended report, quantified correction, auditor conclusion, completed remediation, or later control assessment.",
    "insider-transaction": "The transaction code, footnotes, plan adoption date, subsequent holdings, and any related Form 4 amendment.",
    "compensation": "Vesting, performance results, payment, forfeiture, amendment, or separation-date confirmation.",
    "operations-and-outlook": "The next operating update or periodic report that quantifies revenue, margin, cash flow, capacity, backlog, or demand.",
    "other-disclosed-event": "A later primary filing or exhibit that supplies the missing mechanism, quantity, outcome, or consequence.",
}


class MaterialityAnalysisFailure(ValueError):
    """Raised when a private W5H draft cannot be analyzed safely."""


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


def load_draft(path: Path) -> tuple[dict, bytes]:
    try:
        payload = path.read_bytes()
        draft = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaterialityAnalysisFailure(f"Invalid W5H draft: {path}") from exc
    validate_draft(draft)
    return draft, payload


def validate_draft(draft: dict) -> None:
    if draft.get("schema") != W5H_SCHEMA:
        raise MaterialityAnalysisFailure("Unsupported Signal W5H draft schema.")
    if draft.get("publicReleaseAllowed") is not False:
        raise MaterialityAnalysisFailure("Materiality analysis accepts private drafts only.")
    if draft.get("quality", {}).get("publicationGatePassed") is not False:
        raise MaterialityAnalysisFailure("Input draft has bypassed the private publication gate.")
    what = draft.get("w5h", {}).get("what", {})
    if what.get("status") != "disclosed" or not what.get("evidenceIds"):
        raise MaterialityAnalysisFailure("A source-supported disclosed action is required.")
    known_evidence = {item.get("evidenceId") for item in draft.get("evidence", [])}
    if not set(what["evidenceIds"]).issubset(known_evidence):
        raise MaterialityAnalysisFailure("The disclosed action cites unknown evidence.")
    for field in ("headlineCandidate", "synopsisCandidate"):
        if PROHIBITED_BOILERPLATE.search(str(draft.get(field, ""))):
            raise MaterialityAnalysisFailure(f"{field} contains prohibited filing boilerplate.")


def analysis_text(draft: dict) -> str:
    parts = [draft["headlineCandidate"], draft["synopsisCandidate"]]
    w5h = draft["w5h"]
    parts.extend(item["text"] for item in w5h["who"])
    parts.extend(w5h[key]["text"] for key in ("what", "where", "why", "how"))
    return " ".join(parts)


def classify_category(text: str) -> tuple[str, list[str]]:
    lowered = text.casefold()
    matches: list[tuple[int, int, str, list[str]]] = []
    for priority, (category, terms) in enumerate(CATEGORY_RULES):
        hit = [term for term in terms if term in lowered]
        if hit:
            matches.append((len(hit), -priority, category, hit))
    if not matches:
        return "other-disclosed-event", []
    _, _, category, hit = max(matches)
    return category, hit


def classify_materiality(text: str, draft: dict) -> tuple[str, int, list[str]]:
    score = 20
    reasons = ["source-supported corporate action"]
    for pattern, points, reason in MATERIALITY_RULES:
        if re.search(pattern, text, flags=re.IGNORECASE) and points > score:
            score = points
            reasons = [reason]
    types = {item["type"] for item in draft["evidence"]}
    if "quantity" in types:
        score += 5
        reasons.append("disclosed quantity")
    if draft["w5h"]["how"]["status"] == "disclosed" and re.search(
        r"\b(?:subject to|condition|unless|if|approval|deadline)\b",
        draft["w5h"]["how"]["text"],
        flags=re.IGNORECASE,
    ):
        score += 10
        reasons.append("condition or approval dependency")
    score = min(score, 100)
    if score >= 80:
        label = "critical"
    elif score >= 55:
        label = "important"
    elif score >= 25:
        label = "notable"
    else:
        label = "routine"
    return label, score, reasons


def classify_direction(text: str) -> tuple[str, list[str]]:
    adverse = sorted({match.group(0).lower() for match in ADVERSE_TERMS.finditer(text)})
    constructive = sorted(
        {match.group(0).lower() for match in CONSTRUCTIVE_TERMS.finditer(text)}
    )
    if adverse and constructive:
        return "mixed", adverse + constructive
    if adverse:
        return "adverse", adverse
    if constructive:
        return "constructive", constructive
    return "indeterminate", []


def confidence(draft: dict) -> tuple[str, str]:
    missing = draft["quality"]["dimensionsNotDisclosed"]
    if not missing:
        return "high", "All six W5H dimensions have source-linked support."
    if set(missing).issubset({"why", "where"}) and len(missing) <= 2:
        return "moderate", "The disclosed action is clear, but context remains explicitly undisclosed."
    return "low", "One or more action, timing, location, reason, or mechanism dimensions remain unsupported."


def implication_templates(category: str, evidence_ids: list[str]) -> list[dict]:
    audience = {
        "leadership-and-governance": "common-shareholders",
        "capital-allocation": "common-shareholders",
        "financing-and-dilution": "common-shareholders",
        "liquidity-and-solvency": "debt-holders",
        "ownership-and-activism": "common-shareholders",
        "mergers-acquisitions-and-divestitures": "common-shareholders",
        "legal-and-regulatory": "regulators",
        "accounting-and-disclosure-controls": "common-shareholders",
        "insider-transaction": "common-shareholders",
        "compensation": "employees",
        "operations-and-outlook": "company",
        "other-disclosed-event": "company",
    }[category]
    return [
        {
            "audience": "company",
            "text": CATEGORY_MEANING[category],
            "evidenceIds": evidence_ids,
            "caveat": "This is a measured consequence of the disclosed structure, not a prediction of market price or operating results.",
        },
        {
            "audience": audience,
            "text": "The filing establishes the disclosed action. Its durable economic effect remains unproven until the stated mechanism, condition, or later reported result can be checked.",
            "evidenceIds": evidence_ids,
            "caveat": "Absence of a demonstrated effect is not evidence that the effect will be positive or negative.",
        },
    ]


def build_watch_next(draft: dict, category: str) -> list[dict]:
    checkpoints: list[dict] = []
    for item in draft["w5h"]["when"]:
        if item["status"] not in {"scheduled", "deadline", "conditional", "effective"}:
            continue
        checkpoints.append(
            {
                "checkpoint": f"Confirm the outcome associated with the disclosed {item['status']} date {item['text']}.",
                "dueAt": item["value"],
                "evidenceToSeek": CATEGORY_WATCH[category],
                "evidenceIds": item["evidenceIds"],
            }
        )
    if not checkpoints:
        checkpoints.append(
            {
                "checkpoint": "Check the next relevant primary-source filing for confirmation or a changed outcome.",
                "dueAt": None,
                "evidenceToSeek": CATEGORY_WATCH[category],
                "evidenceIds": draft["w5h"]["what"]["evidenceIds"],
            }
        )
    return checkpoints[:5]


def structural_oddity(draft: dict) -> dict:
    text = " ".join(
        [draft["w5h"]["what"]["text"], draft["w5h"]["how"]["text"]]
    )
    if re.search(r"\b(?:in no event|mutually exclusive|cannot both|either.+or)\b", text, re.IGNORECASE):
        return {
            "flag": True,
            "explanation": "The filing describes mutually exclusive or alternative outcome paths whose applicability depends on disclosed conditions.",
        }
    return {"flag": False, "explanation": None}


def analyze_materiality(
    draft: dict,
    draft_payload: bytes | None = None,
    now: datetime | None = None,
) -> dict:
    validate_draft(draft)
    text = analysis_text(draft)
    category_text = " ".join(
        [draft["w5h"]["what"]["text"], draft["w5h"]["how"]["text"]]
    )
    category, category_terms = classify_category(category_text)
    materiality, score, materiality_reasons = classify_materiality(text, draft)
    direction_text = " ".join(
        [draft["w5h"]["what"]["text"], draft["w5h"]["how"]["text"]]
    )
    direction, direction_terms = classify_direction(direction_text)
    confidence_label, confidence_reason = confidence(draft)
    evidence_ids = list(dict.fromkeys(draft["w5h"]["what"]["evidenceIds"] + draft["w5h"]["how"]["evidenceIds"]))
    payload = draft_payload or json.dumps(
        draft, ensure_ascii=False, sort_keys=True
    ).encode("utf-8")

    return {
        "schema": ANALYSIS_SCHEMA,
        "analysisId": f"analysis:{draft['briefId']}",
        "briefId": draft["briefId"],
        "analyzedAt": iso_utc(now or datetime.now(timezone.utc)),
        "status": "private-analysis",
        "publicReleaseAllowed": False,
        "issuer": draft["issuer"],
        "filing": draft["filing"],
        "sourceDraft": {"sha256": sha256(payload)},
        "headlineCandidate": draft["headlineCandidate"],
        "synopsisCandidate": draft["synopsisCandidate"],
        "readerMeaning": {
            "plainLanguageSummary": draft["w5h"]["what"]["text"],
            "whyItMatters": CATEGORY_MEANING[category],
            "caution": "The filing establishes an action, not its eventual business or market outcome.",
        },
        "classification": {
            "category": category,
            "categoryMatches": category_terms,
            "direction": direction,
            "directionMatches": direction_terms,
            "materiality": materiality,
            "materialityScore": score,
            "materialityReasons": materiality_reasons,
            "confidence": confidence_label,
            "confidenceReason": confidence_reason,
            "structuralOddity": structural_oddity(draft),
        },
        "implications": implication_templates(category, evidence_ids),
        "unknowns": draft["unknowns"],
        "watchNext": build_watch_next(draft, category),
        "w5h": draft["w5h"],
        "evidence": draft["evidence"],
        "quality": {
            "classificationRuleMatched": bool(category_terms),
            "materialityRuleMatched": materiality_reasons != ["source-supported corporate action"],
            "sourceEvidencePreserved": True,
            "semanticAnalysisComplete": True,
            "tradingRecommendationPresent": False,
            "publicationGatePassed": False,
        },
    }


def analyze_file(
    draft_path: Path,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict:
    draft, payload = load_draft(draft_path)
    analysis = analyze_materiality(draft, payload, now)
    atomic_write_json(output_path or draft_path.with_name("materiality-analysis.json"), analysis)
    return analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "drafts",
        nargs="*",
        type=Path,
        help="Private W5H draft paths. Defaults to all collected bundles.",
    )
    parser.add_argument(
        "--documents-dir", type=Path, default=ROOT / "private" / "signal-documents"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    drafts = args.drafts or sorted(args.documents_dir.glob("*/w5h-draft.json"))
    if not drafts:
        raise MaterialityAnalysisFailure("No private Signal W5H drafts found.")
    analyses = [analyze_file(path) for path in drafts]
    counts: dict[str, int] = {}
    for item in analyses:
        label = item["classification"]["materiality"]
        counts[label] = counts.get(label, 0) + 1
    print(f"Analyzed {len(analyses)} private Signal events: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
