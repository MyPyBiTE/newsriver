#!/usr/bin/env python3
"""Fail closed when a Signal event breaks the W5H editorial contract."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "schemas" / "signal-event.schema.json"

PROHIBITED_BOILERPLATE = {
    r"\ba current report flags?\b": "replace form-level boilerplate with the disclosed action",
    r"\bpotentially material event\b": "name the actual event",
    r"\bbetween periodic reports\b": "explain the event rather than the filing cadence",
}

REQUIRED_AUTOMATED_CHECKS = {
    "official-sec-sources",
    "evidence-references",
    "concrete-action",
    "w5h-contract",
    "classification-rules",
    "confidence-threshold",
    "quantities-source-linked",
    "no-trading-language",
    "supported-filing",
    "action-specificity",
    "temporal-relevance",
}


class SignalValidationFailure(ValueError):
    """Raised when an event cannot safely enter the Signal publication pipeline."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SignalValidationFailure(f"Cannot load {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SignalValidationFailure(f"{path} must contain a JSON object")
    return value


def _is_json_type(value: Any, expected: str) -> bool:
    checks = {
        "object": lambda item: isinstance(item, dict),
        "array": lambda item: isinstance(item, list),
        "string": lambda item: isinstance(item, str),
        "null": lambda item: item is None,
        "boolean": lambda item: isinstance(item, bool),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
    }
    return checks.get(expected, lambda _item: True)(value)


def _resolve_reference(schema: dict[str, Any], reference: str) -> dict[str, Any]:
    if not reference.startswith("#/"):
        raise SignalValidationFailure(f"Unsupported external schema reference: {reference}")
    node: Any = schema
    for part in reference[2:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        node = node[key]
    if not isinstance(node, dict):
        raise SignalValidationFailure(f"Schema reference does not resolve to an object: {reference}")
    return node


def _valid_datetime(value: str) -> bool:
    if "T" not in value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _check_schema_node(
    value: Any,
    rule: dict[str, Any],
    root_schema: dict[str, Any],
    path: str,
    errors: list[str],
) -> None:
    if "$ref" in rule:
        _check_schema_node(value, _resolve_reference(root_schema, rule["$ref"]), root_schema, path, errors)
        return

    if "const" in rule and value != rule["const"]:
        errors.append(f"{path}: expected {rule['const']!r}")
    if "enum" in rule and value not in rule["enum"]:
        errors.append(f"{path}: {value!r} is not an allowed value")

    expected = rule.get("type")
    if expected:
        expected_types = expected if isinstance(expected, list) else [expected]
        if not any(_is_json_type(value, item) for item in expected_types):
            errors.append(f"{path}: expected {expected_types}, got {type(value).__name__}")
            return

    if isinstance(value, dict):
        properties = rule.get("properties", {})
        for key in rule.get("required", []):
            if key not in value:
                errors.append(f"{path}: missing required field {key!r}")
        if rule.get("additionalProperties") is False:
            for key in value:
                if key not in properties:
                    errors.append(f"{path}: unexpected field {key!r}")
        for key, child in value.items():
            if key in properties:
                _check_schema_node(child, properties[key], root_schema, f"{path}.{key}", errors)
        return

    if isinstance(value, list):
        if len(value) < rule.get("minItems", 0):
            errors.append(f"{path}: requires at least {rule['minItems']} item(s)")
        if rule.get("uniqueItems"):
            serialized = [json.dumps(item, sort_keys=True) for item in value]
            if len(serialized) != len(set(serialized)):
                errors.append(f"{path}: duplicate items are not allowed")
        item_rule = rule.get("items")
        if item_rule:
            for index, child in enumerate(value):
                _check_schema_node(child, item_rule, root_schema, f"{path}[{index}]", errors)
        return

    if isinstance(value, str):
        if len(value) < rule.get("minLength", 0):
            errors.append(f"{path}: text is shorter than the minimum length")
        if "maxLength" in rule and len(value) > rule["maxLength"]:
            errors.append(f"{path}: text exceeds {rule['maxLength']} characters")
        if "pattern" in rule and not re.fullmatch(rule["pattern"], value):
            errors.append(f"{path}: value does not match the required pattern")
        if rule.get("format") == "date-time" and not _valid_datetime(value):
            errors.append(f"{path}: invalid date-time")
        if rule.get("format") == "uri":
            parsed = urlparse(value)
            if parsed.scheme != "https" or not parsed.netloc:
                errors.append(f"{path}: source URLs must be complete HTTPS URLs")


def _collect_public_text(event: dict[str, Any]) -> list[tuple[str, str]]:
    fields = [
        ("headline", event["headline"]),
        ("synopsis", event["synopsis"]),
        ("w5h.what.text", event["w5h"]["what"]["text"]),
        ("w5h.why.text", event["w5h"]["why"]["text"]),
        ("w5h.how.text", event["w5h"]["how"]["text"]),
    ]
    fields.extend(
        (f"w5h.where[{index}].text", item["text"])
        for index, item in enumerate(event["w5h"]["where"])
    )
    fields.extend(
        (f"implications[{index}].text", item["text"])
        for index, item in enumerate(event["implications"])
    )
    return fields


def _walk_evidence_references(value: Any, path: str = "$") -> list[tuple[str, str]]:
    references: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key == "evidenceIds":
                references.extend((child_path, item) for item in child)
            else:
                references.extend(_walk_evidence_references(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            references.extend(_walk_evidence_references(child, f"{path}[{index}]"))
    return references


def _check_editorial_contract(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    source_ids = [item["sourceId"] for item in event["sources"]]
    evidence_ids = [item["evidenceId"] for item in event["evidence"]]
    if len(source_ids) != len(set(source_ids)):
        errors.append("$.sources: sourceId values must be unique")
    if len(evidence_ids) != len(set(evidence_ids)):
        errors.append("$.evidence: evidenceId values must be unique")

    known_sources = set(source_ids)
    known_evidence = set(evidence_ids)
    for item in event["evidence"]:
        if item["sourceId"] not in known_sources:
            errors.append(
                f"$.evidence[{item['evidenceId']}]: unknown sourceId {item['sourceId']!r}"
            )
    for path, reference in _walk_evidence_references(event):
        if reference not in known_evidence:
            errors.append(f"{path}: unknown evidenceId {reference!r}")

    primary_sources = [item for item in event["sources"] if item["primary"]]
    if not primary_sources:
        errors.append("$.sources: at least one primary source is required")
    for item in primary_sources:
        if item["sourceType"] == "secondary-context":
            errors.append(
                f"$.sources[{item['sourceId']}]: secondary context cannot be marked primary"
            )

    for path, text in _collect_public_text(event):
        for pattern, instruction in PROHIBITED_BOILERPLATE.items():
            if re.search(pattern, text, flags=re.IGNORECASE):
                errors.append(f"$.{path}: {instruction}")

    why = event["w5h"]["why"]
    if why["status"] == "not-disclosed" and not re.search(
        r"(?:did not disclose why|why (?:was|is) not disclosed)", why["text"], re.IGNORECASE
    ):
        errors.append(
            "$.w5h.why.text: a missing reason must plainly say that the company did not disclose why"
        )

    oddity = event["classification"]["structuralOddity"]
    if oddity["flag"] and not oddity["explanation"]:
        errors.append("$.classification.structuralOddity: a flagged oddity needs an explanation")
    if not oddity["flag"] and oddity["explanation"]:
        errors.append("$.classification.structuralOddity: explanation requires flag=true")

    quality = event["quality"]
    materiality = event["classification"]["materiality"]
    review_method = quality.get("reviewMethod", "human")
    automated_checks = set(quality.get("automatedReviewChecks", []))
    automated_passed = quality.get("automatedReviewPassed") is True
    if review_method == "automated":
        missing_checks = REQUIRED_AUTOMATED_CHECKS - automated_checks
        if not automated_passed:
            errors.append("$.quality.automatedReviewPassed: automated review is incomplete")
        if missing_checks:
            errors.append(
                "$.quality.automatedReviewChecks: missing required checks "
                + ", ".join(sorted(missing_checks))
            )
        if quality["humanReviewRequired"]:
            errors.append(
                "$.quality.humanReviewRequired: automated approval cannot also require human review"
            )
    elif materiality in {"critical", "important"} and not quality["humanReviewRequired"]:
        errors.append(
            "$.quality.humanReviewRequired: Critical and Important events require human review or a complete automated review"
        )

    if event["status"] in {"publishable", "published"}:
        for field in ("w5hComplete", "evidenceComplete", "quantitiesVerified"):
            if not quality[field]:
                errors.append(f"$.quality.{field}: must be true before publication")
        if review_method == "automated" and not automated_passed:
            errors.append("$.quality.automatedReviewPassed: required automated review is incomplete")
        if review_method != "automated" and quality["humanReviewRequired"] and not quality["humanReviewed"]:
            errors.append("$.quality.humanReviewed: required review is incomplete")

    return errors


def validate_event(
    event: dict[str, Any], schema: dict[str, Any] | None = None
) -> None:
    contract = schema or load_json(DEFAULT_SCHEMA)
    errors: list[str] = []
    _check_schema_node(event, contract, contract, "$", errors)
    if not errors:
        errors.extend(_check_editorial_contract(event))
    if errors:
        detail = "\n".join(f"- {item}" for item in errors)
        raise SignalValidationFailure(f"Signal event validation failed:\n{detail}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", nargs="+", type=Path, help="Signal event JSON file(s)")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    schema = load_json(args.schema)
    for path in args.events:
        event = load_json(path)
        validate_event(event, schema)
        print(f"Signal event valid: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
