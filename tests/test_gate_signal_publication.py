from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS / "gate_signal_publication.py"
SPEC = importlib.util.spec_from_file_location("gate_signal_publication", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def evidence(identifier: str, kind: str, quote: str, block: int) -> dict:
    return {
        "evidenceId": identifier,
        "type": kind,
        "source": {
            "sequence": 1,
            "role": "primary-document",
            "documentType": "8-K",
            "url": "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/filing.htm",
            "filename": "001-filing.htm",
            "sha256": "a" * 64,
        },
        "locator": {"block": block, "tag": "p"},
        "quote": quote,
    }


def analysis() -> dict:
    action = "The Board appointed Jane Doe and increased its size from 12 directors to 14 directors."
    condition = "The appointment is effective immediately and remains subject to shareholder approval."
    return {
        "schema": "mypybite.signal-materiality-analysis.v1",
        "analysisId": "analysis:w5h:sec:0000000001-26-000001:0000000001",
        "briefId": "w5h:sec:0000000001-26-000001:0000000001",
        "analyzedAt": "2026-10-23T19:00:00Z",
        "status": "private-analysis",
        "publicReleaseAllowed": False,
        "issuer": {"cik": "0000000001", "legalName": "EXAMPLE CORP", "ticker": "EX"},
        "filing": {"accessionNumber": "0000000001-26-000001", "form": "8-K", "acceptedAt": "2026-10-21T12:00:00-04:00"},
        "sourceDraft": {"sha256": "b" * 64},
        "headlineCandidate": "EXAMPLE CORP expands board from 12 to 14 and appoints Jane Doe",
        "synopsisCandidate": action + " " + condition,
        "readerMeaning": {
            "plainLanguageSummary": action,
            "whyItMatters": "The disclosure changes who holds oversight responsibility, but it does not establish the effect on strategy or results.",
            "caution": "The filing establishes an action, not its eventual business or market outcome.",
        },
        "classification": {
            "category": "leadership-and-governance",
            "categoryMatches": ["appointed", "board of directors"],
            "direction": "indeterminate",
            "directionMatches": [],
            "materiality": "important",
            "materialityScore": 60,
            "materialityReasons": ["board or governance change", "disclosed quantity"],
            "confidence": "high",
            "confidenceReason": "All six W5H dimensions have source-linked support.",
            "structuralOddity": {"flag": False, "explanation": None},
        },
        "implications": [
            {
                "audience": "company",
                "text": "The filing establishes the personnel action but does not establish its effect on strategy or results.",
                "evidenceIds": ["ev-action", "ev-condition"],
                "caveat": "This is not a prediction of market price or operating results.",
            }
        ],
        "unknowns": [],
        "watchNext": [
            {
                "checkpoint": "Confirm the outcome associated with the disclosed deadline.",
                "dueAt": "2026-10-31",
                "evidenceToSeek": "A later filing confirming the appointment and resulting governance change.",
                "evidenceIds": ["ev-condition"],
            }
        ],
        "w5h": {
            "who": [{"text": "EXAMPLE CORP", "role": "SEC registrant", "status": "verified-context", "evidenceIds": ["ev-action"]}],
            "what": {"status": "disclosed", "text": action, "evidenceIds": ["ev-action", "ev-quantity"]},
            "when": [{"text": "October 31, 2026", "value": "2026-10-31", "status": "deadline", "evidenceIds": ["ev-condition"]}],
            "where": {"status": "disclosed", "text": "The action changes the Board of Directors.", "evidenceIds": ["ev-action"]},
            "why": {"status": "disclosed", "text": "The company said the appointment would strengthen oversight.", "evidenceIds": ["ev-action"]},
            "how": {"status": "disclosed", "text": condition, "evidenceIds": ["ev-condition"]},
        },
        "evidence": [
            evidence("ev-action", "action", action, 1),
            evidence("ev-quantity", "quantity", action, 1),
            evidence("ev-condition", "condition", condition, 2),
        ],
        "quality": {
            "classificationRuleMatched": True,
            "materialityRuleMatched": True,
            "sourceEvidencePreserved": True,
            "semanticAnalysisComplete": True,
            "tradingRecommendationPresent": False,
            "publicationGatePassed": False,
        },
    }


class PublicationGateTests(unittest.TestCase):
    def test_complete_important_event_is_automatically_approved(self) -> None:
        result = MODULE.gate_analysis(
            analysis(), now=datetime(2026, 10, 23, 20, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(result["decision"], "auto-approved")
        self.assertTrue(result["publicReleaseAllowed"])
        self.assertEqual(result["event"]["quality"]["reviewMethod"], "automated")
        self.assertEqual(set(result["event"]["quality"]["automatedReviewChecks"]), MODULE.REQUIRED_AUTOMATED_CHECKS)

    def test_generated_event_passes_permanent_validator(self) -> None:
        result = MODULE.gate_analysis(analysis())
        MODULE.validate_event(result["event"])

    def test_low_confidence_event_is_quarantined(self) -> None:
        value = analysis()
        value["classification"]["confidence"] = "low"
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertIsNone(result["event"])
        self.assertTrue(any("confidence" in reason.lower() for reason in result["quarantineReasons"]))

    def test_unclassified_event_is_quarantined(self) -> None:
        value = analysis()
        value["classification"]["category"] = "other-disclosed-event"
        value["quality"]["classificationRuleMatched"] = False
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertTrue(any("category" in reason.lower() for reason in result["quarantineReasons"]))

    def test_non_sec_source_is_quarantined(self) -> None:
        value = analysis()
        value["evidence"][0]["source"]["url"] = "https://example.com/filing.htm"
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertTrue(any("official sec" in reason.lower() for reason in result["quarantineReasons"]))

    def test_invalid_generated_event_is_quarantined(self) -> None:
        value = analysis()
        value["w5h"]["where"]["evidenceIds"] = []
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertIsNone(result["event"])
        self.assertTrue(
            any("permanent validation" in reason.lower() for reason in result["quarantineReasons"])
        )

    def test_unsupported_form_is_quarantined(self) -> None:
        value = analysis()
        value["filing"]["form"] = "4"
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertTrue(
            any("form 8-k" in reason.lower() for reason in result["quarantineReasons"])
        )

    def test_filing_instruction_is_not_a_corporate_action(self) -> None:
        value = analysis()
        value["w5h"]["what"]["text"] = (
            "If the form is filed by more than one reporting person, see Instruction 4."
        )
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertTrue(
            any("concrete corporate action" in reason.lower() for reason in result["quarantineReasons"])
        )

    def test_stale_action_in_new_filing_is_quarantined(self) -> None:
        value = analysis()
        value["w5h"]["what"]["text"] = (
            "On September 30, 2025, the Board appointed Jane Doe as a director."
        )
        result = MODULE.gate_analysis(value)
        self.assertEqual(result["decision"], "quarantined")
        self.assertTrue(
            any("current enough" in reason.lower() for reason in result["quarantineReasons"])
        )

    def test_file_entrypoint_writes_only_approved_event(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            path = directory / "materiality-analysis.json"
            path.write_text(json.dumps(analysis()), encoding="utf-8")
            result = MODULE.gate_file(path)
            self.assertEqual(result["decision"], "auto-approved")
            self.assertTrue((directory / "publication-decision.json").exists())
            self.assertTrue((directory / "auto-approved-event.json").exists())
            rejected = analysis()
            rejected["classification"]["confidence"] = "low"
            path.write_text(json.dumps(rejected), encoding="utf-8")
            MODULE.gate_file(path)
            self.assertFalse((directory / "auto-approved-event.json").exists())


if __name__ == "__main__":
    unittest.main()
