from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "analyze_signal_materiality.py"
SPEC = importlib.util.spec_from_file_location("analyze_signal_materiality", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def draft() -> dict:
    action = "On October 21, 2026, the Board of Directors appointed Jane Doe and increased the board from 12 directors to 14 directors."
    condition = "The appointment is effective immediately and remains subject to shareholder approval by October 31, 2026."
    reason = "The appointment will help strengthen oversight of long-term strategy."
    evidence = [
        {"evidenceId": "ev-action", "type": "action"},
        {"evidenceId": "ev-quantity", "type": "quantity"},
        {"evidenceId": "ev-condition", "type": "condition"},
        {"evidenceId": "ev-reason", "type": "stated-reason"},
    ]
    return {
        "schema": "mypybite.signal-w5h-draft.v1",
        "briefId": "w5h:sec:0000000001-26-000001:0000000001",
        "packetId": "evidence:sec:0000000001-26-000001:0000000001",
        "assembledAt": "2026-10-23T18:00:00Z",
        "status": "private-draft",
        "publicReleaseAllowed": False,
        "issuer": {"cik": "0000000001", "legalName": "EXAMPLE CORP", "ticker": "EX"},
        "filing": {"accessionNumber": "0000000001-26-000001", "form": "8-K", "acceptedAt": "2026-10-21T12:00:00-04:00"},
        "sourcePacket": {"sha256": "a" * 64},
        "headlineCandidate": "EXAMPLE CORP: Board appoints Jane Doe and expands from 12 to 14 directors",
        "synopsisCandidate": action + " " + condition,
        "w5h": {
            "who": [{"text": "EXAMPLE CORP", "role": "SEC registrant", "status": "verified-context", "evidenceIds": ["ev-action"]}],
            "what": {"status": "disclosed", "text": action, "evidenceIds": ["ev-action", "ev-quantity"]},
            "when": [{"text": "October 31, 2026", "value": "2026-10-31", "status": "deadline", "evidenceIds": ["ev-condition"]}],
            "where": {"status": "disclosed", "text": "The action changes the Board of Directors.", "evidenceIds": ["ev-action"]},
            "why": {"status": "disclosed", "text": reason, "evidenceIds": ["ev-reason"]},
            "how": {"status": "disclosed", "text": condition, "evidenceIds": ["ev-condition"]},
        },
        "unknowns": [],
        "quality": {
            "w5hFieldsPresent": True,
            "dimensionsSupported": ["who", "what", "when", "where", "why", "how"],
            "dimensionsNotDisclosed": [],
            "usesExactSourceLanguage": True,
            "requiresSemanticAnalysis": True,
            "publicationGatePassed": False,
        },
        "evidence": evidence,
    }


class MaterialityAnalysisTests(unittest.TestCase):
    def test_classifies_governance_action_and_materiality(self) -> None:
        analysis = MODULE.analyze_materiality(
            draft(), now=datetime(2026, 10, 23, 19, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(analysis["classification"]["category"], "leadership-and-governance")
        self.assertEqual(analysis["classification"]["materiality"], "important")
        self.assertEqual(analysis["classification"]["direction"], "indeterminate")
        self.assertEqual(analysis["classification"]["confidence"], "high")

    def test_creates_measured_implications_without_a_trade_call(self) -> None:
        analysis = MODULE.analyze_materiality(draft())
        combined = " ".join(item["text"] for item in analysis["implications"]).lower()
        self.assertIn("does not establish", combined)
        self.assertNotRegex(combined, r"\b(?:buy|sell|hold|bullish|bearish)\b")
        self.assertFalse(analysis["quality"]["tradingRecommendationPresent"])

    def test_uses_disclosed_deadline_for_watch_next(self) -> None:
        analysis = MODULE.analyze_materiality(draft())
        checkpoint = analysis["watchNext"][0]
        self.assertEqual(checkpoint["dueAt"], "2026-10-31")
        self.assertIn("deadline", checkpoint["checkpoint"])
        self.assertEqual(checkpoint["evidenceIds"], ["ev-condition"])

    def test_missing_context_reduces_confidence_without_guessing(self) -> None:
        value = draft()
        value["quality"]["dimensionsNotDisclosed"] = ["why", "where"]
        value["w5h"]["why"] = {"status": "not-disclosed", "text": "The company did not disclose why.", "evidenceIds": []}
        value["w5h"]["where"] = {"status": "not-disclosed", "text": "Not disclosed.", "evidenceIds": []}
        analysis = MODULE.analyze_materiality(value)
        self.assertEqual(analysis["classification"]["confidence"], "moderate")

    def test_generic_filing_boilerplate_is_rejected(self) -> None:
        value = draft()
        value["synopsisCandidate"] = "A current report flags a potentially material event between periodic reports."
        with self.assertRaisesRegex(MODULE.MaterialityAnalysisFailure, "prohibited filing boilerplate"):
            MODULE.analyze_materiality(value)

    def test_analysis_remains_private_and_file_entrypoint_writes(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            source = directory / "w5h-draft.json"
            source.write_text(json.dumps(draft()), encoding="utf-8")
            analysis = MODULE.analyze_file(source)
            saved = json.loads((directory / "materiality-analysis.json").read_text(encoding="utf-8"))
            self.assertFalse(analysis["publicReleaseAllowed"])
            self.assertFalse(saved["quality"]["publicationGatePassed"])
            self.assertEqual(saved["status"], "private-analysis")


if __name__ == "__main__":
    unittest.main()
