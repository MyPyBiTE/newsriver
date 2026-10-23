from __future__ import annotations

import copy
import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_signal_event.py"
SPEC = importlib.util.spec_from_file_location("validate_signal_event", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class SignalEventValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.event = json.loads(
            (ROOT / "signal-event.strategy.sample.json").read_text(encoding="utf-8")
        )
        cls.schema = json.loads(
            (ROOT / "schemas" / "signal-event.schema.json").read_text(encoding="utf-8")
        )

    def validate(self, event: dict) -> None:
        MODULE.validate_event(event, self.schema)

    def test_strategy_gold_standard_is_valid(self) -> None:
        self.validate(self.event)

    def test_generic_filing_boilerplate_is_rejected(self) -> None:
        event = copy.deepcopy(self.event)
        event["synopsis"] = "A current report flags a potentially material event between periodic reports."
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "actual event"):
            self.validate(event)

    def test_broken_evidence_reference_is_rejected(self) -> None:
        event = copy.deepcopy(self.event)
        event["w5h"]["what"]["evidenceIds"] = ["ev-does-not-exist"]
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "unknown evidenceId"):
            self.validate(event)

    def test_primary_evidence_is_required(self) -> None:
        event = copy.deepcopy(self.event)
        for source in event["sources"]:
            source["primary"] = False
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "primary source"):
            self.validate(event)

    def test_important_event_requires_human_review(self) -> None:
        event = copy.deepcopy(self.event)
        event["quality"]["humanReviewRequired"] = False
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "require human review"):
            self.validate(event)

    def test_publishable_event_requires_completed_review(self) -> None:
        event = copy.deepcopy(self.event)
        event["status"] = "publishable"
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "review is incomplete"):
            self.validate(event)

    def test_publishable_reviewed_event_is_valid(self) -> None:
        event = copy.deepcopy(self.event)
        event["status"] = "publishable"
        event["quality"]["humanReviewed"] = True
        self.validate(event)

    def test_complete_automated_review_can_publish_important_event(self) -> None:
        event = copy.deepcopy(self.event)
        event["status"] = "publishable"
        event["quality"].update(
            {
                "humanReviewRequired": False,
                "reviewMethod": "automated",
                "automatedReviewPassed": True,
                "automatedReviewChecks": sorted(MODULE.REQUIRED_AUTOMATED_CHECKS),
                "quarantineReasons": [],
            }
        )
        self.validate(event)

    def test_partial_automated_review_is_rejected(self) -> None:
        event = copy.deepcopy(self.event)
        event["status"] = "publishable"
        event["quality"].update(
            {
                "humanReviewRequired": False,
                "reviewMethod": "automated",
                "automatedReviewPassed": True,
                "automatedReviewChecks": ["official-sec-sources"],
                "quarantineReasons": [],
            }
        )
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "missing required checks"):
            self.validate(event)

    def test_not_disclosed_reason_must_be_plain(self) -> None:
        event = copy.deepcopy(self.event)
        event["w5h"]["why"] = {
            "status": "not-disclosed",
            "text": "The motivation remains contextual.",
            "evidenceIds": [],
        }
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "did not disclose why"):
            self.validate(event)

    def test_unexpected_fields_are_rejected(self) -> None:
        event = copy.deepcopy(self.event)
        event["sentimentScore"] = 99
        with self.assertRaisesRegex(MODULE.SignalValidationFailure, "unexpected field"):
            self.validate(event)


if __name__ == "__main__":
    unittest.main()
