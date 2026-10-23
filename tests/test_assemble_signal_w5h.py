from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "assemble_signal_w5h.py"
SPEC = importlib.util.spec_from_file_location("assemble_signal_w5h", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def fact(identifier: str, kind: str, quote: str, block: int) -> dict:
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
        "normalizedFact": quote,
        "extractionMethod": "deterministic-pattern-v1",
    }


def packet() -> dict:
    action = "On October 21, 2026, the Board appointed Jane Doe and increased its size from 12 directors to 14 directors."
    condition = "The appointment is effective immediately and remains subject to shareholder approval by October 31, 2026."
    reason = "The appointment will help strengthen oversight of the company's long-term strategy."
    return {
        "schema": "mypybite.signal-evidence-packet.v1",
        "packetId": "evidence:sec:0000000001-26-000001:0000000001",
        "candidateId": "sec:0000000001-26-000001:0000000001",
        "extractedAt": "2026-10-23T17:00:00Z",
        "publicReleaseAllowed": False,
        "issuer": {"cik": "0000000001", "legalName": "EXAMPLE CORP", "ticker": "EX"},
        "filing": {"accessionNumber": "0000000001-26-000001", "form": "8-K", "acceptedAt": "2026-10-21T12:00:00-04:00"},
        "sourceBundle": {"collectedAt": "2026-10-23T16:00:00Z", "manifestSha256": "b" * 64},
        "verification": {
            "manifestPrivate": True,
            "allDocumentHashesVerified": True,
            "primaryTextPresent": True,
            "sourceLinksPreserved": True,
        },
        "documents": [],
        "evidence": [
            fact("ev-0001", "action", action, 1),
            fact("ev-0002", "date", action, 1),
            fact("ev-0003", "quantity", action, 1),
            fact("ev-0004", "action", condition, 2),
            fact("ev-0005", "condition", condition, 2),
            fact("ev-0006", "date", condition, 2),
            fact("ev-0007", "stated-reason", reason, 3),
        ],
    }


class W5HAssemblyTests(unittest.TestCase):
    def test_builds_all_six_dimensions_from_source_passages(self) -> None:
        brief = MODULE.assemble_w5h(
            packet(), now=datetime(2026, 10, 23, 18, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(brief["quality"]["dimensionsNotDisclosed"], [])
        self.assertEqual(set(brief["quality"]["dimensionsSupported"]), {"who", "what", "when", "where", "why", "how"})
        self.assertIn("12 directors to 14 directors", brief["w5h"]["what"]["text"])
        self.assertEqual(brief["w5h"]["why"]["status"], "disclosed")

    def test_dates_are_normalized_and_conditions_become_how(self) -> None:
        brief = MODULE.assemble_w5h(packet())
        values = {item["value"] for item in brief["w5h"]["when"]}
        self.assertIn("2026-10-21", values)
        self.assertIn("2026-10-31", values)
        self.assertIn("subject to shareholder approval", brief["w5h"]["how"]["text"])

    def test_missing_reason_is_stated_plainly_not_invented(self) -> None:
        value = packet()
        value["evidence"] = [item for item in value["evidence"] if item["type"] != "stated-reason"]
        brief = MODULE.assemble_w5h(value)
        self.assertEqual(brief["w5h"]["why"]["status"], "not-disclosed")
        self.assertIn("did not disclose why", brief["w5h"]["why"]["text"])
        self.assertIn("why", brief["quality"]["dimensionsNotDisclosed"])

    def test_brief_remains_private_and_carries_only_used_evidence(self) -> None:
        brief = MODULE.assemble_w5h(packet())
        self.assertFalse(brief["publicReleaseAllowed"])
        self.assertFalse(brief["quality"]["publicationGatePassed"])
        used = {item["evidenceId"] for item in brief["evidence"]}
        self.assertTrue(used)
        self.assertTrue(used.issubset({item["evidenceId"] for item in packet()["evidence"]}))

    def test_unverified_or_public_packet_is_rejected(self) -> None:
        public = packet()
        public["publicReleaseAllowed"] = True
        with self.assertRaisesRegex(MODULE.W5HAssemblyFailure, "private evidence"):
            MODULE.assemble_w5h(public)
        unverified = packet()
        unverified["verification"]["allDocumentHashesVerified"] = False
        with self.assertRaisesRegex(MODULE.W5HAssemblyFailure, "source verification"):
            MODULE.assemble_w5h(unverified)

    def test_file_entrypoint_writes_private_draft(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            source = directory / "evidence.json"
            source.write_text(json.dumps(packet()), encoding="utf-8")
            result = MODULE.assemble_file(source)
            saved = json.loads((directory / "w5h-draft.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["briefId"], result["briefId"])
            self.assertEqual(saved["status"], "private-draft")


if __name__ == "__main__":
    unittest.main()
