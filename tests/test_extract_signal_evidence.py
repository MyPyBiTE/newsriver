from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SCRIPT = SCRIPTS / "extract_signal_evidence.py"
SPEC = importlib.util.spec_from_file_location("extract_signal_evidence", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


PRIMARY = b"""<!doctype html><html><body>
<h2>Item 5.02</h2>
<p>On October 21, 2026, the Board appointed Jane Doe effective immediately.</p>
<p>The Board increased its size from 12 directors to 14 directors.</p>
<p>The award remains subject to shareholder approval by October 31, 2026.</p>
</body></html>"""
EXHIBIT = b"""<html><body>
<p>Jane Doe was appointed to strengthen oversight of long-term strategy.</p>
</body></html>"""
BINARY = b"%PDF-test-binary"


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def entry(sequence: int, role: str, filename: str, payload: bytes, media: str) -> dict:
    return {
        "sequence": sequence,
        "role": role,
        "documentType": "8-K" if role == "primary-document" else "EX-99.1",
        "description": role,
        "url": f"https://www.sec.gov/Archives/edgar/data/1/000000000126000001/{filename}",
        "filename": filename,
        "mediaKind": media,
        "byteLength": len(payload),
        "sha256": digest(payload),
    }


def make_bundle(directory: Path, include_primary: bool = True) -> Path:
    documents = []
    if include_primary:
        (directory / "001-primary.htm").write_bytes(PRIMARY)
        documents.append(entry(1, "primary-document", "001-primary.htm", PRIMARY, "text"))
    (directory / "002-exhibit.htm").write_bytes(EXHIBIT)
    documents.append(entry(2, "exhibit", "002-exhibit.htm", EXHIBIT, "text"))
    (directory / "003-attachment.pdf").write_bytes(BINARY)
    documents.append(entry(3, "exhibit", "003-attachment.pdf", BINARY, "binary"))
    manifest = {
        "schema": "mypybite.signal-document-bundle.v1",
        "candidateId": "sec:0000000001-26-000001:0000000001",
        "collectedAt": "2026-10-23T16:00:00Z",
        "publicReleaseAllowed": False,
        "issuer": {"cik": "0000000001", "legalName": "EXAMPLE CORP", "ticker": "EX"},
        "filing": {"accessionNumber": "0000000001-26-000001", "form": "8-K", "acceptedAt": "2026-10-21T12:00:00-04:00"},
        "verification": {
            "officialSecHost": True,
            "accessionDirectoryMatched": True,
            "primaryDocumentPresent": True,
            "allDocumentsHashed": True,
        },
        "documents": documents,
    }
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


class EvidenceExtractionTests(unittest.TestCase):
    def test_extracts_action_date_quantity_condition_and_reason(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            packet = MODULE.extract_bundle(
                make_bundle(directory),
                now=datetime(2026, 10, 23, 17, 0, tzinfo=timezone.utc),
            )
            types = {item["type"] for item in packet["evidence"]}
            self.assertTrue({"action", "date", "quantity", "condition", "stated-reason", "item-heading"}.issubset(types))
            self.assertFalse(packet["publicReleaseAllowed"])
            self.assertTrue(packet["verification"]["allDocumentHashesVerified"])

    def test_every_fact_preserves_exact_source_linkage(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            packet = MODULE.extract_bundle(make_bundle(directory))
            for fact in packet["evidence"]:
                self.assertTrue(fact["quote"])
                self.assertTrue(fact["source"]["url"].startswith("https://www.sec.gov/"))
                self.assertEqual(len(fact["source"]["sha256"]), 64)
                self.assertGreaterEqual(fact["locator"]["block"], 1)

    def test_hash_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            manifest = make_bundle(directory)
            (directory / "001-primary.htm").write_text("tampered", encoding="utf-8")
            with self.assertRaisesRegex(MODULE.EvidenceExtractionFailure, "integrity"):
                MODULE.extract_bundle(manifest)

    def test_missing_primary_text_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            manifest = make_bundle(directory, include_primary=False)
            with self.assertRaisesRegex(MODULE.EvidenceExtractionFailure, "readable primary"):
                MODULE.extract_bundle(manifest)

    def test_binary_document_is_explicitly_skipped(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as name:
            directory = Path(name)
            packet = MODULE.extract_bundle(make_bundle(directory))
            result = next(item for item in packet["documents"] if item["filename"].endswith(".pdf"))
            self.assertEqual(result["status"], "skipped-binary")
            self.assertEqual(result["evidenceCount"], 0)


if __name__ == "__main__":
    unittest.main()
