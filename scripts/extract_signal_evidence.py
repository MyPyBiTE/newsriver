#!/usr/bin/env python3
"""Extract source-linked factual evidence from private SEC document bundles.

This stage is deliberately narrower than editorial analysis. It verifies the
collector's hashes, turns readable filing documents into ordered text blocks,
and labels passages that contain actions, dates, quantities, conditions,
stated reasons, or SEC item headings. It never writes a public Signal event.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUNDLE_SCHEMA = "mypybite.signal-document-bundle.v1"
EVIDENCE_SCHEMA = "mypybite.signal-evidence-packet.v1"
TEXT_SUFFIXES = {".htm", ".html", ".txt", ".xml", ".xhtml"}
BLOCK_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td"}

MONTHS = (
    r"January|February|March|April|May|June|July|August|September|October|"
    r"November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|"
    r"Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\."
)
DATE_PATTERN = re.compile(
    rf"\b(?:{MONTHS})\s+\d{{1,2}}(?:,\s*\d{{4}})?\b|\b\d{{4}}-\d{{2}}-\d{{2}}\b",
    re.IGNORECASE,
)
QUANTITY_PATTERN = re.compile(
    r"(?:\$\s?\d[\d,.]*(?:\s?(?:million|billion|thousand|M|B))?"
    r"|\b\d+(?:\.\d+)?\s?(?:%|percent|per cent|shares?|directors?|officers?)\b"
    r"|\bfrom\s+\d[\d,.]*\s+(?:to|through)\s+\d[\d,.]*\b)",
    re.IGNORECASE,
)
ACTION_PATTERN = re.compile(
    r"\b(?:appoint(?:ed|s|ing)?|elect(?:ed|s|ing)?|resign(?:ed|s|ing)?|"
    r"declar(?:ed|es|ing)|approv(?:ed|es|ing)|authoriz(?:ed|es|ing)|"
    r"acquir(?:ed|es|ing)|sell|sold|issu(?:ed|es|ing)|enter(?:ed|s|ing)?|"
    r"terminat(?:ed|es|ing)|complet(?:ed|es|ing)|increas(?:ed|es|ing)|"
    r"reduc(?:ed|es|ing)|recommend(?:ed|s|ing)?|filed|amend(?:ed|s|ing)?)\b",
    re.IGNORECASE,
)
CONDITION_PATTERN = re.compile(
    r"\b(?:subject to|condition(?:al|ed)? upon|provided that|unless|if|"
    r"contingent upon|at or prior to|in no event)\b",
    re.IGNORECASE,
)
REASON_PATTERN = re.compile(
    r"\b(?:because|in order to|for the purpose of|so that|will help|"
    r"to strengthen|to support|to enable|to finance|to fund)\b",
    re.IGNORECASE,
)
ITEM_PATTERN = re.compile(r"\bitem\s+\d+(?:\.\d+)?\b", re.IGNORECASE)


class EvidenceExtractionFailure(ValueError):
    """Raised when a private document bundle cannot be trusted or read."""


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


class FilingTextParser(HTMLParser):
    """Collect meaningful filing blocks while ignoring script and style content."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ignored_depth = 0
        self.active_tag: str | None = None
        self.parts: list[str] = []
        self.blocks: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"}:
            self.ignored_depth += 1
            return
        if not self.ignored_depth and tag in BLOCK_TAGS:
            self._flush()
            self.active_tag = tag

    def handle_data(self, data: str) -> None:
        if not self.ignored_depth and self.active_tag:
            self.parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self.ignored_depth:
            self.ignored_depth -= 1
            return
        if not self.ignored_depth and tag == self.active_tag:
            self._flush()

    def close(self) -> None:
        super().close()
        self._flush()

    def _flush(self) -> None:
        text = " ".join("".join(self.parts).split())
        if text:
            self.blocks.append({"tag": self.active_tag or "text", "text": text})
        self.active_tag = None
        self.parts = []


def decode_text(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "windows-1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise EvidenceExtractionFailure("SEC text document cannot be decoded.")


def text_blocks(payload: bytes, suffix: str) -> list[dict[str, str]]:
    text = decode_text(payload)
    if suffix.lower() in {".htm", ".html", ".xml", ".xhtml"}:
        parser = FilingTextParser()
        parser.feed(text)
        parser.close()
        return parser.blocks
    return [
        {"tag": "text", "text": " ".join(block.split())}
        for block in re.split(r"\n\s*\n", text)
        if block.strip()
    ]


def evidence_types(tag: str, text: str) -> list[str]:
    types: list[str] = []
    if tag.startswith("h") and ITEM_PATTERN.search(text):
        types.append("item-heading")
    elif ITEM_PATTERN.fullmatch(text.rstrip(". ")):
        types.append("item-heading")
    if ACTION_PATTERN.search(text):
        types.append("action")
    if DATE_PATTERN.search(text):
        types.append("date")
    if QUANTITY_PATTERN.search(text):
        types.append("quantity")
    if CONDITION_PATTERN.search(text):
        types.append("condition")
    if REASON_PATTERN.search(text):
        types.append("stated-reason")
    return types


def load_manifest(path: Path) -> tuple[dict, bytes]:
    try:
        payload = path.read_bytes()
        manifest = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceExtractionFailure(f"Invalid document bundle manifest: {path}") from exc
    if manifest.get("schema") != BUNDLE_SCHEMA:
        raise EvidenceExtractionFailure("Unsupported Signal document bundle schema.")
    if manifest.get("publicReleaseAllowed") is not False:
        raise EvidenceExtractionFailure("Evidence extraction accepts private bundles only.")
    documents = manifest.get("documents")
    if not isinstance(documents, list) or not documents:
        raise EvidenceExtractionFailure("Document bundle has no manifest entries.")
    return manifest, payload


def verified_payload(bundle_dir: Path, entry: dict) -> bytes:
    filename = entry.get("filename")
    if not isinstance(filename, str) or not filename or Path(filename).name != filename:
        raise EvidenceExtractionFailure("Document manifest contains an unsafe filename.")
    path = (bundle_dir / filename).resolve()
    try:
        path.relative_to(bundle_dir.resolve())
        payload = path.read_bytes()
    except (ValueError, OSError) as exc:
        raise EvidenceExtractionFailure(f"Bundle document is missing or unsafe: {filename}") from exc
    if len(payload) != entry.get("byteLength") or sha256(payload) != entry.get("sha256"):
        raise EvidenceExtractionFailure(f"Bundle document failed integrity verification: {filename}")
    return payload


def source_reference(entry: dict) -> dict:
    return {
        "sequence": entry["sequence"],
        "role": entry["role"],
        "documentType": entry["documentType"],
        "url": entry["url"],
        "filename": entry["filename"],
        "sha256": entry["sha256"],
    }


def extract_bundle(
    manifest_path: Path,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict:
    manifest, manifest_payload = load_manifest(manifest_path)
    bundle_dir = manifest_path.parent
    evidence: list[dict] = []
    document_results: list[dict] = []
    primary_text_present = False

    for entry in manifest["documents"]:
        payload = verified_payload(bundle_dir, entry)
        suffix = Path(entry["filename"]).suffix.lower()
        media_kind = entry.get("mediaKind")
        if media_kind != "text" or suffix not in TEXT_SUFFIXES:
            document_results.append(
                {
                    "filename": entry["filename"],
                    "role": entry["role"],
                    "status": "skipped-binary",
                    "blockCount": 0,
                    "evidenceCount": 0,
                }
            )
            continue

        blocks = text_blocks(payload, suffix)
        if entry["role"] == "primary-document" and blocks:
            primary_text_present = True
        before = len(evidence)
        if entry["role"] != "filing-index":
            for paragraph, block in enumerate(blocks, start=1):
                quote = block["text"][:2000]
                for evidence_type in evidence_types(block["tag"], quote):
                    evidence.append(
                        {
                            "evidenceId": f"ev-{len(evidence) + 1:04d}",
                            "type": evidence_type,
                            "source": source_reference(entry),
                            "locator": {
                                "block": paragraph,
                                "tag": block["tag"],
                            },
                            "quote": quote,
                            "normalizedFact": quote,
                            "extractionMethod": "deterministic-pattern-v1",
                        }
                    )
        document_results.append(
            {
                "filename": entry["filename"],
                "role": entry["role"],
                "status": "extracted",
                "blockCount": len(blocks),
                "evidenceCount": len(evidence) - before,
            }
        )

    if not primary_text_present:
        raise EvidenceExtractionFailure("Bundle lacks a readable primary SEC document.")

    packet = {
        "schema": EVIDENCE_SCHEMA,
        "packetId": f"evidence:{manifest['candidateId']}",
        "candidateId": manifest["candidateId"],
        "extractedAt": iso_utc(now or datetime.now(timezone.utc)),
        "publicReleaseAllowed": False,
        "issuer": manifest["issuer"],
        "filing": manifest["filing"],
        "sourceBundle": {
            "collectedAt": manifest["collectedAt"],
            "manifestSha256": sha256(manifest_payload),
        },
        "verification": {
            "manifestPrivate": True,
            "allDocumentHashesVerified": True,
            "primaryTextPresent": True,
            "sourceLinksPreserved": True,
        },
        "documents": document_results,
        "evidence": evidence,
    }
    atomic_write_json(output_path or bundle_dir / "evidence.json", packet)
    return packet


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifests",
        nargs="*",
        type=Path,
        help="Private document bundle manifest paths. Defaults to all collected bundles.",
    )
    parser.add_argument(
        "--documents-dir", type=Path, default=ROOT / "private" / "signal-documents"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    manifests = args.manifests or sorted(args.documents_dir.glob("*/manifest.json"))
    if not manifests:
        raise EvidenceExtractionFailure("No private SEC document bundle manifests found.")
    packets = [extract_bundle(path) for path in manifests]
    evidence_count = sum(len(packet["evidence"]) for packet in packets)
    print(
        f"Extracted {evidence_count} source-linked facts from "
        f"{len(packets)} private SEC document bundles."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
