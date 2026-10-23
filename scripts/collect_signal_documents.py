#!/usr/bin/env python3
"""Collect and verify primary SEC documents for queued Signal candidates.

The collector accepts only official SEC archive URLs inside the candidate's
exact accession directory. It saves the filing index, primary document and
non-XBRL exhibits with hashes in a private bundle; it never publishes events.
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
from typing import Iterable
from urllib.parse import parse_qs, unquote, urljoin, urlsplit, urlunsplit

from fetch_sec_intake import IntakeFailure, REQUEST_INTERVAL_SECONDS, SecClient


ROOT = Path(__file__).resolve().parents[1]
QUEUE_SCHEMA = "mypybite.signal-analysis-queue.v1"
CANDIDATE_SCHEMA = "mypybite.signal-analysis-candidate.v1"
BUNDLE_SCHEMA = "mypybite.signal-document-bundle.v1"
MAX_DOCUMENT_BYTES = 25_000_000
SAFE_FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class DocumentCollectionFailure(ValueError):
    """Raised when a document bundle cannot be verified safely."""


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


class DocumentTableParser(HTMLParser):
    """Read EDGAR's Document Format Files table without presentation assumptions."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.cell_text: list[str] = []
        self.cell_href: str | None = None
        self.row: list[dict[str, str | None]] = []
        self.documents: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "table" and attributes.get("summary") == "Document Format Files":
            self.in_table = True
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.row = []
        elif self.in_row and tag == "td":
            self.in_cell = True
            self.cell_text = []
            self.cell_href = None
        elif self.in_cell and tag == "a" and attributes.get("href"):
            self.cell_href = str(attributes["href"])

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.in_cell:
            self.row.append(
                {
                    "text": " ".join("".join(self.cell_text).split()),
                    "href": self.cell_href,
                }
            )
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if len(self.row) >= 4 and self.row[2].get("href"):
                sequence = str(self.row[0].get("text") or "").strip()
                document_type = str(self.row[3].get("text") or "").strip()
                if sequence.isdigit() and document_type:
                    self.documents.append(
                        {
                            "sequence": sequence,
                            "description": str(self.row[1].get("text") or ""),
                            "href": str(self.row[2]["href"]),
                            "documentType": document_type,
                        }
                    )
            self.in_row = False
            self.row = []
        elif tag == "table" and self.in_table:
            self.in_table = False


def parse_document_table(index_html: str) -> list[dict[str, str]]:
    parser = DocumentTableParser()
    parser.feed(index_html)
    if not parser.documents:
        raise DocumentCollectionFailure("SEC filing index has no Document Format Files.")
    return parser.documents


def accession_directory(index_url: str, accession: str) -> str:
    parsed = urlsplit(index_url)
    if parsed.scheme != "https" or parsed.hostname not in {"sec.gov", "www.sec.gov"}:
        raise DocumentCollectionFailure("Filing index is not an official HTTPS SEC URL.")
    compact = accession.replace("-", "")
    directory = parsed.path.rsplit("/", 1)[0] + "/"
    if not directory.startswith("/Archives/edgar/data/") or f"/{compact}/" not in directory:
        raise DocumentCollectionFailure("Filing index does not match the expected accession directory.")
    return directory


def unwrap_document_href(href: str) -> str:
    parsed = urlsplit(href)
    if parsed.path == "/ix":
        target = parse_qs(parsed.query).get("doc", [])
        if len(target) != 1:
            raise DocumentCollectionFailure("Malformed SEC inline-viewer document URL.")
        return unquote(target[0])
    return href


def verified_document_url(index_url: str, accession: str, href: str) -> str:
    expected_directory = accession_directory(index_url, accession)
    joined = urlsplit(urljoin(index_url, unwrap_document_href(href)))
    if joined.scheme != "https" or joined.hostname not in {"sec.gov", "www.sec.gov"}:
        raise DocumentCollectionFailure("Document URL leaves the official SEC host.")
    path = unquote(joined.path)
    if not path.startswith(expected_directory):
        raise DocumentCollectionFailure("Document URL leaves the expected accession directory.")
    filename = Path(path).name
    if not SAFE_FILENAME.fullmatch(filename) or filename in {".", ".."}:
        raise DocumentCollectionFailure("Document URL has an unsafe filename.")
    return urlunsplit(("https", "www.sec.gov", path, "", ""))


def selected_documents(rows: Iterable[dict[str, str]], form: str) -> list[dict[str, str]]:
    rows = list(rows)
    primary = [row for row in rows if row["documentType"].upper() == form.upper()]
    if not primary:
        raise DocumentCollectionFailure(f"SEC filing index lacks primary {form} document.")
    primary.sort(key=lambda row: int(row["sequence"]))
    selected = [{**primary[0], "role": "primary-document"}]
    exhibits = [
        row
        for row in rows
        if row["documentType"].upper().startswith("EX-")
        and not row["documentType"].upper().startswith("EX-101")
    ]
    exhibits.sort(key=lambda row: int(row["sequence"]))
    selected.extend({**row, "role": "exhibit"} for row in exhibits)
    return selected


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def atomic_write_json(path: Path, value: dict) -> None:
    atomic_write_bytes(
        path, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )


def fetch_bytes(client: SecClient, url: str) -> bytes:
    try:
        text = client.get_text(url)
    except IntakeFailure as exc:
        raise DocumentCollectionFailure(str(exc)) from exc
    if text is None:
        raise DocumentCollectionFailure(f"SEC document was not found: {url}")
    payload = text.encode("latin-1")
    if not payload or len(payload) > MAX_DOCUMENT_BYTES:
        raise DocumentCollectionFailure(f"SEC document has invalid size: {url}")
    return payload


def document_entry(
    *, sequence: int, role: str, document_type: str, description: str,
    url: str, filename: str, payload: bytes
) -> dict:
    suffix = Path(filename).suffix.lower()
    return {
        "sequence": sequence,
        "role": role,
        "documentType": document_type,
        "description": description,
        "url": url,
        "filename": filename,
        "mediaKind": "text" if suffix in {".htm", ".html", ".txt", ".xml", ".xhtml"} else "binary",
        "byteLength": len(payload),
        "sha256": sha256(payload),
    }


def collect_candidate(
    candidate: dict,
    output_root: Path,
    client: SecClient,
    now: datetime | None = None,
) -> dict:
    if candidate.get("schema") != CANDIDATE_SCHEMA:
        raise DocumentCollectionFailure("Unsupported Signal analysis candidate schema.")
    if candidate.get("analysis", {}).get("publicReleaseAllowed") is not False:
        raise DocumentCollectionFailure("Document collection accepts private candidates only.")
    accession = str(candidate.get("filing", {}).get("accessionNumber") or "")
    cik = str(candidate.get("issuer", {}).get("cik") or "")
    form = str(candidate.get("filing", {}).get("form") or "")
    index_url = str(candidate.get("filing", {}).get("indexUrl") or "")
    accession_directory(index_url, accession)

    collected_at = iso_utc(now or datetime.now(timezone.utc))
    bundle_dir = output_root / f"{accession}.{cik}"
    index_payload = fetch_bytes(client, index_url)
    try:
        index_html = index_payload.decode("latin-1")
    except UnicodeDecodeError as exc:
        raise DocumentCollectionFailure("SEC filing index is not readable text.") from exc
    rows = parse_document_table(index_html)
    chosen = selected_documents(rows, form)

    entries: list[dict] = []
    index_filename = f"{accession}-index.html"
    atomic_write_bytes(bundle_dir / index_filename, index_payload)
    entries.append(
        document_entry(
            sequence=0,
            role="filing-index",
            document_type="FILING-INDEX",
            description="SEC filing detail and document manifest",
            url=index_url,
            filename=index_filename,
            payload=index_payload,
        )
    )
    for row in chosen:
        url = verified_document_url(index_url, accession, row["href"])
        payload = fetch_bytes(client, url)
        filename = Path(urlsplit(url).path).name
        saved_name = f"{int(row['sequence']):03d}-{filename}"
        atomic_write_bytes(bundle_dir / saved_name, payload)
        entries.append(
            document_entry(
                sequence=int(row["sequence"]),
                role=row["role"],
                document_type=row["documentType"],
                description=row["description"],
                url=url,
                filename=saved_name,
                payload=payload,
            )
        )

    bundle = {
        "schema": BUNDLE_SCHEMA,
        "candidateId": candidate["candidateId"],
        "collectedAt": collected_at,
        "publicReleaseAllowed": False,
        "issuer": candidate["issuer"],
        "filing": {
            "accessionNumber": accession,
            "form": form,
            "acceptedAt": candidate["filing"].get("acceptedAt"),
        },
        "verification": {
            "officialSecHost": True,
            "accessionDirectoryMatched": True,
            "primaryDocumentPresent": True,
            "allDocumentsHashed": True,
        },
        "documents": entries,
    }
    atomic_write_json(bundle_dir / "manifest.json", bundle)
    return bundle


def load_queue(path: Path) -> dict:
    try:
        queue = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DocumentCollectionFailure(f"Invalid analysis queue: {path}") from exc
    if queue.get("schema") != QUEUE_SCHEMA or queue.get("publicReleaseAllowed") is not False:
        raise DocumentCollectionFailure("Collector requires a private Signal analysis queue.")
    if not isinstance(queue.get("candidates"), list):
        raise DocumentCollectionFailure("Analysis queue lacks candidates.")
    return queue


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queue", type=Path, default=ROOT / "private" / "signal-analysis-queue.json"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=ROOT / "private" / "signal-documents"
    )
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    parser.add_argument("--request-interval", type=float, default=REQUEST_INTERVAL_SECONDS)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    queue = load_queue(args.queue)
    client = SecClient(args.user_agent, args.request_interval)
    bundles = [
        collect_candidate(candidate, args.output_dir, client)
        for candidate in queue["candidates"]
    ]
    print(f"Collected {len(bundles)} private SEC document bundles: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
