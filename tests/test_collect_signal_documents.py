from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS / "collect_signal_documents.py"
SPEC = importlib.util.spec_from_file_location("collect_signal_documents", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


ACCESSION = "0000015847-26-000051"
COMPACT = ACCESSION.replace("-", "")
INDEX_URL = (
    f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/"
    f"{ACCESSION}-index.html"
)
PRIMARY_URL = (
    f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/buks-20261007.htm"
)
EXHIBIT_URL = (
    f"https://www.sec.gov/Archives/edgar/data/15847/{COMPACT}/"
    "bnc-stockbuybackpressrelea.htm"
)


class FakeClient:
    def __init__(self, responses: dict[str, str]) -> None:
        self.responses = responses
        self.requests: list[str] = []

    def get_text(self, url: str):
        self.requests.append(url)
        return self.responses.get(url)


def candidate() -> dict:
    return {
        "schema": "mypybite.signal-analysis-candidate.v1",
        "candidateId": f"sec:{ACCESSION}:0000015847",
        "issuer": {
            "cik": "0000015847",
            "legalName": "BUTLER NATIONAL CORP",
            "ticker": "BUKS",
        },
        "filing": {
            "accessionNumber": ACCESSION,
            "form": "8-K",
            "acceptedAt": "2026-10-07T11:30:53-04:00",
            "indexUrl": INDEX_URL,
        },
        "analysis": {"publicReleaseAllowed": False},
    }


class DocumentCollectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.index_html = (ROOT / "sample-filing-index.html").read_text(
            encoding="utf-8"
        )
        self.client = FakeClient(
            {
                INDEX_URL: self.index_html,
                PRIMARY_URL: "<html><body>Primary filing</body></html>",
                EXHIBIT_URL: "<html><body>Exhibit 99.1</body></html>",
            }
        )

    def test_parses_primary_document_and_non_xbrl_exhibit(self) -> None:
        rows = MODULE.parse_document_table(self.index_html)
        selected = MODULE.selected_documents(rows, "8-K")
        self.assertEqual([item["role"] for item in selected], ["primary-document", "exhibit"])
        self.assertEqual(selected[0]["documentType"], "8-K")
        self.assertEqual(selected[1]["documentType"], "EX-99.1")

    def test_inline_viewer_url_is_unwrapped_and_confined(self) -> None:
        href = f"/ix?doc=/Archives/edgar/data/15847/{COMPACT}/buks-20261007.htm"
        self.assertEqual(
            MODULE.verified_document_url(INDEX_URL, ACCESSION, href), PRIMARY_URL
        )
        with self.assertRaisesRegex(MODULE.DocumentCollectionFailure, "accession directory"):
            MODULE.verified_document_url(
                INDEX_URL,
                ACCESSION,
                "/Archives/edgar/data/15847/other/hostile.htm",
            )

    def test_collects_private_hashed_bundle(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            bundle = MODULE.collect_candidate(
                candidate(),
                Path(directory),
                self.client,
                now=datetime(2026, 10, 23, 16, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(bundle["schema"], "mypybite.signal-document-bundle.v1")
            self.assertFalse(bundle["publicReleaseAllowed"])
            self.assertEqual(len(bundle["documents"]), 3)
            self.assertTrue(bundle["verification"]["allDocumentsHashed"])
            self.assertTrue(all(len(item["sha256"]) == 64 for item in bundle["documents"]))
            bundle_path = Path(directory) / f"{ACCESSION}.0000015847" / "manifest.json"
            self.assertTrue(bundle_path.exists())

    def test_missing_primary_document_fails_closed(self) -> None:
        stripped = self.index_html.replace(">8-K</td>", ">OTHER</td>")
        rows = MODULE.parse_document_table(stripped)
        with self.assertRaisesRegex(MODULE.DocumentCollectionFailure, "primary 8-K"):
            MODULE.selected_documents(rows, "8-K")

    def test_public_candidate_is_rejected(self) -> None:
        value = candidate()
        value["analysis"]["publicReleaseAllowed"] = True
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            with self.assertRaisesRegex(MODULE.DocumentCollectionFailure, "private candidates"):
                MODULE.collect_candidate(value, Path(directory), self.client)


if __name__ == "__main__":
    unittest.main()
