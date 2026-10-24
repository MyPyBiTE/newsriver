#!/usr/bin/env python3
"""Build a browser-safe MYPYBITE Signal payload from SEC research artifacts."""

from __future__ import annotations

import argparse
import json
from collections import OrderedDict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_FILING_LIMIT = 500


FORM_GUIDE = {
    "8-K": {
        "label": "Material event",
        "readerNote": "An 8-K reports a material event between regular reports. The economics depend on the item: financing can change dilution or leverage; leadership changes can alter execution risk; acquisitions can change growth and integration risk.",
    },
    "10-Q": {
        "label": "Quarterly fundamentals",
        "readerNote": "A 10-Q updates revenue, margins, cash flow, liquidity and risks. Compare the trend with prior quarters and management guidance rather than treating the filing itself as bullish or bearish.",
    },
    "10-K": {
        "label": "Annual fundamentals",
        "readerNote": "A 10-K is the annual operating and risk record. It is useful for testing the narrative against audited numbers, obligations, segment performance and disclosed risks.",
    },
    "4": {
        "label": "Insider transaction",
        "readerNote": "Form 4 reports insider ownership changes. Open-market buys can be informative, but grants, tax withholding and planned sales have different economics. Read the transaction code before drawing a conclusion.",
    },
    "13D": {
        "label": "Active ownership",
        "readerNote": "Schedule 13D usually signals a holder above 5% with potential influence or control intent. Review the purpose-of-transaction section and later amendments.",
    },
    "13G": {
        "label": "Large ownership",
        "readerNote": "Schedule 13G reports a large, generally passive ownership position. It establishes who owns size, but does not by itself imply activism or a directional trade.",
    },
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []

    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def build_payload(
    root: Path = ROOT,
    filings_path: Path | None = None,
    metadata_path: Path | None = None,
) -> dict:
    roster = read_json(root / "config" / "management-roster.json")
    discovery = read_json(root / "config" / "leadership-discovery.json")
    scorecard = read_json(root / "config" / "management-scorecard.json")

    filings = read_jsonl(
        filings_path or root / "live-latest-output" / "filings.jsonl"
    )
    metadata = read_jsonl(
        metadata_path or root / "live-latest-metadata" / "filings.jsonl"
    )

    roster_by_cik = {
        issuer["cik"]: issuer
        for issuer in roster["issuers"]
    }

    metadata_by_accession = {
        item["accessionNumber"]: item
        for item in metadata
    }

    deduped: OrderedDict[str, dict] = OrderedDict()

    for filing in filings:
        if filing.get("role") == "Reporting":
            continue

        key = f'{filing.get("accessionNumber")}:{filing.get("cik")}'

        if key in deduped:
            continue

        form = filing.get("form", "Other")
        guide_key = (
            "13D"
            if "13D" in form
            else "13G"
            if "13G" in form
            else form
        )

        guide = FORM_GUIDE.get(
            guide_key,
            {
                "label": "Regulatory filing",
                "readerNote": "Use the linked SEC source to identify the filing purpose, then compare it with prior disclosures and the issuer's capital structure.",
            },
        )

        meta = metadata_by_accession.get(
            filing.get("accessionNumber"),
            {},
        )

        issuer = roster_by_cik.get(filing.get("cik"))

        deduped[key] = {
            "accessionNumber": filing.get("accessionNumber"),
            "cik": filing.get("cik"),
            "companyName": filing.get("companyName"),
            "ticker": issuer.get("ticker") if issuer else None,
            "form": form,
            "filedDate": filing.get("filedDate"),
            "acceptedAt": meta.get("acceptedAt"),
            "items": meta.get("items", []),
            "url": filing.get("filingIndexUrl"),
            "signalLabel": guide["label"],
            "readerNote": guide["readerNote"],
            "coverage": "roster" if issuer else "market-wide",
        }

    public_issuers = []
    candidate_by_ticker: dict[str, list[dict]] = {}

    for seat in discovery["seats"]:
        if seat.get("status") != "candidate-found":
            continue

        candidate_by_ticker.setdefault(
            seat["ticker"],
            [],
        ).extend(
            {
                "seat": seat["seat"],
                "name": candidate["canonicalName"],
                "reportedTitle": candidate["reportedTitle"],
                "lastObservedDate": candidate["lastObservedDate"],
                "sourceUrl": candidate["sourceUrl"],
                "status": "candidate-from-form4",
                "evaluationEligible": False,
                "confidence": "D",
            }
            for candidate in seat.get("candidates", [])
        )

    for issuer in roster["issuers"]:
        public_issuers.append(
            {
                "ticker": issuer["ticker"],
                "cik": issuer["cik"],
                "legalName": issuer["legalName"],
                "cohort": issuer["cohort"],
                "relevance": issuer["relevance"],
                "leadershipStatus": issuer["leadershipStatus"],
                "leadershipCandidates": candidate_by_ticker.get(
                    issuer["ticker"],
                    [],
                ),
            }
        )

    public_filings = sorted(
        deduped.values(),
        key=lambda item: (
            item.get("filedDate") or "",
            item.get("acceptedAt") or "",
            item.get("accessionNumber") or "",
        ),
        reverse=True,
    )[:PUBLIC_FILING_LIMIT]

    return {
        "schema": "mypybite.signal-preview.v1",
        "generatedAt": discovery["generatedAt"],
        "leadershipGeneratedAt": discovery["generatedAt"],
        "disclaimer": "Research preview. Leadership candidates are not confirmed appointments and are not scored. Filing explanations are educational context, not investment advice.",
        "stats": {
            "issuerCount": roster["issuerCount"],
            "seatCount": discovery["seatCount"],
            "candidateSeatCount": discovery["candidateSeatCount"],
            "unresolvedSeatCount": discovery["unresolvedSeatCount"],
            "maximumManagementScore": scorecard["maximumScore"],
            "availableFilingCount": len(deduped),
            "publishedFilingCount": len(public_filings),
        },
        "scorecard": {
            "maximumScore": scorecard["maximumScore"],
            "categories": scorecard["categories"],
            "confidence": scorecard["confidence"],
            "rules": scorecard["rules"],
        },
        "filings": public_filings,
        "issuers": public_issuers,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT.parent / "signal-data.json",
    )
    args = parser.parse_args()

    payload = build_payload()

    args.output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    args.output.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    js_output = args.output.with_suffix(".js")

    js_output.write_text(
        "window.MYPYBITE_SIGNAL_DATA = "
        + json.dumps(payload, separators=(",", ":"))
        + ";\n",
        encoding="utf-8",
    )

    print(
        f"Wrote {args.output} and {js_output} "
        f"({len(payload['filings'])} filings, "
        f"{len(payload['issuers'])} issuers)"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
