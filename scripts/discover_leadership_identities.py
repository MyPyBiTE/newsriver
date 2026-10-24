#!/usr/bin/env python3
"""Refresh CEO/CFO observations from official SEC Form 4 filings.

The output is deliberately conservative. A Form 4 can establish that a person
reported an officer title on a particular date; it cannot establish an
appointment date, continuing tenure, or management quality.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree

from fetch_sec_intake import (
    IntakeFailure,
    REQUEST_INTERVAL_SECONDS,
    SecClient,
    atomic_write_text,
)


SCHEMA = "mypybite.leadership-discovery.v1"
SEC_SUBMISSIONS = "https://data.sec.gov/submissions"
SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
ROLE_PATTERNS = {
    "chief-executive": re.compile(
        r"(?:\bceo\b|chief\s+executive|principal\s+executive)", re.IGNORECASE
    ),
    "chief-financial": re.compile(
        r"(?:\bcfo\b|chief\s+financial|principal\s+financial)", re.IGNORECASE
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def text_at(element: ElementTree.Element, path: str) -> str:
    return " ".join((element.findtext(path) or "").split())


def classify_title(title: str) -> set[str]:
    return {role for role, pattern in ROLE_PATTERNS.items() if pattern.search(title)}


def parse_form4_officers(xml_text: str) -> list[dict[str, object]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise IntakeFailure(f"Invalid Form 4 XML: {exc}") from exc

    period = text_at(root, "periodOfReport")
    issuer_cik = text_at(root, "issuer/issuerCik").zfill(10)
    results: list[dict[str, object]] = []
    for owner in root.findall("reportingOwner"):
        relationship = owner.find("reportingOwnerRelationship")
        if relationship is None or text_at(relationship, "isOfficer").lower() != "true":
            continue
        title = text_at(relationship, "officerTitle")
        roles = sorted(classify_title(title))
        if not roles:
            continue
        person_cik = text_at(owner, "reportingOwnerId/rptOwnerCik").zfill(10)
        name = text_at(owner, "reportingOwnerId/rptOwnerName")
        if not person_cik.isdigit() or not issuer_cik.isdigit() or not name:
            raise IntakeFailure("Form 4 leadership identity is incomplete")
        results.append(
            {
                "personCik": person_cik,
                "canonicalName": name,
                "reportedTitle": title,
                "roles": roles,
                "periodOfReport": period,
                "issuerCik": issuer_cik,
            }
        )
    return results


def recent_filings(submission: dict) -> Iterable[dict[str, str]]:
    recent = submission.get("filings", {}).get("recent", {})
    fields = ("form", "filingDate", "accessionNumber", "primaryDocument")
    arrays = {field: recent.get(field, []) for field in fields}
    if not all(isinstance(value, list) for value in arrays.values()):
        raise IntakeFailure("SEC submissions response has invalid recent filings")
    lengths = {len(value) for value in arrays.values()}
    if len(lengths) != 1:
        raise IntakeFailure("SEC submissions recent filing arrays are misaligned")
    for values in zip(*(arrays[field] for field in fields)):
        yield dict(zip(fields, (str(value) for value in values)))


def raw_primary_document(primary_document: str) -> str:
    return primary_document.rsplit("/", 1)[-1]


def latest_per_role(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    for role in ROLE_PATTERNS:
        matching = [item for item in candidates if item["role"] == role]
        if not matching:
            continue
        newest = max(str(item["lastObservedDate"]) for item in matching)
        selected.extend(
            sorted(
                (item for item in matching if item["lastObservedDate"] == newest),
                key=lambda item: (str(item["canonicalName"]), str(item["personCik"])),
            )[:2]
        )
    return selected


def discover_for_issuer(client: SecClient, issuer: dict, max_form4: int) -> list[dict[str, object]]:
    cik = str(issuer["cik"])
    submission_url = f"{SEC_SUBMISSIONS}/CIK{cik}.json"
    text = client.get_text(submission_url, missing_statuses=frozenset())
    if text is None:
        raise IntakeFailure(f"Missing SEC submissions response: {submission_url}")
    try:
        submission = json.loads(text)
    except json.JSONDecodeError as exc:
        raise IntakeFailure(f"Invalid SEC submissions JSON: {submission_url}") from exc

    found: dict[tuple[str, str], dict[str, object]] = {}
    inspected = 0
    for filing in recent_filings(submission):
        if filing["form"] not in {"4", "4/A"}:
            continue
        inspected += 1
        if inspected > max_form4:
            break
        accession = filing["accessionNumber"]
        compact = accession.replace("-", "")
        document = raw_primary_document(filing["primaryDocument"])
        url = f"{SEC_ARCHIVES}/{int(cik)}/{compact}/{document}"
        xml_text = client.get_text(url)
        if xml_text is None:
            continue
        for candidate in parse_form4_officers(xml_text):
            if candidate["issuerCik"] != cik:
                raise IntakeFailure(f"Issuer CIK mismatch in {url}")
            for role in candidate["roles"]:
                key = (role, str(candidate["personCik"]))
                observed = candidate["periodOfReport"] or filing["filingDate"]
                current = found.get(key)
                if current and str(current["lastObservedDate"]) >= str(observed):
                    continue
                found[key] = {
                    "personCik": candidate["personCik"],
                    "canonicalName": candidate["canonicalName"],
                    "reportedTitle": candidate["reportedTitle"],
                    "lastObservedDate": observed,
                    "accessionNumber": accession,
                    "sourceUrl": url,
                    "identityStatus": "candidate-from-form4",
                    "appointmentDate": None,
                    "appointmentDateStatus": "unresolved",
                    "evaluationEligible": False,
                    "confidence": "D",
                    "role": role,
                }
    return latest_per_role(list(found.values()))


def previous_by_seat(previous: dict | None) -> dict[tuple[str, str], list[dict[str, object]]]:
    result: dict[tuple[str, str], list[dict[str, object]]] = {}
    if not previous:
        return result
    if previous.get("schema") != SCHEMA:
        raise IntakeFailure("Previous leadership file has an unexpected schema")
    for seat in previous.get("seats", []):
        role = str(seat.get("seat") or "")
        ticker = str(seat.get("ticker") or "")
        normalized = []
        for candidate in seat.get("candidates") or []:
            item = dict(candidate)
            item.setdefault("role", role)
            normalized.append(item)
        result[(ticker, role)] = normalized
    return result


def build_discovery(
    roster: dict,
    client: SecClient,
    max_form4: int,
    previous: dict | None = None,
) -> dict:
    seats: list[dict[str, object]] = []
    old = previous_by_seat(previous)
    issuer_count = len(roster["issuers"])
    for index, issuer in enumerate(roster["issuers"], start=1):
        discovered = discover_for_issuer(client, issuer, max_form4)
        for role in ROLE_PATTERNS:
            candidates = [item for item in discovered if item["role"] == role]
            if not candidates:
                candidates = old.get((str(issuer["ticker"]), role), [])
            seats.append(
                {
                    "issuerCik": issuer["cik"],
                    "ticker": issuer["ticker"],
                    "legalName": issuer["legalName"],
                    "seat": role,
                    "status": "candidate-found" if candidates else "unresolved",
                    "candidates": candidates,
                }
            )
        labels = sorted({str(item["role"]) for item in discovered})
        print(
            f"[{index}/{issuer_count}] {issuer['ticker']}: "
            f"{', '.join(labels) if labels else 'no new principal-officer observation'}",
            flush=True,
        )

    resolved = sum(item["status"] == "candidate-found" for item in seats)
    unique_people = {
        str(candidate.get("personCik") or candidate.get("canonicalName"))
        for seat in seats
        for candidate in seat["candidates"]
    }
    return {
        "schema": SCHEMA,
        "generatedAt": utc_now_iso(),
        "rosterAsOf": roster["asOf"],
        "method": "Daily principal-officer observations from official Form 4 titles. Observations are not appointment or tenure confirmation.",
        "form4SearchDepth": max_form4,
        "seatCount": len(seats),
        "candidateSeatCount": resolved,
        "unresolvedSeatCount": len(seats) - resolved,
        "uniqueCandidatePeople": len(unique_people),
        "seats": seats,
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", type=Path, default=root / "config" / "management-roster.json")
    parser.add_argument("--output", type=Path, default=root / "config" / "leadership-discovery.json")
    parser.add_argument("--max-form4", type=int, default=30)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    args = parser.parse_args()
    if not 1 <= args.max_form4 <= 100:
        raise SystemExit("--max-form4 must be between 1 and 100")
    if not args.user_agent:
        raise SystemExit("SEC_USER_AGENT is required")
    roster = json.loads(args.roster.read_text(encoding="utf-8"))
    previous_path = args.previous or args.output
    previous = (
        json.loads(previous_path.read_text(encoding="utf-8"))
        if previous_path.exists()
        else None
    )
    result = build_discovery(
        roster,
        SecClient(args.user_agent, REQUEST_INTERVAL_SECONDS),
        args.max_form4,
        previous,
    )
    atomic_write_text(args.output, json.dumps(result, indent=2) + "\n")
    print(
        f"Leadership refresh complete: {result['candidateSeatCount']} of "
        f"{result['seatCount']} seats have observations."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
