#!/usr/bin/env python3
"""
MYPYBITE: SEC share-count stability validator.

Research-only tool.

Compares SEC-reported common shares outstanding at two dates.
Uses only information filed by each evaluation date.

Important:
- Shares outstanding are NOT free-float shares.
- A stable share count does NOT prove stable free float.
- This script does not modify MYPYBITE scores or public feeds.
"""

import argparse
import json
import os
import re
import sys
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SEC_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
SEC_CONCEPT = "EntityCommonStockSharesOutstanding"

ALLOWED_FORMS = {
    "10-K",
    "10-Q",
    "10-K/A",
    "10-Q/A",
}


def parse_date(value):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def extract_observations(payload):
    """Extract dated common-share observations from SEC facts."""

    concept = (
        payload.get("facts", {})
        .get("dei", {})
        .get(SEC_CONCEPT, {})
    )

    rows = concept.get("units", {}).get("shares", [])

    observations = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        if row.get("form") not in ALLOWED_FORMS:
            continue

        # Shares outstanding is an instant, not a duration.
        if "start" in row:
            continue

        measured = parse_date(row.get("end"))
        filed = parse_date(row.get("filed"))
        shares = row.get("val")

        if not measured or not filed:
            continue

        if measured > filed:
            continue

        if type(shares) is not int or shares <= 0:
            continue

        observations.append({
            "measured": measured,
            "filed": filed,
            "shares": shares,
            "form": row.get("form"),
            "accession": row.get("accn"),
        })

    return observations


def observation_at(observations, cutoff, max_age_days):
    """Select the latest eligible observation known by cutoff."""

    eligible = []

    for row in observations:
        age = (cutoff - row["measured"]).days

        if row["filed"] > cutoff:
            continue

        if age < 0 or age > max_age_days:
            continue

        eligible.append(row)

    if not eligible:
        return None

    eligible.sort(
        key=lambda row: (
            row["measured"],
            row["filed"],
            row["accession"] or "",
        ),
        reverse=True,
    )

    return eligible[0]


def run_self_tests():
    """Offline tests. No SEC connection required."""

    sample = {
        "facts": {
            "dei": {
                SEC_CONCEPT: {
                    "units": {
                        "shares": [
                            {
                                "end": "2026-08-15",
                                "filed": "2026-08-20",
                                "val": 1000000,
                                "form": "10-K",
                                "accn": "0000000000-26-000001",
                            },
                            {
                                "end": "2026-10-22",
                                "filed": "2026-10-27",
                                "val": 1005000,
                                "form": "10-Q",
                                "accn": "0000000000-26-000002",
                            },
                            {
                                # Must NOT be used on October 30.
                                "end": "2026-10-25",
                                "filed": "2026-11-02",
                                "val": 2000000,
                                "form": "10-Q",
                                "accn": "0000000000-26-000003",
                            },
                        ]
                    }
                }
            }
        }
    }

    observations = extract_observations(sample)

    august = observation_at(
        observations,
        date(2026, 8, 24),
        120,
    )

    october = observation_at(
        observations,
        date(2026, 10, 30),
        120,
    )

    assert august is not None
    assert october is not None

    assert august["shares"] == 1000000
    assert october["shares"] == 1005000

    assert october["filed"] <= date(2026, 10, 30)

    print("PASS: SEC share extraction.")
    print("PASS: Earlier reporting-date selection.")
    print("PASS: Later reporting-date selection.")
    print("PASS: Future filings excluded.")
    print("SUCCESS: All offline tests passed.")


def fetch_sec_companyfacts(cik):
    """Retrieve SEC Company Facts with an identified User-Agent."""

    user_agent = os.environ.get("SEC_USER_AGENT", "").strip()

    if not user_agent or "@" not in user_agent:
        sys.exit(
            "FAIL: Set SEC_USER_AGENT with an identifying "
            "name and contact email before live retrieval."
        )

    padded_cik = cik.zfill(10)

    url = f"{SEC_BASE}/CIK{padded_cik}.json"

    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    except HTTPError as error:
        sys.exit(f"FAIL: SEC returned HTTP {error.code}.")

    except (URLError, TimeoutError, ValueError):
        sys.exit("FAIL: SEC request could not be completed.")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--cik", default="0000789019")
    parser.add_argument("--from-date", default="2026-08-24")
    parser.add_argument("--to-date", default="2026-10-30")

    parser.add_argument(
        "--max-age-days",
        type=int,
        default=120,
    )

    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
    )

    args = parser.parse_args()

    if args.self_test:
        run_self_tests()
        return

    if not re.fullmatch(r"[0-9]{1,10}", args.cik):
        sys.exit("FAIL: Invalid SEC CIK.")

    start = parse_date(args.from_date)
    end = parse_date(args.to_date)

    if not start or not end or start >= end:
        sys.exit("FAIL: Invalid comparison dates.")

    if end > date.today():
        sys.exit("FAIL: Comparison date is in the future.")

    if args.max_age_days < 1:
        sys.exit("FAIL: Invalid observation age limit.")

    if not 0 < args.threshold_pct <= 100:
        sys.exit("FAIL: Invalid percentage threshold.")

    payload = fetch_sec_companyfacts(args.cik)

    observations = extract_observations(payload)

    earlier = observation_at(
        observations,
        start,
        args.max_age_days,
    )

    later = observation_at(
        observations,
        end,
        args.max_age_days,
    )

    print("MYPYBITE SEC SHARE-COUNT STABILITY CHECK")
    print("----------------------------------------")

    print("Company:", payload.get("entityName", "Unknown"))
    print("SEC CIK:", args.cik.zfill(10))

    if not earlier or not later:
        print("RESULT: PENDING")
        print("Reason: Insufficient eligible SEC observations.")
        return

    print()
    print("Earlier evaluation:", start)
    print("Earlier measurement:", earlier["measured"])
    print("Earlier filing date:", earlier["filed"])
    print("Earlier accession:", earlier["accession"])
    print("Earlier shares:", earlier["shares"])

    print()
    print("Later evaluation:", end)
    print("Later measurement:", later["measured"])
    print("Later filing date:", later["filed"])
    print("Later accession:", later["accession"])
    print("Later shares:", later["shares"])

    if earlier["measured"] == later["measured"]:
        print()
        print("RESULT: PENDING")
        print("Reason: Both dates use the same measurement.")
        print("No intervening share-count change can be verified.")
        return

    change = (
        (later["shares"] - earlier["shares"])
        / earlier["shares"]
        * 100
    )

    print()
    print("Share-count change:", f"{change:+.6f}%")

    if abs(change) <= args.threshold_pct:
        print("RESULT: WITHIN PROVISIONAL STABILITY THRESHOLD")
    else:
        print("RESULT: REVIEW REQUIRED")
        print("Reason: Significant share-count difference.")

    print()
    print("Free-float stability: NOT VERIFIED")
    print("Corporate actions: NOT FULLY VERIFIED")
    print("Commercial permissions: NOT VERIFIED")
    print("Short-interest score: PENDING")
    print("125-point scorecard: UNCHANGED")
    print("RESEARCH ONLY: No public feeds modified.")


if __name__ == "__main__":
    main()
