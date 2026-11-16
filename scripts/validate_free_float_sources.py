#!/usr/bin/env python3
"""
MYPYBITE independent free-float comparison (research only).

Input: JSON with symbol, cik and observations.

Each observation contains:
- source
- ticker
- cik
- basis = "free_float"
- floatShares
- effectiveDate

Optional evidence fields:
- publishedDate
- outstandingShares
- lineageGroup
- lineageVerified
- methodology
- redistributionAuthorized

No network requests, credentials, file writes,
score changes or publication.
"""

import argparse
import json
import sys

from datetime import date
from pathlib import Path


def iso_date(value):
    if not isinstance(value, str):
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def positive_int(value):
    return type(value) is int and value > 0


def normalize_cik(value):
    if type(value) not in (str, int):
        return None

    text = str(value)

    if text.isdigit() and 1 <= len(text) <= 10:
        return text.zfill(10)

    return None


def validate(
    payload,
    asof,
    max_age_days=120,
    tolerance_pct=2.0,
):
    if not isinstance(payload, dict):
        raise ValueError("Input must be a JSON object")

    symbol = payload.get("symbol")
    cik = normalize_cik(payload.get("cik"))
    rows = payload.get("observations")

    if (
        not isinstance(symbol, str)
        or not symbol.strip()
        or not cik
    ):
        raise ValueError(
            "Input requires a symbol and numeric CIK"
        )

    if not isinstance(rows, list):
        raise ValueError(
            "Input requires an observations list"
        )

    symbol = symbol.upper().strip()

    accepted = []
    rejected = []

    for position, row in enumerate(rows, start=1):
        reason = None

        if not isinstance(row, dict):
            reason = "not an object"

        else:
            provider = row.get("source")
            when = iso_date(row.get("effectiveDate"))
            published = iso_date(row.get("publishedDate"))
            shares = row.get("floatShares")
            outstanding = row.get("outstandingShares")

            if (
                not isinstance(provider, str)
                or not provider.strip()
            ):
                reason = "source missing"

            elif (
                str(row.get("ticker", "")).upper().strip()
                != symbol
                or normalize_cik(row.get("cik")) != cik
            ):
                reason = "ticker/CIK mismatch"

            elif row.get("basis") != "free_float":
                reason = (
                    "not a free-float measurement"
                )

            elif not positive_int(shares):
                reason = "invalid free-float count"

            elif when is None or when > asof:
                reason = (
                    "invalid/future effective date"
                )

            elif (asof - when).days > max_age_days:
                reason = "measurement too old"

            elif (
                row.get("publishedDate") is not None
                and (
                    published is None
                    or published > asof
                )
            ):
                reason = (
                    "invalid/future publication date"
                )

            elif (
                outstanding is not None
                and (
                    not positive_int(outstanding)
                    or shares > outstanding
                )
            ):
                reason = (
                    "free float exceeds/invalid "
                    "outstanding shares"
                )

            else:
                flags = []

                if published is None:
                    flags.append(
                        "publication date unverified"
                    )

                if (
                    outstanding is not None
                    and shares == outstanding
                ):
                    flags.append(
                        "100% float: verify methodology"
                    )

                methodology = row.get("methodology")

                if (
                    not isinstance(methodology, str)
                    or not methodology.strip()
                ):
                    flags.append(
                        "methodology unverified"
                    )

                if (
                    row.get("redistributionAuthorized")
                    is not True
                ):
                    flags.append(
                        "redistribution permission unverified"
                    )

                group = row.get("lineageGroup")

                if not (
                    isinstance(group, str)
                    and group.strip()
                    and row.get("lineageVerified") is True
                ):
                    flags.append(
                        "source independence unverified"
                    )
                    group = None

                accepted.append({
                    "source": provider.strip(),
                    "shares": shares,
                    "effective": when,
                    "group": group,
                    "flags": flags,
                })

        if reason is not None:
            rejected.append((position, reason))

    # Count a verified upstream data lineage only once.
    # Multiple websites using one upstream provider
    # must not be treated as independent evidence.

    independent = {}

    for row in sorted(
        accepted,
        key=lambda r: r["effective"],
        reverse=True,
    ):
        if row["group"] is not None:
            independent.setdefault(
                row["group"],
                row,
            )

    selected = list(independent.values())

    spread = None

    if len(selected) >= 2:
        counts = [
            row["shares"]
            for row in selected
        ]

        midpoint = (
            max(counts) + min(counts)
        ) / 2

        spread = (
            100
            * (max(counts) - min(counts))
            / midpoint
        )

    flags = sorted({
        flag
        for row in accepted
        for flag in row["flags"]
    })

    if len(selected) < 2:
        result = (
            "PENDING: fewer than two "
            "verified-independent source lineages"
        )

    elif spread > tolerance_pct:
        result = (
            "REVIEW: independent free-float "
            "figures disagree"
        )

    else:
        result = (
            "CONSISTENT FOR RESEARCH ONLY: "
            "not independently audited"
        )

    return {
        "symbol": symbol,
        "cik": cik,
        "asof": asof,
        "accepted": accepted,
        "rejected": rejected,
        "independent_count": len(selected),
        "spread_pct": spread,
        "result": result,
        "flags": flags,
    }


def self_test():
    """
    Synthetic offline testing.

    These are fictional measurements.
    They are not Microsoft financial data.
    """

    observation = {
        "source": "Synthetic A",
        "ticker": "MSFT",
        "cik": "0000789019",
        "basis": "free_float",
        "floatShares": 1000000,
        "effectiveDate": "2026-10-01",
        "publishedDate": "2026-10-02",
        "outstandingShares": 1100000,
        "lineageGroup": "a",
        "lineageVerified": True,
        "methodology": "synthetic test",
        "redistributionAuthorized": False,
    }

    second = dict(
        observation,
        source="Synthetic B",
        lineageGroup="b",
        floatShares=1010000,
    )

    payload = {
        "symbol": "MSFT",
        "cik": "789019",
        "observations": [
            observation,
            second,
        ],
    }

    check = validate(
        payload,
        date(2026, 10, 30),
    )

    assert check["independent_count"] == 2

    assert (
        check["spread_pct"] is not None
        and check["spread_pct"] < 2
    )

    assert check["result"].startswith(
        "CONSISTENT"
    )

    # Same upstream provider must not count twice.

    payload["observations"][1] = dict(
        second,
        lineageGroup="a",
    )

    assert (
        validate(
            payload,
            date(2026, 10, 30),
        )["independent_count"]
        == 1
    )

    # Unverified independence must not count.

    payload["observations"][1] = dict(
        second,
        lineageVerified=False,
    )

    assert (
        validate(
            payload,
            date(2026, 10, 30),
        )["independent_count"]
        == 1
    )

    # Reject incorrect measurement types,
    # future disclosures and wrong companies.

    payload["observations"] += [
        dict(
            second,
            basis="shares_outstanding",
        ),
        dict(
            second,
            source="future",
            publishedDate="2026-11-01",
        ),
        dict(
            second,
            ticker="AAPL",
        ),
    ]

    assert len(
        validate(
            payload,
            date(2026, 10, 30),
        )["rejected"]
    ) == 3

    print("PASS: Free-float comparison math.")

    print(
        "PASS: Duplicate upstream lineage "
        "not treated as independent."
    )

    print(
        "PASS: Unverified independence excluded."
    )

    print(
        "PASS: Incompatible, future and "
        "mismatched records rejected."
    )

    print(
        "SUCCESS: Synthetic offline tests passed."
    )

    print(
        "No external financial data was used."
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--input",
        type=Path,
        help="Local observations JSON",
    )

    parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
    )

    parser.add_argument(
        "--max-age-days",
        type=int,
        default=120,
    )

    parser.add_argument(
        "--tolerance-pct",
        type=float,
        default=2.0,
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
    )

    args = parser.parse_args()

    if args.self_test:
        self_test()
        return

    if args.input is None:
        parser.error(
            "Provide --input or --self-test"
        )

    asof = iso_date(args.as_of)

    if (
        asof is None
        or asof > date.today()
    ):
        parser.error(
            "--as-of must be a valid date "
            "not in the future"
        )

    if (
        args.max_age_days < 1
        or not (
            0 < args.tolerance_pct <= 100
        )
    ):
        parser.error(
            "Invalid maximum age or tolerance"
        )

    try:
        data = json.loads(
            args.input.read_text(
                encoding="utf-8"
            )
        )

        report = validate(
            data,
            asof,
            args.max_age_days,
            args.tolerance_pct,
        )

    except (
        OSError,
        json.JSONDecodeError,
        ValueError,
    ) as error:
        sys.exit(f"FAIL: {error}")

    print(
        "MYPYBITE RESEARCH:",
        report["symbol"],
        "/",
        report["cik"],
    )

    print(
        "Evaluation date:",
        report["asof"],
    )

    for row in report["accepted"]:
        print(
            f"SOURCE: {row['source']} | "
            f"effective {row['effective']} | "
            f"shares {row['shares']:,}"
        )

        for flag in row["flags"]:
            print(
                "CAUTION:",
                flag,
            )

    for position, reason in report["rejected"]:
        print(
            f"REJECTED observation "
            f"{position}: {reason}"
        )

    print(
        "Verified independent source lineages:",
        report["independent_count"],
    )

    if report["spread_pct"] is not None:
        print(
            "Float spread (max-min over midpoint): "
            f"{report['spread_pct']:.4f}%"
        )

    print(
        "Provisional tolerance:",
        args.tolerance_pct,
        "%",
    )

    print(
        "RESULT:",
        report["result"],
    )

    print(
        "License and methodology approval: PENDING"
    )

    print(
        "Short-interest score: PENDING"
    )

    print(
        "125-point scorecard: UNCHANGED"
    )

    print(
        "RESEARCH ONLY: No external requests, "
        "files or public feeds modified."
    )


if __name__ == "__main__":
    main()
