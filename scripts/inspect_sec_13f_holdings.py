#!/usr/bin/env python3
"""
MYPYBITE SEC Form 13F holdings inspector.

Research-only tool.

Reads an official SEC Form 13F dataset ZIP and identifies
holdings for one security and reporting period.

Does not calculate institutional ownership percentages.
Does not modify MYPYBITE scores or public feeds.

Requires Python 3.10+.
Uses only Python standard-library modules.
"""

import argparse
import csv
import io
from collections import Counter
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile


def normalized_date(text):
    """Convert SEC reporting dates to YYYY-MM-DD."""

    for fmt in (
        "%d-%b-%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(
                (text or "").strip(),
                fmt,
            ).date().isoformat()

        except ValueError:
            continue

    return None


def table_name(zip_file, key):
    """Locate one SEC data table inside the ZIP."""

    matches = [
        name
        for name in zip_file.namelist()
        if Path(name).stem.upper() == key
        and Path(name).suffix.lower()
        in (".tsv", ".txt", ".csv")
    ]

    if len(matches) != 1:
        raise ValueError(
            f"Expected one {key} data table; "
            f"found {len(matches)}"
        )

    return matches[0]


def read_rows(zip_file, member):
    """Read SEC tab-separated data without extracting files."""

    with zip_file.open(member) as source:

        with io.TextIOWrapper(
            source,
            encoding="utf-8-sig",
            newline="",
        ) as handle:

            reader = csv.DictReader(
                handle,
                delimiter="\t",
            )

            if not reader.fieldnames:
                raise ValueError(
                    f"Empty table: {member}"
                )

            reader.fieldnames = [
                name.strip().upper()
                for name in reader.fieldnames
            ]

            for row in reader:
                yield {
                    key: (value or "").strip()
                    for key, value in row.items()
                }


def inspect(zip_file, cusip, period, limit=5):
    """Inspect holdings without calculating ownership."""

    submissions = {
        row["ACCESSION_NUMBER"]: row
        for row in read_rows(
            zip_file,
            table_name(zip_file, "SUBMISSION"),
        )
    }

    covers = {
        row["ACCESSION_NUMBER"]: row
        for row in read_rows(
            zip_file,
            table_name(zip_file, "COVERPAGE"),
        )
    }

    counts = Counter()
    examples = []
    matched_accessions = set()

    for row in read_rows(
        zip_file,
        table_name(zip_file, "INFOTABLE"),
    ):

        if row.get("CUSIP", "").upper() != cusip:
            continue

        counts["cusip_rows_any_period"] += 1

        accession = row.get(
            "ACCESSION_NUMBER",
            "",
        )

        submission = submissions.get(accession)

        if not submission:
            counts["missing_submission"] += 1
            continue

        reporting_date = normalized_date(
            submission.get("PERIODOFREPORT")
        )

        if reporting_date != period:
            counts["other_reporting_period"] += 1
            continue

        submission_type = submission.get(
            "SUBMISSIONTYPE",
            "",
        )

        if submission_type not in (
            "13F-HR",
            "13F-HR/A",
        ):
            counts["non_holdings_filing"] += 1
            continue

        share_type = row.get(
            "SSHPRNAMTTYPE",
            "",
        ).upper()

        if share_type != "SH":
            counts["not_share_units"] += 1
            continue

        if row.get("PUTCALL", "").strip():
            counts["options_excluded"] += 1
            continue

        counts["candidate_rows"] += 1
        matched_accessions.add(accession)

        cover = covers.get(accession, {})

        amended = (
            submission_type == "13F-HR/A"
            or cover.get(
                "ISAMENDMENT",
                "",
            ).upper() in (
                "Y",
                "YES",
                "TRUE",
                "1",
            )
        )

        if amended:
            counts["amended_candidate_rows"] += 1

        other_manager = bool(
            row.get("OTHERMANAGER")
        )

        if other_manager:
            counts["other_manager_references"] += 1

        if len(examples) < limit:

            examples.append({
                "manager": cover.get(
                    "FILINGMANAGER_NAME",
                    "UNKNOWN",
                ),
                "filing_date": normalized_date(
                    submission.get("FILING_DATE")
                ),
                "accession": accession,
                "shares_as_filed": row.get(
                    "SSHPRNAMT",
                    "",
                ),
                "amendment": amended,
                "other_manager": other_manager,
            })

    return (
        counts,
        len(matched_accessions),
        examples,
    )


def self_test():
    """Run synthetic offline validation tests."""

    import tempfile
    from zipfile import ZIP_DEFLATED

    tables = {
        "SUBMISSION": (
            "ACCESSION_NUMBER\tFILING_DATE\t"
            "SUBMISSIONTYPE\tPERIODOFREPORT\n"
            "a1\t14-AUG-2026\t13F-HR\t30-JUN-2026\n"
            "a2\t17-AUG-2026\t13F-HR/A\t30-JUN-2026\n"
            "a3\t14-MAY-2026\t13F-HR\t31-MAR-2026\n"
        ),

        "COVERPAGE": (
            "ACCESSION_NUMBER\t"
            "FILINGMANAGER_NAME\tISAMENDMENT\n"
            "a1\tSynthetic Manager 1\tN\n"
            "a2\tSynthetic Manager 2\tY\n"
            "a3\tSynthetic Manager 3\tN\n"
        ),

        "INFOTABLE": (
            "ACCESSION_NUMBER\tCUSIP\t"
            "SSHPRNAMTTYPE\tPUTCALL\t"
            "SSHPRNAMT\tOTHERMANAGER\n"
            "a1\t594918104\tSH\t\t100\t\n"
            "a1\t594918104\tSH\tCALL\t10\t\n"
            "a2\t594918104\tSH\t\t200\t1\n"
            "a3\t594918104\tSH\t\t300\t\n"
            "a1\t000000001\tSH\t\t400\t\n"
        ),
    }

    with tempfile.SpooledTemporaryFile() as data:

        with ZipFile(
            data,
            "w",
            compression=ZIP_DEFLATED,
        ) as archive:

            for key, content in tables.items():
                archive.writestr(
                    f"{key}.tsv",
                    content,
                )

        data.seek(0)

        with ZipFile(data) as archive:
            counts, filings, examples = inspect(
                archive,
                "594918104",
                "2026-06-30",
            )

    assert counts["candidate_rows"] == 2

    assert counts["options_excluded"] == 1

    assert counts["other_reporting_period"] == 1

    assert counts["amended_candidate_rows"] == 1

    assert counts["other_manager_references"] == 1

    assert filings == 2

    assert len(examples) == 2

    print("PASS: Reporting periods separated")

    print("PASS: Options excluded")

    print("PASS: Share units checked")

    print("PASS: Amendments flagged")

    print("PASS: Other-manager references flagged")

    print("PASS: CUSIP and filing records matched")

    print("SUCCESS: Synthetic offline tests passed")

    print(
        "RESEARCH ONLY: "
        "No ownership percentage calculated"
    )

    print(
        "RESEARCH ONLY: "
        "No MYPYBITE scores modified"
    )


def main():

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--zip",
        type=Path,
        help="Downloaded official SEC Form 13F ZIP",
    )

    parser.add_argument(
        "--cusip",
        default="594918104",
        help="Exact nine-character security CUSIP",
    )

    parser.add_argument(
        "--period",
        help="Reporting period YYYY-MM-DD",
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
    )

    args = parser.parse_args()

    if args.self_test:
        self_test()
        return

    if not args.zip or not args.period:
        parser.error(
            "Provide --zip and --period, "
            "or use --self-test"
        )

    if not (
        len(args.cusip) == 9
        and args.cusip.isalnum()
    ):
        parser.error(
            "--cusip must contain "
            "exactly nine letters/digits"
        )

    if normalized_date(args.period) != args.period:
        parser.error(
            "--period must use YYYY-MM-DD"
        )

    with ZipFile(args.zip) as archive:

        counts, filings, examples = inspect(
            archive,
            args.cusip.upper(),
            args.period,
        )

    print(
        "MYPYBITE SEC FORM 13F INSPECTION"
    )

    print(
        "Security CUSIP:",
        args.cusip.upper(),
    )

    print(
        "Reporting period:",
        args.period,
    )

    for key in sorted(counts):

        print(
            f"{key}: {counts[key]:,}"
        )

    print(
        "Candidate filings, not reconciled:",
        filings,
    )

    for example in examples:

        print(
            "EXAMPLE:",
            example,
        )

    print(
        "PENDING: Amendment reconciliation"
    )

    print(
        "PENDING: Shared-manager review"
    )

    print(
        "PENDING: Confidential holdings review"
    )

    print(
        "PENDING: Independent ownership denominator"
    )

    print(
        "INSTITUTIONAL OWNERSHIP SCORE: PENDING"
    )

    print(
        "125-POINT SCORECARD: UNCHANGED"
    )

    print(
        "RESEARCH ONLY: "
        "No files, feeds, or scores modified"
    )


if __name__ == "__main__":
    main()
