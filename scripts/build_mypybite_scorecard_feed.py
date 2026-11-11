#!/usr/bin/env python3
"""Build a separate MYPYBITE Signal 125-point feed from the existing calculator feed.

Read-only with respect to the SEC/DCF data. Scores come solely from the existing
MYPYBITE scoring engine. Missing evidence remains pending, never zero.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from mypybite_scorecard_125 import CATEGORIES, assemble

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "sec" / "fundamentals" / "calculator-lookup.json"
DEFAULT_OUTPUT = ROOT / "data" / "sec" / "fundamentals" / "scorecard-lookup.json"
INPUT_SCHEMA = "mypybite.calculator-fundamentals.v1"
OUTPUT_SCHEMA = "mypybite.signal-scorecards.v1"


def build_feed(calculator: dict[str, Any]) -> dict[str, Any]:
    """Score every cached CIK without changing its financial data."""
    if calculator.get("schema") != INPUT_SCHEMA:
        raise ValueError("Unsupported calculator feed schema")

    fields = calculator.get("fields")
    rows = calculator.get("data")
    if (
        not isinstance(fields, list)
        or not fields
        or not all(isinstance(field, str) for field in fields)
        or len(fields) != len(set(fields))
        or not isinstance(rows, list)
    ):
        raise ValueError("Calculator fields or data are invalid")

    required = {"ticker", "cik", "aliases", "legalName"}
    if not required.issubset(fields):
        raise ValueError("Calculator feed is missing company identity columns")

    positions = {name: index for index, name in enumerate(fields)}
    output_rows: list[dict[str, Any]] = []
    seen_ciks: set[str] = set()
    scored_counts = {category: 0 for category, _ in CATEGORIES}
    fully_evaluated = 0
    any_evaluated = 0
    four_financial = 0

    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != len(fields):
            raise ValueError(f"Malformed calculator row at index {index}")

        cik = str(row[positions["cik"]] or "").strip()
        ticker = row[positions["ticker"]]
        aliases = row[positions["aliases"]]

        if (
            not cik
            or cik in seen_ciks
            or not isinstance(ticker, str)
            or not ticker.strip()
            or not isinstance(aliases, list)
        ):
            raise ValueError(f"Invalid or duplicated company at row {index}")

        seen_ciks.add(cik)

        score = assemble(fields, row)
        categories = score["categories"]

        if score["fullModelMaxPoints"] != 125 or len(categories) != len(CATEGORIES):
            raise ValueError(f"Unexpected scorecard structure for {ticker}")

        for category, maximum in CATEGORIES:
            item = categories[category]
            points = item["points"]

            if item["maxPoints"] != maximum or (
                points is not None
                and (type(points) is not int or not 0 <= points <= maximum)
            ):
                raise ValueError(f"Invalid {category} score for {ticker}")

            if points is not None:
                scored_counts[category] += 1

        if score["evaluatedMaxPoints"]:
            any_evaluated += 1

        if score["complete"]:
            fully_evaluated += 1

        if all(
            categories[key]["points"] is not None
            for key in (
                "cashVsDebt",
                "revenueGrowth",
                "operatingMargin",
                "scalability",
            )
        ):
            four_financial += 1

        output_rows.append(
            {
                "ticker": ticker,
                "aliases": aliases,
                "cik": cik,
                "legalName": row[positions["legalName"]],
                "earnedPoints": score["earnedPoints"],
                "evaluatedMaxPoints": score["evaluatedMaxPoints"],
                "fullModelMaxPoints": 125,
                "complete": score["complete"],
                "categories": categories,
            }
        )

    if not output_rows:
        raise ValueError(
            "Refusing to publish an empty MYPYBITE scorecard feed"
        )

    return {
        "schema": OUTPUT_SCHEMA,
        "generatedAt": calculator.get("generatedAt"),
        "model": "MYPYBITE Signal 125 (provisional)",
        "fullModelMaxPoints": 125,
        "categoryWeights": {
            name: maximum for name, maximum in CATEGORIES
        },
        "source": (
            "MYPYBITE SEC calculator feed "
            "and evidence-gated scoring engine"
        ),
        "coverage": {
            "companies": len(output_rows),
            "withAtLeastOneScore": any_evaluated,
            "withAllFourFinancialScores": four_financial,
            "fullyEvaluated": fully_evaluated,
            "scoredByCategory": scored_counts,
        },
        "data": output_rows,
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Never leave a partially written public feed behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".scorecard-",
            suffix=".tmp",
            delete=False,
        ) as file:
            temporary = file.name
            json.dump(
                payload,
                file,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
            file.write("\n")

        os.replace(temporary, path)

    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    args = parser.parse_args()

    calculator = json.loads(
        args.input.read_text(encoding="utf-8")
    )

    feed = build_feed(calculator)
    write_json_atomic(args.output, feed)

    print(
        "MYPYBITE 125-point scorecard coverage:",
        json.dumps(feed["coverage"], sort_keys=True),
    )
    print(
        f"Wrote {args.output} "
        f"with {len(feed['data'])} company scorecards"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
