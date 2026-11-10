#!/usr/bin/env python3
"""Read-only MYPYBITE Signal 125-point scoring engine."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from nash_quantitative_score import score_calculator_row


CATEGORIES = (
    ("cashVsDebt", 10),
    ("revenueGrowth", 10),
    ("operatingMargin", 10),
    ("shortInterest", 10),
    ("institutionalOwnership", 10),
    ("scalability", 10),
    ("fiveYearVsSP500", 10),
    ("competitiveMoat", 10),
    ("ceoQuality", 10),
    ("recessionResistance", 5),
    ("secularTrend", 10),
    ("missionCritical", 10),
    ("talentQuality", 10),
)

assert len(CATEGORIES) == 13
assert sum(points for _, points in CATEGORIES) == 125


def assemble(fields: Sequence[str], row: Sequence[Any]) -> dict[str, Any]:
    """Assemble the 125-point model without inventing missing scores."""

    base = score_calculator_row(fields, row)
    categories: dict[str, dict[str, Any]] = {}

    for category, maximum in CATEGORIES:

        if category in base["categories"]:
            scored = base["categories"][category]
            points = scored["points"]

            if points is not None:
                if type(points) is not int or not 0 <= points <= maximum:
                    raise ValueError(
                        f"Out-of-range score for {category}"
                    )

            categories[category] = {
                **scored,
                "maxPoints": maximum,
            }

        else:
            categories[category] = {
                "points": None,
                "maxPoints": maximum,
                "status": "pending",
                "reason": "Required source or assessment not yet verified",
            }

    earned = sum(
        item["points"]
        for item in categories.values()
        if item["points"] is not None
    )

    evaluated = sum(
        item["maxPoints"]
        for item in categories.values()
        if item["points"] is not None
    )

    return {
        "ticker": base["ticker"],
        "cik": base["cik"],
        "model": "MYPYBITE Signal 125 v0.1 (provisional)",
        "categories": categories,
        "earnedPoints": earned,
        "evaluatedMaxPoints": evaluated,
        "fullModelMaxPoints": 125,
        "complete": evaluated == 125,
        "pendingCategories": [
            key
            for key, item in categories.items()
            if item["points"] is None
        ],
    }
