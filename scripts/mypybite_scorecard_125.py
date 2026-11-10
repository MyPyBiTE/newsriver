#!/usr/bin/env python3
"""Read-only MYPYBITE Signal 125-point scoring engine."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from math import isfinite
from typing import Any, Mapping

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


def _scalability_result(
    points: int | None,
    reason: str,
) -> dict[str, Any]:
    return {
        "points": points,
        "maxPoints": 10,
        "status": "pending" if points is None else "scored",
        "reason": reason,
    }


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None

    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None

    return number if isfinite(number) else None


def _iso_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None

    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None

    return parsed if parsed.isoformat() == value else None


def score_scalability(
    company: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Evaluate whether annual revenue grows faster than operating expenses.

    Provisional scoring:
        10 points: Positive revenue growth exceeds expense growth.
         0 points: The company does not meet that condition.
      Pending: Missing, invalid, or incomparable financial data.

    Inputs must be comparable full-year financial totals.

    The upstream SEC extractor must verify that the observations
    represent annual periods rather than quarterly figures.
    """

    amount_fields = (
        "annualRevenueCurrent",
        "annualRevenuePrior",
        "annualOperatingExpensesCurrent",
        "annualOperatingExpensesPrior",
    )

    values = [
        _finite_number(company.get(field))
        for field in amount_fields
    ]

    if any(value is None for value in values):
        return _scalability_result(
            None,
            "Missing or invalid annual revenue/expense totals",
        )

    revenue_now, revenue_before, expense_now, expense_before = values

    if (
        revenue_before <= 0
        or expense_before <= 0
        or revenue_now < 0
        or expense_now < 0
    ):
        return _scalability_result(
            None,
            "Annual revenues/expenses must be nonnegative "
            "with positive prior-year bases",
        )

    revenue_current_end = _iso_date(
        company.get("annualRevenueCurrentPeriodEnd")
    )

    revenue_prior_end = _iso_date(
        company.get("annualRevenuePriorPeriodEnd")
    )

    expense_current_end = _iso_date(
        company.get("annualOperatingExpensesCurrentPeriodEnd")
    )

    expense_prior_end = _iso_date(
        company.get("annualOperatingExpensesPriorPeriodEnd")
    )

    if (
        revenue_current_end is None
        or revenue_prior_end is None
        or revenue_current_end != expense_current_end
        or revenue_prior_end != expense_prior_end
    ):
        return _scalability_result(
            None,
            "Annual revenue and expense period ends "
            "are missing or mismatched",
        )

    days_between = (
        revenue_current_end - revenue_prior_end
    ).days

    if not 350 <= days_between <= 380:
        return _scalability_result(
            None,
            "Reporting periods are not consecutive fiscal years",
        )

    revenue_growth = (
        revenue_now / revenue_before - 1
    ) * 100

    expense_growth = (
        expense_now / expense_before - 1
    ) * 100

    if not (
        isfinite(revenue_growth)
        and isfinite(expense_growth)
    ):
        return _scalability_result(
            None,
            "Annual revenue/expense growth could not be calculated",
        )

    points = (
        10
        if revenue_growth > 0
        and revenue_growth > expense_growth
        else 0
    )

    return _scalability_result(
        points,
        f"Annual revenue growth: {revenue_growth:.2f}%; "
        f"operating-expense growth: {expense_growth:.2f}%",
    )


def assemble(
    fields: Sequence[str],
    row: Sequence[Any],
) -> dict[str, Any]:
    """Assemble the 125-point model without inventing missing scores."""

    base = score_calculator_row(fields, row)

    company = dict(zip(fields, row))

    categories: dict[str, dict[str, Any]] = {}

    for category, maximum in CATEGORIES:

        if category in base["categories"]:
            scored = base["categories"][category]

        elif category == "scalability":
            scored = score_scalability(company)

        else:
            scored = {
                "points": None,
                "maxPoints": maximum,
                "status": "pending",
                "reason": "Required source or assessment not yet verified",
            }

        points = scored["points"]

        if points is not None:
            if (
                type(points) is not int
                or not 0 <= points <= maximum
            ):
                raise ValueError(
                    f"Out-of-range score for {category}"
                )

        categories[category] = {
            **scored,
            "maxPoints": maximum,
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
        "model": "MYPYBITE Signal 125 v0.2 (provisional)",
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
