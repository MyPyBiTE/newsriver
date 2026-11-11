#!/usr/bin/env python3
"""MYPYBITE Signal 125-point quality scorecard.

Scores quantitative criteria only with comparable observations. Qualitative
criteria require documented analyst assessments. Missing evidence is pending,
never an automatic zero. This module does not collect or publish data.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from math import isfinite
from typing import Any
from urllib.parse import urlparse

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

ANALYST_CATEGORIES = frozenset({
    "competitiveMoat",
    "ceoQuality",
    "recessionResistance",
    "secularTrend",
    "missionCritical",
    "talentQuality",
})

assert len(CATEGORIES) == 13
assert sum(points for _, points in CATEGORIES) == 125


def _result(
    points: int | None,
    maximum: int,
    reason: str,
) -> dict[str, Any]:
    return {
        "points": points,
        "maxPoints": maximum,
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


def _percentage(value: Any) -> float | None:
    number = _finite_number(value)

    if number is None or not 0 <= number <= 100:
        return None

    return number


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
    """Score annual revenue growth versus operating-expense growth."""

    names = (
        "annualRevenueCurrent",
        "annualRevenuePrior",
        "annualOperatingExpensesCurrent",
        "annualOperatingExpensesPrior",
    )

    values = [
        _finite_number(company.get(name))
        for name in names
    ]

    if any(value is None for value in values):
        return _result(
            None,
            10,
            "Missing or invalid annual revenue/expense totals",
        )

    revenue_now, revenue_before, expense_now, expense_before = values

    if (
        revenue_before <= 0
        or expense_before <= 0
        or revenue_now < 0
        or expense_now < 0
    ):
        return _result(
            None,
            10,
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
        return _result(
            None,
            10,
            "Annual revenue and expense period ends "
            "are missing or mismatched",
        )

    days_between = (
        revenue_current_end - revenue_prior_end
    ).days

    if not 350 <= days_between <= 380:
        return _result(
            None,
            10,
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
        return _result(
            None,
            10,
            "Annual revenue/expense growth could not be calculated",
        )

    points = (
        10
        if revenue_growth > 0
        and revenue_growth > expense_growth
        else 0
    )

    return _result(
        points,
        10,
        f"Annual revenue growth: {revenue_growth:.2f}%; "
        f"operating-expense growth: {expense_growth:.2f}%",
    )


def _is_https_source(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    try:
        parsed = urlparse(value)
    except ValueError:
        return False

    return (
        parsed.scheme == "https"
        and bool(parsed.netloc)
    )


def _market_evidence(
    company: Mapping[str, Any],
    prefix: str,
) -> bool:
    """Require a reporting date and identifiable source URL."""

    return (
        _iso_date(company.get(prefix + "AsOf")) is not None
        and _is_https_source(company.get(prefix + "SourceUrl"))
    )


def score_short_interest(
    company: Mapping[str, Any],
) -> dict[str, Any]:
    """Score short interest as a percentage of free float."""

    pct = _percentage(
        company.get("shortInterestPercent")
    )

    if pct is None or not _market_evidence(
        company, "shortInterest"
    ):
        return _result(
            None,
            10,
            "Verified short-interest percent, date, "
            "and source required",
        )

    points = (
        10 if pct < 5
        else 5 if pct < 10
        else 0
    )

    return _result(
        points,
        10,
        f"Short interest: {pct:g}% of float",
    )


def score_institutional_ownership(
    company: Mapping[str, Any],
) -> dict[str, Any]:
    """Score reported institutional ownership percentage."""

    pct = _percentage(
        company.get("institutionalOwnershipPercent")
    )

    if pct is None or not _market_evidence(
        company, "institutionalOwnership"
    ):
        return _result(
            None,
            10,
            "Verified institutional ownership percent, "
            "date, and source required",
        )

    points = (
        0 if pct < 30
        else 5 if pct <= 60
        else 10
    )

    return _result(
        points,
        10,
        f"Institutional ownership: {pct:g}%",
    )


def score_five_year_vs_sp500(
    company: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare five-year total returns on matched dates.

    MYPYBITE interprets the scorecard's 10% and 20%
    outperformance thresholds as percentage-point differences.

    Below 10 percentage points: 0
    From 10 through 20 percentage points: 5
    Above 20 percentage points: 10

    Requires total returns, including dividends.
    """

    stock = _finite_number(
        company.get("companyFiveYearTotalReturnPercent")
    )

    benchmark = _finite_number(
        company.get("sp500FiveYearTotalReturnPercent")
    )

    begin = _iso_date(
        company.get("fiveYearStartDate")
    )

    end = _iso_date(
        company.get("fiveYearEndDate")
    )

    if (
        stock is None
        or benchmark is None
        or begin is None
        or end is None
        or not 1790 <= (end - begin).days <= 1860
        or company.get("fiveYearReturnBasis") != "total_return"
        or not _is_https_source(
            company.get("fiveYearSourceUrl")
        )
        or not _is_https_source(
            company.get("sp500FiveYearSourceUrl")
        )
    ):
        return _result(
            None,
            10,
            "Comparable five-year total returns, "
            "dates, and sources required",
        )

    excess = stock - benchmark

    if not isfinite(excess):
        return _result(
            None,
            10,
            "Invalid five-year excess return",
        )

    points = (
        10 if excess > 20
        else 5 if excess >= 10
        else 0
    )

    return _result(
        points,
        10,
        f"Five-year excess total return: "
        f"{excess:.2f} percentage points",
    )


def score_analyst_assessment(
    company: Mapping[str, Any],
    category: str,
    maximum: int,
) -> dict[str, Any]:
    """Score documented qualitative company assessments.

    Requires a valid score, reviewer, review date,
    written rationale, and supporting source URLs.

    Missing assessments remain pending.
    """

    assessments = company.get("assessments")

    if not isinstance(assessments, Mapping):
        return _result(
            None,
            maximum,
            "Documented analyst assessment required",
        )

    evidence = assessments.get(category)

    if not isinstance(evidence, Mapping):
        return _result(
            None,
            maximum,
            "Documented analyst assessment required",
        )

    points = evidence.get("points")

    permitted = (
        (0, 5)
        if maximum == 5
        else (0, 5, 10)
    )

    sources = evidence.get("sourceUrls")
    rationale = evidence.get("rationale")
    reviewer = evidence.get("reviewer")

    if (
        type(points) is not int
        or points not in permitted
        or _iso_date(evidence.get("reviewedAt")) is None
        or not isinstance(reviewer, str)
        or not reviewer.strip()
        or not isinstance(rationale, str)
        or not rationale.strip()
        or not isinstance(sources, list)
        or not sources
        or not all(
            _is_https_source(source)
            for source in sources
        )
    ):
        return _result(
            None,
            maximum,
            "Assessment missing a valid score, reviewer, "
            "date, rationale, or source",
        )

    result = _result(
        points,
        maximum,
        rationale,
    )

    result["evidence"] = {
        "reviewedAt": evidence["reviewedAt"],
        "reviewer": reviewer,
        "sourceUrls": list(sources),
    }

    return result


def assemble(
    fields: Sequence[str],
    row: Sequence[Any],
) -> dict[str, Any]:
    """Assemble all 13 tests without inventing missing scores."""

    base = score_calculator_row(fields, row)
    company = dict(zip(fields, row))

    categories: dict[str, dict[str, Any]] = {}

    for category, maximum in CATEGORIES:

        if category in base["categories"]:
            scored = base["categories"][category]

        elif category == "scalability":
            scored = score_scalability(company)

        elif category == "shortInterest":
            scored = score_short_interest(company)

        elif category == "institutionalOwnership":
            scored = score_institutional_ownership(company)

        elif category == "fiveYearVsSP500":
            scored = score_five_year_vs_sp500(company)

        elif category in ANALYST_CATEGORIES:
            scored = score_analyst_assessment(
                company,
                category,
                maximum,
            )

        else:
            scored = _result(
                None,
                maximum,
                "No verified scoring method",
            )

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
        value["points"]
        for value in categories.values()
        if value["points"] is not None
    )

    evaluated = sum(
        value["maxPoints"]
        for value in categories.values()
        if value["points"] is not None
    )

    return {
        "ticker": base["ticker"],
        "cik": base["cik"],
        "model": (
            "MYPYBITE Signal 125 v0.3 "
            "(evidence-gated, provisional)"
        ),
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
