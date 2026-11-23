#!/usr/bin/env python3
"""Standalone, read-only first three MYPYBITE quantitative categories.

Provisional Nash-inspired threshold implementation. This module does NOT
write SEC feeds, modify Zoho, or imply the complete 125-point model exists.
"""
from __future__ import annotations

from datetime import date
from math import isfinite
from typing import Any, Mapping, Sequence

MAX_POINTS = 30


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if isfinite(number) else None


def _valid_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _result(points: int | None, reason: str) -> dict[str, Any]:
    return {"points": points, "maxPoints": 10, "status": "pending" if points is None else "scored", "reason": reason}


def cash_vs_debt(cash: Any, debt: Any, cash_period: Any, debt_period: Any) -> dict[str, Any]:
    """0/5/10 thresholds with strict comparable-period and zero-debt rules."""
    if not (_valid_date(cash_period) and cash_period == debt_period):
        return _result(None, "Cash and debt require matching valid period-end dates")
    cash_value, debt_value = _number(cash), _number(debt)
    if cash_value is None or debt_value is None or cash_value < 0 or debt_value < 0:
        return _result(None, "Missing or invalid cash/debt amount")
    if debt_value == 0:
        return _result(10 if cash_value > 0 else None, "No debt and positive cash" if cash_value > 0 else "Both cash and debt are zero")
    ratio = cash_value / debt_value
    return _result(10 if ratio >= 2 else 5 if ratio > 1 else 0, f"Cash/debt ratio: {ratio:.4f}")


def revenue_growth(growth_percent: Any, revenue_period: Any) -> dict[str, Any]:
    """Annual growth, percentage points (not decimal fraction)."""
    growth = _number(growth_percent)
    if growth is None or not _valid_date(revenue_period):
        return _result(None, "Missing annual revenue-growth figure or reporting date")
    return _result(10 if growth > 20 else 5 if growth >= 10 else 0, f"Annual revenue growth: {growth:g}%")


def operating_margin(margin_percent: Any, operating_income_period: Any, revenue_period: Any) -> dict[str, Any]:
    """Margin requires annual revenue and operating income from same period."""
    margin = _number(margin_percent)
    if margin is None or not (_valid_date(revenue_period) and revenue_period == operating_income_period):
        return _result(None, "Missing operating margin or mismatched annual periods")
    return _result(10 if margin > 25 else 5 if margin >= 10 else 0, f"Operating margin: {margin:g}%")


def score_annual_revenue(facts: Mapping[str, Any]) -> dict[str, Any]:
    current = facts.get("revenue") or {}
    prior = facts.get("revenuePriorYear") or {}
    for label, fact in (("Latest", current), ("Prior", prior)):
        if _number(fact.get("value")) is None:
            return _result(None, f"{label} fiscal-year revenue is missing from the SEC cache.")
        start, end = fact.get("start"), fact.get("end")
        if not (_valid_date(start) and _valid_date(end)):
            return _result(None, f"{label} fiscal-year revenue is missing reporting dates.")
        duration = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        if not 350 <= duration <= 380:
            return _result(None, f"{label} revenue does not cover a full fiscal year.")

    if not current.get("unit") or current["unit"] != prior.get("unit"):
        return _result(None, "Annual revenue figures do not have matching currency units.")
    if not current.get("tag") or (current.get("taxonomy"), current["tag"]) != (prior.get("taxonomy"), prior.get("tag")):
        return _result(None, "Annual revenues use different or unverified reporting concepts.")
    gap = (date.fromisoformat(current["start"]) - date.fromisoformat(prior["end"])).days
    if gap != 1:
        return _result(None, "Revenue figures do not cover consecutive fiscal years.")
    if prior["value"] <= 0 or current["value"] < 0:
        return _result(None, "Growth requires positive prior-year revenue and nonnegative current revenue.")

    growth = round((current["value"] / prior["value"] - 1) * 100, 2)
    result = revenue_growth(growth, current["end"])
    result["reason"] = (
        f"Annual revenue growth: {growth:g}%. "
        f"{current['unit']} {current['value']:,} ({current['start']} to {current['end']}) "
        f"versus {prior['unit']} {prior['value']:,} ({prior['start']} to {prior['end']})."
    )
    result["evidence"] = {
        "sourceUrls": list(dict.fromkeys(
            fact["sourceUrl"] for fact in (current, prior) if fact.get("sourceUrl")
        )),
    }
    return result


def score_annual_operating_margin(facts: Mapping[str, Any]) -> dict[str, Any]:
    revenue = facts.get("revenue") or {}
    income = facts.get("operatingIncome") or {}
    revenue_value = _number(revenue.get("value"))
    income_value = _number(income.get("value"))
    if revenue_value is None or revenue_value <= 0:
        return _result(None, "Positive annual revenue is required to calculate operating margin.")
    if income_value is None:
        return _result(None, "Operating income for the same fiscal year is missing from the SEC cache.")
    if not revenue.get("unit") or revenue["unit"] != income.get("unit"):
        return _result(None, "Operating income and revenue must use the same currency.")
    start, end = revenue.get("start"), revenue.get("end")
    if not (_valid_date(start) and _valid_date(end) and start == income.get("start") and end == income.get("end")):
        return _result(None, "Operating income and revenue must cover the same fiscal year.")
    duration = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    if not 350 <= duration <= 380:
        return _result(None, "Operating income and revenue do not cover a full fiscal year.")

    margin = round(income_value / revenue_value * 100, 2)
    result = operating_margin(margin, end, end)
    result["reason"] = (
        f"Operating margin: {margin:g}%. "
        f"Operating income of {income['unit']} {income['value']:,} divided by "
        f"revenue of {revenue['unit']} {revenue['value']:,} ({start} to {end})."
    )
    result["evidence"] = {
        "sourceUrls": list(dict.fromkeys(
            fact["sourceUrl"] for fact in (income, revenue) if fact.get("sourceUrl")
        )),
    }
    return result


def score_company(company: Mapping[str, Any]) -> dict[str, Any]:
    """Calculate only 30 of the planned 125 points; pending is never zero."""
    categories = {
        "cashVsDebt": cash_vs_debt(company.get("cash"), company.get("totalDebt"), company.get("cashPeriodEnd"), company.get("debtPeriodEnd")),
        "revenueGrowth": revenue_growth(company.get("revenueGrowthPercent"), company.get("revenuePeriodEnd")),
        "operatingMargin": operating_margin(company.get("operatingMarginPercent"), company.get("operatingIncomePeriodEnd"), company.get("revenuePeriodEnd")),
    }
    if "annualIncomeStatementFacts" in company:
        categories["revenueGrowth"] = score_annual_revenue(company["annualIncomeStatementFacts"])
        categories["operatingMargin"] = score_annual_operating_margin(company["annualIncomeStatementFacts"])
    scored = [item["points"] for item in categories.values() if item["points"] is not None]
    return {
        "ticker": company.get("ticker"),
        "cik": company.get("cik"),
        "categories": categories,
        "earnedPoints": sum(scored),
        "evaluatedMaxPoints": len(scored) * 10,
        "sectionMaxPoints": MAX_POINTS,
        "fullModelMaxPoints": 125,
        "complete": len(scored) == 3,
    }


def score_calculator_row(fields: Sequence[str], row: Sequence[Any]) -> dict[str, Any]:
    if len(fields) != len(row) or len(set(fields)) != len(fields):
        raise ValueError("Invalid calculator record: field names must be unique and row lengths must match")
    return score_company(dict(zip(fields, row)))
