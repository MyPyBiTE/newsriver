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


def score_company(company: Mapping[str, Any]) -> dict[str, Any]:
    """Calculate only 30 of the planned 125 points; pending is never zero."""
    categories = {
        "cashVsDebt": cash_vs_debt(company.get("cash"), company.get("totalDebt"), company.get("cashPeriodEnd"), company.get("debtPeriodEnd")),
        "revenueGrowth": revenue_growth(company.get("revenueGrowthPercent"), company.get("revenuePeriodEnd")),
        "operatingMargin": operating_margin(company.get("operatingMarginPercent"), company.get("operatingIncomePeriodEnd"), company.get("revenuePeriodEnd")),
    }
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
