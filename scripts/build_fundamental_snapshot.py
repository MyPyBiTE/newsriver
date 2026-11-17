#!/usr/bin/env python3
"""Build conservative company financial-condition snapshots from SEC Companyfacts.

The output is a fast reading aid, not a valuation or investment rating. It keeps
point-in-time balance-sheet facts separate from full-year cash-flow facts and
does not treat investing outflows as automatically good or bad.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

from fetch_sec_intake import REQUEST_INTERVAL_SECONDS, IntakeFailure, SecClient, atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.fundamental-snapshot.v1"
SEC_COMPANYFACTS = "https://data.sec.gov/api/xbrl/companyfacts"
INSTANT_FORMS = {
    "10-Q",
    "10-Q/A",
    "10-K",
    "10-K/A",
    "20-F",
    "20-F/A",
    "40-F",
    "40-F/A",
}
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
FINANCIAL_COHORTS = {"financial-system", "capital-provider"}
MONETARY_NAMESPACES = ("us-gaap", "ifrs-full")

CONCEPTS = {
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashAndDueFromBanks",
        "CashAndCashEquivalents",
    ),
    # Annual income-statement facts for Nash-style quantitative screening.
    # Prefer consolidated revenue concepts and avoid mixing components.
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "Revenue",
    ),
    "operatingIncome": (
        "OperatingIncomeLoss",
        "ProfitLossFromOperatingActivities",
    ),
    # This explicit SEC concept excludes cost of sales. Do not substitute
    # broader or unrelated expense totals when standardized facts are missing.
    "operatingExpenses": ("OperatingExpenses",),
    "assets": ("Assets",),
    "currentAssets": ("AssetsCurrent", "CurrentAssets"),
    "liabilities": ("Liabilities",),
    "currentLiabilities": ("LiabilitiesCurrent", "CurrentLiabilities"),
    "equity": (
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquity",
        "PartnersCapital",
        "Equity",
        "EquityAttributableToOwnersOfParent",
    ),
    "retainedEarnings": ("RetainedEarningsAccumulatedDeficit", "RetainedEarnings"),
    "operatingCashFlow": (
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities",
    ),
    "investingCashFlow": (
        "NetCashProvidedByUsedInInvestingActivities",
        "CashFlowsFromUsedInInvestingActivities",
    ),
    "financingCashFlow": (
        "NetCashProvidedByUsedInFinancingActivities",
        "CashFlowsFromUsedInFinancingActivities",
    ),
    "capitalExpenditures": (
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireOtherProductiveAssets",
        "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
    ),
    "debtIssued": (
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromLongTermDebt",
        "ProceedsFromIssuanceOfDebt",
        "ProceedsFromBorrowingsClassifiedAsFinancingActivities",
        "ProceedsFromCurrentBorrowings",
        "ProceedsFromNoncurrentBorrowings",
    ),
    "debtRepaid": (
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfDebt",
        "RepaymentsOfBorrowingsClassifiedAsFinancingActivities",
        "RepaymentsOfNoncurrentBorrowings",
    ),
    "sharesOutstanding": (
        "EntityCommonStockSharesOutstanding",
        "CommonStockSharesOutstanding",
        "NumberOfSharesOutstanding",
    ),
    "debt": (
        "LongTermDebtAndFinanceLeaseObligations",
        "LongTermDebt",
        "Borrowings",
    ),
    "currentDebt": (
        "LongTermDebtAndFinanceLeaseObligationsCurrent",
        "LongTermDebtCurrent",
        "ShortTermBorrowings",
        "CurrentPortionOfLongtermBorrowings",
        "ShorttermBorrowings",
    ),
    "noncurrentDebt": (
        "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
        "LongTermDebtNoncurrent",
        "LongtermBorrowings",
    ),
}
