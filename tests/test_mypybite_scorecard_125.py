from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "scripts")
)

from mypybite_scorecard_125 import (
    CATEGORIES,
    assemble,
    score_scalability,
)


DATE = "2026-09-26"
PRIOR_DATE = "2025-09-27"

FIELDS = [
    "ticker",
    "cik",
    "cash",
    "totalDebt",
    "cashPeriodEnd",
    "debtPeriodEnd",
    "revenueGrowthPercent",
    "revenuePeriodEnd",
    "operatingMarginPercent",
    "operatingIncomePeriodEnd",
]

SCALABILITY_FIELDS = [
    "annualRevenueCurrent",
    "annualRevenuePrior",
    "annualOperatingExpensesCurrent",
    "annualOperatingExpensesPrior",
    "annualRevenueCurrentPeriodEnd",
    "annualRevenuePriorPeriodEnd",
    "annualOperatingExpensesCurrentPeriodEnd",
    "annualOperatingExpensesPriorPeriodEnd",
]


def scalability_company() -> dict:
    """Fixed example data, not live SEC figures."""

    return {
        "annualRevenueCurrent": 120,
        "annualRevenuePrior": 100,
        "annualOperatingExpensesCurrent": 55,
        "annualOperatingExpensesPrior": 50,
        "annualRevenueCurrentPeriodEnd": DATE,
        "annualRevenuePriorPeriodEnd": PRIOR_DATE,
        "annualOperatingExpensesCurrentPeriodEnd": DATE,
        "annualOperatingExpensesPriorPeriodEnd": PRIOR_DATE,
    }


class TestMypybiteScorecard(unittest.TestCase):

    def test_weighting(self):
        self.assertEqual(len(CATEGORIES), 13)
        self.assertEqual(
            sum(weight for _, weight in CATEGORIES),
            125
        )

    def test_three_categories_only(self):
        data = [
            "TEST",
            "0000000001",
            50,
            20,
            DATE,
            DATE,
            21,
            DATE,
            26,
            DATE,
        ]

        result = assemble(FIELDS, data)

        self.assertEqual(result["earnedPoints"], 30)
        self.assertEqual(result["evaluatedMaxPoints"], 30)
        self.assertEqual(result["fullModelMaxPoints"], 125)
        self.assertFalse(result["complete"])

        self.assertEqual(
            len(result["pendingCategories"]),
            10
        )

        self.assertIsNone(
            result["categories"]["shortInterest"]["points"]
        )

        self.assertEqual(
            result["categories"]["recessionResistance"]["maxPoints"],
            5
        )

    def test_missing_not_zero(self):
        data = [
            "TEST",
            "0000000001",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ]

        result = assemble(FIELDS, data)

        self.assertEqual(result["earnedPoints"], 0)
        self.assertEqual(result["evaluatedMaxPoints"], 0)

        self.assertEqual(
            len(result["pendingCategories"]),
            13
        )

    def test_reject_bad_row(self):
        with self.assertRaises(ValueError):
            assemble(["ticker"], ["ABC", "EXTRA"])

    def test_scalability_revenue_outgrows_expenses(self):
        company = scalability_company()

        result = score_scalability(company)

        self.assertEqual(result["points"], 10)

    def test_scalability_expenses_outgrow_revenue(self):
        company = scalability_company()

        company["annualOperatingExpensesCurrent"] = 65

        result = score_scalability(company)

        self.assertEqual(result["points"], 0)

    def test_scalability_equal_growth_rates_score_zero(self):
        company = scalability_company()

        company["annualOperatingExpensesCurrent"] = 60

        result = score_scalability(company)

        self.assertEqual(result["points"], 0)

    def test_scalability_declining_revenue_scores_zero(self):
        company = scalability_company()

        company["annualRevenueCurrent"] = 90
        company["annualOperatingExpensesCurrent"] = 40

        result = score_scalability(company)

        self.assertEqual(result["points"], 0)

    def test_scalability_missing_values_stay_pending(self):
        company = scalability_company()

        company["annualOperatingExpensesPrior"] = None

        result = score_scalability(company)

        self.assertIsNone(result["points"])

    def test_scalability_mismatched_reporting_periods_stay_pending(self):
        company = scalability_company()

        company["annualOperatingExpensesCurrentPeriodEnd"] = "2026-06-30"

        result = score_scalability(company)

        self.assertIsNone(result["points"])

    def test_scalability_nonconsecutive_years_stay_pending(self):
        company = scalability_company()

        company["annualRevenuePriorPeriodEnd"] = "2024-09-27"
        company["annualOperatingExpensesPriorPeriodEnd"] = "2024-09-27"

        result = score_scalability(company)

        self.assertIsNone(result["points"])

    def test_scalability_invalid_prior_year_base_stays_pending(self):
        company = scalability_company()

        company["annualRevenuePrior"] = 0

        result = score_scalability(company)

        self.assertIsNone(result["points"])

    def test_scalability_invalid_number_stays_pending(self):
        invalid_values = (
            True,
            float("nan"),
            float("inf"),
            "100",
        )

        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                company = scalability_company()

                company["annualRevenueCurrent"] = invalid

                result = score_scalability(company)

                self.assertIsNone(result["points"])

    def test_assemble_includes_scalability_without_changing_125_maximum(self):
        base_row = [
            "TEST",
            "0000000001",
            50,
            20,
            DATE,
            DATE,
            21,
            DATE,
            26,
            DATE,
        ]

        company = scalability_company()

        fields = FIELDS + SCALABILITY_FIELDS

        row = base_row + [
            company[field]
            for field in SCALABILITY_FIELDS
        ]

        result = assemble(fields, row)

        self.assertEqual(result["earnedPoints"], 40)
        self.assertEqual(result["evaluatedMaxPoints"], 40)
        self.assertEqual(result["fullModelMaxPoints"], 125)

        self.assertEqual(
            len(result["pendingCategories"]),
            9
        )

        self.assertFalse(result["complete"])

        self.assertEqual(
            result["categories"]["scalability"]["points"],
            10
        )


if __name__ == "__main__":
    unittest.main()
