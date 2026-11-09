from __future__ import annotations

import unittest
from nash_quantitative_score import cash_vs_debt, revenue_growth, operating_margin, score_company, score_calculator_row

DATE = "2026-09-26"


class QuantitativeScoreTests(unittest.TestCase):
    def test_cash_below_debt(self):
        self.assertEqual(cash_vs_debt(20, 40, DATE, DATE)["points"], 0)

    def test_cash_equal_debt_conservative_boundary(self):
        self.assertEqual(cash_vs_debt(20, 20, DATE, DATE)["points"], 0)

    def test_cash_above_debt(self):
        self.assertEqual(cash_vs_debt(30, 20, DATE, DATE)["points"], 5)

    def test_cash_double_debt(self):
        self.assertEqual(cash_vs_debt(40, 20, DATE, DATE)["points"], 10)

    def test_zero_debt_positive_cash(self):
        self.assertEqual(cash_vs_debt(1, 0, DATE, DATE)["points"], 10)

    def test_both_zero_pending(self):
        self.assertIsNone(cash_vs_debt(0, 0, DATE, DATE)["points"])

    def test_mismatched_period_pending(self):
        self.assertIsNone(cash_vs_debt(40, 20, DATE, "2026-06-30")["points"])

    def test_missing_cash_pending(self):
        self.assertIsNone(cash_vs_debt(None, 20, DATE, DATE)["points"])

    def test_invalid_boolean_rejected(self):
        self.assertIsNone(cash_vs_debt(True, 20, DATE, DATE)["points"])

    def test_growth_thresholds(self):
        self.assertEqual([revenue_growth(x, DATE)["points"] for x in (9.9, 10, 20, 20.01)], [0, 5, 5, 10])

    def test_margin_thresholds(self):
        self.assertEqual([operating_margin(x, DATE, DATE)["points"] for x in (9.9, 10, 25, 25.01)], [0, 5, 5, 10])

    def test_margin_period_mismatch(self):
        self.assertIsNone(operating_margin(30, DATE, "2025-09-27")["points"])

    def test_partial_score_does_not_turn_missing_into_zero(self):
        result = score_company({"ticker": "TEST", "cash": 30, "totalDebt": 20, "cashPeriodEnd": DATE, "debtPeriodEnd": DATE})
        self.assertEqual((result["earnedPoints"], result["evaluatedMaxPoints"], result["sectionMaxPoints"], result["complete"]), (5, 10, 30, False))

    def test_full_score_from_calculator_row(self):
        fields = ["ticker", "cik", "cash", "totalDebt", "cashPeriodEnd", "debtPeriodEnd", "revenueGrowthPercent", "revenuePeriodEnd", "operatingMarginPercent", "operatingIncomePeriodEnd"]
        row = ["TEST", "0000012345", 50, 20, DATE, DATE, 21, DATE, 26, DATE]
        result = score_calculator_row(fields, row)
        self.assertEqual((result["earnedPoints"], result["evaluatedMaxPoints"], result["complete"]), (30, 30, True))

    def test_row_shape_rejected(self):
        with self.assertRaises(ValueError):
            score_calculator_row(["ticker"], ["AAPL", "extra"])

    def test_invalid_date_rejected(self):
        self.assertIsNone(revenue_growth(30, "2033-18-99")["points"])


if __name__ == "__main__":
    unittest.main()
