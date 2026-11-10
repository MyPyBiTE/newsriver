from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "scripts")
)

from mypybite_scorecard_125 import assemble, CATEGORIES


DATE = "2026-09-26"

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


class TestMypybiteScorecard(unittest.TestCase):

    def test_weighting(self):
        self.assertEqual(
            (
                len(CATEGORIES),
                sum(weight for _, weight in CATEGORIES)
            ),
            (13, 125)
        )

    def test_three_categories_only(self):
        data = [
            "TEST", "0000000001",
            50, 20, DATE, DATE,
            21, DATE, 26, DATE
        ]

        result = assemble(FIELDS, data)

        self.assertEqual(
            (
                result["earnedPoints"],
                result["evaluatedMaxPoints"],
                result["fullModelMaxPoints"],
                result["complete"]
            ),
            (30, 30, 125, False)
        )

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
            "TEST", "0000000001",
            None, None, None, None,
            None, None, None, None
        ]

        result = assemble(FIELDS, data)

        self.assertEqual(
            (
                result["earnedPoints"],
                result["evaluatedMaxPoints"],
                len(result["pendingCategories"])
            ),
            (0, 0, 13)
        )

    def test_reject_bad_row(self):
        with self.assertRaises(ValueError):
            assemble(["ticker"], ["ABC", "EXTRA"])


if __name__ == "__main__":
    unittest.main()
