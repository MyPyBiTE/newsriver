from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "scripts")
)

from mypybite_scorecard_125 import CATEGORIES, assemble


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


if __name__ == "__main__":
    unittest.main()
