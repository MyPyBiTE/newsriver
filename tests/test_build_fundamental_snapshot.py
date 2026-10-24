from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "build_fundamental_snapshot", SCRIPTS / "build_fundamental_snapshot.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def fact(label: str, entries: list[dict]) -> dict:
    return {"label": label, "units": {"USD": entries}}


def instant(value: int, end: str, filed: str, accn: str) -> dict:
    return {"end": end, "val": value, "form": "10-Q", "filed": filed, "accn": accn}


def annual(value: int, end: str, filed: str, accn: str) -> dict:
    year = int(end[:4])
    return {
        "start": f"{year}-01-01",
        "end": end,
        "val": value,
        "form": "10-K",
        "filed": filed,
        "accn": accn,
        "fy": year,
        "fp": "FY",
    }


class FundamentalSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.issuer = {
            "ticker": "TEST",
            "cik": "0000000001",
            "legalName": "Test Company",
            "cohort": "technology-platform",
        }
        current_accn = "0000000001-26-000001"
        prior_accn = "0000000001-25-000001"
        self.companyfacts = {
            "facts": {
                "us-gaap": {
                    "CashAndCashEquivalentsAtCarryingValue": fact(
                        "Cash and cash equivalents",
                        [
                            instant(630_000_000, "2026-09-30", "2026-10-20", current_accn),
                            instant(900_000_000, "2025-09-30", "2025-10-20", prior_accn),
                        ],
                    ),
                    "Assets": fact("Assets", [instant(1_000_000_000, "2026-09-30", "2026-10-20", current_accn)]),
                    "Liabilities": fact("Liabilities", [instant(1_200_000_000, "2026-09-30", "2026-10-20", current_accn)]),
                    "StockholdersEquity": fact("Equity", [instant(-200_000_000, "2026-09-30", "2026-10-20", current_accn)]),
                    "RetainedEarningsAccumulatedDeficit": fact("Accumulated deficit", [instant(-800_000_000, "2026-09-30", "2026-10-20", current_accn)]),
                    "NetCashProvidedByUsedInOperatingActivities": fact("Operating cash flow", [annual(-700_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                    "NetCashProvidedByUsedInInvestingActivities": fact("Investing cash flow", [annual(-250_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                    "PaymentsToAcquirePropertyPlantAndEquipment": fact("Capital expenditures", [annual(100_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                    "NetCashProvidedByUsedInFinancingActivities": fact("Financing cash flow", [annual(400_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                    "ProceedsFromIssuanceOfLongTermDebt": fact("Debt issued", [annual(500_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                    "RepaymentsOfLongTermDebt": fact("Debt repaid", [annual(50_000_000, "2025-12-31", "2026-02-20", current_accn)]),
                }
            }
        }

    def test_pressure_case_matches_the_checklist_without_mislabeling_expenses(self):
        result = MODULE.analyze_company(self.issuer, self.companyfacts)
        checks = {item["id"]: item for item in result["checks"]}
        self.assertEqual(result["cashChangePercent"], -30.0)
        self.assertEqual(checks["liquidity"]["status"], "pressure")
        self.assertEqual(checks["operations"]["status"], "pressure")
        self.assertIn("used $700.0M of cash", checks["operations"]["headline"])
        self.assertEqual(checks["investing"]["status"], "context")
        self.assertEqual(checks["financing"]["status"], "watch")
        self.assertEqual(checks["balance-sheet"]["status"], "pressure")
        self.assertGreaterEqual(result["summary"]["pressureSignals"], 3)

    def test_financial_company_uses_sector_review_instead_of_generic_leverage_rule(self):
        bank = dict(self.issuer, ticker="BANK", cohort="financial-system")
        result = MODULE.analyze_company(bank, self.companyfacts)
        checks = {item["id"]: item for item in result["checks"]}
        self.assertEqual(checks["balance-sheet"]["status"], "sector-review")
        self.assertIn("regulatory capital", checks["balance-sheet"]["detail"])

    def test_snapshot_is_json_serializable_and_preserves_source_links(self):
        snapshot = MODULE.build_snapshot(
            {"issuers": [self.issuer]},
            {self.issuer["cik"]: self.companyfacts},
            generated_at="2026-10-24T05:00:00Z",
        )
        cash = snapshot["companies"][0]["facts"]["cash"]
        json.dumps(snapshot)
        self.assertEqual(snapshot["schema"], MODULE.SCHEMA)
        self.assertTrue(cash["sourceUrl"].startswith("https://www.sec.gov/Archives/edgar/data/"))


if __name__ == "__main__":
    unittest.main()
