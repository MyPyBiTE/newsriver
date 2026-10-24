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
from datetime import date, datetime, timezone
from pathlib import Path

from fetch_sec_intake import REQUEST_INTERVAL_SECONDS, IntakeFailure, SecClient, atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "mypybite.fundamental-snapshot.v1"
SEC_COMPANYFACTS = "https://data.sec.gov/api/xbrl/companyfacts"
INSTANT_FORMS = {"10-Q", "10-Q/A", "10-K", "10-K/A"}
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
FINANCIAL_COHORTS = {"financial-system", "capital-provider"}

CONCEPTS = {
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "CashAndDueFromBanks",
    ),
    "assets": ("Assets",),
    "currentAssets": ("AssetsCurrent",),
    "liabilities": ("Liabilities",),
    "currentLiabilities": ("LiabilitiesCurrent",),
    "equity": (
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquity",
        "PartnersCapital",
    ),
    "retainedEarnings": ("RetainedEarningsAccumulatedDeficit",),
    "operatingCashFlow": ("NetCashProvidedByUsedInOperatingActivities",),
    "investingCashFlow": ("NetCashProvidedByUsedInInvestingActivities",),
    "financingCashFlow": ("NetCashProvidedByUsedInFinancingActivities",),
    "capitalExpenditures": ("PaymentsToAcquirePropertyPlantAndEquipment",),
    "debtIssued": (
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromLongTermDebt",
        "ProceedsFromIssuanceOfDebt",
    ),
    "debtRepaid": (
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfDebt",
    ),
    "sharesOutstanding": (
        "EntityCommonStockSharesOutstanding",
        "CommonStockSharesOutstanding",
    ),
    "debt": (
        "LongTermDebtAndFinanceLeaseObligations",
        "LongTermDebt",
    ),
    "currentDebt": (
        "LongTermDebtAndFinanceLeaseObligationsCurrent",
        "LongTermDebtCurrent",
        "ShortTermBorrowings",
    ),
    "noncurrentDebt": (
        "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
        "LongTermDebtNoncurrent",
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def parse_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def source_url(cik: str, accession: object) -> str | None:
    accession_text = str(accession or "")
    if not accession_text:
        return None
    compact = accession_text.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{compact}/"
        f"{accession_text}-index.html"
    )


def concept_entries(
    companyfacts: dict,
    tags: tuple[str, ...],
    *,
    namespaces: tuple[str, ...] = ("us-gaap",),
    unit: str = "USD",
) -> list[dict]:
    results: list[dict] = []
    all_facts = companyfacts.get("facts", {})
    for namespace_rank, namespace in enumerate(namespaces):
        taxonomy = all_facts.get(namespace, {})
        for tag_rank, tag in enumerate(tags):
            concept = taxonomy.get(tag, {})
            units = concept.get("units", {})
            for raw in units.get(unit, []):
                if not isinstance(raw, dict) or not isinstance(raw.get("val"), (int, float)):
                    continue
                item = dict(raw)
                item["tag"] = tag
                item["label"] = concept.get("label") or tag
                item["unit"] = unit
                item["tagRank"] = namespace_rank * len(tags) + tag_rank
                results.append(item)
    return results


def newest_instant(
    companyfacts: dict,
    tags: tuple[str, ...],
    *,
    namespaces: tuple[str, ...] = ("us-gaap",),
    unit: str = "USD",
) -> dict | None:
    rows = [
        row
        for row in concept_entries(companyfacts, tags, namespaces=namespaces, unit=unit)
        if row.get("form") in INSTANT_FORMS
        and parse_date(row.get("end"))
        and not row.get("start")
    ]
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (
            str(row.get("end") or ""),
            str(row.get("filed") or ""),
            -int(row.get("tagRank") or 0),
            str(row.get("accn") or ""),
        ),
    )


def prior_year_instant(companyfacts: dict, latest: dict | None) -> dict | None:
    if not latest:
        return None
    latest_end = parse_date(latest.get("end"))
    if not latest_end:
        return None
    rows = [
        row
        for row in concept_entries(companyfacts, (str(latest["tag"]),))
        if row.get("form") in INSTANT_FORMS
        and not row.get("start")
        and parse_date(row.get("end"))
        and 300 <= (latest_end - parse_date(row["end"])).days <= 460
    ]
    if not rows:
        return None
    return min(
        rows,
        key=lambda row: (
            abs((latest_end - parse_date(row["end"])).days - 365),
            -int(str(row.get("filed") or "0000-00-00").replace("-", "")),
        ),
    )


def newest_annual(companyfacts: dict, tags: tuple[str, ...]) -> dict | None:
    rows = []
    for row in concept_entries(companyfacts, tags):
        start = parse_date(row.get("start"))
        end = parse_date(row.get("end"))
        if (
            row.get("form") in ANNUAL_FORMS
            and start
            and end
            and 300 <= (end - start).days <= 430
        ):
            rows.append(row)
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (
            str(row.get("end") or ""),
            str(row.get("filed") or ""),
            -int(row.get("tagRank") or 0),
            str(row.get("accn") or ""),
        ),
    )


def public_fact(cik: str, row: dict | None) -> dict | None:
    if not row:
        return None
    return {
        "value": row["val"],
        "unit": row["unit"],
        "tag": row["tag"],
        "label": row["label"],
        "start": row.get("start"),
        "end": row.get("end"),
        "filed": row.get("filed"),
        "form": row.get("form"),
        "accessionNumber": row.get("accn"),
        "sourceUrl": source_url(cik, row.get("accn")),
    }


def percent_change(current: dict | None, previous: dict | None) -> float | None:
    if not current or not previous or previous["val"] == 0:
        return None
    return round((current["val"] / previous["val"] - 1) * 100, 1)


def money(value: float | int | None) -> str:
    if value is None:
        return "unavailable"
    sign = "-" if value < 0 else ""
    size = abs(value)
    if size >= 1_000_000_000:
        return f"{sign}${size / 1_000_000_000:.1f}B"
    if size >= 1_000_000:
        return f"{sign}${size / 1_000_000:.1f}M"
    return f"{sign}${size:,.0f}"


def check(
    check_id: str,
    label: str,
    status: str,
    headline: str,
    detail: str,
    evidence: list[str],
) -> dict:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "headline": headline,
        "detail": detail,
        "evidence": evidence,
    }


def analyze_company(issuer: dict, companyfacts: dict) -> dict:
    cik = str(issuer["cik"])
    instant = {
        key: newest_instant(companyfacts, CONCEPTS[key])
        for key in (
            "cash",
            "assets",
            "currentAssets",
            "liabilities",
            "currentLiabilities",
            "equity",
            "retainedEarnings",
        )
    }
    instant["sharesOutstanding"] = newest_instant(
        companyfacts,
        CONCEPTS["sharesOutstanding"],
        namespaces=("dei", "us-gaap"),
        unit="shares",
    )
    for debt_key in ("debt", "currentDebt", "noncurrentDebt"):
        instant[debt_key] = newest_instant(companyfacts, CONCEPTS[debt_key])
    annual = {
        key: newest_annual(companyfacts, CONCEPTS[key])
        for key in (
            "operatingCashFlow",
            "investingCashFlow",
            "financingCashFlow",
            "capitalExpenditures",
            "debtIssued",
            "debtRepaid",
        )
    }
    cash_prior = prior_year_instant(companyfacts, instant["cash"])
    cash_change = percent_change(instant["cash"], cash_prior)
    cash = instant["cash"]["val"] if instant["cash"] else None
    cfo = annual["operatingCashFlow"]["val"] if annual["operatingCashFlow"] else None
    investing = annual["investingCashFlow"]["val"] if annual["investingCashFlow"] else None
    financing = annual["financingCashFlow"]["val"] if annual["financingCashFlow"] else None
    capex = annual["capitalExpenditures"]["val"] if annual["capitalExpenditures"] else None
    debt_issued = annual["debtIssued"]["val"] if annual["debtIssued"] else None
    debt_repaid = annual["debtRepaid"]["val"] if annual["debtRepaid"] else None
    assets = instant["assets"]["val"] if instant["assets"] else None
    liabilities = instant["liabilities"]["val"] if instant["liabilities"] else None
    equity = instant["equity"]["val"] if instant["equity"] else None
    retained = instant["retainedEarnings"]["val"] if instant["retainedEarnings"] else None
    current_assets = instant["currentAssets"]["val"] if instant["currentAssets"] else None
    current_liabilities = (
        instant["currentLiabilities"]["val"] if instant["currentLiabilities"] else None
    )
    reported_debt = instant["debt"]["val"] if instant["debt"] else None
    current_debt = instant["currentDebt"]["val"] if instant["currentDebt"] else None
    noncurrent_debt = instant["noncurrentDebt"]["val"] if instant["noncurrentDebt"] else None
    total_debt = reported_debt
    if total_debt is None and (current_debt is not None or noncurrent_debt is not None):
        total_debt = (current_debt or 0) + (noncurrent_debt or 0)
    free_cash_flow = cfo - capex if cfo is not None and capex is not None else None
    net_debt = total_debt - cash if total_debt is not None and cash is not None else None
    is_financial = issuer.get("cohort") in FINANCIAL_COHORTS

    if cash is None:
        liquidity_status = "insufficient"
        liquidity_headline = "Cash fact unavailable"
    elif cash_change is not None and cash_change <= -20:
        liquidity_status = "pressure"
        liquidity_headline = f"Cash fell {abs(cash_change):.1f}% year over year"
    elif cash_change is not None and cash_change >= 20:
        liquidity_status = "supportive"
        liquidity_headline = f"Cash rose {cash_change:.1f}% year over year"
    else:
        liquidity_status = "neutral"
        liquidity_headline = f"Cash stands at {money(cash)}"
    liquidity_detail = (
        f"Latest cash is {money(cash)}"
        + (
            f", versus {money(cash_prior['val'])} roughly one year earlier."
            if cash_prior
            else ". A comparable prior-year fact was not available."
        )
    )

    if cfo is None:
        operations_status = "insufficient"
        operations_headline = "Annual operating cash flow unavailable"
    elif cfo < 0:
        operations_status = "pressure"
        operations_headline = f"Operations used {money(abs(cfo))} of cash"
    else:
        operations_status = "supportive"
        operations_headline = f"Operations generated {money(cfo)} of cash"
    if cash is not None and cfo is not None and cfo < 0:
        runway = cash / abs(cfo) if cfo else None
        operations_detail = (
            f"At the latest annual operating burn, reported cash equals about "
            f"{runway:.1f} years of that burn. This is a rough pressure check, not a forecast."
        )
    else:
        operations_detail = (
            "Operating cash flow is the net cash generated or consumed by operations; "
            "it is not the same as operating expenses on the income statement."
        )

    investing_headline = (
        f"Investing activities {'used' if investing < 0 else 'provided'} {money(abs(investing))}"
        if investing is not None
        else "Annual investing cash flow unavailable"
    )
    investing_detail = (
        f"Reported capital expenditures were {money(capex)}. Investing outflows require context: "
        "they may represent expansion, acquisitions or asset purchases rather than deterioration."
        if capex is not None
        else "Investing cash flow is contextual until the filing explains acquisitions, asset sales and capital spending."
    )

    financing_parts = []
    if debt_issued is not None:
        financing_parts.append(f"debt issued {money(debt_issued)}")
    if debt_repaid is not None:
        financing_parts.append(f"debt repaid {money(debt_repaid)}")
    financing_status = (
        "watch"
        if debt_issued is not None and debt_issued > (debt_repaid or 0)
        else "neutral"
        if financing is not None or financing_parts
        else "insufficient"
    )
    financing_headline = (
        "; ".join(financing_parts).capitalize()
        if financing_parts
        else f"Financing activities {'provided' if financing and financing > 0 else 'used'} {money(abs(financing))}"
        if financing is not None
        else "Annual financing details unavailable"
    )
    financing_detail = (
        f"Net financing cash flow was {money(financing)}. Positive financing cash flow can mean new debt or equity; inspect the components before judging it."
        if financing is not None
        else "Debt and equity issuance should be read together with repayments, dilution and maturity obligations."
    )

    if is_financial:
        balance_status = "sector-review"
        balance_headline = "Use bank/financial-sector capital measures"
        balance_detail = (
            "Deposits and financial liabilities are part of the operating model. Ordinary-company current and liability ratios can mislead; review regulatory capital, credit quality and liquidity disclosures."
        )
    elif equity is not None and equity < 0:
        balance_status = "pressure"
        balance_headline = f"Shareholders’ equity is negative at {money(equity)}"
        balance_detail = "Negative equity means recorded liabilities and other claims exceed recorded assets attributable to shareholders."
    elif assets is not None and liabilities is not None:
        ratio = liabilities / assets if assets else None
        current_ratio = (
            current_assets / current_liabilities
            if current_assets is not None and current_liabilities not in (None, 0)
            else None
        )
        balance_status = "watch" if ratio is not None and ratio >= 0.8 else "supportive"
        balance_headline = (
            f"Liabilities equal {ratio * 100:.1f}% of assets"
            if ratio is not None
            else "Balance-sheet ratio unavailable"
        )
        balance_detail = (
            f"Assets are {money(assets)}, liabilities {money(liabilities)} and equity {money(equity)}."
            + (f" Current ratio is {current_ratio:.2f}." if current_ratio is not None else "")
            + (f" Retained earnings/accumulated deficit is {money(retained)}." if retained is not None else "")
        )
    else:
        balance_status = "insufficient"
        balance_headline = "Comparable balance-sheet facts unavailable"
        balance_detail = "Open the latest 10-Q or 10-K to inspect assets, liabilities and equity directly."

    checks = [
        check("liquidity", "Cash and liquidity", liquidity_status, liquidity_headline, liquidity_detail, ["cash", "cashPriorYear"]),
        check("operations", "Operating cash flow", operations_status, operations_headline, operations_detail, ["operatingCashFlow"]),
        check("investing", "Investing activity", "context", investing_headline, investing_detail, ["investingCashFlow", "capitalExpenditures"]),
        check("financing", "Financing and debt", financing_status, financing_headline, financing_detail, ["financingCashFlow", "debtIssued", "debtRepaid"]),
        check("balance-sheet", "Liabilities and equity", balance_status, balance_headline, balance_detail, ["assets", "liabilities", "equity", "retainedEarnings", "currentAssets", "currentLiabilities"]),
    ]
    pressure = sum(item["status"] == "pressure" for item in checks)
    watch = sum(item["status"] == "watch" for item in checks)
    supportive = sum(item["status"] == "supportive" for item in checks)
    if pressure >= 2:
        conclusion = "Multiple financial-pressure signals require deeper filing review."
    elif pressure or watch:
        conclusion = "At least one pressure or watch signal requires explanation from the filing."
    elif supportive >= 2:
        conclusion = "The available facts show positive cash-generation and balance-sheet support, subject to filing context."
    else:
        conclusion = "The available standardized facts are not sufficient for a directional conclusion."

    facts = {
        **{key: public_fact(cik, value) for key, value in instant.items()},
        "cashPriorYear": public_fact(cik, cash_prior),
        **{key: public_fact(cik, value) for key, value in annual.items()},
    }
    latest_ends = [item["end"] for item in facts.values() if item and item.get("end")]
    return {
        "ticker": issuer["ticker"],
        "cik": cik,
        "legalName": issuer["legalName"],
        "cohort": issuer.get("cohort"),
        "status": "available" if latest_ends else "insufficient",
        "latestPeriodEnd": max(latest_ends, default=None),
        "cashChangePercent": cash_change,
        "checks": checks,
        "summary": {
            "pressureSignals": pressure,
            "watchSignals": watch,
            "supportiveSignals": supportive,
            "conclusion": conclusion,
        },
        "valuationInputs": {
            "freeCashFlow": free_cash_flow,
            "totalDebt": total_debt,
            "netDebt": net_debt,
            "unit": "USD",
            "method": "Free cash flow equals annual operating cash flow less capital expenditures; net debt equals reported or reconstructed debt less cash.",
            "sourceFactKeys": {
                "freeCashFlow": ["operatingCashFlow", "capitalExpenditures"],
                "totalDebt": ["debt", "currentDebt", "noncurrentDebt"],
                "netDebt": ["debt", "currentDebt", "noncurrentDebt", "cash"],
            },
        },
        "facts": facts,
        "caveats": [
            "Balance-sheet values are point-in-time facts; cash-flow values are from the latest available full fiscal year.",
            "SEC Companyfacts contains standardized taxonomy facts only; company-specific extensions may require direct filing review.",
            "This checklist is an evidence triage tool, not a valuation, credit rating or investment recommendation.",
        ],
    }


def build_snapshot(roster: dict, companyfacts_by_cik: dict[str, dict], generated_at: str | None = None) -> dict:
    companies = []
    for issuer in roster["issuers"]:
        facts = companyfacts_by_cik.get(str(issuer["cik"]), {})
        companies.append(analyze_company(issuer, facts))
    return {
        "schema": SCHEMA,
        "generatedAt": generated_at or utc_now_iso(),
        "source": "SEC EDGAR Companyfacts API",
        "method": "Latest point-in-time balance sheet facts plus latest full-fiscal-year cash-flow facts; no quarterly/annual period mixing.",
        "companies": companies,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", type=Path, default=ROOT / "config" / "management-roster.json")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "sec" / "fundamentals" / "fundamentals.json")
    parser.add_argument("--max-issuers", type=int)
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT", ""))
    args = parser.parse_args()
    if not args.user_agent:
        raise SystemExit("SEC_USER_AGENT is required")
    roster = json.loads(args.roster.read_text(encoding="utf-8"))
    if args.max_issuers is not None:
        roster = dict(roster)
        roster["issuers"] = roster["issuers"][: args.max_issuers]
    client = SecClient(args.user_agent, REQUEST_INTERVAL_SECONDS)
    collected: dict[str, dict] = {}
    failures: list[dict[str, str]] = []
    for index, issuer in enumerate(roster["issuers"], start=1):
        cik = str(issuer["cik"])
        url = f"{SEC_COMPANYFACTS}/CIK{cik}.json"
        try:
            text = client.get_text(url, missing_statuses=frozenset({404}))
            if text is None:
                failures.append({"ticker": issuer["ticker"], "reason": "companyfacts-not-found"})
                collected[cik] = {}
            else:
                collected[cik] = json.loads(text)
        except (IntakeFailure, json.JSONDecodeError) as exc:
            failures.append({"ticker": issuer["ticker"], "reason": str(exc)})
            collected[cik] = {}
        print(f"[{index}/{len(roster['issuers'])}] {issuer['ticker']}", flush=True)
    snapshot = build_snapshot(roster, collected)
    snapshot["audit"] = {
        "requestedIssuers": len(roster["issuers"]),
        "availableCompanies": sum(item["status"] == "available" for item in snapshot["companies"]),
        "failures": failures,
    }
    if snapshot["audit"]["availableCompanies"] == 0:
        raise SystemExit("Refusing to publish a fundamental snapshot with no usable company facts.")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(args.output, json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {args.output} ({snapshot['audit']['availableCompanies']} companies with usable facts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
