#!/usr/bin/env python3
"""
Tier-1 Deterministic Financial Statement & Anomaly Auditor for MSX SAOG JSONs.

Checks all companies in data/index.json for:
  1. Balance Sheet identity: Assets == Liabilities + Total Equity (within 3%)
  2. Current + Non-Current Assets == Total Assets (corporate, within 3%)
  3. Total Equity == Total Assets bug (picking Total Equity & Liabilities as Equity)
  4. EBITDA >= EBIT - 1%
  5. PBT - Tax vs Net Profit consistency
  6. EPS unit/sign sanity vs Net Profit / Shares
  7. Cash Flow outflow sign convention (taxPaid, interestPaid, capex, debtRepaid, divPaid <= 0)
  8. Free Cash Flow formula: FCF == OCF - |CapEx|
  9. Column-shift / duplicate FY2025 == FY2024 check
 10. YoY scale anomalies (> 2.5x or < 0.4x Total Assets; > 3.5x or < 0.25x Revenue)
 11. NaN / Infinity safety + overall statement cell fill-rate metrics
"""

import argparse
import json
import math
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

CF_OUTFLOW_KEYS = ("taxPaid", "interestPaid", "capex", "debtRepaid", "divPaid")


def extract_rows_map(stmt_section):
    out = {}
    for r in stmt_section or []:
        if r.get("rowType") == "section":
            continue
        k = r.get("key")
        if k:
            out[k] = r.get("values") or {}
    return out


def check_nan_inf(obj, path=""):
    bad = []
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            bad.append(path)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            bad.extend(check_nan_inf(v, f"{path}.{k}" if path else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            bad.extend(check_nan_inf(v, f"{path}[{i}]"))
    return bad


def audit_company(ticker, data):
    issues = []
    warnings = []
    st = data.get("statements") or {}
    periods = [p.get("key") for p in (st.get("periods") or []) if p.get("key")]
    template = st.get("template", "corporate")

    is_map = extract_rows_map(st.get("incomeStatement"))
    bs_map = extract_rows_map(st.get("balanceSheet"))
    cf_map = extract_rows_map(st.get("cashFlow"))

    # Fill rate over IS + BS + CF value rows (excluding Key Stats)
    total_cells = 0
    filled_cells = 0
    for sec in (is_map, bs_map, cf_map):
        for _, vals in sec.items():
            for pk in periods:
                total_cells += 1
                if vals.get(pk) is not None:
                    filled_cells += 1

    if total_cells == 0 or filled_cells == 0:
        return {
            "ticker": ticker,
            "template": template,
            "reporting": False,
            "totalCells": total_cells,
            "filledCells": filled_cells,
            "fillPct": 0.0,
            "issues": [],
            "warnings": ["no_financial_statements"],
        }

    # NaN / Inf check
    nan_paths = check_nan_inf(data)
    if nan_paths:
        issues.append({"code": "nan_inf", "detail": f"NaN/Inf at {nan_paths[:5]}"})

    shares_out = data.get("sharesOutRaw") or 0

    for pk in periods:
        assets = (bs_map.get("assets") or {}).get(pk)
        liab = (bs_map.get("liabilities") or {}).get(pk)
        tot_eq = (bs_map.get("totalEquity") or {}).get(pk)
        eq_attr = (bs_map.get("equity") or {}).get(pk)
        eq_val = tot_eq if tot_eq is not None else eq_attr

        # 1. Total Equity == Total Assets bug
        if assets is not None and eq_val is not None and abs(assets) > 10000:
            if abs(eq_val - assets) <= max(1000.0, 0.002 * abs(assets)) and (liab is None or abs(liab) > 0.01 * abs(assets)):
                issues.append({
                    "code": "equity_equals_assets",
                    "period": pk,
                    "detail": f"totalEquity ({eq_val:,.0f}) == assets ({assets:,.0f})",
                })

        # 2. BS identity: Assets == Liab + TotalEquity (incl. AAOIFI Unrestricted Investment Account Holders / Tier-1 on Islamic/conventional banks)
        if assets is not None and liab is not None and eq_val is not None and abs(assets) > 10000:
            diff = abs(assets - (liab + eq_val))
            if template == "banking":
                dep_v = (bs_map.get("deposits") or {}).get(pk) or 0.0
                t1_v = (bs_map.get("tier1Securities") or {}).get(pk) or 0.0
                diff = min(diff, abs(assets - (liab + eq_val + dep_v)), abs(assets - (liab + eq_val + t1_v)), abs(assets - (liab + eq_val + dep_v + t1_v)))
            rel = diff / max(abs(assets), 1.0)
            if rel > 0.03 and diff > 50000:
                issues.append({
                    "code": "bs_identity_mismatch",
                    "period": pk,
                    "detail": f"Assets {assets:,.0f} != Liab {liab:,.0f} + Eq {eq_val:,.0f} (diff {diff:,.0f}, {rel*100:.1f}%)",
                })

        # 3. CA + NCA == Assets (corporate)
        ca = (bs_map.get("totalCurrentAssets") or {}).get(pk)
        nca = (bs_map.get("totalNonCurrentAssets") or {}).get(pk)
        if template == "corporate" and assets is not None and ca is not None and nca is not None and abs(assets) > 10000:
            diff = abs((ca + nca) - assets)
            rel = diff / max(abs(assets), 1.0)
            if rel > 0.03 and diff > 50000:
                issues.append({
                    "code": "ca_nca_mismatch",
                    "period": pk,
                    "detail": f"CA {ca:,.0f} + NCA {nca:,.0f} != Assets {assets:,.0f} ({rel*100:.1f}%)",
                })

        # 4. EBITDA >= EBIT - 1%
        ebitda = (is_map.get("ebitda") or {}).get(pk)
        ebit = (is_map.get("opProfit") or {}).get(pk)
        if ebitda is not None and ebit is not None:
            if ebitda < ebit - max(1000.0, 0.01 * abs(ebit)):
                issues.append({
                    "code": "ebitda_lt_ebit",
                    "period": pk,
                    "detail": f"EBITDA ({ebitda:,.0f}) < EBIT ({ebit:,.0f})",
                })

        # 5. PBT - Tax vs NetProfit
        pbt = (is_map.get("profitBeforeTax") or {}).get(pk)
        tax = (is_map.get("taxExpense") or {}).get(pk)
        np_val = (is_map.get("netProfit") or {}).get(pk)
        if pbt is not None and tax is not None and np_val is not None:
            cand1 = abs((pbt - tax) - np_val)
            cand2 = abs((pbt + tax) - np_val)
            best = min(cand1, cand2)
            base = max(abs(pbt), abs(np_val), 100000.0)
            if best / base > 0.25 and best > 250000:
                warnings.append({
                    "code": "pbt_tax_np_gap",
                    "period": pk,
                    "detail": f"PBT {pbt:,.0f}, Tax {tax:,.0f}, NP {np_val:,.0f} (gap {best:,.0f})",
                })

        # 6. EPS sanity
        eps = (is_map.get("eps") or {}).get(pk)
        if eps is not None and np_val is not None and shares_out > 0 and abs(np_val) > 500000:
            if abs(eps) >= 0.002 and ((eps > 0 and np_val < -500000) or (eps < 0 and np_val > 500000)):
                issues.append({
                    "code": "eps_sign_mismatch",
                    "period": pk,
                    "detail": f"EPS {eps} opposite sign to NetProfit {np_val:,.0f}",
                })
            elif abs(eps) >= 0.001:
                implied = eps * shares_out
                ratio = abs(implied) / max(abs(np_val), 1.0)
                if ratio > 4.0 or ratio < 0.15:
                    warnings.append({
                        "code": "eps_scale_suspect",
                        "period": pk,
                        "detail": f"EPS {eps} * {shares_out:,} = {implied:,.0f} vs NP {np_val:,.0f}",
                    })

        # 7. Cash Flow outflow sign check
        for ok in CF_OUTFLOW_KEYS:
            val = (cf_map.get(ok) or {}).get(pk)
            if val is not None and val > 0.0:
                issues.append({
                    "code": "cf_positive_outflow",
                    "period": pk,
                    "detail": f"{ok} is positive ({val:,.2f}); outflows must be <= 0",
                })

        # 8. FCF == OCF - |CapEx|
        ocf = (cf_map.get("ocf") or {}).get(pk)
        capex = (cf_map.get("capex") or {}).get(pk)
        fcf = (cf_map.get("fcf") or {}).get(pk)
        if ocf is not None and capex is not None and fcf is not None:
            expected_fcf = ocf - abs(capex)
            if abs(expected_fcf - fcf) > max(1000.0, 0.01 * max(abs(ocf), abs(fcf))):
                issues.append({
                    "code": "fcf_formula_mismatch",
                    "period": pk,
                    "detail": f"FCF {fcf:,.0f} != OCF {ocf:,.0f} - |CapEx| {abs(capex):,.0f}",
                })

    # 9. Duplicate FY2025 == FY2024 check (column shift indicator)
    if "FY2025" in periods and "FY2024" in periods:
        dup_fields = []
        check_maps = [
            ("revenue", is_map),
            ("insuranceRevenue", is_map),
            ("opInc", is_map),
            ("netProfit", is_map),
            ("assets", bs_map),
            ("liabilities", bs_map),
            ("ocf", cf_map),
        ]
        for fk, mp in check_maps:
            v25 = (mp.get(fk) or {}).get("FY2025")
            v24 = (mp.get(fk) or {}).get("FY2024")
            if v25 is not None and v24 is not None and abs(v25) > 10000 and abs(v25 - v24) < 1.0:
                dup_fields.append(fk)
        if len(dup_fields) >= 2:
            issues.append({
                "code": "duplicate_fy25_fy24",
                "period": "FY2025/FY2024",
                "detail": f"Identical FY2025 == FY2024 values in {dup_fields}",
            })

    # 10. YoY scale anomalies across annual periods
    annuals = [p for p in periods if p.startswith("FY")]
    rev_key = (
        "insuranceRevenue" if "insuranceRevenue" in is_map
        else ("opInc" if "opInc" in is_map else "revenue")
    )
    for idx in range(1, len(annuals)):
        p_prev, p_cur = annuals[idx - 1], annuals[idx]
        a_prev = (bs_map.get("assets") or {}).get(p_prev)
        a_cur = (bs_map.get("assets") or {}).get(p_cur)
        if a_prev and a_cur and min(abs(a_prev), abs(a_cur)) > 500000:
            ratio = abs(a_cur) / abs(a_prev)
            if ratio > 2.5 or ratio < 0.4:
                warnings.append({
                    "code": "yoy_assets_jump",
                    "period": f"{p_prev}->{p_cur}",
                    "detail": f"Total Assets {a_prev:,.0f} -> {a_cur:,.0f} ({ratio:.2f}x)",
                })
        r_prev = (is_map.get(rev_key) or {}).get(p_prev)
        r_cur = (is_map.get(rev_key) or {}).get(p_cur)
        if r_prev and r_cur and min(abs(r_prev), abs(r_cur)) > 500000:
            ratio = abs(r_cur) / abs(r_prev)
            if ratio > 3.5 or ratio < 0.25:
                warnings.append({
                    "code": "yoy_revenue_jump",
                    "period": f"{p_prev}->{p_cur}",
                    "detail": f"{rev_key} {r_prev:,.0f} -> {r_cur:,.0f} ({ratio:.2f}x)",
                })

    fill_pct = round(100.0 * filled_cells / total_cells, 1) if total_cells else 0.0
    if fill_pct < 60.0:
        warnings.append({"code": "low_fill_rate", "detail": f"Fill rate {fill_pct}% ({filled_cells}/{total_cells})"})

    # 11. Debt Profile sanity & split reconciliation
    dp = data.get("debtProfile") or {}
    if dp.get("hasDebtData"):
        kpis = dp.get("kpis") or {}
        for kk, kv in kpis.items():
            if isinstance(kv, (int, float)) and (math.isnan(kv) or math.isinf(kv)):
                issues.append({"code": "debt_nan_kpi", "period": dp.get("latestPeriod") or "", "detail": f"Non-finite KPI {kk}={kv}"})
        for ph in dp.get("periodHistory") or []:
            td = ph.get("totalDebt")
            parts = [ph.get("shortTerm"), ph.get("longTerm"), ph.get("leases"), ph.get("bondsAndTier1")]
            if td is not None and any(x is not None for x in parts):
                psum = sum(x for x in parts if x is not None)
                if abs(psum - td) > max(10000.0, 0.02 * abs(td)):
                    issues.append({
                        "code": "debt_split_mismatch",
                        "period": ph.get("period") or "",
                        "detail": f"Components sum {psum:,.0f} != totalDebt {td:,.0f}",
                    })
        by_ccy = dp.get("byCurrency") or []
        fy25_hist = next((h for h in (dp.get("periodHistory") or []) if h.get("period") == "FY2025"), None)
        fy25_td = fy25_hist.get("totalDebt") if fy25_hist else None
        if by_ccy and fy25_td and fy25_td > 1_000_000:
            ccy_tot = sum((c.get("amountOmr") or 0) for c in by_ccy)
            if ccy_tot > 0 and abs(ccy_tot - fy25_td) / fy25_td > 0.25:
                warnings.append({
                    "code": "debt_ccy_mismatch",
                    "period": "FY2025",
                    "detail": f"byCurrency sum {ccy_tot:,.0f} vs FY2025 BS debt {fy25_td:,.0f}",
                })
        for b in dp.get("bonds") or []:
            ytm = b.get("ytmPct")
            if isinstance(ytm, (int, float)) and (math.isnan(ytm) or math.isinf(ytm) or ytm < -10.0 or ytm > 40.0):
                warnings.append({"code": "bond_ytm_outlier", "period": "", "detail": f"Bond {b.get('symbol') or b.get('name')} YTM={ytm}"})

    return {
        "ticker": ticker,
        "template": template,
        "reporting": True,
        "periods": periods,
        "totalCells": total_cells,
        "filledCells": filled_cells,
        "fillPct": fill_pct,
        "issues": issues,
        "warnings": warnings,
    }


def main():
    ap = argparse.ArgumentParser(description="Tier-1 deterministic MSX financial data audit")
    ap.add_argument("--output", default="/tmp/audit_report.json", help="Output JSON report path")
    ap.add_argument("--summary", action="store_true", help="Print compact CI summary only")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero if hard accounting issues found")
    args = ap.parse_args()

    idx_path = os.path.join(DATA_DIR, "index.json")
    if not os.path.exists(idx_path):
        print(f"ERROR: {idx_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(idx_path, "r", encoding="utf-8") as f:
        idx = json.load(f)

    companies = idx.get("companies") or []
    results = []
    for c in companies:
        sym = c.get("ticker")
        fpath = os.path.join(DATA_DIR, f"{sym}.json")
        if not os.path.exists(fpath):
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            cdata = json.load(f)
        results.append(audit_company(sym, cdata))

    reporting = [r for r in results if r["reporting"]]
    with_issues = [r for r in reporting if r["issues"]]
    with_warnings = [r for r in reporting if r["warnings"]]
    tot_cells = sum(r["totalCells"] for r in reporting)
    tot_filled = sum(r["filledCells"] for r in reporting)
    overall_fill = round(100.0 * tot_filled / tot_cells, 1) if tot_cells else 0.0

    report = {
        "totalListed": len(results),
        "reportingCompanies": len(reporting),
        "nonReportingCompanies": [r["ticker"] for r in results if not r["reporting"]],
        "overallFillPct": overall_fill,
        "totalCells": tot_cells,
        "filledCells": tot_filled,
        "issueCompanies": len(with_issues),
        "warningCompanies": len(with_warnings),
        "flaggedTickers": sorted({r["ticker"] for r in reporting if r["issues"] or r["warnings"]}),
        "companies": results,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(
        f"[Tier-1 Audit] Listed: {len(results)} | Reporting: {len(reporting)} | "
        f"Fill Rate: {overall_fill}% ({tot_filled:,}/{tot_cells:,}) | "
        f"Hard Issues: {len(with_issues)} | Warnings: {len(with_warnings)}"
    )
    # Summarize Debt Profile coverage across all company JSONs
    debt_cos = debt_ccy = debt_loans = debt_bonds = 0
    for c in companies:
        sym = c.get("ticker")
        fpath = os.path.join(DATA_DIR, f"{sym}.json")
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as df:
                dp = (json.load(df) or {}).get("debtProfile") or {}
            if dp.get("hasDebtData"):
                debt_cos += 1
                if dp.get("byCurrency"): debt_ccy += 1
                if dp.get("loans"): debt_loans += 1
                if dp.get("bonds"): debt_bonds += 1
    print(f"[Debt Coverage] With Debt: {debt_cos}/{len(results)} | Currency Split: {debt_ccy} | Loan Schedules: {debt_loans} | Bond/Sukuk Issuers: {debt_bonds}")

    if with_issues:
        print("\nHard Accounting Issues:")
        for r in with_issues:
            print(f"  {r['ticker']} ({r['template']}, {r['fillPct']}%):")
            for iss in r["issues"]:
                print(f"    - [{iss.get('code')}] {iss.get('period','')}: {iss.get('detail')}")

    if not args.summary and with_warnings:
        print("\nWarnings / Anomalies:")
        for r in with_warnings:
            print(f"  {r['ticker']} ({r['template']}, {r['fillPct']}%):")
            for w in r["warnings"]:
                if isinstance(w, dict):
                    print(f"    - [{w.get('code')}] {w.get('period','')}: {w.get('detail')}")

    if args.strict and with_issues:
        sys.exit(2)


if __name__ == "__main__":
    main()
