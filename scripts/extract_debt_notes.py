#!/usr/bin/env python3
"""
Offline 6_Notes Debt Extractor for MSX SAOG Companies.

Two stages:
  --dump-excerpts : Concurrently downloads FY2025-Annual (fallback FY2024-Annual) ZIPs,
                    scores 6_*Notes*.pdf pages for borrowings/currency/bonds/lease terms,
                    and saves top <=8 pages per indebted company to /tmp/debt_excerpts/.
  --run-gemma     : Runs local LM Studio (google/gemma-4-e4b) over /tmp/debt_excerpts/
                    with JSON-schema constrained output, guards against unit double-scaling,
                    reconciles against Balance Sheet carrying debt, and writes:
                      - data/debt_profiles.json
                      - /tmp/debt_reconcile.json
"""

import argparse
import concurrent.futures
import io
import json
import os
import re
import sys
import threading
import time
import urllib.request
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
EXCERPT_DIR = "/tmp/debt_excerpts"
OUT_PATH = os.path.join(DATA_DIR, "debt_profiles.json")
RECON_PATH = "/tmp/debt_reconcile.json"
LM_URL = "http://127.0.0.1:1234/v1/chat/completions"
MODEL = "google/gemma-4-e4b"

sys.path.insert(0, os.path.join(ROOT, "scripts"))
import sync_all_msx as s

DEBT_KEYWORDS = [
    ("borrowings", 3),
    ("term loan", 4),
    ("revolving", 3),
    ("credit facilit", 3),
    ("bank overdraft", 2),
    ("sukuk", 4),
    ("eurobond", 4),
    ("perpetual", 3),
    ("subordinated", 3),
    ("debenture", 3),
    ("debt securities", 3),
    ("sofr", 3),
    ("hibor", 3),
    ("libor", 2),
    ("interest rate", 2),
    ("effective interest", 3),
    ("maturity", 2),
    ("covenant", 3),
    ("currency risk", 3),
    ("denominated in", 3),
    ("foreign currency", 2),
    ("lease liabilities", 2),
    ("working capital facilit", 3),
    ("syndicated", 3),
]

DEBT_KEYS = (
    "debt",
    "debtSecurities",
    "tier1Securities",
    "borrowingsC",
    "borrowingsNC",
    "leaseC",
    "leaseNC",
)


def find_indebted_targets():
    idx = json.load(open(os.path.join(DATA_DIR, "index.json"), encoding="utf-8"))
    targets = []
    for c in idx.get("companies") or []:
        sym = c["ticker"]
        fpath = os.path.join(DATA_DIR, f"{sym}.json")
        if not os.path.exists(fpath):
            continue
        d = json.load(open(fpath, encoding="utf-8"))
        st = d.get("statements") or {}
        periods = [p["key"] for p in (st.get("periods") or []) if p.get("key")]
        bs = {
            r["key"]: r.get("values") or {}
            for r in (st.get("balanceSheet") or [])
            if r.get("rowType") != "section" and r.get("key")
        }
        if not periods:
            continue
        ref_p = "FY2025" if "FY2025" in periods else periods[-1]
        bs_snap = {k: (bs.get(k) or {}).get(ref_p) for k in DEBT_KEYS}
        bs_debt = bs_snap.get("debt") or 0.0
        bs_ds = bs_snap.get("debtSecurities") or 0.0
        bs_t1 = bs_snap.get("tier1Securities") or 0.0
        tot = bs_debt + bs_ds + bs_t1
        if any((bs_snap.get(k) or 0.0) > 0 for k in DEBT_KEYS) or tot > 0:
            filings = d.get("filings") or []
            ann25 = next((f for f in filings if f.get("filingType") == "FY2025-Annual"), None)
            if not ann25:
                ann25 = next((f for f in filings if "Annual" in (f.get("filingType") or "")), None)
            targets.append({
                "ticker": sym,
                "name": d.get("name") or sym,
                "template": st.get("template") or "corporate",
                "period": ref_p,
                "bs": bs_snap,
                "totalTarget": tot,
                "filingUrl": ann25.get("sourceUrl") if ann25 else None,
            })
    targets.sort(key=lambda x: -x["totalTarget"])
    return targets


def select_debt_pages(pdf_bytes, max_pages=8):
    txt = s.extract_pdf_text_bytes(pdf_bytes, 140)
    if not txt:
        return [], 0, 0
    m_tot = re.search(r"TOTAL_PAGES:\s*(\d+)", txt)
    tot_pages = int(m_tot.group(1)) if m_tot else 0
    chunks = txt.split("=== PAGE ")
    scored = {}
    for ch in chunks[1:]:
        lines = ch.splitlines()
        if not lines:
            continue
        p_m = re.match(r"(\d+)", lines[0].strip())
        if not p_m:
            continue
        pno = int(p_m.group(1))
        body = "\n".join(lines[1:])
        low = body.lower()
        sc = sum(w for kw, w in DEBT_KEYWORDS if kw in low)
        # Bonus if actual borrowing table numbers or currency names appear
        if re.search(r"\b(?:USD|US\s*Dollar|Omani\s*Rial|KWD|SAR|EUR|GBP|AED)\b", body, re.I):
            sc += 2
        if re.search(r"\d+(?:\.\d+)?\s*%", body):
            sc += 1
        if re.search(r"\b(?:NOTE\s+\d+|^\s*\d+\s+(?:BORROWINGS|TERM\s+LOANS?|LOANS|SUBORDINATED|SUKUK|BONDS|DEBT\s+SECURITIES|LEASE\s+LIABILITIES|CURRENCY\s+RISK))\b", body, re.I | re.M):
            sc += 4
        scored[pno] = (sc, body)

    # Pick top seed pages (>=4), plus pno+1 continuation if pno >= 6, capped at max_pages
    ordered = sorted(scored.items(), key=lambda kv: (-kv[1][0], kv[0]))
    picked_set = set()
    for pno, (sc, _) in ordered:
        if sc < 4 and len(picked_set) >= 2:
            break
        picked_set.add(pno)
        if sc >= 7 and (pno + 1) in scored and len(picked_set) < max_pages:
            if scored[pno + 1][0] >= 3:
                picked_set.add(pno + 1)
        if len(picked_set) >= max_pages:
            break
    if not picked_set and ordered:
        for pno, _ in ordered[:3]:
            picked_set.add(pno)

    out_pages = []
    for pno in sorted(picked_set)[:max_pages]:
        out_pages.append((pno, scored[pno][0], scored[pno][1]))
    return out_pages, tot_pages, len(txt)


def dump_one(item):
    sym = item["ticker"]
    url = item.get("filingUrl")
    if not url:
        return {"ticker": sym, "status": "no_filing", "pages": []}
    try:
        blob = s.http_get(url, headers={"Referer": f"https://www.msx.om/snapshot.aspx?s={sym}"}, timeout=40, retries=1)
        if blob[:2] != b"PK":
            return {"ticker": sym, "status": "not_zip", "pages": []}
        zf = zipfile.ZipFile(io.BytesIO(blob))
        names = [n for n in zf.namelist() if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")]
        n6 = next((n for n in names if os.path.basename(n).startswith("6_")), None)
        if not n6:
            return {"ticker": sym, "status": "no_6_notes", "pages": []}
        pages, tot_p, txt_len = select_debt_pages(zf.read(n6), max_pages=8)
        header = (
            f"# {sym} ({item['name']}) | Template: {item['template']} | Ref Period: {item['period']}\n"
            f"# BS Carrying Targets (OMR): total={item['totalTarget']:,.0f} | "
            f"borrowC={item['bs'].get('borrowingsC')} | borrowNC={item['bs'].get('borrowingsNC')} | "
            f"leaseC={item['bs'].get('leaseC')} | leaseNC={item['bs'].get('leaseNC')} | "
            f"debtSec={item['bs'].get('debtSecurities')} | tier1={item['bs'].get('tier1Securities')}\n"
            f"# Source Filing: {url}\n"
            f"# Selected {len(pages)}/{tot_p} pages from 6_Notes (total extracted chars: {txt_len:,})\n\n"
        )
        body_parts = []
        for pno, sc, ptxt in pages:
            compact = "\n".join(re.sub(r"[ \t]{3,}", "  ", ln.strip()) for ln in ptxt.splitlines() if ln.strip())
            body_parts.append(f"--- PAGE {pno} (score={sc}) ---\n{compact}")
        out_file = os.path.join(EXCERPT_DIR, f"{sym}.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(header + "\n\n".join(body_parts))
        return {
            "ticker": sym,
            "status": "ok",
            "totPages": tot_p,
            "pickedPages": [p[0] for p in pages],
            "chars": sum(len(b) for b in body_parts),
            "path": out_file,
        }
    except Exception as e:
        return {"ticker": sym, "status": f"err: {e}", "pages": []}


def stage_dump(targets):
    os.makedirs(EXCERPT_DIR, exist_ok=True)
    t0 = time.time()
    print(f"Dumping 6_Notes debt excerpts for {len(targets)} indebted companies -> {EXCERPT_DIR}...")
    results = []
    lock = threading.Lock()
    done = 0

    def wrap(it):
        nonlocal done
        r = dump_one(it)
        with lock:
            done += 1
            if done % 15 == 0 or done == len(targets):
                print(f"  Progress: {done}/{len(targets)} ({time.time() - t0:.1f}s)")
        return r

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(wrap, it) for it in targets]
        for f in concurrent.futures.as_completed(futs):
            results.append(f.result())

    manifest = {"generatedAt": time.strftime("%Y-%m-%d %H:%M:%S"), "targets": targets, "excerpts": results}
    with open(os.path.join(EXCERPT_DIR, "_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    ok_n = sum(1 for r in results if r["status"] == "ok")
    print(f"Done dumping excerpts: {ok_n}/{len(targets)} OK -> {EXCERPT_DIR}/_manifest.json")


def ask_gemma_debt(sym, excerpt_txt):
    prompt = f"""Extract the complete audited Borrowings, Loans, Bonds/Sukuk, Currency Breakdown, Maturity Profile, Effective Interest Rate, and Covenants from these {sym} FY2025 Financial Statement Notes.

CRITICAL RULES:
1. Check units in the note header ("RO '000", "Thousands", "OMR millions", or exact "OMR" / "RO"). Set "unitMultiplier" to 1000 if amounts in the borrowing tables are in RO '000, 1000000 if in OMR millions, or 1 if exact OMR. Extract table numbers AS PRINTED in the PDF (do NOT pre-multiply them when setting unitMultiplier != 1).
2. Extract ONLY latest year-end carrying values (usually 2025; fallback 2024 if latest shown is 2024) — do NOT mix prior year comparatives or Parent-only totals when Consolidated exists.
3. NEVER invent bank names, renewal dates, or ISINs! If a field (e.g. bank name, renewal date, limit, maturity, ISIN) is not printed in the excerpt, set it to null.
4. For "currencyBreakdown", list carrying amounts of borrowings/debt in OMR equivalent by currency (e.g. "OMR", "USD", "KWD", "SAR", "EUR", "AED"). Note: if the text explicitly states borrowings are only in OMR / no foreign currency exposure, output 1 row for "OMR" equal to total borrowings.
5. For "loans", list each term loan, revolving facility, working capital loan, overdraft, export credit, Islamic Murabaha/Ijara/Wakala financing facility separately if disclosed (or grouped as described in the note).
6. For "bonds", list bonds, sukuk, convertible bonds, subordinated debt, or Tier-1 perpetual sukuk/bonds disclosed in the notes (including fair value / effective interest rate / coupon / maturity).

Return strict JSON matching this structure:
{{
  "unitMultiplier": 1,
  "weightedAvgRate": "5.5% p.a. or null",
  "sourceNoteRef": "Note 21 (Borrowings), Note 30 (Currency Risk)",
  "currencyBreakdown": [{{"currency": "OMR", "amount": 0}}],
  "loans": [{{
    "facility": "Term loan",
    "bank": null,
    "currency": "OMR",
    "totalLimit": null,
    "outstanding": 0,
    "interestRate": "5.15% p.a.",
    "tenor": "7 years",
    "maturityDate": "2028",
    "renewalDate": null,
    "terms": "Secured / Unsecured / repayment profile"
  }}],
  "bonds": [{{
    "name": "Bond or Sukuk name",
    "isin": null,
    "subType": "Bonds",
    "currency": "USD",
    "issueValue": null,
    "outstanding": 0,
    "couponPct": 5.375,
    "maturityDate": "2031",
    "fairValue": null,
    "effectiveRatePct": null,
    "terms": "Senior unsecured / Perpetual"
  }}],
  "covenants": ["..."],
  "maturityProfile": {{"within1Y": null, "years1To5": null, "over5Y": null}}
}}

EXCERPT:
{excerpt_txt[:26000]}
"""
    payload = {
        "model": MODEL,
        "temperature": 0.0,
        "max_tokens": 6000,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "debt_note_extract",
                "schema": {
                    "type": "object",
                    "properties": {
                        "unitMultiplier": {"type": "number"},
                        "weightedAvgRate": {"type": ["string", "null"]},
                        "sourceNoteRef": {"type": ["string", "null"]},
                        "currencyBreakdown": {"type": "array"},
                        "loans": {"type": "array"},
                        "bonds": {"type": "array"},
                        "covenants": {"type": "array"},
                        "maturityProfile": {"type": "object"},
                    },
                    "required": ["unitMultiplier", "currencyBreakdown", "loans", "bonds"],
                },
            },
        },
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an Omani IFRS debt & treasury analyst extracting structured loan, bond, "
                    "and currency risk schedules from MSX audited footnotes. Be exact, never invent "
                    "undisclosed bank names or dates, and output JSON only."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }
    req = urllib.request.Request(
        LM_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        res = json.loads(resp.read().decode("utf-8"))
    txt = (res["choices"][0]["message"].get("content") or "").strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt)
    m = re.search(r"\{.*\}", txt, re.S)
    return json.loads(m.group(0))


def smart_scale(val, mult, bs_ref):
    if val is None or not isinstance(val, (int, float)):
        return None
    v = float(val)
    if v == 0:
        return 0.0
    if mult in (1000.0, 1e6) and bs_ref and bs_ref > 0:
        # Guard against Gemma double-scaling when it already converted RO'000 -> OMR
        raw_err = abs(v - bs_ref) / max(bs_ref, 1.0)
        scaled_err = abs(v * mult - bs_ref) / max(bs_ref, 1.0)
        if scaled_err < raw_err:
            return round(v * mult, 2)
        if raw_err <= 0.35 and scaled_err > 5.0:
            return round(v, 2)
    return round(v * (mult or 1.0), 2)


def normalize_extracted(raw, target):
    bs = target["bs"]
    bs_borrow = (bs.get("borrowingsC") or 0.0) + (bs.get("borrowingsNC") or 0.0)
    bs_tot = bs.get("debt") or 0.0
    bs_bonds = (bs.get("debtSecurities") or 0.0) + (bs.get("tier1Securities") or 0.0)
    bs_ref = max(bs_borrow, bs_tot + bs_bonds, bs_tot, 1.0)
    mult = float(raw.get("unitMultiplier") or 1.0)

    # Decide effective multiplier globally across the company using max sum check
    raw_ccy_sum = sum(float(x.get("amount") or x.get("amountOmr") or 0) for x in (raw.get("currencyBreakdown") or []) if isinstance(x, dict))
    raw_loan_sum = sum(float(x.get("outstanding") or x.get("outstandingOmr") or 0) for x in (raw.get("loans") or []) if isinstance(x, dict))
    raw_bond_sum = sum(float(x.get("outstanding") or x.get("outstandingOmr") or 0) for x in (raw.get("bonds") or []) if isinstance(x, dict))
    probe = max(raw_ccy_sum, raw_loan_sum + raw_bond_sum)
    eff_mult = 1.0
    if mult > 1.0 and probe > 0:
        if abs(probe * mult - bs_ref) < abs(probe - bs_ref):
            eff_mult = mult
    elif probe > 0 and bs_ref >= 1_000_000 and bs_ref / probe > 500:
        eff_mult = 1000.0 if (bs_ref / probe < 5000) else 1e6

    ccy_out = []
    for item in raw.get("currencyBreakdown") or []:
        if not isinstance(item, dict):
            continue
        code = str(item.get("currency") or "").strip().upper()
        amt_raw = item.get("amount") if item.get("amount") is not None else item.get("amountOmr")
        if code and isinstance(amt_raw, (int, float)) and amt_raw > 0:
            ccy_out.append({"currency": code[:12], "amountOmr": round(float(amt_raw) * eff_mult, 2)})

    loans_out = []
    for ln in raw.get("loans") or []:
        if not isinstance(ln, dict):
            continue
        fac = str(ln.get("facility") or "").strip()
        if not fac:
            continue
        lim_raw = ln.get("totalLimit") if ln.get("totalLimit") is not None else ln.get("totalLimitOmr")
        out_raw = ln.get("outstanding") if ln.get("outstanding") is not None else ln.get("outstandingOmr")
        # For individual loan lines, guard if a line was written in OMR while summary was in thousands
        l_mult = eff_mult
        if isinstance(out_raw, (int, float)) and out_raw > bs_ref * 1.8 and eff_mult > 1.0:
            l_mult = 1.0
        loans_out.append({
            "facility": fac,
            "bank": ln.get("bank") or None,
            "currency": (str(ln.get("currency") or "OMR")).strip().upper()[:12],
            "totalLimitOmr": round(float(lim_raw) * l_mult, 2) if isinstance(lim_raw, (int, float)) and lim_raw > 0 else None,
            "outstandingOmr": round(float(out_raw) * l_mult, 2) if isinstance(out_raw, (int, float)) and out_raw >= 0 else None,
            "interestRate": ln.get("interestRate") or None,
            "tenor": ln.get("tenor") or None,
            "maturityDate": ln.get("maturityDate") or None,
            "renewalDate": ln.get("renewalDate") or None,
            "terms": ln.get("terms") or None,
        })

    bonds_out = []
    for bd in raw.get("bonds") or []:
        if not isinstance(bd, dict):
            continue
        nm = str(bd.get("name") or "").strip()
        if not nm:
            continue
        iss_raw = bd.get("issueValue") if bd.get("issueValue") is not None else bd.get("issueValueOmr")
        out_raw = bd.get("outstanding") if bd.get("outstanding") is not None else bd.get("outstandingOmr")
        fv_raw = bd.get("fairValue") if bd.get("fairValue") is not None else bd.get("fairValueOmr")
        b_mult = eff_mult
        if isinstance(out_raw, (int, float)) and out_raw > bs_ref * 1.8 and eff_mult > 1.0:
            b_mult = 1.0
        bonds_out.append({
            "name": nm,
            "isin": bd.get("isin") or None,
            "subType": bd.get("subType") or "Bonds",
            "currency": (str(bd.get("currency") or "OMR")).strip().upper()[:12],
            "issueValueOmr": round(float(iss_raw) * b_mult, 2) if isinstance(iss_raw, (int, float)) and iss_raw > 0 else None,
            "outstandingOmr": round(float(out_raw) * b_mult, 2) if isinstance(out_raw, (int, float)) and out_raw >= 0 else None,
            "couponPct": float(bd["couponPct"]) if isinstance(bd.get("couponPct"), (int, float)) else None,
            "maturityDate": bd.get("maturityDate") or None,
            "fairValueOmr": round(float(fv_raw) * b_mult, 2) if isinstance(fv_raw, (int, float)) and fv_raw > 0 else None,
            "effectiveRatePct": float(bd["effectiveRatePct"]) if isinstance(bd.get("effectiveRatePct"), (int, float)) else None,
            "terms": bd.get("terms") or None,
        })

    mp = raw.get("maturityProfile") or {}
    mp_out = {}
    for k in ("within1Y", "years1To5", "over5Y"):
        v = mp.get(k)
        mp_out[k] = round(float(v) * eff_mult, 2) if isinstance(v, (int, float)) and v >= 0 else None

    covenants = [str(c).strip() for c in (raw.get("covenants") or []) if str(c).strip()][:8]
    return {
        "weightedAvgRate": raw.get("weightedAvgRate") or None,
        "sourceNoteRef": raw.get("sourceNoteRef") or None,
        "sourceFilingUrl": target.get("filingUrl"),
        "currencyBreakdown": ccy_out,
        "loans": loans_out,
        "bonds": bonds_out,
        "covenants": covenants,
        "maturityProfile": mp_out,
    }


def stage_gemma(max_n=0, tickers_filter=None):
    mpath = os.path.join(EXCERPT_DIR, "_manifest.json")
    manifest = json.load(open(mpath, encoding="utf-8"))
    targets = manifest["targets"]
    if tickers_filter:
        tset = {x.strip().upper() for x in tickers_filter.split(",") if x.strip()}
        targets = [t for t in targets if t["ticker"] in tset]
    if max_n and max_n > 0:
        targets = targets[:max_n]

    profiles = json.load(open(OUT_PATH, encoding="utf-8")) if os.path.exists(OUT_PATH) else {}
    recon = []
    t0 = time.time()
    print(f"Running local Gemma ({MODEL}) extraction across {len(targets)} companies...")

    for idx_i, targ in enumerate(targets, 1):
        sym = targ["ticker"]
        epath = os.path.join(EXCERPT_DIR, f"{sym}.txt")
        if not os.path.exists(epath):
            recon.append({"ticker": sym, "status": "no_excerpt", "bsTarget": targ["totalTarget"]})
            continue
        etxt = open(epath, encoding="utf-8").read()
        try:
            raw = ask_gemma_debt(sym, etxt)
            norm = normalize_extracted(raw, targ)
            profiles[sym] = norm
            ccy_sum = sum(x["amountOmr"] for x in norm["currencyBreakdown"])
            lb_sum = sum(x.get("outstandingOmr") or 0 for x in norm["loans"]) + sum(x.get("outstandingOmr") or 0 for x in norm["bonds"])
            bs_ref = (targ["bs"].get("borrowingsC") or 0.0) + (targ["bs"].get("borrowingsNC") or 0.0) + (targ["bs"].get("debtSecurities") or 0.0) + (targ["bs"].get("tier1Securities") or 0.0)
            if bs_ref <= 0:
                bs_ref = targ["totalTarget"]
            ccy_diff_pct = round(abs(ccy_sum - bs_ref) / max(bs_ref, 1.0) * 100.0, 1) if ccy_sum > 0 else None
            recon.append({
                "ticker": sym,
                "status": "ok",
                "bsBorrowPlusBonds": round(bs_ref, 2),
                "bsTotalDebt": round(targ["totalTarget"], 2),
                "ccySum": round(ccy_sum, 2),
                "ccyDiffPct": ccy_diff_pct,
                "loansBondsSum": round(lb_sum, 2),
                "numCcy": len(norm["currencyBreakdown"]),
                "numLoans": len(norm["loans"]),
                "numBonds": len(norm["bonds"]),
                "rate": norm["weightedAvgRate"],
            })
            print(
                f"  [{idx_i}/{len(targets)}] {sym:5s} ({time.time()-t0:5.1f}s) | bs={bs_ref/1e6:7.2f}M | "
                f"ccy={ccy_sum/1e6:7.2f}M (diff={ccy_diff_pct}%) | loans={len(norm['loans'])} bonds={len(norm['bonds'])}"
            )
        except Exception as e:
            recon.append({"ticker": sym, "status": f"err: {e}", "bsTotalDebt": targ["totalTarget"]})
            print(f"  [{idx_i}/{len(targets)}] {sym:5s} ERR: {e}")

        # Save incrementally so interrupted runs preserve progress
        if idx_i % 10 == 0 or idx_i == len(targets):
            with open(OUT_PATH, "w", encoding="utf-8") as f:
                json.dump(profiles, f, indent=2, ensure_ascii=False)
            with open(RECON_PATH, "w", encoding="utf-8") as f:
                json.dump(recon, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(profiles)} debt profiles -> {OUT_PATH} and reconciliation -> {RECON_PATH}")


def main():
    ap = argparse.ArgumentParser(description="Extract MSX 6_Notes debt disclosures")
    ap.add_argument("--dump-excerpts", action="store_true")
    ap.add_argument("--run-gemma", action="store_true")
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--tickers", default="")
    args = ap.parse_args()

    targets = find_indebted_targets()
    if args.tickers and args.dump_excerpts:
        tset = {x.strip().upper() for x in args.tickers.split(",") if x.strip()}
        targets = [t for t in targets if t["ticker"] in tset]
    if args.max and args.dump_excerpts and not args.run_gemma:
        targets = targets[: args.max]

    if args.dump_excerpts:
        stage_dump(targets)
    if args.run_gemma:
        stage_gemma(max_n=args.max, tickers_filter=args.tickers)
    if not args.dump_excerpts and not args.run_gemma:
        print(f"Found {len(targets)} indebted companies. Pass --dump-excerpts and/or --run-gemma.")


if __name__ == "__main__":
    main()
