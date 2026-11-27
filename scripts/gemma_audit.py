#!/usr/bin/env python3
"""
Tier-2 Local Gemma (`google/gemma-4-e4b`) Semantic & OCR Auditor.

Reads Tier-1 flagged tickers from /tmp/audit_report.json plus all entries in
data/historical_ocr_backfill.json, inspects statement rows alongside any raw
OCR text (e.g. /tmp/ins_ocr_raw.txt) or unit metadata, and asks local LM Studio
to verify signs, units, column alignment, and accounting plausibility.
"""

import argparse
import json
import os
import re
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
LM_URL = "http://127.0.0.1:1234/v1/chat/completions"
MODEL = "google/gemma-4-e4b"


def load_ocr_snippets(raw_path="/tmp/ins_ocr_raw.txt"):
    if not os.path.exists(raw_path):
        return {}
    raw = open(raw_path, "r", encoding="utf-8").read()
    by_sym = {}
    cur_sym = None
    buf = []
    for line in raw.splitlines():
        if line.startswith("==================== "):
            if cur_sym and buf:
                by_sym.setdefault(cur_sym, []).append("\n".join(buf))
            cur_sym = line.strip("= ").split()[0].strip()
            buf = [line]
        else:
            buf.append(line)
    if cur_sym and buf:
        by_sym.setdefault(cur_sym, []).append("\n".join(buf))
    return {sym: "\n".join(chunks)[:7000] for sym, chunks in by_sym.items()}


def query_gemma(prompt):
    payload = {
        "model": MODEL,
        "temperature": 0.1,
        "max_tokens": 4096,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "audit_verdict",
                "schema": {
                    "type": "object",
                    "properties": {
                        "verdict": {"type": "string", "enum": ["PASS", "WARN", "FAIL"]},
                        "issues": {"type": "array", "items": {"type": "string"}},
                        "summary": {"type": "string"},
                    },
                    "required": ["verdict", "issues", "summary"],
                },
            },
        },
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an MSX IFRS financial statement QA auditor. Compare the parsed JSON "
                    "values against any Tier-1 flags and raw OCR excerpts. Check for unit scaling errors "
                    "(e.g. 1x vs 1000x), column shifts, sign inversions, or Equity vs Total Equity & Liabilities mistakes. "
                    "Be concise and respond immediately with JSON only."
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
    with urllib.request.urlopen(req, timeout=120) as resp:
        res = json.loads(resp.read().decode("utf-8"))
    msg = res["choices"][0]["message"]
    txt = (msg.get("content") or msg.get("reasoning_content") or "").strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt)
    m = re.search(r"\{.*\}", txt, re.S)
    if m:
        return json.loads(m.group(0))
    return {"verdict": "WARN", "issues": ["Unstructured reply"], "summary": txt[:200]}


def compact_statements(cdata):
    st = cdata.get("statements") or {}
    out = {"template": st.get("template"), "paidUpCapital": cdata.get("paidUpCapital"), "sharesOutRaw": cdata.get("sharesOutRaw")}
    for sec_name in ("incomeStatement", "balanceSheet", "cashFlow"):
        rows = {}
        for r in st.get(sec_name) or []:
            if r.get("rowType") == "section":
                continue
            rows[r["key"]] = r.get("values") or {}
        out[sec_name] = rows
    return out


def main():
    ap = argparse.ArgumentParser(description="Tier-2 Local Gemma financial statement QA")
    ap.add_argument("--audit-report", default="/tmp/audit_report.json")
    ap.add_argument("--output", default="/tmp/gemma_audit_report.json")
    ap.add_argument("--tickers", default="", help="Comma-separated override tickers")
    ap.add_argument("--max", type=int, default=40, help="Max companies to audit via local Gemma")
    args = ap.parse_args()

    backfill_path = os.path.join(DATA_DIR, "historical_ocr_backfill.json")
    backfill = json.load(open(backfill_path)) if os.path.exists(backfill_path) else {}
    tier1 = json.load(open(args.audit_report)) if os.path.exists(args.audit_report) else {}
    tier1_map = {c["ticker"]: c for c in (tier1.get("companies") or [])}
    ocr_snippets = load_ocr_snippets()

    if args.tickers:
        targets = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        hard_first = [c["ticker"] for c in (tier1.get("companies") or []) if c.get("issues")]
        ocr_tickers = sorted(backfill.keys())
        warn_tickers = [c["ticker"] for c in (tier1.get("companies") or []) if c.get("warnings") and c.get("reporting")]
        targets = []
        for t in hard_first + ocr_tickers + warn_tickers:
            if t not in targets:
                targets.append(t)
        targets = targets[: args.max]

    print(f"[Tier-2 Gemma Audit] Checking {len(targets)} companies using {MODEL}...")
    results = []
    for sym in targets:
        fpath = os.path.join(DATA_DIR, f"{sym}.json")
        if not os.path.exists(fpath):
            continue
        cdata = json.load(open(fpath))
        t1 = tier1_map.get(sym) or {}
        prompt = (
            f"Ticker: {sym} ({cdata.get('name','')})\n"
            f"Tier-1 Issues: {json.dumps(t1.get('issues') or [])}\n"
            f"Tier-1 Warnings: {json.dumps(t1.get('warnings') or [])}\n"
            f"Parsed Statements (OMR):\n{json.dumps(compact_statements(cdata))}\n"
        )
        if sym in ocr_snippets:
            prompt += f"\nRaw OCR Filing Excerpt:\n{ocr_snippets[sym][:5500]}\n"
        try:
            verdict = query_gemma(prompt)
        except Exception as e:
            verdict = {"verdict": "ERROR", "issues": [str(e)], "summary": "LM Studio request failed"}
        v = verdict.get("verdict", "WARN")
        print(f"  {sym:6s} -> {v:5s} | {verdict.get('summary','')[:110]}")
        results.append({"ticker": sym, **verdict})

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({"model": MODEL, "checked": len(results), "results": results}, f, indent=2)
    print(f"Saved Tier-2 Gemma report to {args.output}")


if __name__ == "__main__":
    main()
