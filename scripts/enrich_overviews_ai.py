#!/usr/bin/env python3
"""
Oman Stock Intelligence — AI-Grounded Company Overview & Business Segment Enricher

Primary Sources ONLY (Zero Third-Party Aggregators):
  1. Official Corporate Website (homepage + About / Business / Operations pages)
  2. MSX Audited Annual Report ZIP (1_CompanyReport, 6_Notes IFRS 8 Segments, 9_MD&A)

Uses the official Google GenAI SDK (`google-genai` >= 3.2.0) Interactions API
(`client.interactions.create`) with `gemini-4-hydrogen`, `url_context`, and strict
JSON Schema structured output to populate `data/ai_profiles.json`.

Usage:
  # Enrich all missing non-flagship MSX SAOG companies (requires GEMINI_API_KEY)
  python3 scripts/enrich_overviews_ai.py --only-missing

  # Enrich specific tickers
  python3 scripts/enrich_overviews_ai.py --tickers OTEL,OQBI,OMVS,BKNZ

  # Harvest primary-source bundles only (no AI key needed — for offline inspection/audit)
  python3 scripts/enrich_overviews_ai.py --harvest-only /tmp/msx_sources --concurrency 8
"""

import argparse
import concurrent.futures
import datetime
import html
import io
import json
import os
import re
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import zipfile
from html.parser import HTMLParser

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
PROFILES_PATH = os.path.join(DATA_DIR, "ai_profiles.json")
MAC_PDF_BIN = "/Users/rajesh/.gemini/antigravity/brain/19e417e8-4296-4db5-a420-639a61d8dd0e/scratch/pdf_extract"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

SEGMENT_COLORS = [
    "bg-indigo-500",
    "bg-cyan-400",
    "bg-violet-400",
    "bg-emerald-400",
    "bg-amber-400",
    "bg-rose-400",
]

# Verified corporate websites for SAOGs where msx.om left the website field blank
VERIFIED_WEBSITE_FALLBACKS = {
    "SMNP": "https://www.smnpower.com/",
    "GECP": "https://www.galfar.com/",
    "OOMP": "https://www.oomco.com/",
    "SOMP": "https://www.shelloman.com.om/",
    "RCRI": "https://www.raysutcement.com.om/",
    "AMII": "https://www.almadinainvestment.com/",
    "PRFD": "https://www.pearlreif.com/",
}

PROFILE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "description": {
            "type": "string",
            "description": "3-4 sentence investor-grade overview of what the business actually does, its core products/services/brands/assets, and its role in Oman. Never write generic MSX listing boilerplate.",
        },
        "business_highlights": {
            "type": "array",
            "items": {"type": "string"},
            "description": "3 concise operational or strategic highlights grounded strictly in the corporate website and Annual Report.",
        },
        "segment_mode": {
            "type": "string",
            "enum": ["verified_pct", "qualitative"],
            "description": "Use 'verified_pct' ONLY when exact audited revenue/income percentages by segment (summing to ~100%) or a verified 100% single-segment revenue stream are explicitly disclosed in the provided Annual Report or corporate website. Never guess or estimate percentages; use 'qualitative' whenever exact % are not disclosed.",
        },
        "segment_basis": {
            "type": "string",
            "description": "Short provenance label such as 'FY2025 Audited Revenue Mix (IFRS 8)' or 'Core Operating Divisions (Website & FY2025 AR)'.",
        },
        "segments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Concise business segment or operating division name.",
                    },
                    "pct": {
                        "type": ["number", "null"],
                        "description": "Exact audited revenue/income contribution % (0-100) if segment_mode is 'verified_pct'; otherwise null.",
                    },
                    "summary": {
                        "type": "string",
                        "description": "Concise 1-sentence explanation of what this segment or business line covers.",
                    },
                },
                "required": ["name", "pct", "summary"],
            },
            "description": "2 to 6 key business segments or core operating lines (or 1 at 100% for a single-asset IPP/REIT/monoline company).",
        },
        "source_urls": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Exact primary URLs (corporate website and/or msx.om Annual Report) used.",
        },
    },
    "required": [
        "description",
        "business_highlights",
        "segment_mode",
        "segment_basis",
        "segments",
        "source_urls",
    ],
}

_tls = threading.local()


def get_opener():
    if not hasattr(_tls, "opener"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        _tls.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))
    return _tls.opener


def http_get_bytes(url, referer=None, timeout=18):
    hdrs = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        hdrs["Referer"] = referer
    req = urllib.request.Request(url, headers=hdrs)
    with get_opener().open(req, timeout=timeout) as r:
        return r.read(), r.geturl()


class _TextAndLinkParser(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript", "svg", "iframe", "head"}

    def __init__(self):
        super().__init__()
        self._skip = 0
        self.chunks = []
        self.links = []
        self._cur_href = None
        self._cur_a_text = []
        self.meta_desc = ""
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        t = tag.lower()
        attr_d = dict(attrs)
        if t == "title":
            self._in_title = True
        if t == "meta":
            name = (attr_d.get("name") or attr_d.get("property") or "").lower()
            if name in ("description", "og:description") and attr_d.get("content") and not self.meta_desc:
                self.meta_desc = attr_d["content"].strip()
        if t in self.SKIP_TAGS:
            self._skip += 1
            return
        if t == "a" and attr_d.get("href"):
            self._cur_href = attr_d["href"].strip()
            self._cur_a_text = []

    def handle_endtag(self, tag):
        t = tag.lower()
        if t == "title":
            self._in_title = False
        if t in self.SKIP_TAGS and self._skip > 0:
            self._skip -= 1
            return
        if t == "a" and self._cur_href is not None:
            txt = " ".join(self._cur_a_text).strip()
            self.links.append((self._cur_href, txt))
            self._cur_href = None
            self._cur_a_text = []
        if t in ("p", "div", "li", "h1", "h2", "h3", "h4", "section", "article", "br", "tr"):
            self.chunks.append("\n")

    def handle_data(self, data):
        if self._in_title:
            self.title += data
        if self._skip > 0:
            return
        s = data.strip()
        if not s:
            return
        self.chunks.append(s + " ")
        if self._cur_href is not None:
            self._cur_a_text.append(s)


def clean_html_text(raw_bytes):
    text = raw_bytes.decode("utf-8", errors="ignore")
    p = _TextAndLinkParser()
    try:
        p.feed(text)
    except Exception:
        pass
    joined = html.unescape("".join(p.chunks))
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in joined.splitlines()]
    lines = [ln for ln in lines if len(ln) >= 20]
    dedup = []
    seen = set()
    for ln in lines:
        key = ln.lower()[:80]
        if key not in seen:
            seen.add(key)
            dedup.append(ln)
    return {
        "title": re.sub(r"\s+", " ", html.unescape(p.title)).strip(),
        "meta_desc": html.unescape(p.meta_desc).strip(),
        "text": "\n".join(dedup),
        "links": p.links,
    }


def fetch_website_context(url, max_chars=7000):
    """Fetch corporate homepage + up to 2 relevant About/Business/Operations pages."""
    if not url or not url.startswith("http"):
        return {"urls": [], "text": ""}
    urls_used = []
    parts = []
    # Prefer HTTPS first, fall back to original URL
    candidates = [url]
    if url.startswith("http://"):
        candidates.insert(0, "https://" + url[len("http://"):])

    home_parsed = None
    home_final_url = None
    for cand in candidates:
        try:
            blob, final_url = http_get_bytes(cand, timeout=14)
            if len(blob) > 300:
                home_parsed = clean_html_text(blob)
                home_final_url = final_url
                break
        except Exception:
            continue

    if not home_parsed:
        return {"urls": [], "text": ""}

    urls_used.append(home_final_url)
    header = []
    if home_parsed["title"]:
        header.append(f"TITLE: {home_parsed['title']}")
    if home_parsed["meta_desc"]:
        header.append(f"META: {home_parsed['meta_desc']}")
    if home_parsed["text"]:
        header.append(home_parsed["text"][:3500])
    parts.append(f"--- CORPORATE HOMEPAGE ({home_final_url}) ---\n" + "\n".join(header))

    # Discover up to 2 high-value About / Business / Operations links on same host
    home_host = urllib.parse.urlparse(home_final_url).netloc.lower()
    sub_scores = []
    seen_sub = {home_final_url.rstrip("/")}
    for href, anchor in home_parsed["links"]:
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        full = urllib.parse.urljoin(home_final_url, href)
        parsed = urllib.parse.urlparse(full)
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc.lower() != home_host:
            continue
        if re.search(r"\.(pdf|jpg|jpeg|png|gif|zip|doc|docx|mp4|svg)$", parsed.path, re.I):
            continue
        norm = full.split("#")[0].rstrip("/")
        if norm in seen_sub:
            continue
        hay = (parsed.path + " " + anchor).lower()
        score = 0
        for kw, w in [
            ("about", 5),
            ("who-we-are", 5),
            ("who we are", 5),
            ("overview", 4),
            ("profile", 4),
            ("business", 4),
            ("operations", 4),
            ("services", 3),
            ("products", 3),
            ("divisions", 4),
            ("segments", 5),
            ("subsidiaries", 4),
            ("portfolio", 3),
        ]:
            if kw in hay:
                score += w
        if "/ar/" in hay or "arabic" in hay:
            score -= 10
        if score > 0:
            seen_sub.add(norm)
            sub_scores.append((score, norm))

    sub_scores.sort(key=lambda x: -x[0])
    for _, sub_url in sub_scores[:2]:
        try:
            blob, final_sub = http_get_bytes(sub_url, referer=home_final_url, timeout=12)
            sp = clean_html_text(blob)
            if len(sp["text"]) > 120:
                urls_used.append(final_sub)
                parts.append(f"--- CORPORATE SUBPAGE ({final_sub}) ---\n" + sp["text"][:2200])
        except Exception:
            continue

    combined = "\n\n".join(parts)[:max_chars]
    return {"urls": urls_used, "text": combined}


def extract_pdf_pages_list(pdf_bytes, max_pages=35):
    """Return list of page texts using macOS PDFKit if available, else pypdf."""
    if not pdf_bytes:
        return []
    if os.path.isfile(MAC_PDF_BIN) and os.access(MAC_PDF_BIN, os.X_OK):
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            tf.write(pdf_bytes)
            tmp_path = tf.name
        try:
            res = subprocess.run(
                [MAC_PDF_BIN, tmp_path, str(max_pages)],
                capture_output=True,
                text=True,
                timeout=20,
            )
            raw = res.stdout or ""
            pages = re.split(r"=== PAGE \d+ ===\n?", raw)
            return [p.strip() for p in pages if p.strip()]
        except Exception:
            return []
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages[:max_pages]:
            try:
                txt = page.extract_text(extraction_mode="layout") or ""
            except Exception:
                txt = page.extract_text() or ""
            pages.append(txt.strip())
        return pages
    except Exception:
        return []


def extract_annual_report_context(company_data, max_chars=9000):
    """Extract Chairman's Report, Principal Activities, and IFRS 8 Segment Note excerpts from MSX FY2025/FY2024 ZIP."""
    filings = company_data.get("filings") or []
    # Pick latest Annual audited filing first, else latest filing available
    target = None
    for f in filings:
        per = str(f.get("period", "")).upper()
        if "FY2025" in per or "ANNUAL" in str(f.get("title", "")).upper():
            target = f
            break
    if not target:
        for f in filings:
            if "FY2024" in str(f.get("period", "")).upper():
                target = f
                break
    if not target and filings:
        target = filings[0]
    if not target or not target.get("sourceUrl"):
        return {"url": "", "period": "", "text": ""}

    zip_url = target["sourceUrl"]
    referer = company_data.get("msxUrl") or "https://www.msx.om/"
    try:
        blob, _ = http_get_bytes(zip_url, referer=referer, timeout=25)
        if blob[:2] != b"PK":
            return {"url": zip_url, "period": target.get("period", ""), "text": ""}
        zf = zipfile.ZipFile(io.BytesIO(blob))
        names = [n for n in zf.namelist() if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")]
    except Exception:
        return {"url": zip_url, "period": target.get("period", ""), "text": ""}

    def find_pdf(prefixes):
        for pref in prefixes:
            for n in names:
                if os.path.basename(n).startswith(pref):
                    return n, zf.read(n)
        return None, None

    sections = []

    # 1. Company / Chairman Report (business overview & operations)
    nm1, b1 = find_pdf(["1_"])
    if b1:
        p1 = extract_pdf_pages_list(b1, max_pages=5)
        if p1:
            sections.append(f"--- MSX ANNUAL REPORT: {os.path.basename(nm1)} ---\n" + "\n\n".join(p1[:3])[:3000])

    # 2. MD&A Report (business lines & performance)
    nm9, b9 = find_pdf(["9_"])
    if b9:
        p9 = extract_pdf_pages_list(b9, max_pages=8)
        if p9:
            scored9 = []
            for idx, pg in enumerate(p9):
                low = pg.lower()
                sc = sum(
                    low.count(kw) * w
                    for kw, w in [
                        ("segment", 4),
                        ("revenue", 2),
                        ("division", 3),
                        ("business", 2),
                        ("operations", 2),
                        ("market share", 3),
                        ("products", 2),
                    ]
                )
                scored9.append((sc, idx, pg))
            scored9.sort(key=lambda x: (-x[0], x[1]))
            top9 = [pg for _, _, pg in scored9[:2] if pg]
            if top9:
                sections.append(f"--- MSX MD&A: {os.path.basename(nm9)} ---\n" + "\n\n".join(top9)[:2600])

    # 3. Notes to Financial Statements (Principal Activities + IFRS 8 Operating Segments / Revenue Disaggregation)
    nm6, b6 = find_pdf(["6_"])
    if b6:
        p6 = extract_pdf_pages_list(b6, max_pages=55)
        if p6:
            # Always include Page 1 (Legal status & Principal activities)
            picked_indices = [0]
            scored6 = []
            for idx, pg in enumerate(p6[1:], start=1):
                low = pg.lower()
                sc = 0
                for kw, w in [
                    ("operating segment", 12),
                    ("segmental", 10),
                    ("segment information", 12),
                    ("segment reporting", 12),
                    ("disaggregation of revenue", 12),
                    ("revenue from contracts with customers", 7),
                    ("principal activities", 8),
                    ("business segment", 10),
                    ("segment revenue", 10),
                    ("revenue by", 8),
                ]:
                    if kw in low:
                        sc += w
                # Bonus if page actually contains tabular numbers
                num_hits = len(re.findall(r"\b\d{1,3}(?:,\d{3})+\b", pg))
                if sc >= 8 and num_hits >= 4:
                    sc += 6
                if sc >= 8:
                    scored6.append((sc, idx, pg))
            scored6.sort(key=lambda x: (-x[0], x[1]))
            for _, idx, _ in scored6[:3]:
                if idx not in picked_indices:
                    picked_indices.append(idx)
            picked_indices.sort()
            note_chunks = [f"[Notes p.{i+1}]\n{p6[i]}" for i in picked_indices if i < len(p6)]
            if note_chunks:
                sections.append(f"--- MSX NOTES (IFRS 8 / PRINCIPAL ACTIVITIES): {os.path.basename(nm6)} ---\n" + "\n\n".join(note_chunks)[:4500])

    return {
        "url": zip_url,
        "period": target.get("period", ""),
        "text": "\n\n".join(sections)[:max_chars],
    }


def harvest_company_context(company_data):
    """Collect corporate website text + MSX FY2025 Annual Report excerpts for one ticker."""
    ticker = company_data["ticker"]
    web_url = (company_data.get("website") or "").strip()
    if (not web_url or not web_url.startswith("http")) and ticker in VERIFIED_WEBSITE_FALLBACKS:
        web_url = VERIFIED_WEBSITE_FALLBACKS[ticker]

    web_ctx = fetch_website_context(web_url)
    ar_ctx = extract_annual_report_context(company_data)

    return {
        "ticker": ticker,
        "name": company_data.get("name", ""),
        "sector": company_data.get("sector", ""),
        "sectorGroup": company_data.get("sectorGroup", ""),
        "market": company_data.get("market", ""),
        "websiteUrl": web_url,
        "websiteUrlsFetched": web_ctx["urls"],
        "websiteText": web_ctx["text"],
        "annualReportUrl": ar_ctx["url"],
        "annualReportPeriod": ar_ctx["period"],
        "annualReportText": ar_ctx["text"],
        "financialSummary": {
            "latestFiscalYear": company_data.get("latestFiscalYear"),
            "marketCapOmr": company_data.get("marketCapOmr"),
            "years": company_data.get("financials", {}).get("years", []),
            "primaryLabel": company_data.get("financials", {}).get("primaryLabel", ""),
            "primaryM": company_data.get("financials", {}).get("primary", []),
            "netIncomeM": company_data.get("financials", {}).get("netIncome", []),
        },
    }


def validate_and_normalize_profile(raw_obj, ctx, model_name):
    """Enforce strict anti-hallucination guardrails on AI-generated profile JSON."""
    desc = re.sub(r"\s+", " ", str(raw_obj.get("description") or "")).strip()
    highlights = [
        re.sub(r"\s+", " ", str(h)).strip()
        for h in (raw_obj.get("business_highlights") or raw_obj.get("businessHighlights") or [])
        if str(h).strip()
    ][:4]

    mode = raw_obj.get("segment_mode") or raw_obj.get("segmentMode") or "qualitative"
    basis = re.sub(r"\s+", " ", str(raw_obj.get("segment_basis") or raw_obj.get("segmentNote") or "")).strip()
    raw_segs = raw_obj.get("segments") or []

    clean_segs = []
    for i, s in enumerate(raw_segs[:6]):
        name = re.sub(r"\s+", " ", str(s.get("name") or "")).strip()
        summary = re.sub(r"\s+", " ", str(s.get("summary") or "")).strip()
        if not name:
            continue
        pct = s.get("pct")
        try:
            pct_val = round(float(pct), 1) if pct is not None else None
        except Exception:
            pct_val = None
        clean_segs.append(
            {
                "name": name,
                "pct": pct_val,
                "summary": summary,
                "color": SEGMENT_COLORS[i % len(SEGMENT_COLORS)],
            }
        )

    # Strict validation: verified_pct ONLY allowed if every segment has a positive pct summing to 95-105%
    if mode == "verified_pct" and clean_segs:
        pcts = [s["pct"] for s in clean_segs]
        if all(p is not None and p > 0 for p in pcts):
            total = sum(pcts)
            if 95.0 <= total <= 105.0:
                if abs(total - 100.0) > 0.05:
                    # Normalize minor rounding drift cleanly to 100.0%
                    scaled = [round(p * 100.0 / total, 1) for p in pcts]
                    diff = round(100.0 - sum(scaled), 1)
                    scaled[0] = round(scaled[0] + diff, 1)
                    for s, p in zip(clean_segs, scaled):
                        s["pct"] = p
            else:
                mode = "qualitative"
        else:
            mode = "qualitative"
    else:
        mode = "qualitative"

    if mode == "qualitative":
        for s in clean_segs:
            s["pct"] = None
        if not basis or "audited revenue mix" in basis.lower():
            basis = "Core Business Lines (Segment % not disclosed in FY2025 FS)"

    # Filter sources to primary domains only (never third-party aggregators or hijacked cross-domain redirects)
    forbidden = ("yahoo", "investing.com", "tradingview", "google.com/finance", "marketscreener", "simplywall", "wikipedia", "bloomberg", "reuters", "zawya", "mubasher", "slotbet", "casino", "gibraltarliterary")
    sources = []
    explicit_sources = raw_obj.get("source_urls") if "source_urls" in raw_obj else raw_obj.get("overviewSources")
    for u in (explicit_sources or []):
        us = str(u).strip()
        if us.startswith("http") and not any(f in us.lower() for f in forbidden) and us not in sources:
            sources.append(us)
    if not sources:
        orig_host = urllib.parse.urlparse(ctx.get("websiteUrl") or "").netloc.lower().replace("www.", "")
        fetched = (ctx.get("websiteUrlsFetched") or [None])[0]
        fetched_host = urllib.parse.urlparse(fetched or "").netloc.lower().replace("www.", "")
        if fetched and orig_host and (orig_host in fetched_host or fetched_host in orig_host) and not any(f in fetched.lower() for f in forbidden):
            sources.append(fetched)
        if ctx.get("annualReportUrl"):
            sources.append(ctx["annualReportUrl"])
        if not sources and ctx.get("ticker"):
            sources.append(f"https://www.msx.om/snapshot.aspx?s={ctx['ticker']}")

    return {
        "description": desc,
        "businessHighlights": highlights,
        "segmentMode": mode,
        "segmentNote": basis or ("FY2025 Audited Segment Mix" if mode == "verified_pct" else "Core Business Lines"),
        "segments": clean_segs,
        "overviewSources": sources,
        "websiteOverride": ctx.get("websiteUrl") if ctx.get("ticker") in VERIFIED_WEBSITE_FALLBACKS else None,
        "generatedAt": datetime.date.today().isoformat(),
        "model": model_name,
    }


def enrich_with_gemini(ctx, model="gemini-4-hydrogen"):
    """Call Google GenAI Interactions API with url_context + strict JSON Schema."""
    from google import genai

    client = genai.Client()

    web_hint = f"Official Corporate Website: {ctx['websiteUrl']}" if ctx.get("websiteUrl") else "Official Corporate Website: Not listed on msx.om"
    prompt = f"""You are a senior Muscat Stock Exchange (MSX) equity research analyst writing the Company Overview card for Oman Stock Intelligence.

Company: {ctx['name']} (Ticker: {ctx['ticker']}, Sector: {ctx['sector']}, Market: {ctx['market']})
{web_hint}
MSX Annual Report: {ctx.get('annualReportPeriod') or 'Latest'} ({ctx.get('annualReportUrl') or 'N/A'})

STRICT RULES:
1. Base your answer EXCLUSIVELY on the company's official corporate website (you may read {ctx.get('websiteUrl') or 'N/A'} via url_context) and the MSX Annual Report excerpts provided below. Do NOT cite third-party financial aggregators.
2. Write a crisp, informative 3-4 sentence `description` explaining what the company actually does, its products/services/brands/facilities, and its role in Oman (similar to a US equity research snapshot). Never repeat generic MSX boilerplate.
3. For `segments`:
   - Set `segment_mode` to `"verified_pct"` ONLY if the MSX Annual Report Notes (IFRS 8 Operating Segments / Revenue Disaggregation) or corporate website explicitly disclose segment revenue/income figures that let you compute exact percentages summing to ~100% (or if it is explicitly a single-segment business like a single contracted IPP power plant at 100%).
   - NEVER guess, estimate, or fabricate percentages! If exact segment revenue splits are not disclosed in the provided text, set `segment_mode` to `"qualitative"`, set every segment's `pct` to `null`, and list 2-5 core operating divisions / business lines with concise 1-sentence summaries.
4. Keep `business_highlights` to 3 crisp, factual operational bullets (no fluff, no "official" endorsement claims).

--- EXTRACTED CORPORATE WEBSITE TEXT ---
{ctx.get('websiteText') or '(Website unreachable or unavailable)'}

--- EXTRACTED MSX ANNUAL REPORT EXCERPTS ---
{ctx.get('annualReportText') or '(Annual report text unavailable)'}
"""

    tools = [{"type": "url_context"}] if ctx.get("websiteUrl") else []
    kwargs = {
        "model": model,
        "input": prompt,
        "response_format": {
            "type": "text",
            "mime_type": "application/json",
            "schema": PROFILE_JSON_SCHEMA,
        },
        "store": False,
    }
    if tools:
        kwargs["tools"] = tools

    interaction = client.interactions.create(**kwargs)
    raw = json.loads(interaction.output_text)
    return validate_and_normalize_profile(raw, ctx, model)


def main():
    parser = argparse.ArgumentParser(description="Enrich MSX company overviews & business segments from primary sources.")
    parser.add_argument("--tickers", help="Comma-separated list of MSX tickers to process")
    parser.add_argument("--only-missing", action="store_true", help="Skip tickers already present in data/ai_profiles.json")
    parser.add_argument("--include-flagships", action="store_true", help="Also enrich the 8 flagship companies (skipped by default)")
    parser.add_argument("--model", default="gemini-4-hydrogen", help="Gemini model ID (default: gemini-4-hydrogen)")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallel worker threads")
    parser.add_argument("--harvest-only", metavar="OUT_DIR", help="Only harvest website + MSX AR context bundles to OUT_DIR (no AI API call)")
    args = parser.parse_args()

    with open(os.path.join(DATA_DIR, "index.json"), encoding="utf-8") as f:
        index_data = json.load(f)

    existing = {}
    if os.path.isfile(PROFILES_PATH):
        with open(PROFILES_PATH, encoding="utf-8") as f:
            existing = json.load(f)

    # Determine target tickers
    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers.split(",") if t.strip()}
        targets = [c for c in index_data["companies"] if c["ticker"] in wanted]
    else:
        targets = [
            c for c in index_data["companies"]
            if args.include_flagships or not c.get("isFlagship")
        ]
        if args.only_missing and not args.harvest_only:
            targets = [c for c in targets if c["ticker"] not in existing]

    # Sort largest Market Cap first so key issuers complete early
    targets.sort(key=lambda c: -float(c.get("marketCapOmr") or 0))
    print(f"Target MSX companies: {len(targets)}")

    if args.harvest_only:
        os.makedirs(args.harvest_only, exist_ok=True)
        t0 = time.time()
        done = 0
        stats = {"web_ok": 0, "ar_ok": 0}

        def _harvest_one(c):
            d = json.load(open(os.path.join(ROOT_DIR, c["dataFile"]), encoding="utf-8"))
            ctx = harvest_company_context(d)
            out_path = os.path.join(args.harvest_only, f"{c['ticker']}.json")
            with open(out_path, "w", encoding="utf-8") as wf:
                json.dump(ctx, wf, indent=2, ensure_ascii=False)
            return c["ticker"], bool(ctx["websiteText"]), bool(ctx["annualReportText"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            for tk, w_ok, ar_ok in ex.map(_harvest_one, targets):
                done += 1
                if w_ok:
                    stats["web_ok"] += 1
                if ar_ok:
                    stats["ar_ok"] += 1
                if done % 15 == 0 or done == len(targets):
                    print(f"  Harvested {done}/{len(targets)} ({time.time()-t0:.1f}s) | web_ok={stats['web_ok']} ar_ok={stats['ar_ok']}", flush=True)
        return

    if not os.environ.get("GEMINI_API_KEY"):
        print("ERROR: GEMINI_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    lock = threading.Lock()
    t0 = time.time()
    ok_count = 0
    err_count = 0

    def _process_one(c):
        d = json.load(open(os.path.join(ROOT_DIR, c["dataFile"]), encoding="utf-8"))
        ctx = harvest_company_context(d)
        profile = enrich_with_gemini(ctx, model=args.model)
        return c["ticker"], profile

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = {ex.submit(_process_one, c): c["ticker"] for c in targets}
        for fut in concurrent.futures.as_completed(futs):
            tk = futs[fut]
            try:
                _, prof = fut.result()
                with lock:
                    existing[tk] = prof
                    ok_count += 1
                    with open(PROFILES_PATH, "w", encoding="utf-8") as wf:
                        json.dump(dict(sorted(existing.items())), wf, indent=2, ensure_ascii=False)
                print(
                    f"  [{ok_count + err_count}/{len(targets)}] {tk}: {prof['segmentMode']} ({len(prof['segments'])} segs) — {time.time()-t0:.1f}s",
                    flush=True,
                )
            except Exception as exc:
                with lock:
                    err_count += 1
                print(f"  [{ok_count + err_count}/{len(targets)}] {tk}: FAILED ({exc})", flush=True)

    print(f"Done: {ok_count} enriched, {err_count} failed -> {PROFILES_PATH}")


if __name__ == "__main__":
    main()
