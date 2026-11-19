#!/usr/bin/env python3
"""
Oman Stock Intelligence — Full MSX Automated Data Sync Pipeline
Runs locally (macOS PDFKit or pypdf) and unattended in GitHub Actions (pypdf).

Data Sources (Primary Public Disclosures Only — Zero Third-Party Aggregators):
  1. msx.om — Listed company universe, market quotes, 1Y daily closing prices,
     standardized XBRL financial statement PDFs (linked directly to msx.om),
     Board of Directors registry, and regulatory disclosures.
  2. mcd.om — Listed instruments registry (ISIN, total issued shares, paid-up
     capital, registered shareholders), Major Investors (5%+), and nationality
     ownership splits (Omani / GCC / Arab / Foreign).
  3. Corporate Websites — Enriched Board & Executive Management profiles where
     verified (with automatic fallback to msx.om Board/Profile records).
"""

import concurrent.futures
import datetime
import http.cookiejar
import io
import json
import math
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

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(ROOT_DIR, ".msx_cache")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Optional native macOS PDFKit helper compiled during pilot setup; falls back to pypdf on Linux/CI
MAC_PDF_BIN = "/Users/rajesh/.gemini/antigravity/brain/19e417e8-4296-4db5-a420-639a61d8dd0e/scratch/pdf_extract"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

MCD_URLS = {
    "listed": "https://www.mcd.om/en/Default/Statistic/ListedInstrumentsInfo",
    "major": "https://www.mcd.om/en/Default/Statistic/MajorInvestors5OrMore",
    "nationality": "https://www.mcd.om/en/Default/Statistic/InvestorOwnership",
}

ARABIC_HOLDER_ENGLISH = {
    "الشركه العمانيه العالميه القابضه ش م ع م": "Oman International Holding SAOC (الشركة العمانية العالمية القابضة)",
    "سهيل سالم عبدالله المخينى بهوان": "Suhail Salim Abdullah Al Mukhaini Bahwan (سهيل سالم عبدالله المخيني بهوان)",
    "شركه ملتن للتجاره والمقاولات": "Multitech Trading & Contracting (شركة ملتن للتجارة والمقاولات)",
    "عبدالحافظ سالم رجب العجيلى": "Eng. Abdul Hafidh Salim Rajab Al Aujaili (عبدالحافظ سالم رجب العجيلي)",
    "مؤسسه قيس العمانيه": "Qais Omani Establishment (مؤسسة قيس العمانية)",
    "مؤسسة الراتب التجاريه": "Al Ratib Commercial Establishment (مؤسسة الراتب التجارية)",
}

# Import flagship 8 curated corporate-website overrides & verified narratives from generate_data.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from generate_data import COMPANY_VERIFIED as FLAGSHIP_VERIFIED
except Exception:
    FLAGSHIP_VERIFIED = {}

FLAGSHIP_ORDER = ["BKMB", "BKSB", "NBOB", "BKDB", "OQGN", "OQEP", "ABRJ", "PHPC"]

MARKET_NAMES = {
    "1": "Regular Market",
    "2": "Parallel Market",
    "3": "Third Market",
    "5": "Under Monitoring",
    "9": "Funds & REITs",
}

# Thread-local HTTP opener so cookies & SSL context are thread-safe
_tls = threading.local()


def get_opener():
    if not hasattr(_tls, "opener"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        cj = http.cookiejar.CookieJar()
        _tls.cj = cj
        _tls.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cj),
            urllib.request.HTTPSHandler(context=ctx),
        )
    return _tls.opener


def http_get(url, headers=None, timeout=25, retries=2):
    hdrs = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
    if headers:
        hdrs.update(headers)
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with get_opener().open(req, timeout=timeout) as r:
                return r.read()
        except Exception:
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))


def http_post_json(url, payload, referer=None, timeout=25, retries=2):
    data = json.dumps(payload).encode("utf-8")
    hdrs = {
        "User-Agent": UA,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        hdrs["Referer"] = referer
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
            with get_opener().open(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8-sig", errors="ignore"))
        except Exception:
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))


def extract_pdf_text_bytes(pdf_bytes, max_pages=4):
    """Extract text from PDF bytes using macOS PDFKit binary if available, else pypdf."""
    if os.path.isfile(MAC_PDF_BIN) and os.access(MAC_PDF_BIN, os.X_OK):
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            tf.write(pdf_bytes)
            tmp_path = tf.name
        try:
            res = subprocess.run([MAC_PDF_BIN, tmp_path, str(max_pages)], capture_output=True, text=True, timeout=15)
            return res.stdout
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        out = []
        for i, page in enumerate(reader.pages[:max_pages]):
            try:
                txt = page.extract_text(extraction_mode="layout") or ""
            except Exception:
                txt = page.extract_text() or ""
            out.append(f"=== PAGE {i+1} ===\n" + txt)
        return "\n".join(out)
    except Exception:
        return ""


def parse_float(val, default=0.0):
    if val is None:
        return default
    try:
        s = str(val).replace(",", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        return float(s)
    except Exception:
        return default


def fmt_omr_large(num):
    if num is None or (isinstance(num, float) and math.isnan(num)):
        return "N/A"
    n = float(num)
    abs_n = abs(n)
    sign = "-" if n < 0 else ""
    if abs_n >= 1e9:
        return f"{sign}OMR {abs_n / 1e9:.2f}B"
    if abs_n >= 1e6:
        return f"{sign}OMR {abs_n / 1e6:.2f}M"
    if abs_n >= 1e3:
        return f"{sign}OMR {abs_n / 1e3:.1f}K"
    return f"{sign}OMR {abs_n:.3f}"


def fmt_shares(num):
    if num is None:
        return "N/A"
    n = abs(float(num))
    if n >= 1e9:
        return f"{n / 1e9:.2f}B"
    if n >= 1e6:
        return f"{n / 1e6:.2f}M"
    if n >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{int(round(n)):,}"


def extract_json_array(text, marker):
    idx = text.find(marker)
    if idx < 0:
        return []
    start = text.rfind("[", 0, idx)
    dec = json.JSONDecoder()
    obj, _ = dec.raw_decode(text, start)
    return obj


# ==============================================================================
# DETERMINISTIC MSX XBRL PDF STATEMENT PARSER
# ==============================================================================
NUM_TOKEN_RE = re.compile(r"\(\s*[\d,]+(?:\.\d+)?\s*\)|-?[\d,]+(?:\.\d+)?")


def parse_line_numbers(line):
    """Extract numeric tokens from the right side of an XBRL PDF statement line."""
    # Remove trailing date tokens like 31/12/2025
    clean = re.sub(r"\b\d{2}/\d{2}/\d{4}\b", " ", line)
    toks = NUM_TOKEN_RE.findall(clean)
    nums = []
    for tk in toks:
        s = tk.replace(" ", "").replace(",", "")
        neg = s.startswith("(") and s.endswith(")")
        if neg:
            s = s[1:-1]
        try:
            v = float(s)
            if neg:
                v = -v
            nums.append(v)
        except Exception:
            pass
    return nums


def pick_pair(nums, is_four_col=False, is_interim_is=False):
    """
    In MSX XBRL PDFs:
      - Standard 2-col Annual: [current, prior]
      - 4-col Annual (Consolidated + Parent): [cons_curr, par_curr, cons_prior, par_prior]
      - 3-col Q3 Balance Sheet: [30/09/curr, 30/09/prior, 31/12/prior]
      - 4-col Q3 Income Statement: [Q3_curr, Q3_prior, 9M_curr, 9M_prior]
      - 8-col Q3 Income Statement (Cons+Par): [Q3_c_cur, Q3_p_cur, Q3_c_pri, Q3_p_pri, 9M_c_cur, 9M_p_cur, 9M_c_pri, 9M_p_pri]
    """
    if not nums:
        return None, None
    if is_interim_is:
        if len(nums) >= 8:
            return nums[4], nums[6]
        if len(nums) >= 4:
            return nums[2], nums[3]
        if len(nums) >= 2:
            return nums[0], nums[1]
        return nums[0], None
    if is_four_col:
        if len(nums) >= 4:
            return nums[0], nums[2]
        if len(nums) >= 2:
            return nums[0], nums[1]
        return nums[0], None
    if len(nums) >= 2:
        return nums[0], nums[1]
    return nums[0], None


def parse_xbrl_statement_text(text, patterns, is_interim_is=False):
    """Search lines before Subclassifications / Comprehensive Income for target line items."""
    for cut in ["Subclassifications of", "Statement of comprehensive income", "Analysis of income", "Analysis of Income"]:
        idx = text.find(cut)
        if idx > 80:
            text = text[:idx]

    # Detect unit declaration in header
    unit_mult = 1.0
    if re.search(r"Thousands\s*/\s*Omani\s+Rial|RO\s*'000|OMR\s*'000|\(\s*in\s+thousands\s*\)", text[:600], re.I):
        unit_mult = 1000.0
    elif re.search(r"Millions\s*/\s*Omani\s+Rial", text[:600], re.I):
        unit_mult = 1e6

    # Detect 4-column Consolidated + Separate layout (4 dates on one header line or 2 pairs of same year)
    header_zone = text[:900]
    dates_found = re.findall(r"\d{2}/\d{2}/\d{4}", header_zone)
    is_four_col = False
    if not is_interim_is and len(dates_found) >= 4:
        # e.g., 31/12/2024 31/12/2024 31/12/2025 31/12/2025
        if len(set(dates_found[:4])) <= 2:
            is_four_col = True

    out = {"_unitMult": unit_mult, "_fourCol": is_four_col}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for key, regex_list in patterns.items():
        for rx in regex_list:
            matched = False
            for ln in lines:
                if re.search(rx, ln, re.I):
                    # Ensure label comes before numbers and avoid false-positive totals
                    nums = parse_line_numbers(ln)
                    if nums:
                        c_val, p_val = pick_pair(nums, is_four_col=is_four_col, is_interim_is=is_interim_is)
                        if c_val is not None:
                            out[key] = (c_val, p_val)
                            matched = True
                            break
            if matched:
                break
    return out


BS_PATTERNS = {
    "shareCapital": [r"^Share\s+capital\b"],
    "totalAssets": [r"^Total\s+assets\b"],
    "totalEquity": [r"^Total\s+equity\b"],
    "attrEquity": [r"^Total\s+[Ee]quity\s+attributable\s+to"],
    "totalLiabilities": [r"^Total\s+liabilities\b"],
    "loans": [r"^Loans\s+and\s+advances\b"],
    "islamicFin": [r"^Islamic\s+financing\s+receivables\b"],
    "deposits": [r"^Deposits\s+from\s+customers\b"],
    "islamicDep": [r"^Islamic\s+customers?\s+deposit"],
    "cash": [r"^Cash\s+and\s+bank\s+balances\b", r"^Cash\s+and\s+cash\s+equivalents\b"],
    "borrowingsNC": [r"^Borrowings,\s*non-current\b", r"^Non-current\s+borrowings\b", r"^Term\s+loans,\s*non-current\b"],
    "borrowingsC": [r"^Borrowings,\s*current\b", r"^Current\s+borrowings\b", r"^Short\s+term\s+borrowings\b"],
    "nav": [r"^Net\s+assets\s+per\s+share\b"],
}

IS_PATTERNS = {
    "revenue": [r"^Revenue\b", r"^Total\s+revenue\b", r"^Insurance\s+revenue\b", r"^Gross\s+written\s+premium", r"^Rental\s+income\b"],
    "grossProfit": [r"^Gross\s+profit\b"],
    "opProfit": [r"^Profit\s*\(loss\)\s+from\s+operating\s+activities\b", r"^Operating\s+profit\b"],
    "opInc": [r"^Total\s+operating\s+income\b", r"^Total\s+income\b"],
    "nii": [r"^Net\s+interest\s+income\b"],
    "islamicNetInc": [r"^Net\s+income\s+from\s+islamic\s+financing\b"],
    "opEx": [r"^Total\s+operating\s+expenses\b"],
    "da": [r"^Depreciation(?:,\s*depletion)?\s+and\s+amortis?ation"],
    "netProfit": [
        r"^Profit\s*\(loss\)\s+for\s+(?:the\s+)?period\b",
        r"^Profit\s*\(loss\)\s+from\s+continuing\s+operations\b",
        r"^Profit\s*\(loss\),\s*attributable\s+to\s+owners\b",
        r"^Profit\s*\(loss\)\s+[\(\-\d]",
        r"^Net\s+profit\s*\(loss\)",
    ],
    "eps": [r"^Total\s+basic\s+earnings\s*\(loss\)\s+per\s+share\b", r"^Basic\s+earnings\s*\(loss\)\s+per\s+share\b"],
}

CF_PATTERNS = {
    "ocf": [r"^Net\s+cash\s+flows?\s+from\s*\(used\s+in\)\s+operating\s+activities\b"],
    "capex": [r"^Purchase\s+of\s+property,\s*plant\s+and\s+equipment\b", r"^Additions?\s+to\s+property,\s*plant\s+and\s+equipment\b"],
    "investing": [r"^Net\s+cash\s+flows?\s+from\s*\(used\s+in\)\s+investing\s+activities\b"],
    "da": [r"^Adjustments?\s+for\s+depreciation(?:[/\s]+and)?[/\s]+amortis?ation\b"],
    "divPaid": [r"^Cash\s+dividends?\s+paid\b", r"^Dividends?\s+paid\s+to\s+equity\s+holders\b", r"^Dividends?\s+paid\b"],
}


def parse_filing_zip_in_memory(zip_url, referer, paid_up_cap_omr, is_interim=False):
    """Download MSX filing ZIP into memory and parse 2_BS, 3_IS, 4_CF PDFs."""
    try:
        blob = http_get(zip_url, headers={"Referer": referer}, timeout=30, retries=1)
        if blob[:2] != b"PK":
            return None
        zf = zipfile.ZipFile(io.BytesIO(blob))
        names = [n for n in zf.namelist() if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")]

        def find_pdf(prefix):
            for n in names:
                base = os.path.basename(n)
                if base.startswith(prefix):
                    return zf.read(n)
            return None

        bs_bytes = find_pdf("2_")
        is_bytes = find_pdf("3_")
        cf_bytes = find_pdf("4_")

        bs = parse_xbrl_statement_text(extract_pdf_text_bytes(bs_bytes, 2), BS_PATTERNS, False) if bs_bytes else {}
        inc = parse_xbrl_statement_text(extract_pdf_text_bytes(is_bytes, 2), IS_PATTERNS, is_interim) if is_bytes else {}
        cf = parse_xbrl_statement_text(extract_pdf_text_bytes(cf_bytes, 2), CF_PATTERNS, False) if cf_bytes else {}

        # Auto-calibrate unit multiplier (1x vs 1000x) using MCD paid-up capital vs BS Share Capital
        mult = bs.get("_unitMult", 1.0)
        sc_pair = bs.get("shareCapital")
        if sc_pair and sc_pair[0] and paid_up_cap_omr and paid_up_cap_omr > 100000:
            ratio = paid_up_cap_omr / abs(sc_pair[0])
            if 500 <= ratio <= 2000:
                mult = 1000.0
            elif 0.5 <= ratio <= 2.0:
                mult = 1.0
        elif inc.get("_unitMult", 1.0) == 1000.0:
            mult = 1000.0

        def scale(d, k, idx=0):
            pair = d.get(k)
            if not pair or pair[idx] is None:
                return None
            val = pair[idx]
            if k in ("eps", "nav"):
                # Normalize baisa (> 2.0 on a par 0.100 stock) to OMR
                return round(val / 1000.0, 4) if abs(val) > 5.0 else round(val, 4)
            return round(val * mult, 2)

        def build_period(idx):
            loans_c = scale(bs, "loans", idx)
            isl_fin = scale(bs, "islamicFin", idx) or 0.0
            dep_c = scale(bs, "deposits", idx)
            isl_dep = scale(bs, "islamicDep", idx) or 0.0
            nii_c = scale(inc, "nii", idx)
            isl_inc = scale(inc, "islamicNetInc", idx) or 0.0
            b_nc = scale(bs, "borrowingsNC", idx) or 0.0
            b_c = scale(bs, "borrowingsC", idx) or 0.0
            op_p = scale(inc, "opProfit", idx)
            da_v = abs(scale(cf, "da", idx) or scale(inc, "da", idx) or 0.0)
            ebitda = round(op_p + da_v, 2) if op_p is not None else None
            capex = scale(cf, "capex", idx)
            if capex is not None:
                capex = abs(capex)
            div_paid = scale(cf, "divPaid", idx)
            if div_paid is not None:
                div_paid = abs(div_paid)

            return {
                "revenue": scale(inc, "revenue", idx),
                "opInc": scale(inc, "opInc", idx),
                "nii": round(nii_c + isl_inc, 2) if nii_c is not None else None,
                "grossProfit": scale(inc, "grossProfit", idx),
                "opProfit": op_p,
                "ebitda": ebitda,
                "opEx": scale(inc, "opEx", idx),
                "netProfit": scale(inc, "netProfit", idx),
                "eps": scale(inc, "eps", idx),
                "assets": scale(bs, "totalAssets", idx),
                "equity": scale(bs, "attrEquity", idx) or scale(bs, "totalEquity", idx),
                "totalEquity": scale(bs, "totalEquity", idx),
                "liabilities": scale(bs, "totalLiabilities", idx),
                "loans": round(loans_c + isl_fin, 2) if loans_c is not None else None,
                "deposits": round(dep_c + isl_dep, 2) if dep_c is not None else None,
                "cash": scale(bs, "cash", idx),
                "debt": round(b_nc + b_c, 2) if (b_nc or b_c) else 0.0,
                "nav": scale(bs, "nav", idx),
                "ocf": scale(cf, "ocf", idx),
                "capex": capex,
                "divPaid": div_paid,
            }

        return {"current": build_period(0), "prior": build_period(1), "mult": mult}
    except Exception:
        return None


def parse_upload_date(dstr):
    try:
        return datetime.datetime.strptime(dstr.strip(), "%b %d, %Y").strftime("%Y-%m-%d")
    except Exception:
        return ""


def parse_hist_series(raw_pts):
    by_day = {}
    for pt in raw_pts or []:
        d_raw = str(pt.get("Date") or "").strip()
        if not d_raw:
            continue
        day_str = d_raw[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day_str):
            continue
        p = parse_float(pt.get("LTP") if pt.get("LTP") is not None else pt.get("Value"), None)
        if p is None or p <= 0:
            continue
        v = parse_float(pt.get("Volume"), 0.0)
        by_day[day_str] = (round(p, 3), int(round(v)))

    all_days = sorted(by_day.keys())
    if not all_days:
        return [], [], [], [], [], []

    all_prices = [by_day[d][0] for d in all_days]
    all_vols = [by_day[d][1] for d in all_days]
    sma50_all, sma200_all = [], []
    for i in range(len(all_prices)):
        w50 = all_prices[max(0, i - 49):i + 1]
        w200 = all_prices[max(0, i - 199):i + 1]
        sma50_all.append(round(sum(w50) / len(w50), 3))
        sma200_all.append(round(sum(w200) / len(w200), 3))

    last_dt = datetime.datetime.strptime(all_days[-1], "%Y-%m-%d")
    cutoff = (last_dt - datetime.timedelta(days=366)).strftime("%Y-%m-%d")
    start_idx = next((i for i, d in enumerate(all_days) if d >= cutoff), 0)

    days_1y = all_days[start_idx:]
    prices_1y = all_prices[start_idx:]
    vols_1y = all_vols[start_idx:]
    sma50_1y = sma50_all[start_idx:]
    sma200_1y = sma200_all[start_idx:]
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    disp_dates = [f"{months[int(d.split('-')[1])-1]} {d.split('-')[2]}, '{d[2:4]}" for d in days_1y]
    return disp_dates, days_1y, prices_1y, vols_1y, sma50_1y, sma200_1y


def classify_disclosure(title):
    t = (title or "").lower()
    if any(k in t for k in ["financial", "profit", "quarter", "results", "audited", "unaudited", "revenue"]):
        return "Financials", "Bullish" if any(w in t for w in ["growth", "increase", "profit"]) else "Neutral"
    if any(k in t for k in ["dividend", "distribution", "cash", "buyback"]):
        return "Dividends", "Bullish"
    if any(k in t for k in ["board", "agm", "general meeting", "nominee", "director", "chief", "cfo", "ceo", "resign", "governance"]):
        return "Governance", "Neutral"
    return "Corporate", "Neutral"


def normalize_website(raw_url):
    u = (raw_url or "").strip()
    if not u or u in ("-", "N/A", "http://", "https://"):
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


def clean_company_name(raw):
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if s.isupper():
        # Clean title case preserving acronyms
        words = s.split(" ")
        keep_upper = {"SAOG", "SAOC", "OQ", "NBO", "OMAN", "GCC", "IPO", "REIT", "REIF", "IT", "AI", "OIFC", "GIC", "MHAS", "BKSB"}
        out = []
        for w in words:
            if w in keep_upper or len(w) <= 2:
                out.append(w)
            else:
                out.append(w.capitalize())
        s = " ".join(out)
    return s


# ==============================================================================
# STEP 1: FETCH GLOBAL MCD + MSX TABLES
# ==============================================================================
def fetch_global_registries():
    print("1/4 Fetching MCD registries (ListedInstrumentsInfo, MajorInvestors5OrMore, InvestorOwnership)...")
    mcd_listed_html = http_get(MCD_URLS["listed"]).decode("utf-8", errors="ignore")
    listed_all = extract_json_array(mcd_listed_html, '"SecuritySymbol":"BKMB"')
    mcd_saog = {
        r["SecuritySymbol"].strip().upper(): r
        for r in listed_all
        if "SAOG" in str(r.get("SecurityType", "")) and r.get("SecuritySymbol")
    }

    mcd_major_html = http_get(MCD_URLS["major"]).decode("utf-8", errors="ignore")
    major_all = extract_json_array(mcd_major_html, '"ISIN":"OM0000002796"')
    majors_by_isin = {}
    for row in major_all:
        isin = row.get("ISIN")
        if isin:
            majors_by_isin.setdefault(isin, []).append(row)

    mcd_own_html = http_get(MCD_URLS["nationality"]).decode("utf-8", errors="ignore")
    tok_m = re.search(r"RequestVerificationToken:\s*'([^']+)'", mcd_own_html)
    xsrf = tok_m.group(1) if tok_m else ""
    # Extract cookie header from thread-local jar so worker threads can reuse MCD session
    mcd_cookies = "; ".join(f"{c.name}={c.value}" for c in getattr(_tls, "cj", []))

    print("2/4 Fetching MSX company universe & Q3 financial performance summary...")
    http_get("https://www.msx.om/")
    msx_list = http_post_json("https://www.msx.om/companies.aspx/List", {}, "https://www.msx.om/companies.aspx").get("d", [])
    msx_by_sym = {r["Symbol"].strip().upper(): r for r in msx_list if r.get("Symbol")}

    fin_pref_rows = []
    try:
        fin_pref_rows = http_post_json("https://www.msx.om/Companies-Fin-Pref.aspx/List", {}, "https://www.msx.om/Companies-Fin-Pref.aspx").get("d", [])
    except Exception:
        pass
    fin_pref_by_sym = {str(r.get("Symbol", "")).strip().upper(): r for r in fin_pref_rows if r.get("Symbol")}

    # Intersect MCD SAOGs with MSX listed companies
    universe = sorted(set(mcd_saog.keys()) & set(msx_by_sym.keys()))
    print(f"    Discovered {len(universe)} active MSX SAOG public companies.")
    return universe, mcd_saog, majors_by_isin, xsrf, mcd_cookies, msx_by_sym, fin_pref_by_sym


# ==============================================================================
# STEP 2: PER-COMPANY WORKER (MSX + MCD NATIONALITY + XBRL PDF PARSER)
# ==============================================================================
def fetch_one_company(sym, mcd_info, majors_raw, xsrf, mcd_cookies, msx_row, fin_pref):
    isin = mcd_info.get("ISINCode", "")
    snap_url = f"https://www.msx.om/snapshot.aspx?s={sym}"
    paid_up_cap = parse_float(mcd_info.get("CapitalPaid"), 0.0)
    shares_out = int(round(parse_float(mcd_info.get("TotalNoOfShares"), 0.0)))

    # 1. MCD Nationality Ownership
    nat = {}
    if isin and xsrf:
        try:
            raw = http_get(
                f"https://www.mcd.om/Statistic/GetInveOwnershipPerInstrumentInfo/{isin}",
                headers={
                    "RequestVerificationToken": xsrf,
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": MCD_URLS["nationality"],
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Cookie": mcd_cookies,
                },
                timeout=18,
                retries=1,
            )
            nat = json.loads(raw.decode("utf-8", errors="ignore")) or {}
        except Exception:
            nat = {}

    # 2. MSX Snapshot HTML labels
    labels = {}
    try:
        snap_html = http_get(snap_url, timeout=22, retries=1).decode("utf-8", errors="ignore")
        for m in re.finditer(r'id="ctl00_ContentPlaceHolder1_([^"]+)"[^>]*>(.*?)</(?:span|a)>', snap_html, re.S | re.I):
            val = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(2))).strip()
            if val:
                labels[m.group(1)] = val
    except Exception:
        pass

    # 3. MSX Live Quote JSON
    quote = {}
    try:
        q_arr = http_post_json("https://www.msx.om/snapshot.aspx/company", {"Symbol": sym}, snap_url, timeout=18, retries=1).get("d")
        if isinstance(q_arr, list) and q_arr:
            quote = q_arr[0]
    except Exception:
        pass

    # 4. MSX Historical Daily Prices
    hist_raw = []
    try:
        raw_h = http_get(f"https://www.msx.om/company-chart-data.aspx?s={sym}", headers={"Referer": snap_url}, timeout=22, retries=1)
        hist_raw = json.loads(raw_h.decode("utf-8-sig", errors="ignore")) or []
    except Exception:
        hist_raw = []

    # 5. MSX Board of Directors
    bod_raw = []
    try:
        raw_b = http_get(f"https://www.msx.om/BODMembersSnap.aspx?s={sym}", headers={"Referer": snap_url}, timeout=18, retries=1)
        bod_raw = json.loads(raw_b.decode("utf-8-sig", errors="ignore")) or []
    except Exception:
        bod_raw = []

    # 6. MSX 2026 Disclosures
    news_raw = []
    try:
        raw_n = http_get(f"https://www.msx.om/company-news.aspx?s={sym}&y=2026&f=&t=&i=all", headers={"Referer": snap_url}, timeout=18, retries=1)
        news_raw = json.loads(raw_n.decode("utf-8-sig", errors="ignore")) or []
    except Exception:
        news_raw = []

    # 7. MSX Financial Reports list (2026, 2025, 2024, 2023)
    fin_reports = []
    for yr in ["2026", "2025", "2024", "2023"]:
        try:
            fr = http_post_json("https://www.msx.om/snapshot.aspx/FinancialsReports", {"Symbol": sym, "Year": yr}, snap_url, timeout=18, retries=1).get("d", [])
            if isinstance(fr, list):
                fin_reports.extend(fr)
        except Exception:
            pass

    # Select key filing links (directly pointing to msx.om)
    chosen_filings = []
    r26 = [r for r in fin_reports if r.get("ReportYear") == "2026" and r.get("FileNameEn")]
    r25_aud = [r for r in fin_reports if r.get("ReportYear") == "2025" and "Yearly (Audited)" in (r.get("NameEn") or "") and r.get("FileNameEn")]
    r25_un = [r for r in fin_reports if r.get("ReportYear") == "2025" and "Yearly" in (r.get("NameEn") or "") and r.get("FileNameEn")]
    r24_aud = [r for r in fin_reports if r.get("ReportYear") == "2024" and "Yearly (Audited)" in (r.get("NameEn") or "") and r.get("FileNameEn")]
    r23_aud = [r for r in fin_reports if r.get("ReportYear") == "2023" and "Yearly (Audited)" in (r.get("NameEn") or "") and r.get("FileNameEn")]

    rep_q26 = r26[0] if r26 else None
    rep_fy25 = r25_aud[0] if r25_aud else (r25_un[0] if r25_un else None)
    rep_fy23 = r23_aud[0] if r23_aud else (r24_aud[0] if r24_aud else None)

    for tag, rep in [("Latest-2026-Quarterly", rep_q26), ("FY2025-Annual", rep_fy25), ("FY2023-Annual", rep_fy23)]:
        if rep:
            enc = urllib.parse.quote(rep["FileNameEn"])
            chosen_filings.append({
                "filingType": tag,
                "period": rep.get("NameEn") or tag,
                "year": rep.get("ReportYear") or "",
                "uploadDate": parse_upload_date(rep.get("UploadDate", "")),
                "title": rep.get("TitleEn") or f"{rep.get('NameEn','')} {rep.get('ReportYear','')}",
                "sourceUrl": f"https://www.msx.om/MSMDOCS/FinancialReports/{enc}",
            })

    # Parse XBRL PDFs in memory for non-flagship companies (flagship 8 already have hand-verified audited numbers!)
    parsed_fin = {}
    if sym not in FLAGSHIP_VERIFIED:
        if rep_fy25:
            u25 = f"https://www.msx.om/MSMDOCS/FinancialReports/{urllib.parse.quote(rep_fy25['FileNameEn'])}"
            res25 = parse_filing_zip_in_memory(u25, snap_url, paid_up_cap, is_interim=False)
            if res25:
                parsed_fin["FY2025"] = res25["current"]
                parsed_fin["FY2024"] = res25["prior"]
        if rep_fy23:
            yr_tag = rep_fy23.get("ReportYear", "2023")
            u23 = f"https://www.msx.om/MSMDOCS/FinancialReports/{urllib.parse.quote(rep_fy23['FileNameEn'])}"
            res23 = parse_filing_zip_in_memory(u23, snap_url, paid_up_cap, is_interim=False)
            if res23:
                if yr_tag == "2023":
                    parsed_fin["FY2023"] = res23["current"]
                    parsed_fin["FY2022"] = res23["prior"]
                elif yr_tag == "2024" and "FY2024" not in parsed_fin:
                    parsed_fin["FY2024"] = res23["current"]
                    parsed_fin["FY2023"] = res23["prior"]
        if rep_q26:
            uq = f"https://www.msx.om/MSMDOCS/FinancialReports/{urllib.parse.quote(rep_q26['FileNameEn'])}"
            resq = parse_filing_zip_in_memory(uq, snap_url, paid_up_cap, is_interim=True)
            if resq:
                parsed_fin["Q26_CUR"] = resq["current"]
                parsed_fin["Q26_PRI"] = resq["prior"]
                parsed_fin["Q26_LABEL"] = rep_q26.get("NameEn", "2026 Interim")

    return {
        "sym": sym,
        "mcdInfo": mcd_info,
        "majorsRaw": majors_raw,
        "nat": nat,
        "msxRow": msx_row,
        "finPref": fin_pref,
        "labels": labels,
        "quote": quote,
        "histRaw": hist_raw,
        "bodRaw": bod_raw,
        "newsRaw": news_raw,
        "filings": chosen_filings,
        "parsedFin": parsed_fin,
    }


def build_company_json(rec, bench_map, synced_stamp):
    sym = rec["sym"]
    mcd = rec["mcdInfo"]
    labels = rec["labels"]
    quote = rec["quote"]
    nat = rec["nat"]
    msx_row = rec["msxRow"]
    fin_pref = rec["finPref"] or {}
    filings = rec["filings"]
    flag = FLAGSHIP_VERIFIED.get(sym)

    disp_dates, full_dates, prices, vols, sma50, sma200 = parse_hist_series(rec["histRaw"])
    price = round(parse_float(quote.get("LTP") or labels.get("LTPLabel"), prices[-1] if prices else 0.0), 3)
    prev_close = round(parse_float(quote.get("PrevClose") or labels.get("PrevCloseLabel"), price), 3)
    open_price = round(parse_float(quote.get("OpenPrice") or labels.get("OpenLabel"), price), 3)
    day_high = round(parse_float(quote.get("High"), price), 3)
    day_low = round(parse_float(quote.get("Low"), price), 3)
    day_change = round(parse_float(quote.get("ChangeVal"), price - prev_close), 3)
    day_change_pct = round(parse_float(quote.get("Change"), ((price - prev_close) / prev_close * 100) if prev_close else 0.0), 2)
    volume_raw = parse_float(quote.get("Volume"), 0.0)
    turnover_raw = parse_float(quote.get("Turnover"), 0.0)

    if prices and price > 0:
        prices[-1] = price
    elif not prices and price > 0:
        today_iso = datetime.datetime.now().strftime("%Y-%m-%d")
        disp_dates, full_dates, prices, vols, sma50, sma200 = ["Latest"], [today_iso], [price], [int(volume_raw)], [price], [price]

    low_52w = round(min(prices) if prices else price, 3)
    high_52w = round(max(prices) if prices else price, 3)

    if full_dates and prices:
        b0 = bench_map.get(full_dates[0], 1.0)
        bench_indexed = [round(prices[0] * (bench_map.get(d, b0) / b0), 3) for d in full_dates]
    else:
        bench_indexed = []

    daily_rets = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices)) if prices[i - 1] > 0]
    if len(daily_rets) > 10:
        mean_r = sum(daily_rets) / len(daily_rets)
        var_r = sum((r - mean_r) ** 2 for r in daily_rets) / (len(daily_rets) - 1)
        ann_vol = f"{math.sqrt(var_r) * math.sqrt(252) * 100:.1f}%"
    else:
        ann_vol = "N/A"

    shares_out_raw = int(round(parse_float(mcd.get("TotalNoOfShares") or labels.get("IssuedSharesLabel"), 0)))
    mcap_omr = shares_out_raw * price
    paid_up_cap = parse_float(mcd.get("CapitalPaid") or labels.get("PaidupcapitalLabel"), 0.0)
    face_val = parse_float(mcd.get("FaceValue") or labels.get("ParValueLabel"), 0.1)
    participants = int(round(parse_float(mcd.get("NoOfParticipant"), 0)))
    isin = mcd.get("ISINCode") or labels.get("ISINLabel") or ""
    market_label = quote.get("GroupEn") or labels.get("MarketLabel") or MARKET_NAMES.get(str(msx_row.get("Market", "")), "MSX")

    emp_om = int(parse_float(labels.get("OmaniLabel"), 0))
    emp_non = int(parse_float(labels.get("NonOmaniLabel"), 0))
    emp_tot = emp_om + emp_non
    employees_str = f"{emp_tot:,} ({emp_om:,} Omani)" if emp_tot > 0 else "Not disclosed on msx.om"
    founded_str = labels.get("EstablishedInLabel") or "N/A"
    listed_date_str = labels.get("ListedDateLabel") or "N/A"
    auditor_str = (labels.get("AuditorLabel") or "N/A").replace("&amp;", "&")

    mcd_sector = (mcd.get("Sector") or "Equities").strip()
    sub_sector = (labels.get("SubSectorLabel") or mcd_sector).strip()
    is_bank = (flag and flag["sectorGroup"] == "Banking") or ("bank" in sub_sector.lower())
    is_energy = (flag and flag["sectorGroup"] == "Energy & Utilities") or any(k in sub_sector.lower() for k in ["oil", "gas", "energy", "power", "drilling", "mining", "utilities"])

    if flag:
        display_name = flag["displayName"]
        sector_group = flag["sectorGroup"]
        sector_label = flag["sectorLabel"]
        website = flag["website"]
        hq = flag["hq"]
        description = flag["description"]
        segments = flag["segments"]
        segment_note = flag["segmentNote"]
    else:
        display_name = clean_company_name(labels.get("CompanyNameLabel") or mcd.get("SecurityName") or msx_row.get("LongNameEn") or sym)
        if not display_name.upper().endswith("SAOG") and "FUND" not in display_name.upper() and "REIT" not in display_name.upper():
            display_name = f"{display_name} SAOG"
        sector_group = "Banking" if is_bank else ("Energy & Utilities" if is_energy else mcd_sector)
        sector_label = f"{mcd_sector} • {sub_sector}" if sub_sector and sub_sector.lower() != mcd_sector.lower() else mcd_sector
        website = normalize_website(labels.get("WebsiteHyperLink") or labels.get("WebsiteHyperLink1"))
        hq = labels.get("HeadOfficeLabel") or labels.get("AddressLabel") or "Sultanate of Oman"
        if len(hq) > 75:
            hq = hq[:72].rstrip(" ,;-") + "..."
        description = f"{display_name} ({sym}) is a publicly listed company on the Muscat Stock Exchange ({market_label}) operating in the {sector_label} sector. Established {founded_str}, with {shares_out_raw:,} issued shares (ISIN {isin}) and {participants:,} registered shareholders recorded on Muscat Clearing & Depository (mcd.om)."
        segments = []
        segment_note = "Segment breakdown available in annual filing"

    # ==========================================================================
    # OWNERSHIP & MAJOR INVESTORS (100% LIVE FROM MCD.OM)
    # ==========================================================================
    omani_pct = round(parse_float(nat.get("omaniCurr"), 0.0), 2)
    gcc_pct = round(parse_float(nat.get("gulfCurr"), 0.0), 2)
    arab_pct = round(parse_float(nat.get("arabCurr"), 0.0), 2)
    foreign_pct = round(parse_float(nat.get("foreignerCurr"), 0.0), 2)
    other_non_omani = round(arab_pct + foreign_pct, 2)
    if omani_pct == 0 and gcc_pct == 0 and other_non_omani == 0:
        omani_pct = 100.0

    nationality_breakdown = [
        {"label": "Omani Investors", "pct": omani_pct, "shares": int(round(parse_float(nat.get("omaniCurrSe"), 0))), "limitPct": parse_float(nat.get("omaniMax"), 100.0), "color": "#6366f1"},
        {"label": "GCC Investors", "pct": gcc_pct, "shares": int(round(parse_float(nat.get("gulfCurrSe"), 0))), "limitPct": parse_float(nat.get("gulfMax"), 100.0), "color": "#06b6d4"},
        {"label": "Foreign & Arab Investors", "pct": other_non_omani, "arabPct": arab_pct, "foreignerPct": foreign_pct, "shares": int(round(parse_float(nat.get("arabCurrSe"), 0) + parse_float(nat.get("foreignerCurrSe"), 0))), "limitPct": parse_float(nat.get("foreignerMax"), 100.0), "color": "#10b981"},
    ]

    majors_sorted = sorted(rec["majorsRaw"], key=lambda x: parse_float(x.get("OwnershipPercentage"), 0.0), reverse=True)
    major_holders = []
    total_major_pct = 0.0
    for h in majors_sorted:
        raw_name = (h.get("InvestorName") or "").strip()
        disp_name = ARABIC_HOLDER_ENGLISH.get(raw_name, raw_name)
        pct_val = round(parse_float(h.get("OwnershipPercentage"), 0.0), 2)
        sh_owned = int(round(parse_float(h.get("TotalSharesOwned"), 0)))
        total_major_pct += pct_val
        major_holders.append({
            "name": disp_name,
            "rawMcdName": raw_name,
            "stakePct": pct_val,
            "stake": f"{pct_val:.2f}%",
            "shares": sh_owned,
            "sharesFmt": f"{sh_owned:,}",
            "valueOmr": round(sh_owned * price, 2),
            "value": fmt_omr_large(sh_owned * price),
        })
    public_float_pct = round(max(0.0, 100.0 - total_major_pct), 2)

    # ==========================================================================
    # GOVERNANCE (FLAGSHIP CORPORATE SITES + AUTOMATED MSX REGISTRY FOR ALL)
    # ==========================================================================
    if flag:
        gov = dict(flag["governance"])
        # Scrub "Official Website" wording cleanly
        gov["boardSourceLabel"] = gov["boardSourceLabel"].replace("Official Website", "Corporate Website").replace("MSX Official Profile", "msx.om Profile")
        gov["managementSourceLabel"] = gov["managementSourceLabel"].replace("Official Website", "Corporate Website")
        ceo_name = gov["management"][0]["name"] if gov["management"] else labels.get("ChiefExecutivePresidentLabel", "Not disclosed")
    else:
        chair_name = (labels.get("ChairmanLabel") or "").strip()
        dep_name = (labels.get("DeputyLabel") or "").strip()
        ceo_raw = (labels.get("ChiefExecutivePresidentLabel") or "").strip()
        sec_name = (labels.get("SecretaryLabel") or "").strip()
        ir_name = (labels.get("lblInvRelOff") or "").strip()
        int_aud = (labels.get("InternalAuditorLabel") or "").strip()
        leg_adv = (labels.get("LegalAdvisorLabel") or "").strip()

        board_list = []
        seen_b = set()
        for m in rec["bodRaw"]:
            mname = re.sub(r"\s+", " ", (m.get("MemberNameEn") or "").strip())
            if not mname or mname.lower() in seen_b:
                continue
            seen_b.add(mname.lower())
            role = "Board Member"
            if chair_name and (chair_name.lower() in mname.lower() or mname.lower() in chair_name.lower()):
                role = "Chairman"
            elif dep_name and (dep_name.lower() in mname.lower() or mname.lower() in dep_name.lower()):
                role = "Deputy Chairman"
            ind_flag = str(m.get("Independent", "")) == "1"
            rep = (m.get("RepresentingTextEn") or "").strip()
            ind_str = "Independent Director" if ind_flag else ("Non-Executive / Non-Independent" + (f" ({rep})" if rep and rep != "-" else ""))
            board_list.append({"name": mname, "role": role, "independence": ind_str})

        if not board_list and chair_name:
            board_list.append({"name": chair_name, "role": "Chairman", "independence": "Non-Executive Director"})
        if dep_name and not any(b["role"] == "Deputy Chairman" for b in board_list):
            board_list.insert(1 if board_list else 0, {"name": dep_name, "role": "Deputy Chairman", "independence": "Non-Executive Director"})

        # Sort Chairman first, Deputy second
        board_list.sort(key=lambda b: 0 if b["role"] == "Chairman" else (1 if b["role"] == "Deputy Chairman" else 2))

        mgmt_list = []
        if ceo_raw and ceo_raw != "-":
            # Handle "Name - Title" format common on MSX
            if " - " in ceo_raw:
                cn, ct = ceo_raw.split(" - ", 1)
                mgmt_list.append({"name": cn.strip(), "title": ct.strip()})
                ceo_name = cn.strip()
            else:
                mgmt_list.append({"name": ceo_raw, "title": "Chief Executive Officer / President"})
                ceo_name = ceo_raw
        else:
            ceo_name = "Not disclosed on msx.om"
        if sec_name and sec_name != "-":
            mgmt_list.append({"name": sec_name, "title": "Board Secretary"})
        if int_aud and int_aud != "-":
            mgmt_list.append({"name": int_aud, "title": "Internal Auditor"})
        if leg_adv and leg_adv != "-":
            mgmt_list.append({"name": leg_adv, "title": "Legal Advisor"})
        if ir_name and ir_name != "-":
            mgmt_list.append({"name": ir_name, "title": "Investor Relations Officer"})

        gov = {
            "boardSourceLabel": f"msx.om Corporate Registry ({sym})",
            "boardSourceUrl": f"https://www.msx.om/snapshot.aspx?s={sym}",
            "boardDisclosedOnWebsite": False,
            "boardFallbackUrl": f"https://www.msx.om/snapshot.aspx?s={sym}",
            "managementSourceLabel": f"msx.om Corporate Registry ({sym})",
            "managementSourceUrl": f"https://www.msx.om/snapshot.aspx?s={sym}",
            "managementDisclosedOnWebsite": False,
            "board": board_list,
            "management": mgmt_list,
        }

    # ==========================================================================
    # FINANCIALS, VALUATION CARDS & DPS (FLAGSHIP VERIFIED OR PARSED XBRL)
    # ==========================================================================
    if flag:
        ann_list = flag["annuals"]
        fy25 = ann_list[-1]
        q3 = flag["q3_2026"]
        dps = flag["dps"]
        div_yield_pct = (dps / price * 100) if price > 0 and dps else 0.0
        pe_fy25 = (mcap_omr / fy25["netProfit"]) if fy25["netProfit"] > 0 else None
        nav_latest = q3["nav"]
        pb_ratio = (price / nav_latest) if nav_latest and nav_latest > 0 else None
        roe_fy25 = (fy25["netProfit"] / fy25["equity"] * 100) if fy25["equity"] > 0 else None
        roa_fy25 = (fy25["netProfit"] / fy25["assets"] * 100) if fy25["assets"] > 0 else None
        np_yoy_9m = ((q3["netProfit"] - q3["netProfitPrior"]) / abs(q3["netProfitPrior"]) * 100) if q3["netProfitPrior"] else 0.0

        if is_bank:
            ldr_q3 = (q3["loans"] / q3["deposits"] * 100) if q3["deposits"] > 0 else 0.0
            cir_fy25 = (fy25["opEx"] / fy25["opInc"] * 100) if fy25["opInc"] > 0 else 0.0
            opinc_yoy_9m = ((q3["opInc"] - q3["opIncPrior"]) / abs(q3["opIncPrior"]) * 100) if q3["opIncPrior"] else 0.0
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "P/E Ratio (FY25)", "value": f"{pe_fy25:.2f}x" if pe_fy25 else "N/A", "sub": f"FY25 EPS: OMR {fy25['eps']:.3f}", "status": "neutral"},
                {"label": "Price / Book (Q3 26)", "value": f"{pb_ratio:.2f}x" if pb_ratio else "N/A", "sub": f"NAV/Sh: OMR {nav_latest:.3f}", "status": "neutral"},
                {"label": "Net Interest & Islamic Inc", "value": fmt_omr_large(fy25["nii"]), "sub": f"9M 26: {fmt_omr_large(q3['nii'])}", "status": "positive"},
                {"label": "Operating Income (FY25)", "value": fmt_omr_large(fy25["opInc"]), "sub": f"9M 26: {fmt_omr_large(q3['opInc'])} ({opinc_yoy_9m:+.1f}%)", "status": "positive"},
                {"label": "Net Profit (FY25)", "value": fmt_omr_large(fy25["netProfit"]), "sub": f"9M 26: {fmt_omr_large(q3['netProfit'])} ({np_yoy_9m:+.1f}%)", "status": "positive"},
                {"label": "Net Loans & Financing", "value": fmt_omr_large(q3["loans"]), "sub": f"Q3 26 (FY25: {fmt_omr_large(fy25['loans'])})", "status": "positive"},
                {"label": "Customer Deposits", "value": fmt_omr_large(q3["deposits"]), "sub": f"Q3 26 (FY25: {fmt_omr_large(fy25['deposits'])})", "status": "positive"},
                {"label": "Total Assets (Q3 26)", "value": fmt_omr_large(q3["assets"]), "sub": f"Equity: {fmt_omr_large(q3['totalEquity'])}", "status": "positive"},
                {"label": "Loan-to-Deposit Ratio", "value": f"{ldr_q3:.1f}%", "sub": f"Cost/Inc (FY25): {cir_fy25:.1f}%", "status": "neutral"},
                {"label": "Return on Equity (FY25)", "value": f"{roe_fy25:.1f}%" if roe_fy25 else "N/A", "sub": f"ROA: {roa_fy25:.2f}% • CAR: {flag.get('car','N/A')}", "status": "positive"},
                {"label": "Cash Dividend Yield", "value": f"{div_yield_pct:.2f}%", "sub": f"DPS: OMR {dps:.4f}", "status": "positive"},
            ]
            fin_chart = {
                "unit": "OMR M",
                "primaryLabel": "Operating Income (OMR M)",
                "secondaryLabel": "Net Interest & Islamic Inc (OMR M)",
                "tertiaryLabel": "Net Profit (OMR M)",
                "years": [a["year"] for a in ann_list],
                "primary": [round(a["opInc"] / 1e6, 2) for a in ann_list],
                "secondary": [round(a["nii"] / 1e6, 2) for a in ann_list],
                "netIncome": [round(a["netProfit"] / 1e6, 2) for a in ann_list],
                "tableRows": [
                    {"period": a["year"], "primaryMetric": a["opInc"], "secondaryMetric": a["nii"], "netProfit": a["netProfit"], "dps": a.get("dps"), "assets": a["assets"], "loans": a["loans"], "deposits": a["deposits"], "equity": a["equity"]}
                    for a in ann_list
                ] + [
                    {"period": "9M 2026", "primaryMetric": q3["opInc"], "secondaryMetric": q3["nii"], "netProfit": q3["netProfit"], "dps": q3.get("dps"), "assets": q3["assets"], "loans": q3["loans"], "deposits": q3["deposits"], "equity": q3["equity"]}
                ],
            }
            quarterly_bars = [
                {"label": "9M 2026 Operating Income", "current": fmt_omr_large(q3["opInc"]), "prior": fmt_omr_large(q3["opIncPrior"]), "currentOmr": q3["opInc"], "priorOmr": q3["opIncPrior"], "yoyPct": round(opinc_yoy_9m, 1)},
                {"label": "9M 2026 Net Interest & Islamic", "current": fmt_omr_large(q3["nii"]), "prior": fmt_omr_large(q3["niiPrior"]), "currentOmr": q3["nii"], "priorOmr": q3["niiPrior"], "yoyPct": round((q3["nii"] - q3["niiPrior"]) / q3["niiPrior"] * 100, 1)},
                {"label": "9M 2026 Net Profit", "current": fmt_omr_large(q3["netProfit"]), "prior": fmt_omr_large(q3["netProfitPrior"]), "currentOmr": q3["netProfit"], "priorOmr": q3["netProfitPrior"], "yoyPct": round(np_yoy_9m, 1)},
            ]
            highlights = [
                f"Market Capitalization of {fmt_omr_large(mcap_omr)} across {fmt_shares(shares_out_raw)} issued shares (OMR {price:.3f}/sh, ISIN {isin}).",
                f"FY2025 Audited Results: Operating Income of {fmt_omr_large(fy25['opInc'])}, Net Interest & Islamic Income of {fmt_omr_large(fy25['nii'])}, and Net Profit of {fmt_omr_large(fy25['netProfit'])}.",
                f"Q3 2026 Balance Sheet Scale: {fmt_omr_large(q3['assets'])} Total Assets, {fmt_omr_large(q3['loans'])} Net Loans & Islamic Financing, and {fmt_omr_large(q3['deposits'])} Customer Deposits ({np_yoy_9m:+.1f}% 9M Net Profit YoY).",
            ]
        else:
            net_debt_q3 = q3["debt"] - q3["cash"]
            ev_omr = mcap_omr + net_debt_q3
            ev_ebitda = (ev_omr / fy25["ebitda"]) if fy25["ebitda"] > 0 else None
            ebitda_margin = (fy25["ebitda"] / fy25["revenue"] * 100) if fy25["revenue"] > 0 else 0.0
            net_margin = (fy25["netProfit"] / fy25["revenue"] * 100) if fy25["revenue"] > 0 else 0.0
            fcf_fy25 = fy25["ocf"] - fy25["capex"]
            rev_yoy_9m = ((q3["revenue"] - q3["revenuePrior"]) / abs(q3["revenuePrior"]) * 100) if q3["revenuePrior"] else 0.0
            ebitda_yoy_9m = ((q3["ebitda"] - q3["ebitdaPrior"]) / abs(q3["ebitdaPrior"]) * 100) if q3["ebitdaPrior"] else 0.0
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "Enterprise Value", "value": fmt_omr_large(ev_omr), "sub": f"Net Debt (Q3 26): {fmt_omr_large(net_debt_q3)}", "status": "neutral"},
                {"label": "P/E Ratio (FY25)", "value": f"{pe_fy25:.2f}x" if pe_fy25 else "N/A", "sub": f"FY25 EPS: OMR {fy25['eps']:.3f}", "status": "neutral"},
                {"label": "EV / EBITDA (FY25)", "value": f"{ev_ebitda:.2f}x" if ev_ebitda else "N/A", "sub": f"P/B (Q3 26): {pb_ratio:.2f}x", "status": "neutral"},
                {"label": "Revenue (FY25)", "value": fmt_omr_large(fy25["revenue"]), "sub": f"9M 26: {fmt_omr_large(q3['revenue'])} ({rev_yoy_9m:+.1f}%)", "status": "positive"},
                {"label": "EBITDA (FY25)", "value": fmt_omr_large(fy25["ebitda"]), "sub": f"EBITDA Margin: {ebitda_margin:.1f}%", "status": "positive"},
                {"label": "Net Profit (FY25)", "value": fmt_omr_large(fy25["netProfit"]), "sub": f"9M 26: {fmt_omr_large(q3['netProfit'])} ({np_yoy_9m:+.1f}%)", "status": "positive"},
                {"label": "Operating Cash Flow", "value": fmt_omr_large(fy25["ocf"]), "sub": f"9M 26 OCF: {fmt_omr_large(q3['ocf'])}", "status": "positive"},
                {"label": "CapEx (FY25)", "value": fmt_omr_large(fy25["capex"]), "sub": f"9M 26 CapEx: {fmt_omr_large(q3['capex'])}", "status": "neutral"},
                {"label": "Free Cash Flow (FY25)", "value": fmt_omr_large(fcf_fy25), "sub": f"Net Margin: {net_margin:.1f}%", "status": "positive" if fcf_fy25 >= 0 else "negative"},
                {"label": "Return on Equity (FY25)", "value": f"{roe_fy25:.1f}%" if roe_fy25 else "N/A", "sub": f"ROA: {roa_fy25:.1f}% • NAV: OMR {nav_latest:.3f}", "status": "positive"},
                {"label": "Cash Dividend Yield", "value": f"{div_yield_pct:.2f}%", "sub": f"DPS: OMR {dps:.4f}", "status": "positive"},
            ]
            fin_chart = {
                "unit": "OMR M",
                "primaryLabel": "Revenue (OMR M)",
                "secondaryLabel": "EBITDA (OMR M)",
                "tertiaryLabel": "Net Profit (OMR M)",
                "years": [a["year"] for a in ann_list],
                "primary": [round(a["revenue"] / 1e6, 2) for a in ann_list],
                "secondary": [round(a["ebitda"] / 1e6, 2) for a in ann_list],
                "netIncome": [round(a["netProfit"] / 1e6, 2) for a in ann_list],
                "tableRows": [
                    {"period": a["year"], "primaryMetric": a["revenue"], "secondaryMetric": a["ebitda"], "netProfit": a["netProfit"], "dps": a.get("dps"), "ocf": a["ocf"], "capex": a["capex"], "assets": a["assets"], "debt": a["debt"], "equity": a["equity"]}
                    for a in ann_list
                ] + [
                    {"period": "9M 2026", "primaryMetric": q3["revenue"], "secondaryMetric": q3["ebitda"], "netProfit": q3["netProfit"], "dps": q3.get("dps"), "ocf": q3["ocf"], "capex": q3["capex"], "assets": q3["assets"], "debt": q3["debt"], "equity": q3["equity"]}
                ],
            }
            quarterly_bars = [
                {"label": "9M 2026 Revenue", "current": fmt_omr_large(q3["revenue"]), "prior": fmt_omr_large(q3["revenuePrior"]), "currentOmr": q3["revenue"], "priorOmr": q3["revenuePrior"], "yoyPct": round(rev_yoy_9m, 1)},
                {"label": "9M 2026 EBITDA", "current": fmt_omr_large(q3["ebitda"]), "prior": fmt_omr_large(q3["ebitdaPrior"]), "currentOmr": q3["ebitda"], "priorOmr": q3["ebitdaPrior"], "yoyPct": round(ebitda_yoy_9m, 1)},
                {"label": "9M 2026 Net Profit", "current": fmt_omr_large(q3["netProfit"]), "prior": fmt_omr_large(q3["netProfitPrior"]), "currentOmr": q3["netProfit"], "priorOmr": q3["netProfitPrior"], "yoyPct": round(np_yoy_9m, 1)},
            ]
            highlights = [
                f"Market Capitalization of {fmt_omr_large(mcap_omr)} and Enterprise Value of {fmt_omr_large(ev_omr)} ({fmt_shares(shares_out_raw)} shares at OMR {price:.3f}/sh).",
                f"FY2025 Audited Results: Revenue of {fmt_omr_large(fy25['revenue'])}, EBITDA of {fmt_omr_large(fy25['ebitda'])} ({ebitda_margin:.1f}% margin), and Net Profit of {fmt_omr_large(fy25['netProfit'])}.",
                f"Cash Flow & CapEx: {fmt_omr_large(fy25['ocf'])} FY2025 Operating Cash Flow, {fmt_omr_large(fy25['capex'])} CapEx, and {fmt_omr_large(q3['netProfit'])} 9M 2026 Net Profit ({np_yoy_9m:+.1f}% YoY).",
            ]
        latest_fy_label = "FY2025 Audited + Q3 2026"
    else:
        # Automated XBRL Parsed Financials for all other MSX SAOG companies
        pf = rec["parsedFin"]
        yr_keys = [y for y in ["FY2022", "FY2023", "FY2024", "FY2025"] if y in pf and any(pf[y].get(k) is not None for k in ("revenue", "opInc", "netProfit", "assets"))]
        latest_ann = pf[yr_keys[-1]] if yr_keys else {}
        latest_yr_name = yr_keys[-1] if yr_keys else "Latest"
        q_cur = pf.get("Q26_CUR") or {}
        q_pri = pf.get("Q26_PRI") or {}
        q_lbl = pf.get("Q26_LABEL") or "9M 2026"

        # Pull Q3 2026 Net Profit from Companies-Fin-Pref if XBRL interim parser didn't have it
        fp_cp = parse_float(fin_pref.get("Net_Profit_CP"), None)
        fp_pp = parse_float(fin_pref.get("Net_Profit_PP"), None)
        if q_cur.get("netProfit") is None and fp_cp is not None:
            q_cur["netProfit"] = fp_cp
        if q_pri.get("netProfit") is None and fp_pp is not None:
            q_pri["netProfit"] = fp_pp

        # Derive per-year DPS from Cash Dividend Paid / Issued Shares
        def calc_dps(period_dict):
            dp = period_dict.get("divPaid")
            if dp and shares_out_raw > 0:
                val = dp / shares_out_raw
                if 0 < val < 5.0:
                    return round(val, 4)
            return None

        dps_latest = calc_dps(q_cur) or calc_dps(latest_ann) or 0.0
        div_yield_pct = (dps_latest / price * 100) if price > 0 and dps_latest > 0 else 0.0
        np_ann = latest_ann.get("netProfit")
        eq_latest = q_cur.get("equity") or latest_ann.get("equity")
        ast_latest = q_cur.get("assets") or latest_ann.get("assets")
        nav_latest = q_cur.get("nav") or latest_ann.get("nav") or ((eq_latest / shares_out_raw) if (eq_latest and shares_out_raw) else None)
        pe_val = (mcap_omr / np_ann) if (np_ann and np_ann > 0 and mcap_omr > 0) else None
        pb_val = (price / nav_latest) if (nav_latest and nav_latest > 0 and price > 0) else None
        roe_val = (np_ann / latest_ann["equity"] * 100) if (np_ann is not None and latest_ann.get("equity") and latest_ann["equity"] > 0) else None
        roa_val = (np_ann / latest_ann["assets"] * 100) if (np_ann is not None and latest_ann.get("assets") and latest_ann["assets"] > 0) else None

        if is_bank:
            loans_v = q_cur.get("loans") or latest_ann.get("loans")
            dep_v = q_cur.get("deposits") or latest_ann.get("deposits")
            ldr_v = (loans_v / dep_v * 100) if (loans_v and dep_v and dep_v > 0) else None
            opinc_v = latest_ann.get("opInc") or latest_ann.get("revenue")
            nii_v = latest_ann.get("nii")
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": f"P/E Ratio ({latest_yr_name})", "value": f"{pe_val:.2f}x" if pe_val else "N/A", "sub": f"EPS: OMR {latest_ann['eps']:.3f}" if latest_ann.get("eps") is not None else "EPS: N/A", "status": "neutral"},
                {"label": "Price / Book", "value": f"{pb_val:.2f}x" if pb_val else "N/A", "sub": f"NAV/Sh: OMR {nav_latest:.3f}" if nav_latest else "NAV: N/A", "status": "neutral"},
                {"label": f"Net Int & Islamic ({latest_yr_name})", "value": fmt_omr_large(nii_v), "sub": f"2026 Int: {fmt_omr_large(q_cur.get('nii'))}", "status": "positive"},
                {"label": f"Operating Income ({latest_yr_name})", "value": fmt_omr_large(opinc_v), "sub": f"2026 Int: {fmt_omr_large(q_cur.get('opInc'))}", "status": "positive"},
                {"label": f"Net Profit ({latest_yr_name})", "value": fmt_omr_large(np_ann), "sub": f"9M 26: {fmt_omr_large(q_cur.get('netProfit'))}", "status": "positive" if (np_ann or 0) >= 0 else "negative"},
                {"label": "Net Loans & Financing", "value": fmt_omr_large(loans_v), "sub": "Latest Balance Sheet", "status": "positive"},
                {"label": "Customer Deposits", "value": fmt_omr_large(dep_v), "sub": "Latest Balance Sheet", "status": "positive"},
                {"label": "Total Assets", "value": fmt_omr_large(ast_latest), "sub": f"Equity: {fmt_omr_large(eq_latest)}", "status": "positive"},
                {"label": "Loan-to-Deposit Ratio", "value": f"{ldr_v:.1f}%" if ldr_v else "N/A", "sub": f"Paid-Up: {fmt_omr_large(paid_up_cap)}", "status": "neutral"},
                {"label": f"Return on Equity ({latest_yr_name})", "value": f"{roe_val:.1f}%" if roe_val is not None else "N/A", "sub": f"ROA: {roa_val:.2f}%" if roa_val is not None else "ROA: N/A", "status": "positive"},
                {"label": "Cash Dividend Yield", "value": f"{div_yield_pct:.2f}%" if dps_latest > 0 else "N/A", "sub": f"DPS: OMR {dps_latest:.4f}" if dps_latest > 0 else "No cash div parsed", "status": "positive"},
            ]
            fin_chart = {
                "unit": "OMR M",
                "primaryLabel": "Operating Income (OMR M)",
                "secondaryLabel": "Net Interest & Islamic Inc (OMR M)",
                "tertiaryLabel": "Net Profit (OMR M)",
                "years": yr_keys,
                "primary": [round((pf[y].get("opInc") or pf[y].get("revenue") or 0) / 1e6, 2) for y in yr_keys],
                "secondary": [round((pf[y].get("nii") or 0) / 1e6, 2) for y in yr_keys],
                "netIncome": [round((pf[y].get("netProfit") or 0) / 1e6, 2) for y in yr_keys],
                "tableRows": [
                    {"period": y, "primaryMetric": pf[y].get("opInc") or pf[y].get("revenue"), "secondaryMetric": pf[y].get("nii"), "netProfit": pf[y].get("netProfit"), "dps": calc_dps(pf[y]), "assets": pf[y].get("assets"), "loans": pf[y].get("loans"), "deposits": pf[y].get("deposits"), "equity": pf[y].get("equity")}
                    for y in yr_keys
                ] + ([{"period": "9M 2026", "primaryMetric": q_cur.get("opInc") or q_cur.get("revenue"), "secondaryMetric": q_cur.get("nii"), "netProfit": q_cur.get("netProfit"), "dps": calc_dps(q_cur), "assets": q_cur.get("assets"), "loans": q_cur.get("loans"), "deposits": q_cur.get("deposits"), "equity": q_cur.get("equity")}] if q_cur else []),
            }
        else:
            rev_v = latest_ann.get("revenue") or latest_ann.get("opInc")
            ebitda_v = latest_ann.get("ebitda") or latest_ann.get("opProfit")
            debt_v = q_cur.get("debt") if q_cur.get("debt") is not None else (latest_ann.get("debt") or 0.0)
            cash_v = q_cur.get("cash") if q_cur.get("cash") is not None else (latest_ann.get("cash") or 0.0)
            net_debt = debt_v - cash_v
            ev_v = mcap_omr + net_debt if mcap_omr > 0 else None
            ev_eb = (ev_v / ebitda_v) if (ev_v and ebitda_v and ebitda_v > 0) else None
            ocf_v = latest_ann.get("ocf")
            capex_v = latest_ann.get("capex")
            fcf_v = (ocf_v - capex_v) if (ocf_v is not None and capex_v is not None) else None
            eb_margin = (ebitda_v / rev_v * 100) if (ebitda_v and rev_v and rev_v > 0) else None
            net_margin = (np_ann / rev_v * 100) if (np_ann is not None and rev_v and rev_v > 0) else None

            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "Enterprise Value", "value": fmt_omr_large(ev_v), "sub": f"Net Debt: {fmt_omr_large(net_debt)}", "status": "neutral"},
                {"label": f"P/E Ratio ({latest_yr_name})", "value": f"{pe_val:.2f}x" if pe_val else "N/A", "sub": f"EPS: OMR {latest_ann['eps']:.3f}" if latest_ann.get("eps") is not None else "EPS: N/A", "status": "neutral"},
                {"label": f"EV / EBITDA ({latest_yr_name})", "value": f"{ev_eb:.2f}x" if ev_eb else "N/A", "sub": f"P/B: {pb_val:.2f}x" if pb_val else "P/B: N/A", "status": "neutral"},
                {"label": f"Revenue ({latest_yr_name})", "value": fmt_omr_large(rev_v), "sub": f"9M 26: {fmt_omr_large(q_cur.get('revenue') or q_cur.get('opInc'))}", "status": "positive"},
                {"label": f"EBITDA ({latest_yr_name})", "value": fmt_omr_large(ebitda_v), "sub": f"Margin: {eb_margin:.1f}%" if eb_margin is not None else "Operating Proxy", "status": "positive"},
                {"label": f"Net Profit ({latest_yr_name})", "value": fmt_omr_large(np_ann), "sub": f"9M 26: {fmt_omr_large(q_cur.get('netProfit'))}", "status": "positive" if (np_ann or 0) >= 0 else "negative"},
                {"label": f"Operating Cash Flow ({latest_yr_name})", "value": fmt_omr_large(ocf_v), "sub": f"9M 26 OCF: {fmt_omr_large(q_cur.get('ocf'))}", "status": "positive" if (ocf_v or 0) >= 0 else "negative"},
                {"label": f"CapEx ({latest_yr_name})", "value": fmt_omr_large(capex_v), "sub": f"9M 26 CapEx: {fmt_omr_large(q_cur.get('capex'))}", "status": "neutral"},
                {"label": f"Free Cash Flow ({latest_yr_name})", "value": fmt_omr_large(fcf_v), "sub": f"Net Margin: {net_margin:.1f}%" if net_margin is not None else f"Assets: {fmt_omr_large(ast_latest)}", "status": "positive" if (fcf_v or 0) >= 0 else "negative"},
                {"label": f"Return on Equity ({latest_yr_name})", "value": f"{roe_val:.1f}%" if roe_val is not None else "N/A", "sub": f"NAV: OMR {nav_latest:.3f}" if nav_latest else f"Equity: {fmt_omr_large(eq_latest)}", "status": "positive" if (roe_val or 0) >= 0 else "negative"},
                {"label": "Cash Dividend Yield", "value": f"{div_yield_pct:.2f}%" if dps_latest > 0 else "N/A", "sub": f"DPS: OMR {dps_latest:.4f}" if dps_latest > 0 else "No cash div parsed", "status": "positive"},
            ]
            fin_chart = {
                "unit": "OMR M",
                "primaryLabel": "Revenue (OMR M)",
                "secondaryLabel": "EBITDA (OMR M)",
                "tertiaryLabel": "Net Profit (OMR M)",
                "years": yr_keys,
                "primary": [round((pf[y].get("revenue") or pf[y].get("opInc") or 0) / 1e6, 2) for y in yr_keys],
                "secondary": [round((pf[y].get("ebitda") or pf[y].get("opProfit") or 0) / 1e6, 2) for y in yr_keys],
                "netIncome": [round((pf[y].get("netProfit") or 0) / 1e6, 2) for y in yr_keys],
                "tableRows": [
                    {"period": y, "primaryMetric": pf[y].get("revenue") or pf[y].get("opInc"), "secondaryMetric": pf[y].get("ebitda") or pf[y].get("opProfit"), "netProfit": pf[y].get("netProfit"), "dps": calc_dps(pf[y]), "ocf": pf[y].get("ocf"), "capex": pf[y].get("capex"), "assets": pf[y].get("assets"), "debt": pf[y].get("debt"), "equity": pf[y].get("equity")}
                    for y in yr_keys
                ] + ([{"period": "9M 2026", "primaryMetric": q_cur.get("revenue") or q_cur.get("opInc"), "secondaryMetric": q_cur.get("ebitda") or q_cur.get("opProfit"), "netProfit": q_cur.get("netProfit"), "dps": calc_dps(q_cur), "ocf": q_cur.get("ocf"), "capex": q_cur.get("capex"), "assets": q_cur.get("assets"), "debt": q_cur.get("debt"), "equity": q_cur.get("equity")}] if q_cur else []),
            }

        # Build 9M 2026 Momentum bars dynamically where current & prior exist
        quarterly_bars = []
        rev_k = "opInc" if is_bank else "revenue"
        rev_lbl = "9M 2026 Operating Income" if is_bank else "9M 2026 Revenue"
        for lbl, k in [(rev_lbl, rev_k), ("9M 2026 Net Profit", "netProfit")]:
            cv, pv = q_cur.get(k), q_pri.get(k)
            if cv is not None and pv is not None and pv != 0:
                yoy = round((cv - pv) / abs(pv) * 100, 1)
                quarterly_bars.append({"label": lbl, "current": fmt_omr_large(cv), "prior": fmt_omr_large(pv), "currentOmr": cv, "priorOmr": pv, "yoyPct": yoy})
        if not quarterly_bars and q_cur.get("netProfit") is not None:
            quarterly_bars.append({"label": f"{q_lbl} Net Profit", "current": fmt_omr_large(q_cur["netProfit"]), "prior": "N/A", "currentOmr": q_cur["netProfit"], "yoyPct": 0.0})

        highlights = [
            f"Market Capitalization of {fmt_omr_large(mcap_omr)} across {fmt_shares(shares_out_raw)} issued shares at OMR {price:.3f}/sh ({market_label}, ISIN {isin}).",
            f"{latest_yr_name} Financial Snapshot: {'Operating Income' if is_bank else 'Revenue'} of {fmt_omr_large(latest_ann.get('opInc') if is_bank else (latest_ann.get('revenue') or latest_ann.get('opInc')))} and Net Profit of {fmt_omr_large(np_ann)} (Total Assets: {fmt_omr_large(ast_latest)}).",
            f"Ownership Registry (mcd.om): {participants:,} registered shareholders • Omani {omani_pct:.1f}% | GCC {gcc_pct:.1f}% | Foreign & Arab {other_non_omani:.1f}% ({len(major_holders)} disclosed 5%+ holders).",
        ]
        dps = dps_latest
        latest_fy_label = f"{latest_yr_name} + 2026 Interim" if q_cur else latest_yr_name

    # ==========================================================================
    # DISCLOSURES (2026 MSX NEWS)
    # ==========================================================================
    disclosures = []
    seen_titles = set()
    for item in rec["newsRaw"]:
        title = (item.get("TitleEn") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        cat, sent = classify_disclosure(title)
        doc_file = item.get("Doc_News")
        pdf_link = f"https://www.msx.om/MSMDocs/Images/NewsDocs/{doc_file}" if doc_file else f"https://www.msx.om/snapshot.aspx?s={sym}&se=4&ni={item.get('NewsID','')}"
        disclosures.append({
            "title": title,
            "source": "Muscat Stock Exchange (msx.om)",
            "date": item.get("DateTime", ""),
            "link": pdf_link,
            "category": cat,
            "sentiment": sent,
            "summary": f"MSX regulatory disclosure filed by {display_name} ({sym}) on {item.get('DateTime', '2026')}." + (f" File: {doc_file}" if doc_file else ""),
        })
        if len(disclosures) >= 9:
            break

    verification_urls = {
        "msxProfile": f"https://www.msx.om/snapshot.aspx?s={sym}",
        "msxHistoricalPrices": f"https://www.msx.om/company-chart-data.aspx?s={sym}",
        "msxDisclosures": f"https://www.msx.om/snapshot.aspx?s={sym}#news",
        "msxFinancialReports": filings[0]["sourceUrl"] if filings else f"https://www.msx.om/snapshot.aspx?s={sym}",
        "mcdListedInfo": MCD_URLS["listed"],
        "mcdMajorInvestors": MCD_URLS["major"],
        "mcdNationalityOwnership": MCD_URLS["nationality"],
        "corporateWebsite": website or f"https://www.msx.om/snapshot.aspx?s={sym}",
        "boardOfDirectors": gov["boardSourceUrl"],
        "executiveManagement": gov["managementSourceUrl"],
    }

    company_obj = {
        "ticker": sym,
        "exchange": "MSX",
        "market": market_label,
        "isin": isin,
        "currency": "OMR",
        "currencySymbol": "OMR ",
        "name": display_name,
        "sectorGroup": sector_group,
        "sector": sector_label,
        "ceo": ceo_name,
        "hq": hq,
        "employees": employees_str,
        "founded": founded_str,
        "listedDate": listed_date_str,
        "auditor": auditor_str,
        "website": website,
        "msxUrl": f"https://www.msx.om/snapshot.aspx?s={sym}",
        "price": price,
        "prevClose": prev_close,
        "openPrice": open_price,
        "dayHigh": day_high,
        "dayLow": day_low,
        "dayChange": day_change,
        "dayChangePct": day_change_pct,
        "volume": fmt_shares(volume_raw),
        "volumeRaw": int(round(volume_raw)),
        "turnover": fmt_omr_large(turnover_raw),
        "turnoverOmr": round(turnover_raw, 2),
        "low52w": low_52w,
        "high52w": high_52w,
        "volatility": ann_vol,
        "sharesOutRaw": shares_out_raw,
        "sharesOut": fmt_shares(shares_out_raw),
        "paidUpCapital": fmt_omr_large(paid_up_cap),
        "faceValue": f"OMR {face_val:.3f}",
        "shareholderCount": f"{participants:,}",
        "marketCapOmr": round(mcap_omr, 2),
        "marketCapFmt": fmt_omr_large(mcap_omr),
        "dividendYieldPct": round(div_yield_pct, 2),
        "dpsOmr": dps,
        "latestFiscalYear": latest_fy_label,
        "description": description,
        "highlights": highlights,
        "segments": segments,
        "segmentNote": segment_note,
        "series": {
            "dates": disp_dates,
            "fullDates": full_dates,
            "prices": prices,
            "volumes": vols,
            "sma50": sma50,
            "sma200": sma200,
            "benchmarkIndexed": bench_indexed,
            "benchmarkLabel": "MSX Market Benchmark (Indexed)",
        },
        "valuation": valuation_cards,
        "ownership": {
            "breakdown": nationality_breakdown,
            "majorInvestors": major_holders,
            "majorTotalPct": round(total_major_pct, 2),
            "publicFloatPct": public_float_pct,
            "totalShares": shares_out_raw,
            "totalSharesFmt": f"{shares_out_raw:,}",
            "shareholderCount": participants,
            "paidUpCapitalOmr": paid_up_cap,
            "faceValueOmr": face_val,
            "sourceUrls": {
                "majorInvestors": MCD_URLS["major"],
                "nationalityOwnership": MCD_URLS["nationality"],
                "listedInstruments": MCD_URLS["listed"],
            },
            "note": f"Total Issued Shares: {shares_out_raw:,} • Registered Shareholders: {participants:,} • Omani: {omani_pct:.2f}% | GCC: {gcc_pct:.2f}% | Arab: {arab_pct:.2f}% | Foreign: {foreign_pct:.2f}% (Source: mcd.om).",
        },
        "financials": fin_chart,
        "quarterlyMomentum": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "bars": quarterly_bars,
            "sourceUrl": filings[0]["sourceUrl"] if filings else f"https://www.msx.om/snapshot.aspx?s={sym}",
        },
        "governance": gov,
        "filings": filings,
        "disclosures": disclosures,
        "verificationUrls": verification_urls,
        "lastUpdated": synced_stamp,
        "dataSource": f"msx.om & mcd.om ({sym}:MSX)",
    }

    index_entry = {
        "ticker": sym,
        "name": display_name,
        "exchange": "MSX",
        "market": market_label,
        "isin": isin,
        "sectorGroup": sector_group,
        "sector": sector_label,
        "price": price,
        "dayChange": day_change,
        "dayChangePct": day_change_pct,
        "marketCapOmr": round(mcap_omr, 2),
        "marketCapFmt": fmt_omr_large(mcap_omr),
        "isFlagship": sym in FLAGSHIP_VERIFIED,
        "dataFile": f"data/{sym}.json",
    }
    return company_obj, index_entry


def main():
    t0 = time.time()
    synced_stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    universe, mcd_saog, majors_by_isin, xsrf, mcd_cookies, msx_by_sym, fin_pref_by_sym = fetch_global_registries()

    print(f"3/4 Syncing all {len(universe)} MSX SAOG companies concurrently (6 threads)...")
    records = {}
    done_cnt = 0
    lock = threading.Lock()

    def worker(sym):
        nonlocal done_cnt
        info = mcd_saog[sym]
        isin = info.get("ISINCode", "")
        rec = fetch_one_company(sym, info, majors_by_isin.get(isin, []), xsrf, mcd_cookies, msx_by_sym[sym], fin_pref_by_sym.get(sym))
        with lock:
            records[sym] = rec
            done_cnt += 1
            if done_cnt % 15 == 0 or done_cnt == len(universe):
                print(f"    Progress: {done_cnt}/{len(universe)} companies synced ({time.time() - t0:.1f}s)")

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(worker, s) for s in universe]
        for f in concurrent.futures.as_completed(futs):
            try:
                f.result()
            except Exception as e:
                print(f"    [WARN] Worker exception: {e}")

    # Build equal-weighted MSX Flagship benchmark from the 8 flagship series
    all_dates = set()
    flag_hist = {}
    for s in FLAGSHIP_ORDER:
        if s in records:
            _, fd, pr, _, _, _ = parse_hist_series(records[s]["histRaw"])
            if pr:
                flag_hist[s] = (fd, pr)
                all_dates.update(fd)
    common_dates = sorted(all_dates)
    norm_by_d = {d: [] for d in common_dates}
    for s, (fd, pr) in flag_hist.items():
        p0 = pr[0]
        pm = dict(zip(fd, pr))
        last_p = p0
        for d in common_dates:
            if d in pm:
                last_p = pm[d]
            norm_by_d[d].append(last_p / p0)
    bench_map = {d: (sum(v) / len(v)) for d, v in norm_by_d.items() if v}

    print(f"4/4 Generating structured JSON files in /data/ for {len(records)} companies...")
    index_companies = []
    for sym in universe:
        if sym not in records:
            continue
        comp_obj, idx_entry = build_company_json(records[sym], bench_map, synced_stamp)
        with open(os.path.join(DATA_DIR, f"{sym}.json"), "w", encoding="utf-8") as f:
            json.dump(comp_obj, f, ensure_ascii=False, separators=(",", ":"))
        index_companies.append(idx_entry)

    # Sort index: flagship 8 first (in flagship order), then remaining companies by Market Cap descending
    def sort_key(item):
        if item["ticker"] in FLAGSHIP_ORDER:
            return (0, FLAGSHIP_ORDER.index(item["ticker"]), 0)
        return (1, 0, -item["marketCapOmr"])

    index_companies.sort(key=sort_key)

    index_payload = {
        "exchange": "Muscat Stock Exchange (MSX)",
        "currency": "OMR",
        "generatedAt": synced_stamp,
        "totalCompanies": len(index_companies),
        "sources": [
            {"name": "Muscat Stock Exchange (msx.om)", "url": "https://www.msx.om/"},
            {"name": "Muscat Clearing & Depository (mcd.om)", "url": "https://www.mcd.om/"},
            {"name": "Company Corporate Websites", "url": "Linked per company"},
        ],
        "companies": index_companies,
    }
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index_payload, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - t0
    print(f"\nDONE! Synced {len(index_companies)} MSX SAOG companies in {elapsed:.1f}s -> data/index.json & data/<TICKER>.json")


if __name__ == "__main__":
    main()
