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

# Load AI-grounded company overviews & business segment profiles (populated by enrich_overviews_ai.py)
AI_PROFILES_PATH = os.path.join(DATA_DIR, "ai_profiles.json")
try:
    with open(AI_PROFILES_PATH, encoding="utf-8") as _apf:
        AI_PROFILES = json.load(_apf)
except Exception:
    AI_PROFILES = {}

# Load verified pre-IPO historical financials from official MSX IPO Prospectuses
IPO_PROSPECTUS_PATH = os.path.join(DATA_DIR, "ipo_prospectus_financials.json")
try:
    with open(IPO_PROSPECTUS_PATH, encoding="utf-8") as _ipf:
        IPO_PROSPECTUS_FIN = json.load(_ipf)
except Exception:
    IPO_PROSPECTUS_FIN = {}

# Load verified corporate-website & MSX Corporate Governance Report Board/Management profiles
CORP_GOV_PATH = os.path.join(DATA_DIR, "corporate_governance_overrides.json")
try:
    with open(CORP_GOV_PATH, encoding="utf-8") as _cgf:
        CORP_GOV_OVERRIDES = json.load(_cgf)
except Exception:
    CORP_GOV_OVERRIDES = {}

# Load Arabic-to-English major shareholder name translations (mcd.om)
SHAREHOLDER_TRANS_PATH = os.path.join(DATA_DIR, "shareholder_translations.json")
try:
    with open(SHAREHOLDER_TRANS_PATH, encoding="utf-8") as _stf:
        _raw_st = json.load(_stf)
        SHAREHOLDER_TRANSLATIONS = {re.sub(r"\s+", " ", k).strip(): v for k, v in _raw_st.items()}
except Exception:
    SHAREHOLDER_TRANSLATIONS = {}

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
    "cash": [
        r"^Cash\s+and\s+bank\s+balances\b",
        r"^Cash\s+and\s+cash\s+equivalents\b",
        r"^Total\s+cash\s+and\s+bank\s+balances\b",
        r"^Total\s+cash\s+and\s+cash\s+equivalents\b",
        r"^Balances\s+with\s+banks\b",
    ],
    "borrowingsNC": [
        r"^Borrowings,\s*non-current\b",
        r"^Non-current\s+borrowings\b",
        r"^Term\s+loans,\s*non-current\b",
        r"^Long[\s-]+term\s+(?:borrowings|loans)\b",
        r"^Bank\s+borrowings,\s*non-current\b",
    ],
    "borrowingsC": [
        r"^Borrowings,\s*current\b",
        r"^Current\s+borrowings\b",
        r"^Short[\s-]+term\s+(?:borrowings|loans)\b",
        r"^Bank\s+borrowings,\s*current\b",
        r"^Bank\s+overdrafts?\b",
    ],
    "leaseNC": [r"^Non-current\s+lease\s+liabilities\b", r"^Lease\s+liabilities,\s*non-current\b"],
    "leaseC": [r"^Current\s+lease\s+liabilities\b", r"^Lease\s+liabilities,\s*current\b"],
    "nav": [r"^Net\s+assets\s+per\s+share\b"],
    "receivables": [r"^Trade\s+and\s+other\s+(?:current\s+)?receivables\b", r"^Trade\s+receivables\b", r"^Accounts\s+receivable\b"],
    "inventories": [r"^Inventories\b", r"^Current\s+inventories\b"],
    "totalCurrentAssets": [r"^Total\s+current\s+assets\b"],
    "ppe": [r"^Property,\s*plant\s+and\s+equipment\b", r"^Property\s+and\s+equipment\b"],
    "rouAssets": [r"^Right[\s-]+of[\s-]+use\s+assets?\b"],
    "intangibles": [r"^Intangible\s+assets\b", r"^Goodwill\s+and\s+intangible\s+assets\b"],
    "totalNonCurrentAssets": [r"^Total\s+non-current\s+assets\b"],
    "payables": [r"^Trade\s+and\s+other\s+(?:current\s+)?payables\b", r"^Trade\s+payables\b", r"^Accounts\s+payable\b"],
    "totalCurrentLiabilities": [r"^Total\s+current\s+liabilities\b"],
    "totalNonCurrentLiabilities": [r"^Total\s+non-current\s+liabilities\b"],
    "retainedEarnings": [r"^Retained\s+earnings\b", r"^Accumulated\s+(?:profits?|losses?)\b"],
    "reserves": [r"^Legal\s+reserve\b", r"^Other\s+reserves\b", r"^Statutory\s+reserve\b"],
    "tier1Securities": [r"^Perpetual\s+Tier\s*1\b", r"^Subordinated\s+(?:debt|liabilities|sukuk)\b"],
    "dueFromBanks": [r"^Due\s+from\s+banks\b", r"^Placements?\s+with\s+banks\b"],
    "investmentSecurities": [r"^Investment\s+securities\b", r"^Financial\s+investments?\b", r"^Investments?\s+at\s+fair\s+value\b"],
    "dueToBanks": [r"^Due\s+to\s+banks\b", r"^Deposits\s+from\s+banks\b"],
    "debtSecurities": [r"^Debt\s+securities\s+issued\b", r"^Euro\s+medium\s+term\s+notes\b", r"^Bonds\s+and\s+sukuk\b"],
    "insuranceLiabilities": [r"^Insurance\s+contract\s+liabilities\b", r"^Gross\s+insurance\s+contract\s+liabilities\b"],
}

IS_PATTERNS = {
    "revenue": [r"^Revenue\b", r"^Total\s+revenue\b", r"^Insurance\s+revenue\b", r"^Gross\s+written\s+premium", r"^Rental\s+income\b"],
    "costOfSales": [r"^Cost\s+of\s+sales\b", r"^Cost\s+of\s+revenues?\b", r"^Direct\s+costs?\b"],
    "grossProfit": [r"^Gross\s+profit\b"],
    "otherOpInc": [r"^Other\s+(?:operating\s+)?income\b"],
    "sgaExpenses": [
        r"^General\s+and\s+administrative\s+expenses\b",
        r"^Administrative\s+and\s+general\s+expenses\b",
        r"^Selling(?:,\s*general)?\s+and\s+(?:administrative|distribution)\s+expenses\b",
    ],
    "opProfit": [r"^Profit\s*\(loss\)\s+from\s+operating\s+activities\b", r"^Operating\s+profit\b"],
    "opInc": [r"^Total\s+operating\s+income\b", r"^Total\s+income\b"],
    "interestIncome": [r"^Interest\s+income\b"],
    "interestExpense": [r"^Interest\s+expense\b"],
    "nii": [r"^Net\s+interest\s+income\b"],
    "islamicIncome": [r"^Income\s+from\s+Islamic\s+financing\b"],
    "islamicExpense": [r"^Distribution\s+to\s+depositors\b", r"^Profit\s+paid\s+on\s+Islamic\s+deposits\b"],
    "islamicNetInc": [r"^Net\s+income\s+from\s+islamic\s+financing\b"],
    "feeCommission": [r"^Net\s+fee\s+and\s+commission\s+income\b", r"^Commission\s+and\s+fee\s+income\b", r"^Fee\s+and\s+commission\s+income\b"],
    "investmentIncome": [r"^Net\s+investment\s+income\b", r"^Investment\s+income\b"],
    "staffExpenses": [r"^Staff\s+(?:costs?|expenses?)\b", r"^Employee\s+(?:costs?|benefits?\s+expense)\b"],
    "opEx": [r"^Total\s+operating\s+expenses\b"],
    "da": [r"^Depreciation(?:,\s*depletion)?\s+and\s+amortis?ation"],
    "financeIncome": [r"^Finance\s+income\b"],
    "financeCosts": [r"^Finance\s+costs?\b", r"^Finance\s+charges?\b"],
    "impairment": [
        r"^(?:Net\s+)?[Ii]mpairment\s+(?:losses?|charges?)\b",
        r"^Credit\s+loss\s+expense\b",
        r"^Allowance\s+for\s+credit\s+losses\b",
        r"^Expected\s+credit\s+loss",
    ],
    "insuranceServiceExp": [r"^Insurance\s+service\s+expenses?\b"],
    "insuranceServiceResult": [r"^Insurance\s+service\s+result\b"],
    "profitBeforeTax": [r"^Profit\s*\(loss\)\s+before\s+tax\b", r"^Profit\s+before\s+(?:income\s+)?tax\b"],
    "taxExpense": [r"^Income\s+tax\s+(?:expense|credit|benefit)\b", r"^Tax\s+expense\b", r"^Taxation\b"],
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
    "pbtCf": [r"^Profit\s*\(loss\)\s+before\s+tax\b", r"^Profit\s+before\s+tax\b"],
    "ocfBeforeWc": [
        r"^Operating\s+profit\s+before\s+working\s+capital\b",
        r"^Cash\s+(?:flows?\s+)?generated\s+from\s+operations\s+before\s+working\s+capital\b",
    ],
    "taxPaid": [r"^Income\s+taxes?\s+paid\b", r"^Tax(?:es)?\s+paid\b"],
    "interestPaid": [r"^Interest\s+paid\b", r"^Finance\s+costs?\s+paid\b"],
    "ocf": [r"^Net\s+cash\s+flows?\s+from\s*\(used\s+in\)\s+operating\s+activities\b"],
    "capex": [r"^Purchase\s+of\s+property,\s*plant\s+and\s+equipment\b", r"^Additions?\s+to\s+property,\s*plant\s+and\s+equipment\b"],
    "investing": [r"^Net\s+cash\s+flows?\s+from\s*\(used\s+in\)\s+investing\s+activities\b"],
    "da": [r"^Adjustments?\s+for\s+depreciation(?:[/\s]+and)?[/\s]+amortis?ation\b"],
    "debtProceeds": [r"^Proceeds\s+from\s+borrowings\b", r"^Proceeds\s+from\s+(?:term\s+)?loans\b"],
    "debtRepaid": [r"^Repayments?\s+of\s+borrowings\b", r"^Repayments?\s+of\s+(?:term\s+)?loans\b"],
    "divPaid": [r"^Cash\s+dividends?\s+paid\b", r"^Dividends?\s+paid\s+to\s+equity\s+holders\b", r"^Dividends?\s+paid\b"],
    "financing": [r"^Net\s+cash\s+flows?\s+from\s*\(used\s+in\)\s+financing\s+activities\b"],
    "netChangeCash": [
        r"^Net\s+(?:increase|change)\s*\(decrease\)\s+in\s+cash\s+and\s+cash\s+equivalents\b",
        r"^Net\s+change\s+in\s+cash\b",
    ],
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
        fi_bytes = find_pdf("11_")

        bs_text = extract_pdf_text_bytes(bs_bytes, 4) if bs_bytes else ""
        is_text = extract_pdf_text_bytes(is_bytes, 3) if is_bytes else ""
        cf_text = extract_pdf_text_bytes(cf_bytes, 3) if cf_bytes else ""
        fi_text = extract_pdf_text_bytes(fi_bytes, 1) if fi_bytes else ""

        bs = parse_xbrl_statement_text(bs_text, BS_PATTERNS, False) if bs_text else {}
        inc = parse_xbrl_statement_text(is_text, IS_PATTERNS, is_interim) if is_text else {}
        cf = parse_xbrl_statement_text(cf_text, CF_PATTERNS, False) if cf_text else {}

        # Auto-calibrate unit multiplier (1x vs 1000x) using 11_FilingInformation, statement headers, and MCD paid-up capital
        mult = bs.get("_unitMult", 1.0)
        if re.search(r"Level\s+of\s+rounding\s+off\s+for\s+monetary\s+values\s+Thousands", fi_text, re.I):
            mult = 1000.0
        elif re.search(r"Level\s+of\s+rounding\s+off\s+for\s+monetary\s+values\s+Millions", fi_text, re.I):
            mult = 1e6
        elif inc.get("_unitMult", 1.0) == 1000.0 or cf.get("_unitMult", 1.0) == 1000.0:
            mult = 1000.0

        sc_pair = bs.get("shareCapital")
        if sc_pair and sc_pair[0] and paid_up_cap_omr and paid_up_cap_omr > 100000:
            ratio = paid_up_cap_omr / abs(sc_pair[0])
            if 500 <= ratio <= 2000:
                mult = 1000.0
            elif 0.5 <= ratio <= 2.0:
                mult = 1.0
        elif mult == 1.0 and paid_up_cap_omr and paid_up_cap_omr >= 1000000:
            # Fallback check: if Total Assets or Total Equity is ~1000x smaller than Paid-Up Capital, statement is in OMR Thousands
            ast_raw = (bs.get("totalAssets") or (None, None))[0]
            eq_raw = (bs.get("totalEquity") or bs.get("attrEquity") or (None, None))[0]
            ref_raw = ast_raw if (ast_raw and ast_raw > 0) else (abs(eq_raw) if eq_raw else None)
            if ref_raw and (paid_up_cap_omr / ref_raw) > 50:
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
            l_nc = scale(bs, "leaseNC", idx) or 0.0
            l_c = scale(bs, "leaseC", idx) or 0.0
            tot_debt = b_nc + b_c + l_nc + l_c
            op_p = scale(inc, "opProfit", idx)
            da_v = abs(scale(cf, "da", idx) or scale(inc, "da", idx) or 0.0)
            ebitda = round(op_p + da_v, 2) if op_p is not None else None
            capex = scale(cf, "capex", idx)
            if capex is not None:
                capex = abs(capex)
            div_paid = scale(cf, "divPaid", idx)
            if div_paid is not None:
                div_paid = abs(div_paid)

            ocf_v = scale(cf, "ocf", idx)
            fcf_v = round(ocf_v - capex, 2) if (ocf_v is not None and capex is not None) else None
            cash_v = scale(bs, "cash", idx)
            net_debt_v = round(tot_debt - cash_v, 2) if cash_v is not None else None

            return {
                "revenue": scale(inc, "revenue", idx),
                "costOfSales": scale(inc, "costOfSales", idx),
                "grossProfit": scale(inc, "grossProfit", idx),
                "otherOpInc": scale(inc, "otherOpInc", idx),
                "sgaExpenses": scale(inc, "sgaExpenses", idx),
                "opInc": scale(inc, "opInc", idx),
                "interestIncome": scale(inc, "interestIncome", idx),
                "interestExpense": scale(inc, "interestExpense", idx),
                "nii": round(nii_c + isl_inc, 2) if nii_c is not None else None,
                "islamicIncome": scale(inc, "islamicIncome", idx),
                "islamicExpense": scale(inc, "islamicExpense", idx),
                "islamicNetInc": scale(inc, "islamicNetInc", idx),
                "feeCommission": scale(inc, "feeCommission", idx),
                "investmentIncome": scale(inc, "investmentIncome", idx),
                "staffExpenses": scale(inc, "staffExpenses", idx),
                "opProfit": op_p,
                "ebitda": ebitda,
                "da": round(da_v, 2) if da_v > 0 else None,
                "opEx": scale(inc, "opEx", idx),
                "financeIncome": scale(inc, "financeIncome", idx),
                "financeCosts": scale(inc, "financeCosts", idx),
                "impairment": scale(inc, "impairment", idx),
                "insuranceServiceExp": scale(inc, "insuranceServiceExp", idx),
                "insuranceServiceResult": scale(inc, "insuranceServiceResult", idx),
                "profitBeforeTax": scale(inc, "profitBeforeTax", idx),
                "taxExpense": scale(inc, "taxExpense", idx),
                "netProfit": scale(inc, "netProfit", idx),
                "eps": scale(inc, "eps", idx),
                "shareCapital": scale(bs, "shareCapital", idx),
                "assets": scale(bs, "totalAssets", idx),
                "equity": scale(bs, "attrEquity", idx) or scale(bs, "totalEquity", idx),
                "totalEquity": scale(bs, "totalEquity", idx),
                "liabilities": scale(bs, "totalLiabilities", idx),
                "loans": round(loans_c + isl_fin, 2) if loans_c is not None else None,
                "deposits": round(dep_c + isl_dep, 2) if dep_c is not None else None,
                "cash": cash_v,
                "receivables": scale(bs, "receivables", idx),
                "inventories": scale(bs, "inventories", idx),
                "totalCurrentAssets": scale(bs, "totalCurrentAssets", idx),
                "ppe": scale(bs, "ppe", idx),
                "rouAssets": scale(bs, "rouAssets", idx),
                "intangibles": scale(bs, "intangibles", idx),
                "totalNonCurrentAssets": scale(bs, "totalNonCurrentAssets", idx),
                "payables": scale(bs, "payables", idx),
                "borrowingsC": scale(bs, "borrowingsC", idx),
                "leaseC": scale(bs, "leaseC", idx),
                "totalCurrentLiabilities": scale(bs, "totalCurrentLiabilities", idx),
                "borrowingsNC": scale(bs, "borrowingsNC", idx),
                "leaseNC": scale(bs, "leaseNC", idx),
                "totalNonCurrentLiabilities": scale(bs, "totalNonCurrentLiabilities", idx),
                "retainedEarnings": scale(bs, "retainedEarnings", idx),
                "reserves": scale(bs, "reserves", idx),
                "tier1Securities": scale(bs, "tier1Securities", idx),
                "dueFromBanks": scale(bs, "dueFromBanks", idx),
                "investmentSecurities": scale(bs, "investmentSecurities", idx),
                "dueToBanks": scale(bs, "dueToBanks", idx),
                "debtSecurities": scale(bs, "debtSecurities", idx),
                "insuranceLiabilities": scale(bs, "insuranceLiabilities", idx),
                "debt": round(tot_debt, 2) if tot_debt else 0.0,
                "netDebt": net_debt_v,
                "nav": scale(bs, "nav", idx),
                "pbtCf": scale(cf, "pbtCf", idx),
                "ocfBeforeWc": scale(cf, "ocfBeforeWc", idx),
                "taxPaid": scale(cf, "taxPaid", idx),
                "interestPaid": scale(cf, "interestPaid", idx),
                "ocf": ocf_v,
                "capex": capex,
                "fcf": fcf_v,
                "investing": scale(cf, "investing", idx),
                "debtProceeds": scale(cf, "debtProceeds", idx),
                "debtRepaid": scale(cf, "debtRepaid", idx),
                "divPaid": div_paid,
                "financing": scale(cf, "financing", idx),
                "netChangeCash": scale(cf, "netChangeCash", idx),
            }

        return {"current": build_period(0), "prior": build_period(1), "mult": mult}
    except Exception:
        return None


def parse_upload_date(dstr):
    try:
        return datetime.datetime.strptime(dstr.strip(), "%b %d, %Y").strftime("%Y-%m-%d")
    except Exception:
        return ""


# Known MSX stock splits (effectiveDate = first trading day at post-split price, factor = new shares / old shares)
STOCK_SPLITS = {
    "VOES": [{"effectiveDate": "2026-06-08", "factor": 10.0}],
}


def adjust_series_for_splits(all_days, all_prices, all_vols, sym=None):
    """Backward-adjust historical prices (divide) and volumes (multiply) for known and persistent-step MSX stock splits."""
    prices = list(all_prices)
    vols = list(all_vols)

    # 1. Apply explicit known splits (most recent to oldest)
    applied_dates = set()
    for sp in sorted(STOCK_SPLITS.get(sym or "", []), key=lambda x: x["effectiveDate"], reverse=True):
        eff = sp["effectiveDate"]
        fac = float(sp["factor"])
        if fac <= 0 or fac == 1.0:
            continue
        applied_dates.add(eff)
        for i, d in enumerate(all_days):
            if d < eff:
                prices[i] = round(prices[i] / fac, 4)
                vols[i] = int(round(vols[i] * fac))

    # 2. Automatic persistent-step split detector for future unlisted MSX splits (2x, 3x, 4x, 5x, 10x)
    #    Requires 5 stable sessions before & after so illiquid single-print ticks are never falsely adjusted.
    n = len(prices)
    if n >= 12:
        std_factors = (2.0, 3.0, 4.0, 5.0, 10.0)
        for i in range(5, n - 5):
            if all_days[i] in applied_dates:
                continue
            pre_win = sorted(prices[i - 5:i])
            post_win = sorted(prices[i:i + 5])
            pre_med = pre_win[2]
            post_med = post_win[2]
            if pre_med <= 0 or post_med <= 0:
                continue
            ratio = pre_med / post_med
            matched_fac = next((f for f in std_factors if abs(ratio - f) / f <= 0.10), None)
            if not matched_fac:
                continue
            # Confirm stability before and after the step (not an illiquid spike)
            pre_stable = all(abs(x - pre_med) / pre_med <= 0.25 for x in pre_win)
            post_stable = all(abs(x - post_med) / post_med <= 0.25 for x in post_win)
            if pre_stable and post_stable:
                for j in range(i):
                    prices[j] = round(prices[j] / matched_fac, 4)
                    vols[j] = int(round(vols[j] * matched_fac))

    return [round(p, 3) for p in prices], vols


def parse_hist_series(raw_pts, sym=None):
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
    all_prices, all_vols = adjust_series_for_splits(all_days, all_prices, all_vols, sym=sym)
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

    # Parse XBRL PDFs in memory for ALL companies (including flagship 8, so every company gets full P&L, BS, CF line items)
    parsed_fin = {}
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


def normalize_ytd_label(raw_name):
    """Map MSX interim filing names (e.g. 'Third Quarter', 'First Quarter') to ('9M 2026', 4/3) etc."""
    s = (raw_name or "").lower()
    if any(k in s for k in ("q1", "first", "3m", "three month")):
        return "Q1 2026", 4.0, "Q1 × 4"
    if any(k in s for k in ("q2", "second", "half", "h1", "6m", "six month")):
        return "H1 2026", 2.0, "H1 × 2"
    if any(k in s for k in ("q3", "third", "9m", "nine month")):
        return "9M 2026", 4.0 / 3.0, "9M × 4/3"
    return "2026 YTD", 1.0, None


def build_statements_payload(sym, sector_label, is_bank, flag, parsed_fin, prosp_cfg, fin_pref, shares_out_raw, dps_by_period=None, q26_dps=None):
    """Build structured 4-year + latest YTD Income Statement, Balance Sheet, Cash Flow, and Sector-Adapted Key Stats."""
    if is_bank:
        template = "banking"
        template_label = "Banking IFRS"
    elif "insurance" in (sector_label or "").lower() or "takaful" in (sector_label or "").lower():
        template = "insurance"
        template_label = "Insurance IFRS"
    else:
        template = "corporate"
        template_label = "Corporate IFRS"

    # 1. Assemble unified period_map from XBRL parsed_fin + IPO prospectus backfill + Flagship verified overrides
    period_map = {}
    for k, v in (parsed_fin or {}).items():
        if isinstance(v, dict):
            period_map[k] = dict(v)

    for py, pdata in ((prosp_cfg or {}).get("annuals") or {}).items():
        if py not in period_map or pdata.get("override"):
            base = dict(period_map.get(py) or {})
            for fk, fv in pdata.items():
                if fk != "override" and fv is not None:
                    base[fk] = fv
            period_map[py] = base
        else:
            for fk, fv in pdata.items():
                if fk != "override" and fv is not None and period_map[py].get(fk) is None:
                    period_map[py][fk] = fv

    if flag:
        for a in flag.get("annuals") or []:
            yr = a["year"]
            base = dict(period_map.get(yr) or {})
            for fk, fv in a.items():
                if fk != "year" and fv is not None:
                    base[fk] = fv
            period_map[yr] = base
        q3 = flag.get("q3_2026")
        if q3:
            q_cur_b = dict(period_map.get("Q26_CUR") or {})
            for fk, fv in q3.items():
                if not fk.endswith("Prior") and fv is not None:
                    q_cur_b[fk] = fv
            period_map["Q26_CUR"] = q_cur_b
            q_pri_b = dict(period_map.get("Q26_PRI") or {})
            for fk in ("revenue", "ebitda", "opInc", "nii", "netProfit"):
                if q3.get(fk + "Prior") is not None:
                    q_pri_b[fk] = q3[fk + "Prior"]
            period_map["Q26_PRI"] = q_pri_b

    # Pull Q3 2026 Net Profit from Companies-Fin-Pref if XBRL interim parser didn't have it
    if fin_pref:
        fp_cp = parse_float(fin_pref.get("Net_Profit_CP"), None)
        fp_pp = parse_float(fin_pref.get("Net_Profit_PP"), None)
        if fp_cp is not None:
            period_map.setdefault("Q26_CUR", {})
            if period_map["Q26_CUR"].get("netProfit") is None:
                period_map["Q26_CUR"]["netProfit"] = fp_cp
        if fp_pp is not None:
            period_map.setdefault("Q26_PRI", {})
            if period_map["Q26_PRI"].get("netProfit") is None:
                period_map["Q26_PRI"]["netProfit"] = fp_pp

    # Attach DPS per period and derive consistent subtotals (fcf, netDebt, ebit, ebitda, grossProfit)
    for k, d in period_map.items():
        if not isinstance(d, dict):
            continue
        if dps_by_period and k in dps_by_period and d.get("dps") is None:
            d["dps"] = dps_by_period[k]
        if k == "Q26_CUR" and q26_dps is not None and d.get("dps") is None:
            d["dps"] = q26_dps
        if d.get("fcf") is None and d.get("ocf") is not None and d.get("capex") is not None:
            d["fcf"] = round(d["ocf"] - abs(d["capex"]), 2)
        if d.get("netDebt") is None and d.get("debt") is not None and d.get("cash") is not None:
            d["netDebt"] = round(d["debt"] - d["cash"], 2)
        if d.get("opProfit") is None and d.get("ebitda") is not None and d.get("da") is not None:
            d["opProfit"] = round(d["ebitda"] - abs(d["da"]), 2)
        if d.get("ebitda") is None and d.get("opProfit") is not None and d.get("da") is not None:
            d["ebitda"] = round(d["opProfit"] + abs(d["da"]), 2)
        if d.get("grossProfit") is None and d.get("revenue") is not None and d.get("costOfSales") is not None:
            d["grossProfit"] = round(d["revenue"] - abs(d["costOfSales"]), 2)
        if d.get("nav") is None and d.get("equity") is not None and shares_out_raw and shares_out_raw > 0:
            d["nav"] = round(d["equity"] / shares_out_raw, 4)
        if d.get("eps") is None and d.get("netProfit") is not None and shares_out_raw and shares_out_raw > 0:
            d["eps"] = round(d["netProfit"] / shares_out_raw, 4)
        # Adjust pre-split per-share EPS & NAV for known stock splits (only when XBRL value is on pre-split share count)
        if sym in STOCK_SPLITS and k.startswith("FY") and shares_out_raw and shares_out_raw > 0:
            yr_num = int(k[2:])
            cum_fac = 1.0
            for sp in STOCK_SPLITS[sym]:
                if yr_num < int(sp["effectiveDate"][:4]):
                    cum_fac *= float(sp["factor"])
            if cum_fac > 1.0:
                if d.get("eps") is not None and d.get("netProfit"):
                    exp_eps = d["netProfit"] / shares_out_raw
                    if exp_eps != 0 and abs((d["eps"] / exp_eps) - cum_fac) / cum_fac < 0.35:
                        d["eps"] = round(d["eps"] / cum_fac, 4)
                if d.get("nav") is not None and d.get("equity"):
                    exp_nav = d["equity"] / shares_out_raw
                    if exp_nav != 0 and abs((d["nav"] / exp_nav) - cum_fac) / cum_fac < 0.35:
                        d["nav"] = round(d["nav"] / cum_fac, 4)

    # 2. Select last 4 audited fiscal years + latest YTD interim period
    all_yrs = [
        y for y in ["FY2021", "FY2022", "FY2023", "FY2024", "FY2025", "FY2026"]
        if y in period_map and any(period_map[y].get(k) is not None for k in ("revenue", "opInc", "netProfit", "assets", "equity"))
    ]
    sel_yrs = all_yrs[-4:]
    periods = [{"key": y, "label": y, "isYtd": False, "annualizeFactor": 1.0} for y in sel_yrs]

    q_cur = period_map.get("Q26_CUR") or {}
    q_pri = period_map.get("Q26_PRI") or {}
    has_ytd = any(q_cur.get(k) is not None for k in ("revenue", "opInc", "netProfit", "assets", "equity"))
    ytd_label, ytd_factor, ytd_formula = (None, 1.0, None)
    if has_ytd:
        raw_q_lbl = (parsed_fin or {}).get("Q26_LABEL") or ("9M 2026" if flag else "2026 YTD")
        ytd_label, ytd_factor, ytd_formula = normalize_ytd_label(raw_q_lbl)
        periods.append({
            "key": "Q26_CUR",
            "label": ytd_label,
            "isYtd": True,
            "annualizeFactor": round(ytd_factor, 4),
        })

    def clean_num(v, decimals=2, limit=None):
        if v is None or isinstance(v, bool):
            return None
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return None
        if math.isnan(fv) or math.isinf(fv):
            return None
        if limit is not None and abs(fv) > limit:
            return None
        return round(fv, decimals)

    def make_field_rows(spec_list):
        """Convert [(key, label, fmt, style), ...] into pruned rows with section headers."""
        raw_rows = []
        for item in spec_list:
            if item[0] == "__section__":
                raw_rows.append({"key": item[1], "label": item[2], "rowType": "section"})
                continue
            fkey, flabel, ffmt, fstyle = item
            vals = {}
            any_val = False
            for p in periods:
                d = period_map.get(p["key"]) or {}
                dec = 4 if ffmt == "perShare" else 2
                v = clean_num(d.get(fkey), dec)
                vals[p["key"]] = v
                if v is not None:
                    any_val = True
            if any_val:
                raw_rows.append({
                    "key": fkey,
                    "label": flabel,
                    "fmt": ffmt,
                    "style": fstyle,
                    "values": vals,
                })
        # Prune section headers that have no data rows immediately following them
        pruned = []
        for idx, r in enumerate(raw_rows):
            if r.get("rowType") == "section":
                nxt = raw_rows[idx + 1] if idx + 1 < len(raw_rows) else None
                if nxt and nxt.get("rowType") != "section":
                    pruned.append(r)
            else:
                pruned.append(r)
        return pruned

    # 3. Build Sector-Adapted Income Statement, Balance Sheet, Cash Flow
    if template == "banking":
        is_spec = [
            ("__section__", "sec_rev", "Operating Income"),
            ("interestIncome", "Interest Income", "currency", "normal"),
            ("interestExpense", "Interest Expense", "currency", "normal"),
            ("nii", "Net Interest & Islamic Income", "currency", "subtotal"),
            ("islamicIncome", "Income from Islamic Financing", "currency", "normal"),
            ("islamicExpense", "Distribution to Islamic Depositors", "currency", "normal"),
            ("feeCommission", "Net Fee & Commission Income", "currency", "normal"),
            ("investmentIncome", "Investment & Other Income", "currency", "normal"),
            ("opInc", "Total Operating Income", "currency", "total"),
            ("__section__", "sec_exp", "Operating Expenses & Provisions"),
            ("staffExpenses", "Staff Costs", "currency", "normal"),
            ("da", "Depreciation & Amortisation", "currency", "normal"),
            ("opEx", "Total Operating Expenses", "currency", "subtotal"),
            ("impairment", "Net Impairment / Credit Loss Provisions", "currency", "normal"),
            ("__section__", "sec_bot", "Profitability & Per Share"),
            ("profitBeforeTax", "Profit Before Tax", "currency", "subtotal"),
            ("taxExpense", "Income Tax Expense", "currency", "normal"),
            ("netProfit", "Net Profit for the Period", "currency", "total"),
            ("eps", "Basic EPS (OMR / Share)", "perShare", "normal"),
            ("dps", "Cash Dividend per Share (OMR)", "perShare", "normal"),
        ]
        bs_spec = [
            ("__section__", "sec_assets", "Assets"),
            ("cash", "Cash & Balances with Central Bank", "currency", "normal"),
            ("dueFromBanks", "Due from Banks & Placements", "currency", "normal"),
            ("loans", "Net Loans, Advances & Islamic Financing", "currency", "subtotal"),
            ("investmentSecurities", "Investment Securities", "currency", "normal"),
            ("ppe", "Property, Equipment & Other Assets", "currency", "normal"),
            ("assets", "Total Assets", "currency", "total"),
            ("__section__", "sec_liab", "Liabilities"),
            ("dueToBanks", "Due to Banks & Borrowings", "currency", "normal"),
            ("deposits", "Customer Deposits & Islamic Deposits", "currency", "subtotal"),
            ("debtSecurities", "Debt Securities / Bonds & Sukuk", "currency", "normal"),
            ("liabilities", "Total Liabilities", "currency", "total"),
            ("__section__", "sec_eq", "Shareholders' Equity"),
            ("shareCapital", "Share Capital", "currency", "normal"),
            ("reserves", "Legal & Other Reserves", "currency", "normal"),
            ("retainedEarnings", "Retained Earnings", "currency", "normal"),
            ("tier1Securities", "Perpetual Tier 1 / Subordinated Securities", "currency", "normal"),
            ("equity", "Equity Attributable to Shareholders", "currency", "subtotal"),
            ("totalEquity", "Total Equity", "currency", "total"),
            ("nav", "Book Value / NAV per Share (OMR)", "perShare", "normal"),
        ]
    elif template == "insurance":
        is_spec = [
            ("__section__", "sec_rev", "Insurance & Underwriting"),
            ("revenue", "Insurance Revenue / Gross Premiums", "currency", "subtotal"),
            ("insuranceServiceExp", "Insurance Service Expenses", "currency", "normal"),
            ("insuranceServiceResult", "Insurance Service Result", "currency", "subtotal"),
            ("investmentIncome", "Net Investment Income", "currency", "normal"),
            ("otherOpInc", "Other Operating Income", "currency", "normal"),
            ("opEx", "Operating & Administrative Expenses", "currency", "normal"),
            ("opProfit", "Operating Profit (EBIT)", "currency", "subtotal"),
            ("__section__", "sec_bot", "Profitability & Per Share"),
            ("financeCosts", "Finance Costs", "currency", "normal"),
            ("profitBeforeTax", "Profit Before Tax", "currency", "subtotal"),
            ("taxExpense", "Income Tax Expense", "currency", "normal"),
            ("netProfit", "Net Profit for the Period", "currency", "total"),
            ("eps", "Basic EPS (OMR / Share)", "perShare", "normal"),
            ("dps", "Cash Dividend per Share (OMR)", "perShare", "normal"),
        ]
        bs_spec = [
            ("__section__", "sec_assets", "Assets"),
            ("cash", "Cash & Bank Balances", "currency", "normal"),
            ("investmentSecurities", "Financial Investments", "currency", "normal"),
            ("receivables", "Insurance & Other Receivables", "currency", "normal"),
            ("ppe", "Property & Equipment", "currency", "normal"),
            ("assets", "Total Assets", "currency", "total"),
            ("__section__", "sec_liab", "Liabilities"),
            ("insuranceLiabilities", "Insurance Contract Liabilities", "currency", "subtotal"),
            ("payables", "Payables & Other Liabilities", "currency", "normal"),
            ("debt", "Total Debt & Lease Liabilities", "currency", "normal"),
            ("liabilities", "Total Liabilities", "currency", "total"),
            ("__section__", "sec_eq", "Shareholders' Equity"),
            ("shareCapital", "Share Capital", "currency", "normal"),
            ("reserves", "Legal & Other Reserves", "currency", "normal"),
            ("retainedEarnings", "Retained Earnings", "currency", "normal"),
            ("equity", "Equity Attributable to Shareholders", "currency", "subtotal"),
            ("totalEquity", "Total Equity", "currency", "total"),
            ("nav", "Book Value / NAV per Share (OMR)", "perShare", "normal"),
        ]
    else:
        is_spec = [
            ("__section__", "sec_rev", "Revenue & Operating Profit"),
            ("revenue", "Revenue", "currency", "subtotal"),
            ("costOfSales", "Cost of Sales", "currency", "normal"),
            ("grossProfit", "Gross Profit", "currency", "subtotal"),
            ("otherOpInc", "Other Operating Income", "currency", "normal"),
            ("sgaExpenses", "General & Administrative Expenses", "currency", "normal"),
            ("opEx", "Total Operating Expenses", "currency", "normal"),
            ("da", "Depreciation & Amortisation", "currency", "normal"),
            ("ebitda", "EBITDA", "currency", "subtotal"),
            ("opProfit", "Operating Profit (EBIT)", "currency", "subtotal"),
            ("__section__", "sec_fin", "Finance & Pre-Tax"),
            ("financeIncome", "Finance Income", "currency", "normal"),
            ("financeCosts", "Finance Costs", "currency", "normal"),
            ("impairment", "Impairment / Provisions", "currency", "normal"),
            ("profitBeforeTax", "Profit Before Tax", "currency", "subtotal"),
            ("taxExpense", "Income Tax Expense", "currency", "normal"),
            ("__section__", "sec_bot", "Net Profit & Per Share"),
            ("netProfit", "Net Profit for the Period", "currency", "total"),
            ("eps", "Basic EPS (OMR / Share)", "perShare", "normal"),
            ("dps", "Cash Dividend per Share (OMR)", "perShare", "normal"),
        ]
        bs_spec = [
            ("__section__", "sec_ca", "Current Assets"),
            ("cash", "Cash & Bank Balances", "currency", "normal"),
            ("receivables", "Trade & Other Receivables", "currency", "normal"),
            ("inventories", "Inventories", "currency", "normal"),
            ("totalCurrentAssets", "Total Current Assets", "currency", "subtotal"),
            ("__section__", "sec_nca", "Non-Current Assets"),
            ("ppe", "Property, Plant & Equipment", "currency", "normal"),
            ("rouAssets", "Right-of-Use Assets", "currency", "normal"),
            ("intangibles", "Intangible Assets & Goodwill", "currency", "normal"),
            ("investmentSecurities", "Investments & Financial Assets", "currency", "normal"),
            ("totalNonCurrentAssets", "Total Non-Current Assets", "currency", "subtotal"),
            ("assets", "Total Assets", "currency", "total"),
            ("__section__", "sec_cl", "Current Liabilities"),
            ("payables", "Trade & Other Payables", "currency", "normal"),
            ("borrowingsC", "Short-Term Borrowings", "currency", "normal"),
            ("leaseC", "Current Lease Liabilities", "currency", "normal"),
            ("totalCurrentLiabilities", "Total Current Liabilities", "currency", "subtotal"),
            ("__section__", "sec_ncl", "Non-Current Liabilities"),
            ("borrowingsNC", "Long-Term Borrowings", "currency", "normal"),
            ("leaseNC", "Non-Current Lease Liabilities", "currency", "normal"),
            ("totalNonCurrentLiabilities", "Total Non-Current Liabilities", "currency", "subtotal"),
            ("liabilities", "Total Liabilities", "currency", "total"),
            ("__section__", "sec_eq", "Shareholders' Equity & Debt Summary"),
            ("shareCapital", "Share Capital", "currency", "normal"),
            ("reserves", "Legal & Other Reserves", "currency", "normal"),
            ("retainedEarnings", "Retained Earnings", "currency", "normal"),
            ("equity", "Equity Attributable to Shareholders", "currency", "subtotal"),
            ("totalEquity", "Total Equity", "currency", "total"),
            ("debt", "Total Debt (Borrowings + Leases)", "currency", "normal"),
            ("netDebt", "Net Debt (Total Debt − Cash)", "currency", "normal"),
            ("nav", "Book Value / NAV per Share (OMR)", "perShare", "normal"),
        ]

    cf_spec = [
        ("__section__", "sec_op", "Operating Activities"),
        ("pbtCf", "Profit Before Tax", "currency", "normal"),
        ("da", "Depreciation & Amortisation", "currency", "normal"),
        ("ocfBeforeWc", "Operating Cash Flow Before Working Capital", "currency", "normal"),
        ("taxPaid", "Income Taxes Paid", "currency", "normal"),
        ("interestPaid", "Interest / Finance Costs Paid", "currency", "normal"),
        ("ocf", "Net Cash from Operating Activities (OCF)", "currency", "total"),
        ("__section__", "sec_inv", "Investing Activities"),
        ("capex", "Capital Expenditure (CapEx)", "currency", "normal"),
        ("investing", "Net Cash from Investing Activities", "currency", "subtotal"),
        ("fcf", "Free Cash Flow (OCF − CapEx)", "currency", "total"),
        ("__section__", "sec_fin", "Financing Activities"),
        ("debtProceeds", "Proceeds from Borrowings", "currency", "normal"),
        ("debtRepaid", "Repayments of Borrowings", "currency", "normal"),
        ("divPaid", "Cash Dividends Paid", "currency", "normal"),
        ("financing", "Net Cash from Financing Activities", "currency", "subtotal"),
        ("netChangeCash", "Net Change in Cash & Equivalents", "currency", "total"),
    ]

    income_rows = make_field_rows(is_spec)
    balance_rows = make_field_rows(bs_spec)
    cashflow_rows = make_field_rows(cf_spec)

    # 4. Compute Sector-Adapted Key Stats & Ratios (with annualised YTD flow-to-stock ratios)
    def get_prior(idx):
        p = periods[idx]
        if p["isYtd"]:
            return q_pri
        if idx > 0:
            return period_map.get(periods[idx - 1]["key"]) or {}
        # Check if there is an earlier annual year before sel_yrs[0] (e.g. FY2021 for FY2022 growth)
        if all_yrs and sel_yrs and sel_yrs[0] in all_yrs:
            pos = all_yrs.index(sel_yrs[0])
            if pos > 0:
                return period_map.get(all_yrs[pos - 1]) or {}
        return {}

    def pct_growth(cur_v, pri_v):
        if cur_v is None or pri_v is None or pri_v == 0:
            return None
        return clean_num((cur_v - pri_v) / abs(pri_v) * 100.0, 1, limit=999.0)

    def pct_ratio(num, den, require_pos_den=True):
        if num is None or den is None or den == 0:
            return None
        if require_pos_den and den <= 0:
            return None
        return clean_num((num / den) * 100.0, 1, limit=999.0)

    def mult_ratio(num, den, require_pos_den=True):
        if num is None or den is None or den == 0:
            return None
        if require_pos_den and den <= 0:
            return None
        return clean_num(num / den, 2, limit=200.0)

    def build_stat_rows(ratio_defs):
        raw_rows = []
        for item in ratio_defs:
            if item[0] == "__section__":
                raw_rows.append({"key": item[1], "label": item[2], "rowType": "section"})
                continue
            rkey, rlabel, rfmt, rstyle, ann_ytd, fn = item
            vals = {}
            any_val = False
            for idx, p in enumerate(periods):
                d = period_map.get(p["key"]) or {}
                pri = get_prior(idx)
                af = p["annualizeFactor"] if (ann_ytd and p["isYtd"]) else 1.0
                v = fn(d, pri, af, p["isYtd"])
                vals[p["key"]] = v
                if v is not None:
                    any_val = True
            if any_val:
                row_obj = {
                    "key": rkey,
                    "label": rlabel,
                    "fmt": rfmt,
                    "style": rstyle,
                    "values": vals,
                }
                if ann_ytd and has_ytd and ytd_factor > 1.0:
                    row_obj["annualizedYtd"] = True
                raw_rows.append(row_obj)
        pruned = []
        for idx, r in enumerate(raw_rows):
            if r.get("rowType") == "section":
                nxt = raw_rows[idx + 1] if idx + 1 < len(raw_rows) else None
                if nxt and nxt.get("rowType") != "section":
                    pruned.append(r)
            else:
                pruned.append(r)
        return pruned

    def payout_ratio(d, pri, af, is_ytd):
        if is_ytd:
            return None
        np_v = d.get("netProfit")
        if np_v is None or np_v <= 0:
            return None
        if d.get("dps") is not None and shares_out_raw and shares_out_raw > 0:
            return pct_ratio(d["dps"] * shares_out_raw, np_v)
        if d.get("divPaid") is not None:
            return pct_ratio(abs(d["divPaid"]), np_v)
        return None

    if template == "banking":
        ratio_defs = [
            ("__section__", "ks_growth", "Growth (YoY)"),
            ("opincGrowth", "Operating Income Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("opInc"), pri.get("opInc"))),
            ("niiGrowth", "Net Interest & Islamic Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("nii"), pri.get("nii"))),
            ("npGrowth", "Net Profit Growth (YoY)", "pct", "subtotal", False, lambda d, pri, af, y: pct_growth(d.get("netProfit"), pri.get("netProfit"))),
            ("loansGrowth", "Net Loans & Financing Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: None if y else pct_growth(d.get("loans"), pri.get("loans"))),
            ("depGrowth", "Customer Deposits Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: None if y else pct_growth(d.get("deposits"), pri.get("deposits"))),
            ("__section__", "ks_eff", "Efficiency & Margins"),
            ("cir", "Cost-to-Income Ratio (CIR)", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(abs(d["opEx"]) if d.get("opEx") is not None else None, d.get("opInc"))),
            ("nimAssets", "Net Interest Margin (NII / Assets)", "pct", "normal", True, lambda d, pri, af, y: pct_ratio(d["nii"] * af if d.get("nii") is not None else None, d.get("assets"))),
            ("nonIntShare", "Non-Interest Income Share", "pct", "normal", False, lambda d, pri, af, y: pct_ratio((d["opInc"] - d["nii"]) if (d.get("opInc") is not None and d.get("nii") is not None) else None, d.get("opInc"))),
            ("preProvMargin", "Pre-Provision Profit Margin", "pct", "normal", False, lambda d, pri, af, y: pct_ratio((d["opInc"] - abs(d["opEx"])) if (d.get("opInc") is not None and d.get("opEx") is not None) else None, d.get("opInc"))),
            ("netMargin", "Net Profit Margin (Net Profit / Op. Inc)", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(d.get("netProfit"), d.get("opInc"))),
            ("__section__", "ks_bs", "Balance Sheet & Capital"),
            ("ldr", "Loan-to-Deposit Ratio (LDR)", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(d.get("loans"), d.get("deposits"))),
            ("loansToAssets", "Net Loans / Total Assets", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("loans"), d.get("assets"))),
            ("eqToAssets", "Equity / Total Assets", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("totalEquity") or d.get("equity"), d.get("assets"))),
            ("__section__", "ks_ret", "Returns & Payout"),
            ("roe", "Return on Equity (ROE)", "pct", "subtotal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("equity"))),
            ("roa", "Return on Assets (ROA)", "pct", "normal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("assets"))),
            ("payout", "Dividend Payout Ratio", "pct", "normal", False, payout_ratio),
        ]
    elif template == "insurance":
        ratio_defs = [
            ("__section__", "ks_growth", "Growth (YoY)"),
            ("revGrowth", "Insurance Revenue Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("revenue"), pri.get("revenue"))),
            ("npGrowth", "Net Profit Growth (YoY)", "pct", "subtotal", False, lambda d, pri, af, y: pct_growth(d.get("netProfit"), pri.get("netProfit"))),
            ("__section__", "ks_margin", "Underwriting & Profitability"),
            ("insMargin", "Insurance Service Margin", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("insuranceServiceResult"), d.get("revenue"))),
            ("ebitMargin", "Operating (EBIT) Margin", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("opProfit"), d.get("revenue"))),
            ("netMargin", "Net Profit Margin", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(d.get("netProfit"), d.get("revenue"))),
            ("invIncShare", "Investment Income / Revenue", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("investmentIncome"), d.get("revenue"))),
            ("__section__", "ks_solv", "Capital, Solvency & Returns"),
            ("roe", "Return on Equity (ROE)", "pct", "subtotal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("equity"))),
            ("roa", "Return on Assets (ROA)", "pct", "normal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("assets"))),
            ("eqToAssets", "Equity / Total Assets", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("totalEquity") or d.get("equity"), d.get("assets"))),
            ("debtToEquity", "Debt / Equity", "multiple", "normal", False, lambda d, pri, af, y: mult_ratio(d.get("debt"), d.get("totalEquity") or d.get("equity"))),
            ("debtToCap", "Debt / Capitalization", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("debt"), (d["debt"] + (d.get("totalEquity") or d.get("equity"))) if (d.get("debt") is not None and (d.get("totalEquity") or d.get("equity"))) else None)),
            ("payout", "Dividend Payout Ratio", "pct", "normal", False, payout_ratio),
        ]
    else:
        ratio_defs = [
            ("__section__", "ks_growth", "Growth (YoY)"),
            ("revGrowth", "Revenue Growth (YoY)", "pct", "subtotal", False, lambda d, pri, af, y: pct_growth(d.get("revenue"), pri.get("revenue"))),
            ("gpGrowth", "Gross Profit Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("grossProfit"), pri.get("grossProfit"))),
            ("ebitdaGrowth", "EBITDA Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("ebitda"), pri.get("ebitda"))),
            ("ebitGrowth", "EBIT Growth (YoY)", "pct", "normal", False, lambda d, pri, af, y: pct_growth(d.get("opProfit"), pri.get("opProfit"))),
            ("npGrowth", "Net Profit Growth (YoY)", "pct", "subtotal", False, lambda d, pri, af, y: pct_growth(d.get("netProfit"), pri.get("netProfit"))),
            ("__section__", "ks_margin", "Margins & Profitability"),
            ("grossMargin", "Gross Margin", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("grossProfit"), d.get("revenue"))),
            ("ebitdaMargin", "EBITDA Margin", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(d.get("ebitda"), d.get("revenue"))),
            ("ebitMargin", "EBIT Margin", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("opProfit"), d.get("revenue"))),
            ("netMargin", "Net Profit Margin", "pct", "subtotal", False, lambda d, pri, af, y: pct_ratio(d.get("netProfit"), d.get("revenue"))),
            ("roe", "Return on Equity (ROE)", "pct", "normal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("equity"))),
            ("roa", "Return on Assets (ROA)", "pct", "normal", True, lambda d, pri, af, y: pct_ratio(d["netProfit"] * af if d.get("netProfit") is not None else None, d.get("assets"))),
            ("__section__", "ks_cf", "Cash Conversion & CapEx"),
            ("ocfToEbitda", "OCF / EBITDA", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("ocf"), d.get("ebitda"))),
            ("fcfToEbitda", "FCF / EBITDA", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("fcf"), d.get("ebitda"))),
            ("fcfMargin", "FCF Margin (FCF / Revenue)", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("fcf"), d.get("revenue"))),
            ("capexToRev", "CapEx / Revenue", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(abs(d["capex"]) if d.get("capex") is not None else None, d.get("revenue"))),
            ("__section__", "ks_lev", "Leverage, Coverage & Liquidity"),
            ("debtToEbitda", "Debt / EBITDA", "multiple", "subtotal", True, lambda d, pri, af, y: mult_ratio(d.get("debt"), d["ebitda"] * af if d.get("ebitda") is not None else None)),
            ("netDebtToEbitda", "Net Debt / EBITDA", "multiple", "subtotal", True, lambda d, pri, af, y: mult_ratio(d.get("netDebt"), d["ebitda"] * af if d.get("ebitda") is not None else None)),
            ("debtToEquity", "Debt / Equity", "multiple", "normal", False, lambda d, pri, af, y: mult_ratio(d.get("debt"), d.get("totalEquity") or d.get("equity"))),
            ("debtToCap", "Debt / Capitalization", "pct", "normal", False, lambda d, pri, af, y: pct_ratio(d.get("debt"), (d["debt"] + (d.get("totalEquity") or d.get("equity"))) if (d.get("debt") is not None and (d.get("totalEquity") or d.get("equity"))) else None)),
            ("intCov", "Interest Coverage (EBIT / Finance Costs)", "multiple", "normal", False, lambda d, pri, af, y: mult_ratio(d.get("opProfit"), abs(d["financeCosts"]) if d.get("financeCosts") else None)),
            ("currentRatio", "Current Ratio", "multiple", "normal", False, lambda d, pri, af, y: mult_ratio(d.get("totalCurrentAssets"), d.get("totalCurrentLiabilities"))),
            ("assetTurnover", "Asset Turnover (Revenue / Assets)", "multiple", "normal", True, lambda d, pri, af, y: mult_ratio(d["revenue"] * af if d.get("revenue") is not None else None, d.get("assets"))),
            ("payout", "Dividend Payout Ratio", "pct", "normal", False, payout_ratio),
        ]

    key_stats_rows = build_stat_rows(ratio_defs)

    ann_note = None
    if has_ytd and ytd_factor > 1.0 and ytd_formula:
        ann_note = (
            f"† Annualised YTD: For the partial-year {ytd_label} column, flow-to-stock ratios marked with † "
            f"(e.g. Debt / EBITDA, Net Debt / EBITDA, ROE, ROA, NIM, Asset Turnover) annualise the YTD income-statement flow "
            f"({ytd_formula}) against period-end balance sheet stock. YoY growth compares {ytd_label} against the prior-year interim period."
        )

    return {
        "template": template,
        "templateLabel": template_label,
        "periods": periods,
        "annualizeNote": ann_note,
        "incomeStatement": income_rows,
        "balanceSheet": balance_rows,
        "cashFlow": cashflow_rows,
        "keyStats": key_stats_rows,
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

    disp_dates, full_dates, prices, vols, sma50, sma200 = parse_hist_series(rec["histRaw"], sym=sym)
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

    ai_prof = AI_PROFILES.get(sym) if not flag else None
    segment_mode = "verified_pct"
    overview_sources = []
    overview_generated_at = None

    if flag:
        display_name = flag["displayName"]
        sector_group = flag["sectorGroup"]
        sector_label = flag["sectorLabel"]
        website = flag["website"]
        hq = flag["hq"]
        description = flag["description"]
        segments = flag["segments"]
        segment_note = flag["segmentNote"]
        segment_mode = "verified_pct"
        overview_sources = [flag["website"]]
    else:
        display_name = clean_company_name(labels.get("CompanyNameLabel") or mcd.get("SecurityName") or msx_row.get("LongNameEn") or sym)
        if not display_name.upper().endswith("SAOG") and "FUND" not in display_name.upper() and "REIT" not in display_name.upper():
            display_name = f"{display_name} SAOG"
        sector_group = "Banking" if is_bank else ("Energy & Utilities" if is_energy else mcd_sector)
        sector_label = f"{mcd_sector} • {sub_sector}" if sub_sector and sub_sector.lower() != mcd_sector.lower() else mcd_sector
        website = normalize_website(labels.get("WebsiteHyperLink") or labels.get("WebsiteHyperLink1"))
        if not website and ai_prof and ai_prof.get("websiteOverride"):
            website = normalize_website(ai_prof["websiteOverride"])
        hq = labels.get("HeadOfficeLabel") or labels.get("AddressLabel") or "Sultanate of Oman"
        if len(hq) > 75:
            hq = hq[:72].rstrip(" ,;-") + "..."
        if ai_prof and ai_prof.get("description"):
            description = ai_prof["description"]
            segments = ai_prof.get("segments") or []
            segment_note = ai_prof.get("segmentNote") or "Core Business Lines"
            segment_mode = ai_prof.get("segmentMode") or ("verified_pct" if segments and all(s.get("pct") is not None for s in segments) else "qualitative")
            overview_sources = ai_prof.get("overviewSources") or ([website] if website else [])
            overview_generated_at = ai_prof.get("generatedAt")
        else:
            description = f"{display_name} ({sym}) is a publicly listed company on the Muscat Stock Exchange ({market_label}) operating in the {sector_label} sector. Established {founded_str}, with {shares_out_raw:,} issued shares (ISIN {isin}) and {participants:,} registered shareholders recorded on Muscat Clearing & Depository (mcd.om)."
            segments = []
            segment_note = "Segment breakdown available in annual filing"
            segment_mode = "qualitative"

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

    omani_sh = int(round(parse_float(nat.get("omaniCurrSe"), 0))) or int(round(shares_out_raw * omani_pct / 100.0))
    gcc_sh = int(round(parse_float(nat.get("gulfCurrSe"), 0))) or int(round(shares_out_raw * gcc_pct / 100.0))
    arab_sh = int(round(parse_float(nat.get("arabCurrSe"), 0))) or int(round(shares_out_raw * arab_pct / 100.0))
    foreign_sh = int(round(parse_float(nat.get("foreignerCurrSe"), 0))) or int(round(shares_out_raw * foreign_pct / 100.0))

    nationality_breakdown = [
        {"label": "Omani Investors", "pct": omani_pct, "shares": omani_sh, "limitPct": parse_float(nat.get("omaniMax"), 100.0), "color": "#6366f1"},
        {"label": "GCC Investors", "pct": gcc_pct, "shares": gcc_sh, "limitPct": parse_float(nat.get("gulfMax"), 100.0), "color": "#06b6d4"},
        {"label": "Foreign & Arab Investors", "pct": other_non_omani, "arabPct": arab_pct, "foreignerPct": foreign_pct, "arabShares": arab_sh, "foreignerShares": foreign_sh, "shares": arab_sh + foreign_sh, "limitPct": parse_float(nat.get("foreignerMax"), 100.0), "color": "#10b981"},
    ]

    majors_sorted = sorted(rec["majorsRaw"], key=lambda x: parse_float(x.get("OwnershipPercentage"), 0.0), reverse=True)
    major_holders = []
    total_major_pct = 0.0
    for h in majors_sorted:
        raw_name = (h.get("InvestorName") or "").strip()
        disp_name = ARABIC_HOLDER_ENGLISH.get(raw_name)
        if not disp_name:
            norm_key = re.sub(r"\s+", " ", raw_name).strip()
            en_tr = SHAREHOLDER_TRANSLATIONS.get(norm_key)
            disp_name = f"{en_tr} ({raw_name})" if en_tr else raw_name
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
        elif chair_name and chair_name != "-" and not any("chair" in b["role"].lower() and "deputy" not in b["role"].lower() and "vice" not in b["role"].lower() for b in board_list):
            board_list.insert(0, {"name": chair_name, "role": "Chairman", "independence": "Non-Executive Director"})
        if dep_name and dep_name != "-" and not any(b["role"] == "Deputy Chairman" for b in board_list):
            board_list.insert(1 if board_list else 0, {"name": dep_name, "role": "Deputy Chairman", "independence": "Non-Executive Director"})

        # Apply verified Corporate Website / Prospectus / MSX Corporate Governance Report overrides
        cg_ov = CORP_GOV_OVERRIDES.get(sym) or {}
        if cg_ov.get("board"):
            board_list = [dict(b) for b in cg_ov["board"]]
        elif cg_ov.get("chairman") and not any("chair" in b["role"].lower() and "deputy" not in b["role"].lower() and "vice" not in b["role"].lower() for b in board_list):
            ch_entry = cg_ov["chairman"]
            ch_name = ch_entry["name"] if isinstance(ch_entry, dict) else str(ch_entry)
            ch_ind = ch_entry.get("independence", "Non-Executive Director") if isinstance(ch_entry, dict) else "Non-Executive Director"
            # Upgrade matching member or prepend
            upgraded = False
            for b in board_list:
                if ch_name.lower() in b["name"].lower() or b["name"].lower() in ch_name.lower():
                    b["role"] = "Chairman"
                    upgraded = True
                    break
            if not upgraded:
                board_list.insert(0, {"name": ch_name, "role": "Chairman", "independence": ch_ind})

        # Sort Chairman first, Deputy second
        board_list.sort(key=lambda b: 0 if ("chair" in b["role"].lower() and "deputy" not in b["role"].lower() and "vice" not in b["role"].lower()) else (1 if ("deputy" in b["role"].lower() or "vice" in b["role"].lower()) else 2))

        mgmt_list = []
        if cg_ov.get("management"):
            mgmt_list = [dict(m) for m in cg_ov["management"]]
            ceo_name = mgmt_list[0]["name"] if mgmt_list else (ceo_raw if ceo_raw and ceo_raw != "-" else "Not disclosed on msx.om")
        else:
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
            if cg_ov.get("executives"):
                seen_m = {m["name"].lower() for m in mgmt_list}
                for ex_item in cg_ov["executives"]:
                    if ex_item.get("name") and ex_item["name"].lower() not in seen_m:
                        mgmt_list.append({"name": ex_item["name"], "title": ex_item["title"]})
                        seen_m.add(ex_item["name"].lower())
            if sec_name and sec_name != "-":
                mgmt_list.append({"name": sec_name, "title": "Board Secretary"})
            if int_aud and int_aud != "-":
                mgmt_list.append({"name": int_aud, "title": "Internal Auditor"})
            if leg_adv and leg_adv != "-":
                mgmt_list.append({"name": leg_adv, "title": "Legal Advisor"})
            if ir_name and ir_name != "-":
                mgmt_list.append({"name": ir_name, "title": "Investor Relations Officer"})

        gov = {
            "boardSourceLabel": cg_ov.get("boardSourceLabel") or f"msx.om Corporate Registry ({sym})",
            "boardSourceUrl": cg_ov.get("boardSourceUrl") or f"https://www.msx.om/snapshot.aspx?s={sym}",
            "boardDisclosedOnWebsite": bool(cg_ov.get("boardDisclosedOnWebsite", False)),
            "boardFallbackUrl": f"https://www.msx.om/snapshot.aspx?s={sym}",
            "managementSourceLabel": cg_ov.get("managementSourceLabel") or f"msx.om Corporate Registry ({sym})",
            "managementSourceUrl": cg_ov.get("managementSourceUrl") or f"https://www.msx.om/snapshot.aspx?s={sym}",
            "managementDisclosedOnWebsite": bool(cg_ov.get("managementDisclosedOnWebsite", False)),
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
            fy24 = ann_list[-2] if len(ann_list) >= 2 else {}
            opinc_yoy_fy = ((fy25["opInc"] - fy24["opInc"]) / abs(fy24["opInc"]) * 100) if fy24.get("opInc") else None
            nii_yoy_fy = ((fy25["nii"] - fy24["nii"]) / abs(fy24["nii"]) * 100) if fy24.get("nii") else None
            net_margin_fy = (fy25["netProfit"] / fy25["opInc"] * 100) if fy25.get("opInc") else None
            ldr_q3 = (q3["loans"] / q3["deposits"] * 100) if q3["deposits"] > 0 else 0.0
            cir_fy25 = (fy25["opEx"] / fy25["opInc"] * 100) if fy25["opInc"] > 0 else 0.0
            opinc_yoy_9m = ((q3["opInc"] - q3["opIncPrior"]) / abs(q3["opIncPrior"]) * 100) if q3["opIncPrior"] else 0.0
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "P/E Ratio (FY25)", "value": f"{pe_fy25:.2f}x" if pe_fy25 else "N/A", "sub": f"FY25 EPS: OMR {fy25['eps']:.3f}", "status": "neutral"},
                {"label": "Price / Book (Q3 26)", "value": f"{pb_ratio:.2f}x" if pb_ratio else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f}", "status": "neutral"},
                {"label": "Net Interest & Islamic Inc", "value": fmt_omr_large(fy25["nii"]), "sub": f"YoY Growth: {nii_yoy_fy:+.1f}%" if nii_yoy_fy is not None else f"9M 26: {fmt_omr_large(q3['nii'])}", "status": "positive"},
                {"label": "Operating Income (FY25)", "value": fmt_omr_large(fy25["opInc"]), "sub": f"YoY Growth: {opinc_yoy_fy:+.1f}%" if opinc_yoy_fy is not None else f"9M 26: {fmt_omr_large(q3['opInc'])}", "status": "positive"},
                {"label": "Net Profit (FY25)", "value": fmt_omr_large(fy25["netProfit"]), "sub": f"Net Margin: {net_margin_fy:.1f}%" if net_margin_fy is not None else f"9M 26: {fmt_omr_large(q3['netProfit'])}", "status": "positive"},
                {"label": "Net Loans & Financing", "value": fmt_omr_large(q3["loans"]), "sub": f"Q3 26 (FY25: {fmt_omr_large(fy25['loans'])})", "status": "positive"},
                {"label": "Customer Deposits", "value": fmt_omr_large(q3["deposits"]), "sub": f"Q3 26 (FY25: {fmt_omr_large(fy25['deposits'])})", "status": "positive"},
                {"label": "Total Assets (Q3 26)", "value": fmt_omr_large(q3["assets"]), "sub": f"Equity: {fmt_omr_large(q3['totalEquity'])}", "status": "positive"},
                {"label": "Loan-to-Deposit Ratio", "value": f"{ldr_q3:.1f}%", "sub": f"Cost/Inc (FY25): {cir_fy25:.1f}%", "status": "neutral"},
                {"label": "Return on Equity (FY25)", "value": f"{roe_fy25:.1f}%" if roe_fy25 else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f} • ROA: {roa_fy25:.2f}%", "status": "positive"},
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
            fy24 = ann_list[-2] if len(ann_list) >= 2 else {}
            rev_yoy_fy = ((fy25["revenue"] - fy24["revenue"]) / abs(fy24["revenue"]) * 100) if fy24.get("revenue") else None
            net_debt_q3 = q3["debt"] - q3["cash"]
            ev_omr = mcap_omr + net_debt_q3
            ev_ebitda = (ev_omr / fy25["ebitda"]) if fy25["ebitda"] > 0 else None
            ebitda_margin = (fy25["ebitda"] / fy25["revenue"] * 100) if fy25["revenue"] > 0 else 0.0
            net_margin = (fy25["netProfit"] / fy25["revenue"] * 100) if fy25["revenue"] > 0 else 0.0
            fcf_fy25 = fy25["ocf"] - fy25["capex"]
            ocf_conv = (fy25["ocf"] / fy25["ebitda"] * 100) if fy25.get("ebitda") and fy25["ebitda"] > 0 else None
            capex_pct_rev = (fy25["capex"] / fy25["revenue"] * 100) if fy25.get("revenue") and fy25["revenue"] > 0 else None
            fcf_conv = (fcf_fy25 / fy25["ebitda"] * 100) if fy25.get("ebitda") and fy25["ebitda"] > 0 else None
            rev_yoy_9m = ((q3["revenue"] - q3["revenuePrior"]) / abs(q3["revenuePrior"]) * 100) if q3["revenuePrior"] else 0.0
            ebitda_yoy_9m = ((q3["ebitda"] - q3["ebitdaPrior"]) / abs(q3["ebitdaPrior"]) * 100) if q3["ebitdaPrior"] else 0.0
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "Enterprise Value", "value": fmt_omr_large(ev_omr), "sub": f"Net Debt: {fmt_omr_large(net_debt_q3)}", "status": "neutral"},
                {"label": "P/E Ratio (FY25)", "value": f"{pe_fy25:.2f}x" if pe_fy25 else "N/A", "sub": f"FY25 EPS: OMR {fy25['eps']:.3f}", "status": "neutral"},
                {"label": "EV / EBITDA (FY25)", "value": f"{ev_ebitda:.2f}x" if ev_ebitda else "N/A", "sub": f"P/B (Q3 26): {pb_ratio:.2f}x", "status": "neutral"},
                {"label": "Revenue (FY25)", "value": fmt_omr_large(fy25["revenue"]), "sub": f"YoY Growth: {rev_yoy_fy:+.1f}%" if rev_yoy_fy is not None else f"9M 26: {fmt_omr_large(q3['revenue'])}", "status": "positive"},
                {"label": "EBITDA (FY25)", "value": fmt_omr_large(fy25["ebitda"]), "sub": f"EBITDA Margin: {ebitda_margin:.1f}%", "status": "positive"},
                {"label": "Net Profit (FY25)", "value": fmt_omr_large(fy25["netProfit"]), "sub": f"Net Margin: {net_margin:.1f}%", "status": "positive"},
                {"label": "Operating Cash Flow", "value": fmt_omr_large(fy25["ocf"]), "sub": f"OCF / EBITDA: {ocf_conv:.1f}%" if ocf_conv is not None else f"9M 26 OCF: {fmt_omr_large(q3['ocf'])}", "status": "positive"},
                {"label": "CapEx (FY25)", "value": fmt_omr_large(fy25["capex"]), "sub": f"% of Revenue: {capex_pct_rev:.1f}%" if capex_pct_rev is not None else f"9M 26 CapEx: {fmt_omr_large(q3['capex'])}", "status": "neutral"},
                {"label": "Free Cash Flow (FY25)", "value": fmt_omr_large(fcf_fy25), "sub": f"FCF / EBITDA: {fcf_conv:.1f}%" if fcf_conv is not None else f"Net Margin: {net_margin:.1f}%", "status": "positive" if fcf_fy25 >= 0 else "negative"},
                {"label": "Return on Equity (FY25)", "value": f"{roe_fy25:.1f}%" if roe_fy25 else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f}", "status": "positive"},
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
        # Automated XBRL Parsed Financials + MSX IPO Prospectus Historical Backfill
        pf = dict(rec["parsedFin"])
        prosp_cfg = IPO_PROSPECTUS_FIN.get(sym) or {}
        for py, pdata in (prosp_cfg.get("annuals") or {}).items():
            if py not in pf or pdata.get("override"):
                pf[py] = dict(pdata)

        yr_keys = [y for y in ["FY2021", "FY2022", "FY2023", "FY2024", "FY2025", "FY2026"] if y in pf and any(pf[y].get(k) is not None for k in ("revenue", "opInc", "netProfit", "assets"))]
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

        # Derive per-year DPS for fiscal year `y` from explicit DPS or dividends announced for `y`
        # (In Oman, annual dividends for FY(T) are approved at the March/April T+1 AGM and paid in T+1,
        # unless the company pays semi-annual interim + final dividends within the same year).
        def get_div_paid_dps(period_dict):
            if not period_dict:
                return None
            if period_dict.get("dps") is not None:
                return period_dict["dps"]
            dp = period_dict.get("divPaid")
            if dp and shares_out_raw > 0:
                val = dp / shares_out_raw
                if 0 < val < 5.0:
                    return round(val, 4)
            return None

        dps_by_period = {}
        for idx_y, y in enumerate(yr_keys):
            p_cur = pf[y]
            if p_cur.get("dps") is not None:
                dps_by_period[y] = p_cur["dps"]
                continue
            # Check if dividends for fiscal year y were paid in the following period (y+1 or 9M 2026)
            next_p = pf.get("Q26_CUR") if (y == "FY2025" and pf.get("Q26_CUR")) else (pf.get(yr_keys[idx_y + 1]) if idx_y + 1 < len(yr_keys) else None)
            cur_dp = get_div_paid_dps(p_cur)
            nxt_dp = get_div_paid_dps(next_p) if next_p else None
            # For FY2025, if 9M 2026 has cash dividends paid (March/April 2026 AGM payout for FY2025) and FY2025 wasn't a semi-annual payer where 9M 2026 is smaller than H1+H2
            if y == "FY2025" and nxt_dp is not None and (cur_dp is None or nxt_dp >= cur_dp * 0.75):
                dps_by_period[y] = nxt_dp
            elif cur_dp is not None:
                dps_by_period[y] = cur_dp
            else:
                dps_by_period[y] = nxt_dp

        # 9M 2026 interim row: only show a 9M 2026 DPS if an interim dividend for 2026 was explicitly set (not the FY2025 final dividend paid in March/April 2026)
        q26_dps = q_cur.get("dps")
        if q26_dps is None:
            q_dp = get_div_paid_dps(q_cur)
            # If q_dp was already attributed as the FY2025 annual dividend paid in March/April 2026, do not double-count it as a 9M 2026 interim dividend
            if q_dp is not None and dps_by_period.get("FY2025") != q_dp:
                q26_dps = q_dp

        # Dividend Yield strictly from last full audited fiscal year (never interim periods)
        dps_latest = dps_by_period.get(latest_yr_name) or 0.0
        div_yield_pct = (dps_latest / price * 100) if price > 0 and dps_latest > 0 else 0.0
        np_ann = latest_ann.get("netProfit")
        prior_ann = pf[yr_keys[-2]] if len(yr_keys) >= 2 else {}
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
            prior_opinc_v = prior_ann.get("opInc") or prior_ann.get("revenue")
            opinc_yoy = ((opinc_v - prior_opinc_v) / abs(prior_opinc_v) * 100) if (opinc_v is not None and prior_opinc_v) else None
            nii_v = latest_ann.get("nii")
            prior_nii_v = prior_ann.get("nii")
            nii_yoy = ((nii_v - prior_nii_v) / abs(prior_nii_v) * 100) if (nii_v is not None and prior_nii_v) else None
            net_margin_b = (np_ann / opinc_v * 100) if (np_ann is not None and opinc_v and opinc_v > 0) else None
            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": f"P/E Ratio ({latest_yr_name})", "value": f"{pe_val:.2f}x" if pe_val else "N/A", "sub": f"EPS: OMR {latest_ann['eps']:.3f}" if latest_ann.get("eps") is not None else "EPS: N/A", "status": "neutral"},
                {"label": "Price / Book", "value": f"{pb_val:.2f}x" if pb_val else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f}" if nav_latest else "Book Value/Sh: N/A", "status": "neutral"},
                {"label": f"Net Int & Islamic ({latest_yr_name})", "value": fmt_omr_large(nii_v), "sub": f"YoY Growth: {nii_yoy:+.1f}%" if nii_yoy is not None else f"2026 Int: {fmt_omr_large(q_cur.get('nii'))}", "status": "positive"},
                {"label": f"Operating Income ({latest_yr_name})", "value": fmt_omr_large(opinc_v), "sub": f"YoY Growth: {opinc_yoy:+.1f}%" if opinc_yoy is not None else f"2026 Int: {fmt_omr_large(q_cur.get('opInc'))}", "status": "positive"},
                {"label": f"Net Profit ({latest_yr_name})", "value": fmt_omr_large(np_ann), "sub": f"Net Margin: {net_margin_b:.1f}%" if net_margin_b is not None else f"9M 26: {fmt_omr_large(q_cur.get('netProfit'))}", "status": "positive" if (np_ann or 0) >= 0 else "negative"},
                {"label": "Net Loans & Financing", "value": fmt_omr_large(loans_v), "sub": "Latest Balance Sheet", "status": "positive"},
                {"label": "Customer Deposits", "value": fmt_omr_large(dep_v), "sub": "Latest Balance Sheet", "status": "positive"},
                {"label": "Total Assets", "value": fmt_omr_large(ast_latest), "sub": f"Equity: {fmt_omr_large(eq_latest)}", "status": "positive"},
                {"label": "Loan-to-Deposit Ratio", "value": f"{ldr_v:.1f}%" if ldr_v else "N/A", "sub": f"Paid-Up: {fmt_omr_large(paid_up_cap)}", "status": "neutral"},
                {"label": f"Return on Equity ({latest_yr_name})", "value": f"{roe_val:.1f}%" if roe_val is not None else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f}" if nav_latest else (f"ROA: {roa_val:.2f}%" if roa_val is not None else "ROA: N/A"), "status": "positive"},
                {"label": f"Cash Dividend Yield ({latest_yr_name})", "value": f"{div_yield_pct:.2f}%" if dps_latest > 0 else "N/A", "sub": f"{latest_yr_name} DPS: OMR {dps_latest:.4f}" if dps_latest > 0 else f"No {latest_yr_name} cash div", "status": "positive"},
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
                    {"period": y, "primaryMetric": pf[y].get("opInc") or pf[y].get("revenue"), "secondaryMetric": pf[y].get("nii"), "netProfit": pf[y].get("netProfit"), "dps": dps_by_period.get(y), "assets": pf[y].get("assets"), "loans": pf[y].get("loans"), "deposits": pf[y].get("deposits"), "equity": pf[y].get("equity")}
                    for y in yr_keys
                ] + ([{"period": "9M 2026", "primaryMetric": q_cur.get("opInc") or q_cur.get("revenue"), "secondaryMetric": q_cur.get("nii"), "netProfit": q_cur.get("netProfit"), "dps": q26_dps, "assets": q_cur.get("assets"), "loans": q_cur.get("loans"), "deposits": q_cur.get("deposits"), "equity": q_cur.get("equity")}] if q_cur else []),
            }
        else:
            rev_v = latest_ann.get("revenue") or latest_ann.get("opInc")
            prior_rev_v = prior_ann.get("revenue") or prior_ann.get("opInc")
            rev_yoy_fy = ((rev_v - prior_rev_v) / abs(prior_rev_v) * 100) if (rev_v is not None and prior_rev_v) else None
            ebitda_v = latest_ann.get("ebitda") or latest_ann.get("opProfit")
            # Prefer positive debt or positive cash across Q3 2026 and latest audited annual so 0.0 default in interim doesn't wipe out audited debt/cash
            q_debt = q_cur.get("debt")
            a_debt = latest_ann.get("debt")
            debt_v = q_debt if (q_debt is not None and q_debt > 0) else (a_debt if a_debt is not None else 0.0)
            q_cash = q_cur.get("cash")
            a_cash = latest_ann.get("cash")
            cash_v = q_cash if (q_cash is not None and q_cash > 0) else (a_cash if a_cash is not None else 0.0)
            net_debt = debt_v - cash_v
            ev_v = mcap_omr + net_debt if mcap_omr > 0 else None
            ev_eb = (ev_v / ebitda_v) if (ev_v and ebitda_v and ebitda_v > 0) else None
            ocf_v = latest_ann.get("ocf")
            capex_v = latest_ann.get("capex")
            fcf_v = (ocf_v - capex_v) if (ocf_v is not None and capex_v is not None) else None
            eb_margin = (ebitda_v / rev_v * 100) if (ebitda_v and rev_v and rev_v > 0) else None
            net_margin = (np_ann / rev_v * 100) if (np_ann is not None and rev_v and rev_v > 0) else None
            ocf_conv = (ocf_v / ebitda_v * 100) if (ocf_v is not None and ebitda_v and ebitda_v > 0) else None
            capex_pct_rev = (capex_v / rev_v * 100) if (capex_v is not None and rev_v and rev_v > 0) else None
            fcf_conv = (fcf_v / ebitda_v * 100) if (fcf_v is not None and ebitda_v and ebitda_v > 0) else None

            # Use 9M YoY revenue growth as fallback if prior full year revenue is not available
            q_rev_c = q_cur.get("revenue") or q_cur.get("opInc")
            q_rev_p = q_pri.get("revenue") or q_pri.get("opInc")
            rev_yoy_9m = ((q_rev_c - q_rev_p) / abs(q_rev_p) * 100) if (q_rev_c is not None and q_rev_p) else None
            rev_sub_str = f"YoY Growth: {rev_yoy_fy:+.1f}%" if rev_yoy_fy is not None else (f"9M YoY Growth: {rev_yoy_9m:+.1f}%" if rev_yoy_9m is not None else "YoY Growth: N/A")

            valuation_cards = [
                {"label": "Market Cap", "value": fmt_omr_large(mcap_omr), "sub": f"Shares: {fmt_shares(shares_out_raw)}", "status": "positive"},
                {"label": "Enterprise Value", "value": fmt_omr_large(ev_v), "sub": f"Net Debt: {fmt_omr_large(net_debt)}", "status": "neutral"},
                {"label": f"P/E Ratio ({latest_yr_name})", "value": f"{pe_val:.2f}x" if pe_val else "N/A", "sub": f"EPS: OMR {latest_ann['eps']:.3f}" if latest_ann.get("eps") is not None else "EPS: N/A", "status": "neutral"},
                {"label": f"EV / EBITDA ({latest_yr_name})", "value": f"{ev_eb:.2f}x" if ev_eb else "N/A", "sub": f"P/B: {pb_val:.2f}x" if pb_val else "P/B: N/A", "status": "neutral"},
                {"label": f"Revenue ({latest_yr_name})", "value": fmt_omr_large(rev_v), "sub": rev_sub_str, "status": "positive"},
                {"label": f"EBITDA ({latest_yr_name})", "value": fmt_omr_large(ebitda_v), "sub": f"Margin: {eb_margin:.1f}%" if eb_margin is not None else "Operating Proxy", "status": "positive"},
                {"label": f"Net Profit ({latest_yr_name})", "value": fmt_omr_large(np_ann), "sub": f"Net Margin: {net_margin:.1f}%" if net_margin is not None else f"ROE: {roe_val:.1f}%" if roe_val is not None else "Net Margin: N/A", "status": "positive" if (np_ann or 0) >= 0 else "negative"},
                {"label": f"Operating Cash Flow ({latest_yr_name})", "value": fmt_omr_large(ocf_v), "sub": f"OCF / EBITDA: {ocf_conv:.1f}%" if ocf_conv is not None else "OCF / EBITDA: N/A", "status": "positive" if (ocf_v or 0) >= 0 else "negative"},
                {"label": f"CapEx ({latest_yr_name})", "value": fmt_omr_large(capex_v), "sub": f"% of Revenue: {capex_pct_rev:.1f}%" if capex_pct_rev is not None else "% of Revenue: N/A", "status": "neutral"},
                {"label": f"Free Cash Flow ({latest_yr_name})", "value": fmt_omr_large(fcf_v), "sub": f"FCF / EBITDA: {fcf_conv:.1f}%" if fcf_conv is not None else "FCF / EBITDA: N/A", "status": "positive" if (fcf_v or 0) >= 0 else "negative"},
                {"label": f"Return on Equity ({latest_yr_name})", "value": f"{roe_val:.1f}%" if roe_val is not None else "N/A", "sub": f"Book Value/Sh: OMR {nav_latest:.3f}" if nav_latest else f"Equity: {fmt_omr_large(eq_latest)}", "status": "positive" if (roe_val or 0) >= 0 else "negative"},
                {"label": f"Cash Dividend Yield ({latest_yr_name})", "value": f"{div_yield_pct:.2f}%" if dps_latest > 0 else "N/A", "sub": f"{latest_yr_name} DPS: OMR {dps_latest:.4f}" if dps_latest > 0 else f"No {latest_yr_name} cash div", "status": "positive"},
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
                    {"period": y, "primaryMetric": pf[y].get("revenue") or pf[y].get("opInc"), "secondaryMetric": pf[y].get("ebitda") or pf[y].get("opProfit"), "netProfit": pf[y].get("netProfit"), "dps": dps_by_period.get(y), "ocf": pf[y].get("ocf"), "capex": pf[y].get("capex"), "assets": pf[y].get("assets"), "debt": pf[y].get("debt"), "equity": pf[y].get("equity")}
                    for y in yr_keys
                ] + ([{"period": "9M 2026", "primaryMetric": q_cur.get("revenue") or q_cur.get("opInc"), "secondaryMetric": q_cur.get("ebitda") or q_cur.get("opProfit"), "netProfit": q_cur.get("netProfit"), "dps": q26_dps, "ocf": q_cur.get("ocf"), "capex": q_cur.get("capex"), "assets": q_cur.get("assets"), "debt": q_cur.get("debt"), "equity": q_cur.get("equity")}] if q_cur else []),
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

        ai_biz_hl = (ai_prof.get("businessHighlights") or []) if ai_prof else []
        highlights = []
        if ai_biz_hl:
            highlights.extend(ai_biz_hl[:2])
        highlights.append(
            f"{latest_yr_name} Financial Snapshot: {'Operating Income' if is_bank else 'Revenue'} of {fmt_omr_large(latest_ann.get('opInc') if is_bank else (latest_ann.get('revenue') or latest_ann.get('opInc')))} and Net Profit of {fmt_omr_large(np_ann)} (Total Assets: {fmt_omr_large(ast_latest)})."
        )
        if not ai_biz_hl:
            highlights.insert(
                0,
                f"Market Capitalization of {fmt_omr_large(mcap_omr)} across {fmt_shares(shares_out_raw)} issued shares at OMR {price:.3f}/sh ({market_label}, ISIN {isin}).",
            )
        highlights.append(
            f"Ownership Registry (mcd.om): {participants:,} registered shareholders • Omani {omani_pct:.1f}% | GCC {gcc_pct:.1f}% | Foreign & Arab {other_non_omani:.1f}% ({len(major_holders)} disclosed 5%+ holders)."
        )
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

    # Append MSX IPO/FPO Prospectus direct PDF links if available
    prosp_filings = (IPO_PROSPECTUS_FIN.get(sym) or {}).get("prospectusFilings") or []
    existing_furls = {f.get("sourceUrl") for f in filings}
    for pf_item in prosp_filings:
        if pf_item.get("sourceUrl") and pf_item["sourceUrl"] not in existing_furls:
            filings.append(dict(pf_item))
            existing_furls.add(pf_item["sourceUrl"])

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

    # Build full multi-period financial statements & sector-adapted Key Stats payload
    statements_payload = build_statements_payload(
        sym=sym,
        sector_label=sector_label,
        is_bank=is_bank,
        flag=flag,
        parsed_fin=rec.get("parsedFin") or {},
        prosp_cfg=IPO_PROSPECTUS_FIN.get(sym) or {},
        fin_pref=fin_pref,
        shares_out_raw=shares_out_raw,
        dps_by_period=dps_by_period if not flag else None,
        q26_dps=q26_dps if not flag else None,
    )

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
        "overviewSources": overview_sources,
        "overviewGeneratedAt": overview_generated_at,
        "highlights": highlights,
        "segments": segments,
        "segmentMode": segment_mode,
        "segmentNote": segment_note,
        "series": {
            "dates": disp_dates,
            "fullDates": full_dates,
            "prices": prices,
            "volumes": vols,
            "sma50": sma50,
            "sma200": sma200,
            "benchmarkIndexed": bench_indexed,
            "benchmarkLabel": "MSX 30 Index (Indexed)",
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
        "statements": statements_payload,
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

    # Build MSX 30 Index benchmark from the top 30 liquid MSX SAOG constituents by Market Cap
    def get_mcap_est(s):
        r = records.get(s)
        if not r:
            return 0.0
        sh = parse_float(r["mcdInfo"].get("TotalNoOfShares"), 0.0)
        p = parse_float(r["quote"].get("LTP") or r["labels"].get("LTPLabel"), 0.0)
        return sh * p

    top30_syms = sorted(records.keys(), key=get_mcap_est, reverse=True)[:30]
    all_dates = set()
    msx30_hist = {}
    for s in top30_syms:
        _, fd, pr, _, _, _ = parse_hist_series(records[s]["histRaw"])
        if len(pr) >= 15:
            msx30_hist[s] = (fd, pr)
            all_dates.update(fd)
    common_dates = sorted(all_dates)
    norm_by_d = {d: [] for d in common_dates}
    for s, (fd, pr) in msx30_hist.items():
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
