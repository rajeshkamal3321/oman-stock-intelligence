#!/usr/bin/env python3
"""
MSX Company Visualizer — Official Data Pipeline Builder
Generates structured JSON files inside /data matching the static frontend schema.

Strict Official Data Sources ONLY:
  1. msx.om — Company profiles, live/latest market quotes, daily historical prices,
     audited annual & quarterly financial statement PDFs (/filings), and 2026 disclosures.
  2. mcd.om — Listed Instruments Info (total issued shares, paid-up capital, shareholder count),
     Major Investors (5%+), and Investor Ownership (Omani / GCC / Arab / Foreign).
  3. Official Corporate Websites — Board of Directors & Executive Management pages
     (with fallback to MSX FY2025 Audited Corporate Governance Report PDF where needed).
"""

import json
import math
import os
import re
from datetime import datetime, timedelta

WORKSPACE = "/Users/rajesh/Documents/Projects/Company Visualizer"
DATA_DIR = os.path.join(WORKSPACE, "data")
RAW_DIR = "/Users/rajesh/.gemini/antigravity/brain/19e417e8-4296-4db5-a420-639a61d8dd0e/scratch/raw_msx_mcd"

os.makedirs(DATA_DIR, exist_ok=True)

TICKERS = ["BKMB", "BKSB", "NBOB", "BKDB", "OQGN", "OQEP", "ABRJ", "PHPC"]

MCD_URLS = {
    "listed": "https://www.mcd.om/en/Default/Statistic/ListedInstrumentsInfo",
    "major": "https://www.mcd.om/en/Default/Statistic/MajorInvestors5OrMore",
    "nationality": "https://www.mcd.om/en/Default/Statistic/InvestorOwnership",
}

# Clean English display names for Arabic shareholder names reported verbatim on mcd.om
ARABIC_HOLDER_ENGLISH = {
    "الشركه العمانيه العالميه القابضه ش م ع م": "Oman International Holding SAOC (الشركة العمانية العالمية القابضة)",
    "سهيل سالم عبدالله المخينى بهوان": "Suhail Salim Abdullah Al Mukhaini Bahwan (سهيل سالم عبدالله المخيني بهوان)",
    "شركه ملتن للتجاره والمقاولات": "Multitech Trading & Contracting (شركة ملتن للتجارة والمقاولات)",
    "عبدالحافظ سالم رجب العجيلى": "Eng. Abdul Hafidh Salim Rajab Al Aujaili (عبدالحافظ سالم رجب العجيلي)",
    "مؤسسه قيس العمانيه": "Qais Omani Establishment (مؤسسة قيس العمانية)",
    "مؤسسة الراتب التجاريه": "Al Ratib Commercial Establishment (مؤسسة الراتب التجارية)",
}

# Verified company-specific profiles, audited financials (in exact OMR),
# and Board of Directors / Executive Management from official corporate websites & MSX filings.
COMPANY_VERIFIED = {
    "BKMB": {
        "displayName": "Bank Muscat SAOG",
        "sectorGroup": "Banking",
        "sectorLabel": "Financials • Banking",
        "website": "https://www.bankmuscat.om/",
        "hq": "Airport Heights, Seeb, Muscat, Oman",
        "description": "Bank Muscat SAOG is the flagship financial services provider in the Sultanate of Oman with over four decades of operations. Serving retail, corporate, wholesale, investment banking, treasury, asset management, and Meethaq Islamic Banking clients across a nationwide network of 200 branches and 916 self-service devices, Bank Muscat holds the largest market share in Omani banking assets, loans, and customer deposits.",
        "segments": [
            {"name": "Conventional Net Interest Income", "pct": 63.8, "color": "bg-indigo-500"},
            {"name": "Fee, Commission & FX / Other Income", "pct": 22.5, "color": "bg-cyan-400"},
            {"name": "Net Investment Income", "pct": 7.3, "color": "bg-violet-400"},
            {"name": "Meethaq Islamic Banking Net Financing Income", "pct": 6.4, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Operating Income Mix (Audited FS)",
        "dps": 0.018,
        "dpsNote": "FY2025 Cash Dividend: 18.0 baisa/sh (18% of par)",
        "car": "19.66%",
        "annuals": [
            {"year": "FY2022", "opInc": 480965000, "nii": 344855000, "netProfit": 200753000, "assets": 12775982000, "loans": 9416894000, "deposits": 8646821000, "equity": 1726804000, "totalEquity": 2232124000, "opEx": 244465000, "eps": 0.026, "dps": 0.0150},
            {"year": "FY2023", "opInc": 513271000, "nii": 374822000, "netProfit": 212446000, "assets": 13673371000, "loans": 9877110000, "deposits": 9437792000, "equity": 1849535000, "totalEquity": 2354855000, "opEx": 261051000, "eps": 0.025, "dps": 0.0155},
            {"year": "FY2024", "opInc": 543701000, "nii": 397701000, "netProfit": 225580000, "assets": 13911955000, "loans": 10237448000, "deposits": 9777068000, "equity": 1939513000, "totalEquity": 2444833000, "opEx": 273668000, "eps": 0.027, "dps": 0.0165},
            {"year": "FY2025", "opInc": 588284000, "nii": 413005000, "netProfit": 255535000, "assets": 15129096000, "loans": 10742405000, "deposits": 10430281000, "equity": 2086806000, "totalEquity": 2592126000, "opEx": 223251000, "impairment": 60969000, "eps": 0.031, "nav": 0.278, "dps": 0.0180},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "opInc": 453467000, "opIncPrior": 434898000,
            "nii": 321400000, "niiPrior": 310897000,
            "netProfit": 209183000, "netProfitPrior": 191572000,
            "assets": 16003943000, "loans": 11557227000, "deposits": 10919122000,
            "equity": 2177199000, "totalEquity": 2682519000, "nav": 0.290, "eps9m": 0.026, "dps": None,
        },
        "governance": {
            "boardSourceLabel": "Bank Muscat Official Website — Board of Directors",
            "boardSourceUrl": "https://www.bankmuscat.om/en/about/pages/bod.aspx",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "Bank Muscat Official Website — Management Team",
            "managementSourceUrl": "https://www.bankmuscat.om/en/about/pages/management.aspx",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Sheikh Khalid bin Mustahail Al Mashani", "role": "Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sheikh Ahmed bin Hamed bin Hilal Al Sadi", "role": "Deputy Chairman", "independence": "Non-Executive / Non-Independent (RCA)"},
                {"name": "Dr. Faisal bin Abdullah bin Shaban Al Farsi", "role": "Director", "independence": "Independent Director"},
                {"name": "Sheikh Said bin Mohammed Alharthy", "role": "Director", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sheikh Dr. Saud bin Mustahail Al Mashani", "role": "Director", "independence": "Independent Director"},
                {"name": "Mr. Khalid Nasser Al Shamsi", "role": "Director", "independence": "Independent Director"},
                {"name": "Dr. Saif bin Salim Al Harthi", "role": "Director", "independence": "Independent Director"},
                {"name": "Mr. Saud bin Nasser bin Rashid Al Shukaili", "role": "Director", "independence": "Independent Director"},
                {"name": "Mr. Badar Al Shanfari", "role": "Director", "independence": "Non-Executive Director"},
            ],
            "management": [
                {"name": "Sheikh Waleed Khamis Al Hashar", "title": "Chief Executive Officer"},
                {"name": "Ahmed Faqir Al Bulushi", "title": "Deputy CEO – Banking"},
                {"name": "Sheikha Yousuf Al Farsi", "title": "Deputy CEO – Operations"},
                {"name": "T. Ganesh", "title": "Deputy CEO – Finance and Investment Banking"},
                {"name": "Antonio Gamez Munoz", "title": "Chief Risk Officer"},
                {"name": "Damian John O'Riordan", "title": "Chief Internal Auditor"},
                {"name": "Ilham Murtadha Al Hamaid", "title": "General Manager – Corporate Banking"},
                {"name": "Abdulnasir Noori Al Raisi", "title": "General Manager – Personal Banking"},
                {"name": "Khaled Alkayd", "title": "General Manager – Islamic Banking (Meethaq)"},
                {"name": "Ahmed Omar Al Ojaili", "title": "General Manager – Technology"},
                {"name": "Salim Mohammed Al Kaabi", "title": "General Manager – Credit & Legal"},
                {"name": "Said Salim Al Aufi", "title": "General Manager – Human Resources & Administration"},
            ],
        },
    },
    "BKSB": {
        "displayName": "Sohar International Bank SAOG",
        "sectorGroup": "Banking",
        "sectorLabel": "Financials • Banking",
        "website": "https://www.sib.om/",
        "hq": "Shatti Al Qurum, Muscat, Oman",
        "description": "Sohar International Bank SAOG is Oman's fastest-growing commercial bank following its 2023 merger with HSBC Bank Oman and regional expansion into Saudi Arabia (KSA branch). Surpassing OMR 10.2 billion in total assets by Q3 2026, the bank operates across Retail, Wholesale & Government Banking, Wealth Management, Treasury, and Sohar Islamic.",
        "segments": [
            {"name": "Conventional Net Interest Income", "pct": 60.5, "color": "bg-indigo-500"},
            {"name": "Fees, FX, Investment & Other Income", "pct": 33.7, "color": "bg-cyan-400"},
            {"name": "Sohar Islamic Net Financing Income", "pct": 5.8, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Operating Income Mix (Audited FS)",
        "dps": 0.004,
        "dpsNote": "Cash Dividends Paid 9M 2026: OMR 26.47M (post capital expansion)",
        "car": "Reported in FY25 Annual FS",
        "annuals": [
            {"year": "FY2022", "opInc": 132846000, "nii": 103683000, "netProfit": 34878000, "assets": 4131001000, "loans": 2924294000, "deposits": 2560098000, "equity": 565498000, "totalEquity": 665498000, "opEx": 94830000, "eps": 0.006, "dps": 0.0045},
            {"year": "FY2023", "opInc": 163407000, "nii": 125879000, "netProfit": 70335000, "assets": 6688720000, "loans": 3921192000, "deposits": 5103045000, "equity": 702074000, "totalEquity": 802074000, "opEx": 181997000, "eps": 0.013, "dps": 0.0055},
            {"year": "FY2024", "opInc": 244946000, "nii": 180993000, "netProfit": 100181000, "assets": 7360751000, "loans": 4269808000, "deposits": 5777040000, "equity": 898306000, "totalEquity": 898306000, "opEx": 136269000, "eps": 0.015, "dps": 0.0080},
            {"year": "FY2025", "opInc": 252477000, "nii": 167415000, "netProfit": 100467000, "assets": 9128796000, "loans": 5763904000, "deposits": 6826799000, "equity": 953586000, "totalEquity": 1153586000, "opEx": 137764000, "eps": 0.015, "nav": 0.144, "dps": 0.0040},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "opInc": 224342000, "opIncPrior": 180798000,
            "nii": 153259000, "niiPrior": 122698000,
            "netProfit": 77637000, "netProfitPrior": 70718000,
            "assets": 10207762000, "loans": 7189399000, "deposits": 7648361000,
            "equity": 1126011000, "totalEquity": 1351961000, "nav": 0.147, "eps9m": 0.010, "dps": None,
        },
        "governance": {
            "boardSourceLabel": "Sohar International Official Website — Board of Directors",
            "boardSourceUrl": "https://www.sib.om/about-us/board-of-directors",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "Sohar International Official Website — Management Team",
            "managementSourceUrl": "https://www.sib.om/about-us/management-team",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Said Mohamed Al-Aufi", "role": "Chairman", "independence": "Independent Director"},
                {"name": "Tareq Mohamed Al Mugheiry", "role": "Deputy Chairman", "independence": "Independent Director"},
                {"name": "Salim Mohamed Masaud Al Mashaikhi", "role": "Director", "independence": "Independent Director"},
                {"name": "Said Ahmed Safrar", "role": "Director", "independence": "Independent Director"},
                {"name": "Sheikh Aimen Ahmed Sultan Al Hosni", "role": "Director", "independence": "Independent Director"},
                {"name": "Ghusen Hilal Abdulla Al Abri", "role": "Director", "independence": "Non-Independent (Royal Court Affairs)"},
                {"name": "Ahmed Dawood Al Busaidi", "role": "Director", "independence": "Independent Director"},
            ],
            "management": [
                {"name": "Abdulwahid Mohamed Al Murshidi", "title": "Chief Executive Officer"},
                {"name": "Craig Barrington Bell", "title": "Chief Financial Officer"},
                {"name": "Hamood Abdullah Al Sawai", "title": "Chief Operating Officer"},
                {"name": "Majid Nasser Al Busaidi", "title": "Chief Risk Officer"},
                {"name": "Sajeel Bashiruddin", "title": "Chief Technology & Digital Officer"},
                {"name": "Abdul Qadir Al Sumali", "title": "Chief Retail & Wealth Management Officer"},
                {"name": "Khalil Salim Al Hedaifi", "title": "Acting Chief Wholesale Banking Officer"},
                {"name": "Fahad Al Zadjali", "title": "Chief Islamic Banking Officer"},
                {"name": "Khalid Al Guthami", "title": "Country CEO – KSA Branch"},
                {"name": "Mahira Saleh Al Raisi", "title": "Chief People Officer"},
                {"name": "Elsamawal Abdulhadi Idris Mohammed", "title": "Chief Legal & Governance Officer"},
                {"name": "Hamood Khalfan Badar Al Aisri", "title": "Chief Internal Auditor"},
            ],
        },
    },
    "NBOB": {
        "displayName": "National Bank of Oman SAOG",
        "sectorGroup": "Banking",
        "sectorLabel": "Financials • Banking",
        "website": "https://www.nbo.om/",
        "hq": "Ruwi, Muscat, Sultanate of Oman",
        "description": "Founded in 1973 as Oman's first local commercial bank, National Bank of Oman SAOG (NBO) offers comprehensive retail, corporate, investment, government, and Muzn Islamic banking services across Oman and the UAE. Backed by strategic anchor shareholder Commercial Bank of Qatar (34.9%), NBO maintains a 20.4% Total Capital Adequacy Ratio.",
        "segments": [
            {"name": "Conventional Net Interest Income", "pct": 63.4, "color": "bg-indigo-500"},
            {"name": "Net Commission & Fee Income", "pct": 17.8, "color": "bg-cyan-400"},
            {"name": "FX, Investment & Other Operating Income", "pct": 13.7, "color": "bg-violet-400"},
            {"name": "Muzn Islamic Banking Net Financing Income", "pct": 5.1, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Operating Income Mix (Audited FS)",
        "dps": 0.0105,
        "dpsNote": "Cash Dividends Paid 9M 2026: OMR 17.07M (10.5 baisa/sh)",
        "car": "20.4% (CET1: 11.7%)",
        "annuals": [
            {"year": "FY2022", "opInc": 138237000, "nii": 103539000, "netProfit": 48211000, "assets": 4294080000, "loans": 3353699000, "deposits": 3047422000, "equity": 473701000, "totalEquity": 640834000, "opEx": 80765000, "eps": 0.024, "dps": 0.0074},
            {"year": "FY2023", "opInc": 145867000, "nii": 110542000, "netProfit": 58029000, "assets": 4822006000, "loans": 3495926000, "deposits": 3573033000, "equity": 507410000, "totalEquity": 674543000, "opEx": 77607000, "eps": 0.028, "dps": 0.0085},
            {"year": "FY2024", "opInc": 151326000, "nii": 108783000, "netProfit": 63063000, "assets": 5242545000, "loans": 3926147000, "deposits": 4128523000, "equity": 543124000, "totalEquity": 768161000, "opEx": 77119000, "eps": 0.031, "dps": 0.0094},
            {"year": "FY2025", "opInc": 163494000, "nii": 111939000, "netProfit": 70207000, "assets": 5591224000, "loans": 4150354000, "deposits": 4009117000, "equity": 601719000, "totalEquity": 1000006000, "opEx": 80931000, "eps": 0.033, "nav": 0.370, "dps": 0.0105},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "opInc": 134789000, "opIncPrior": 121109000,
            "nii": 90679000, "niiPrior": 82053000,
            "netProfit": 57575000, "netProfitPrior": 51768000,
            "assets": 6072132000, "loans": 4516503000, "deposits": 4458649000,
            "equity": 644984000, "totalEquity": 927771000, "nav": 0.397, "eps9m": 0.035, "dps": None,
        },
        "governance": {
            "boardSourceLabel": "MSX Official Profile & FY2025 Audited Corporate Governance Report (nbo.om unreachable via automated fetch)",
            "boardSourceUrl": "https://www.msx.om/snapshot.aspx?s=NBOB",
            "boardDisclosedOnWebsite": False,
            "boardFallbackUrl": "https://www.msx.om/MSMDOCS/FinancialReports/NBOB_2025_Yearly%20(Audited)_e.zip",
            "managementSourceLabel": "NBOB FY2025 Audited Corporate Governance Report (msx.om)",
            "managementSourceUrl": "https://www.msx.om/MSMDOCS/FinancialReports/NBOB_2025_Yearly%20(Audited)_e.zip",
            "managementDisclosedOnWebsite": False,
            "board": [
                {"name": "Ms. Amal Suhail Bahwan", "role": "Chairperson", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sheikh Abdulla Bin Ali Bin Jabor Al Thani", "role": "Deputy Chairperson", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Mohammed Ismail Mandani Al Emadi", "role": "Director", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Fahad Abdulrahman Badar", "role": "Director", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Stephen Moss", "role": "Director", "independence": "Non-Executive / Non-Independent"},
                {"name": "Brigadier Jamal Said Mohamed Al Tai", "role": "Director", "independence": "Independent Director"},
                {"name": "Mr. Nabil Hamad Al Mahrouqi", "role": "Director", "independence": "Independent Director"},
                {"name": "H.E. Faisal Abdullah Said Al Rawas", "role": "Director", "independence": "Independent Director (2026 MSX)"},
                {"name": "Dr. Hisham Mohamed Aqeel Ba Omar", "role": "Director", "independence": "Independent Director (2026 MSX)"},
                {"name": "Mr. Khalid Abdullah Salim Al Eisri", "role": "Director", "independence": "Independent Director (2026 MSX)"},
                {"name": "Al Sayyid Shabib Abdullah Mohamed Al Busaidi", "role": "Director", "independence": "Non-Executive Director (2026 MSX)"},
            ],
            "management": [
                {"name": "Abdullah Zahran Al Hinai", "title": "Chief Executive Officer"},
                {"name": "Srinivasaraghava Varadachari Giridhar", "title": "General Manager – Chief Financial Officer"},
                {"name": "Sulaiman Said Al Lamki", "title": "General Manager – Chief Risk Officer"},
                {"name": "Faisal Hamed Al Wahaibi", "title": "General Manager – Chief Retail & Digital Banking Officer"},
                {"name": "Pullattu Radhakrishnan Anil Kumar", "title": "General Manager – Chief Corporate Banking Officer"},
                {"name": "Hassan Abdul Amir Shaban", "title": "General Manager – Chief Government & Alliance Banking Officer"},
                {"name": "Dr. Ali Mousa Al Abri", "title": "General Manager – Chief Human Resources Officer"},
                {"name": "Salah Abdullah Al Sharji", "title": "General Manager – Chief Internal Auditor"},
            ],
        },
    },
    "BKDB": {
        "displayName": "Bank Dhofar SAOG",
        "sectorGroup": "Banking",
        "sectorLabel": "Financials • Banking",
        "website": "https://www.bankdhofar.com/",
        "hq": "Central Business District, Ruwi, Muscat, Oman",
        "description": "Established in 1990, Bank Dhofar SAOG is one of Oman's leading financial institutions with over OMR 6.3 billion in total assets as of Q3 2026. Operating conventional retail, wholesale, SME (Numo), and wealth franchises alongside Dhofar Islamic—one of Oman's largest Islamic banking windows—the bank posted 17.1% net profit growth in FY2025.",
        "segments": [
            {"name": "Conventional Net Interest Income", "pct": 56.0, "color": "bg-indigo-500"},
            {"name": "Net Commission & Fee Income", "pct": 21.1, "color": "bg-cyan-400"},
            {"name": "Dhofar Islamic Net Financing Income", "pct": 13.5, "color": "bg-emerald-400"},
            {"name": "Investment, FX & Other Income", "pct": 9.4, "color": "bg-amber-400"},
        ],
        "segmentNote": "FY2025 Operating Income Mix (Audited FS)",
        "dps": 0.0075,
        "dpsNote": "FY2025 Cash Dividend: 7.5 baisa/sh (7.5% of par)",
        "car": "NPL Ratio: 4.80% (FY2025)",
        "annuals": [
            {"year": "FY2022", "opInc": 143152000, "nii": 122416000, "netProfit": 34173000, "assets": 4317332000, "loans": 3430486000, "deposits": 2891819000, "equity": 560077000, "totalEquity": 717077000, "opEx": 102948000, "eps": 0.008, "dps": 0.0050},
            {"year": "FY2023", "opInc": 144083000, "nii": 110821000, "netProfit": 38758000, "assets": 4685797000, "loans": 3765584000, "deposits": 3299223000, "equity": 577454000, "totalEquity": 732954000, "opEx": 100308000, "eps": 0.009, "dps": 0.00775},
            {"year": "FY2024", "opInc": 152842000, "nii": 114169000, "netProfit": 43609000, "assets": 5085047000, "loans": 3933615000, "deposits": 3762861000, "equity": 584931000, "totalEquity": 740431000, "opEx": 102655000, "eps": 0.011, "dps": 0.00655},
            {"year": "FY2025", "opInc": 169784000, "nii": 117935000, "netProfit": 51051000, "assets": 5390751000, "loans": 4176548000, "deposits": 4116829000, "equity": 614144000, "totalEquity": 769644000, "opEx": 111364000, "eps": 0.013, "nav": 0.202, "dps": 0.0075},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "opInc": 142836000, "opIncPrior": 126404000,
            "nii": 95869000, "niiPrior": 89091000,
            "netProfit": 38851000, "netProfitPrior": 36020000,
            "assets": 6329566000, "loans": 4686820000, "deposits": 4717916000,
            "equity": 639536000, "totalEquity": 795036000, "nav": 0.210, "eps9m": 0.010, "dps": None,
        },
        "governance": {
            "boardSourceLabel": "Bank Dhofar Official Website — Board of Directors",
            "boardSourceUrl": "https://www.bankdhofar.com/about-us/overview/board-of-directors/",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "Bank Dhofar Official Website — Management Team",
            "managementSourceUrl": "https://www.bankdhofar.com/about-us/overview/management-team/",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Eng. Abdul Hafidh Salim Rajab Al Ojaili", "role": "Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Ahmed Said Mohammed Al Mahrezi", "role": "Deputy Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Mohammed Yousuf Alawi Al Ibrahim", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Mr. Tariq Abdul Hafidh Al Aujaili", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sheikh Khalid Said Salim Al Wahaibi", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Sheikh Tariq Salim Mustahil Al Mashani", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Mr. Ahmed Sultan Rashid Al Yaqoubi", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Mr. Zahir Marhoon Mohammed Al Abri", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mr. Khalid Sulaiman Salim Al Kamyani", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
            ],
            "management": [
                {"name": "Karumathil Gopakumar", "title": "Acting Chief Executive Officer"},
                {"name": "Vikesh Mirani", "title": "Chief Financial Officer"},
                {"name": "Leen Kumar Sugumaran", "title": "Chief Risk Officer"},
                {"name": "Mallikarjuna Korisepati", "title": "Chief Wholesale Banking Officer"},
                {"name": "Ahmed Said Salim Al Ibrahim", "title": "Chief Retail & Government Banking Officer"},
                {"name": "Amor Said Mohamed Al Amri", "title": "Chief Islamic Banking Officer"},
                {"name": "Amjad Iqbal Hassan Al Lawati", "title": "Chief Corporate Services Officer"},
                {"name": "Tariq Saleh Mohamed Taha", "title": "Chief Information Officer"},
                {"name": "Dr. Khalid Salim Ali Al Hamadani", "title": "Chief Human Resources Officer"},
                {"name": "Ali Khamis Hussain Al Zadjali", "title": "Chief Compliance Officer"},
                {"name": "Ezideen Juma Mohamed Al Mazroui", "title": "Acting Chief Internal Auditor"},
            ],
        },
    },
    "OQGN": {
        "displayName": "OQ Gas Networks SAOG",
        "sectorGroup": "Energy & Utilities",
        "sectorLabel": "Energy & Utilities • Gas Transmission",
        "website": "https://oqgn.om/home",
        "hq": "Al Khuwair, Muscat Governorate, Oman",
        "description": "OQ Gas Networks SAOG (OQGN) is the exclusive owner and operator of Oman's natural gas transportation network (NGTN), managing over 4,298 km of high-pressure pipelines delivering 42.29 BCM of gas in FY2025 with 100% asset availability and zero interruptions. Operating under a 50-year regulated concession framework (RAB-based returns) with OQ SAOC holding a 51% majority stake.",
        "segments": [
            {"name": "Regulated Gas Transportation & Tariff Revenue", "pct": 53.0, "color": "bg-indigo-500"},
            {"name": "Concession Construction Revenue (IFRIC 12)", "pct": 33.0, "color": "bg-cyan-400"},
            {"name": "Gas Supply, Interconnection & Other Services", "pct": 14.0, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Revenue Composition (Audited FS)",
        "dps": 0.0105,
        "dpsNote": "FY2025 Cash Dividends Paid: OMR 45.56M (H2 approved 5.6 baisa/sh)",
        "annuals": [
            {"year": "FY2022", "revenue": 149816076, "grossProfit": 77419186, "opProfit": 70466976, "ebitda": 71336770, "netProfit": 45596224, "ocf": 109706204, "capex": 15249630, "assets": 1121536649, "equity": 608592564, "debt": 388395485, "cash": 67377590, "eps": 0.011, "dps": None},
            {"year": "FY2023", "revenue": 161643345, "grossProfit": 83758693, "opProfit": 73794445, "ebitda": 74807616, "netProfit": 55511788, "ocf": 90655392, "capex": 53865988, "assets": 1101684159, "equity": 646980227, "debt": 329468355, "cash": 23770963, "eps": 0.013, "dps": 0.0076},
            {"year": "FY2024", "revenue": 146861867, "grossProfit": 88273586, "opProfit": 80842344, "ebitda": 81827815, "netProfit": 47795425, "ocf": 71030046, "capex": 34575053, "assets": 1110828122, "equity": 625829861, "debt": 354937734, "cash": 15816311, "eps": 0.01104, "dps": 0.0103},
            {"year": "FY2025", "revenue": 206996690, "grossProfit": 94617302, "opProfit": 81477190, "ebitda": 82407261, "netProfit": 51243854, "ocf": 137915615, "capex": 110915537, "assets": 1224571918, "equity": 631178778, "debt": 381214462, "cash": 23480811, "eps": 0.01184, "nav": 0.146, "dps": 0.0105},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "revenue": 140525981, "revenuePrior": 146526244,
            "ebitda": 65246410, "ebitdaPrior": 62201890,
            "netProfit": 39742554, "netProfitPrior": 39130576,
            "ocf": 42001916, "capex": 71000052,
            "assets": 1294316804, "equity": 646983857, "debt": 442840018, "cash": 27121364, "nav": 0.149, "eps9m": 0.009, "dps": 0.0056,
        },
        "governance": {
            "boardSourceLabel": "OQ Gas Networks Official Website — Board & Leadership",
            "boardSourceUrl": "https://oqgn.om/leadership",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "OQ Gas Networks Official Website — Board & Leadership",
            "managementSourceUrl": "https://oqgn.om/leadership",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Abdulwahhab Al Hinai", "role": "Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Ayad Al Balushi", "role": "Deputy Chairman", "independence": "Non-Executive Director"},
                {"name": "Fathi Al Balushi", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Raiya Al Salmi", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sultan Al Burtmani", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Ben De Waele", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Irfan Mian", "role": "Board Member", "independence": "Independent Director"},
            ],
            "management": [
                {"name": "Mansoor Ali Al Abdali", "title": "Chief Executive Officer"},
                {"name": "Musaab Bingaradi", "title": "Chief Finance Officer"},
                {"name": "Khalifa Al Makhmari", "title": "Chief Operating Officer"},
                {"name": "Saif Al Hosni", "title": "Chief Business Development & Commercial Officer"},
                {"name": "Faisal Al Mamari", "title": "Chief People, Technology & Culture Officer"},
                {"name": "Haitham Al Lawati", "title": "General Counsel"},
                {"name": "Ahmed Al Abdali", "title": "Head of Internal Audit"},
            ],
        },
    },
    "OQEP": {
        "displayName": "OQ Exploration & Production SAOG",
        "sectorGroup": "Energy & Utilities",
        "sectorLabel": "Energy & Utilities • Upstream Oil & Gas",
        "website": "https://oqep.om/",
        "hq": "Knowledge Oasis Muscat (KOM), Al Seeb, Oman",
        "description": "OQ Exploration & Production SAOG (OQEP) is Oman's largest pure-play upstream oil and gas exploration and production company, listed on MSX in October 2024 in the Sultanate's largest-ever IPO. Producing ~249,000 boepd across operated flagship Block 60 (Bisat) and non-operated Tier-1 Oman blocks with industry-leading operating costs below US$10/boe and 50.6% ROCE.",
        "segments": [
            {"name": "Crude Oil & Condensate Sales", "pct": 74.0, "color": "bg-indigo-500"},
            {"name": "Natural Gas & Gas Processing (Block 60 / Block 10 / Block 61)", "pct": 22.0, "color": "bg-cyan-400"},
            {"name": "Upstream Joint Ventures & Exploration Assets", "pct": 4.0, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Hydrocarbon Production & Revenue Mix (MD&A)",
        "dps": 0.0344,
        "dpsNote": "FY2025 Cash Dividends Disbursed: OMR 274.88M (~34.4 baisa/sh)",
        "annuals": [
            {"year": "FY2022", "revenue": 1266290000, "opProfit": 628454000, "ebitda": 1025474000, "netProfit": 617330000, "ocf": 1129691000, "capex": 362225000, "assets": 1960658000, "equity": 1275059000, "debt": 206664000, "cash": 158501000, "eps": 0.077, "dps": None},
            {"year": "FY2023", "revenue": 1085793000, "opProfit": 427198000, "ebitda": 790481000, "netProfit": 627010000, "ocf": 806647000, "capex": 265011000, "assets": 2046710000, "equity": 1371058000, "debt": 244212000, "cash": 256843000, "eps": 0.078, "dps": None},
            {"year": "FY2024", "revenue": 1264265000, "opProfit": 716104000, "ebitda": 994017000, "netProfit": 326563000, "ocf": 496237000, "capex": 245633000, "assets": 1559698000, "equity": 912380000, "debt": 484807000, "cash": 160154000, "eps": 0.041, "dps": 0.0216},
            {"year": "FY2025", "revenue": 1179466000, "opProfit": 643534000, "ebitda": 932179000, "netProfit": 278036000, "ocf": 522540000, "capex": 257541000, "assets": 1540408000, "equity": 905317000, "debt": 382610000, "cash": 159836000, "eps": 0.035, "nav": 0.113, "dps": 0.0344},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "revenue": 1033893000, "revenuePrior": 923155000,
            "ebitda": 827114000, "ebitdaPrior": 737691000,
            "netProfit": 297784000, "netProfitPrior": 236869000,
            "ocf": 436241000, "capex": 200867000,
            "assets": 1608836000, "equity": 990297000, "debt": 382908000, "cash": 213316000, "nav": 0.124, "eps": 0.037, "eps9m": 0.037, "dps": 0.0240,
        },
        "governance": {
            "boardSourceLabel": "OQEP Official Website — Board of Directors",
            "boardSourceUrl": "https://oqep.om/bod",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "OQEP Official Website — Executive Leadership",
            "managementSourceUrl": "https://oqep.om/leadership",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Ashraf Al Mamari", "role": "Chairman", "independence": "Non-Executive / Non-Independent (OQ Group CEO)"},
                {"name": "Abdulwahhab Al Hinai", "role": "Deputy Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Aflah Al Lawati", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Dr. Sulaiman Al Toubi", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Alwaleed Al Shukaili", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Intisar Al Kindi", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Khalid Al Kamyani", "role": "Board Member", "independence": "Independent Director"},
            ],
            "management": [
                {"name": "Mahmoud Al Hashmi", "title": "Chief Executive Officer"},
                {"name": "Khalid Al Qassabi", "title": "Chief Financial Officer"},
                {"name": "Jamal Al Naamani", "title": "Chief Operating Officer"},
                {"name": "Dr. Anwar Al Kharusi", "title": "Chief Executive – Commercial & Acting General Counsel"},
                {"name": "Said Al Hashmi", "title": "Chief Executive – People, Technology & Culture"},
            ],
        },
    },
    "ABRJ": {
        "displayName": "Abraj Energy Services SAOG",
        "sectorGroup": "Energy & Utilities",
        "sectorLabel": "Energy & Utilities • Oilfield & Drilling Services",
        "website": "https://abrajenergy.com/",
        "hq": "Al Mawaleh, Muscat Governorate, Oman",
        "description": "Abraj Energy Services SAOG is Oman's leading onshore drilling and well-services provider with the highest market share in domestic drilling operations and a growing GCC presence in Kuwait. Operating 21+ drilling rigs, workover units, cementing, hydraulic fracturing, and coiled tubing fleets for PDO, OXY, BP, and OQEP, Abraj drilled 357 wells (878+ km depth) in FY2025.",
        "segments": [
            {"name": "Drilling & Workover Services (Oman & Kuwait)", "pct": 76.0, "color": "bg-indigo-500"},
            {"name": "Well Services (Fracturing, Cementing & Coiled Tubing)", "pct": 24.0, "color": "bg-cyan-400"},
        ],
        "segmentNote": "FY2025 Operating Segments (MD&A)",
        "dps": 0.0169,
        "dpsNote": "Cash Dividends Paid 9M 2026: OMR 13.00M (16.9 baisa/sh)",
        "annuals": [
            {"year": "FY2022", "revenue": 137040000, "grossProfit": 33408000, "opProfit": 25187000, "ebitda": 47253000, "netProfit": 16519000, "ocf": 29864000, "capex": 11460000, "assets": 282437000, "equity": 144621000, "debt": 95794000, "cash": 4120000, "eps": 0.021, "dps": 0.0200},
            {"year": "FY2023", "revenue": 144346000, "grossProfit": 32916000, "opProfit": 24920000, "ebitda": 46987000, "netProfit": 16561000, "ocf": 32570000, "capex": 18264000, "assets": 276471000, "equity": 145778000, "debt": 85292000, "cash": 8071000, "eps": 0.022, "dps": 0.0212},
            {"year": "FY2024", "revenue": 151646000, "grossProfit": 37980000, "opProfit": 28519000, "ebitda": 51429000, "netProfit": 16870000, "ocf": 38119000, "capex": 27902000, "assets": 283342000, "equity": 146320000, "debt": 90818000, "cash": 5473000, "eps": 0.022, "dps": 0.0225},
            {"year": "FY2025", "revenue": 147188000, "grossProfit": 36160000, "opProfit": 27178000, "ebitda": 49354000, "netProfit": 17670000, "ocf": 38253000, "capex": 26424000, "assets": 284690000, "equity": 146169000, "debt": 99053000, "cash": 7050000, "eps": 0.023, "nav": 0.190, "dps": 0.0169},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "revenue": 117355000, "revenuePrior": 108830000,
            "ebitda": 37965000, "ebitdaPrior": 37351000,
            "netProfit": 14603000, "netProfitPrior": 13965000,
            "ocf": 37564000, "capex": 49133000,
            "assets": 324958000, "equity": 148353000, "debt": 124625000, "cash": 5956000, "nav": 0.193, "eps9m": 0.019, "dps": None,
        },
        "governance": {
            "boardSourceLabel": "Abraj Energy Services Official Website — Board of Directors",
            "boardSourceUrl": "https://abrajenergy.com/board",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "Abraj Energy Services Official Website — Leadership Team",
            "managementSourceUrl": "https://abrajenergy.com/leadership",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Tahir Al Amri", "role": "Chairman", "independence": "Independent Director"},
                {"name": "Ali Al Lawati", "role": "Deputy Chairman", "independence": "Non-Executive / Non-Independent"},
                {"name": "Mahmoud Al Hashmi", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Taghreed Al Lawati", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Azzan Al Abdullatif", "role": "Board Member", "independence": "Non-Executive / Non-Independent"},
                {"name": "Sadhak Bindal", "role": "Board Member", "independence": "Independent Director"},
                {"name": "Abdullah Al Mamari", "role": "Board Member", "independence": "Independent Director"},
            ],
            "management": [
                {"name": "Saif Al Hamhami", "title": "Chief Executive Officer"},
                {"name": "Hubert Lafeuille", "title": "Chief Financial Officer"},
                {"name": "Zahran Al Kindi", "title": "Chief Operating Officer"},
                {"name": "Hilal Al Siyabi", "title": "Chief People Officer"},
                {"name": "Salah Al Harthy", "title": "Chief Corporate Services Officer"},
                {"name": "Hood Al Brashdi", "title": "Acting Director of Business Development"},
                {"name": "Dr. Abdul Raouf Al Tobi", "title": "General Counsel"},
            ],
        },
    },
    "PHPC": {
        "displayName": "Phoenix Power Company SAOG",
        "sectorGroup": "Energy & Utilities",
        "sectorLabel": "Energy & Utilities • Power Generation (IPP)",
        "website": "https://www.phoenixpoweroman.com/",
        "hq": "Qurum, Muscat, Sultanate of Oman",
        "description": "Phoenix Power Company SAOG owns and operates the 2,000 MW Sur Independent Power Plant (Sur IPP)—the largest single power plant in Oman—under a 15-year Power Purchase Agreement (PPA) with Nama Power & Water Procurement Company (PWP). Achieving 99.91% commercial availability in FY2025, Phoenix Power delivers predictable contracted cash flows and rapid debt deleveraging.",
        "segments": [
            {"name": "Fuel Charge & Energy Output Revenue (Pass-Through)", "pct": 54.0, "color": "bg-indigo-500"},
            {"name": "Contracted Capacity Charge Revenue (PPA)", "pct": 46.0, "color": "bg-emerald-400"},
        ],
        "segmentNote": "FY2025 Sur IPP PPA Revenue Structure (MD&A)",
        "dps": 0.0055,
        "dpsNote": "FY2025 Ordinary Cash Dividend: 5.50 baisa/sh (OMR 8.05M)",
        "annuals": [
            {"year": "FY2022", "revenue": 149159000, "grossProfit": 37957000, "opProfit": 36701000, "ebitda": 52819000, "netProfit": 20362000, "ocf": 47892000, "capex": 14000, "assets": 541137000, "equity": 237294000, "debt": 217943000, "cash": 20259000, "eps": 0.014, "dps": 0.0041},
            {"year": "FY2023", "revenue": 158490000, "grossProfit": 38674000, "opProfit": 37208000, "ebitda": 53347000, "netProfit": 21923000, "ocf": 41811000, "capex": 72000, "assets": 523656000, "equity": 251369000, "debt": 184302000, "cash": 20006000, "eps": 0.015, "dps": 0.0053},
            {"year": "FY2024", "revenue": 167786000, "grossProfit": 38294000, "opProfit": 36751000, "ebitda": 52900000, "netProfit": 22999000, "ocf": 44782000, "capex": 0, "assets": 508553000, "equity": 266467000, "debt": 148710000, "cash": 21263000, "eps": 0.016, "dps": 0.00525},
            {"year": "FY2025", "revenue": 174061000, "grossProfit": 39484000, "opProfit": 38503000, "ebitda": 54652000, "netProfit": 26022000, "ocf": 42076000, "capex": 0, "assets": 488936000, "equity": 284606000, "debt": 112542000, "cash": 18729000, "eps": 0.018, "nav": 0.195, "dps": 0.0055},
        ],
        "q3_2026": {
            "period": "9M 2026 (Ended 30 Sep 2026)",
            "revenue": 133675000, "revenuePrior": 143849000,
            "ebitda": 52260000, "ebitdaPrior": 50072000,
            "netProfit": 29931000, "netProfitPrior": 28363000,
            "ocf": 40668000, "capex": 0,
            "assets": 549561000, "equity": 326297000, "debt": 130838000, "cash": 73643000, "nav": 0.223, "eps9m": 0.020, "dps": 0.0020,
        },
        "governance": {
            "boardSourceLabel": "Phoenix Power Official Website — Board of Directors",
            "boardSourceUrl": "https://www.phoenixpoweroman.com/board-of-directors",
            "boardDisclosedOnWebsite": True,
            "managementSourceLabel": "Phoenix Power Official Website — Senior Management",
            "managementSourceUrl": "https://www.phoenixpoweroman.com/senior-management",
            "managementDisclosedOnWebsite": True,
            "board": [
                {"name": "Khalid Jolo", "role": "Chairman", "independence": "Independent Director"},
                {"name": "Adrian Röthlisberger", "role": "Deputy Chairman", "independence": "Non-Executive Director"},
                {"name": "Abdelkariem Fatih", "role": "Director", "independence": "Non-Executive Director"},
                {"name": "Sultan Al Tai", "role": "Director", "independence": "Independent Director"},
                {"name": "Hitoshi Nakahara", "role": "Director", "independence": "Non-Executive Director"},
                {"name": "Naohiro Aoki", "role": "Director", "independence": "Non-Executive Director"},
                {"name": "Ahmed Mahmood AbdulAti Ibrahim", "role": "Director", "independence": "Non-Executive Director"},
            ],
            "management": [
                {"name": "Mohammed Said Al Shuaili", "title": "Chief Executive Officer"},
                {"name": "Ahmed Zahir Hamad Al Abri", "title": "Chief Financial Officer"},
                {"name": "Khalid Al Maawali", "title": "Plant General Manager (POMCo)"},
                {"name": "Saif Al Hatmi", "title": "Engineering & Maintenance Manager (POMCo)"},
                {"name": "Issa Al Julandani", "title": "Operation Manager (POMCo)"},
                {"name": "Abdulaziz Al Saadi", "title": "HSSE Manager"},
                {"name": "Maitham Mohammed Mohsin Al Talib", "title": "Head of Internal Audit"},
            ],
        },
    },
}


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
    return f"{int(round(n)):,}"


def parse_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return default


def parse_hist_daily(ticker):
    raw = json.load(open(f"{RAW_DIR}/hist_{ticker}.json"))
    by_day = {}
    for pt in raw:
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
        # Keep the latest entry for each trading day
        by_day[day_str] = (round(p, 3), int(round(v)))

    sorted_days = sorted(by_day.keys())
    if not sorted_days:
        return [], [], [], []
    # Keep last ~252 trading days (1 year) + enough lookback for 200D SMA!
    all_days = sorted_days
    all_prices = [by_day[d][0] for d in all_days]
    all_vols = [by_day[d][1] for d in all_days]

    # Calculate 50D and 200D SMA over full history before slicing last 1 year
    sma50_all = []
    sma200_all = []
    for i in range(len(all_prices)):
        w50 = all_prices[max(0, i - 49):i + 1]
        w200 = all_prices[max(0, i - 199):i + 1]
        sma50_all.append(round(sum(w50) / len(w50), 3))
        sma200_all.append(round(sum(w200) / len(w200), 3))

    # Filter to last 366 calendar days from latest date
    last_dt = datetime.strptime(all_days[-1], "%Y-%m-%d")
    cutoff = (last_dt - timedelta(days=366)).strftime("%Y-%m-%d")
    start_idx = 0
    for i, d in enumerate(all_days):
        if d >= cutoff:
            start_idx = i
            break

    days_1y = all_days[start_idx:]
    prices_1y = all_prices[start_idx:]
    vols_1y = all_vols[start_idx:]
    sma50_1y = sma50_all[start_idx:]
    sma200_1y = sma200_all[start_idx:]

    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    disp_dates = []
    for d in days_1y:
        y, m, dd = d.split("-")
        disp_dates.append(f"{months[int(m)-1]} {dd}, '{y[-2:]}")

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


def main():
    msx_all = json.load(open(f"{RAW_DIR}/msx_all.json"))
    mcd_all = json.load(open(f"{RAW_DIR}/mcd_all.json"))
    manifest = json.load(open(f"{RAW_DIR}/filings_manifest.json"))

    # First pass: parse 1Y series for all 8 tickers to build an equal-weighted
    # MSX 8-Flagship Benchmark (purely from official msx.om daily prices!)
    hist_cache = {}
    all_dates_set = set()
    for t in TICKERS:
        hist_cache[t] = parse_hist_daily(t)
        all_dates_set.update(hist_cache[t][1])

    common_dates = sorted(all_dates_set)
    # Build forward-filled normalized price (base=100) for each ticker to compute MSX Flagship Benchmark
    norm_by_date = {d: [] for d in common_dates}
    for t in TICKERS:
        _, fd, pr, _, _, _ = hist_cache[t]
        if not pr:
            continue
        p0 = pr[0]
        p_map = dict(zip(fd, pr))
        last_p = p0
        for d in common_dates:
            if d in p_map:
                last_p = p_map[d]
            norm_by_date[d].append(last_p / p0)
    bench_map = {d: (sum(vals) / len(vals)) for d, vals in norm_by_date.items() if vals}

    index_companies = []

    for t in TICKERS:
        cfg = COMPANY_VERIFIED[t]
        msx = msx_all[t]
        mcd = mcd_all[t]
        quote = (msx.get("quote") or [{}])[0]
        labels = msx.get("labels") or {}
        listed = mcd.get("listedInfo") or {}
        majors = mcd.get("majorInvestors") or []
        nat = mcd.get("nationalityOwnership") or {}

        disp_dates, full_dates, prices, vols, sma50, sma200 = hist_cache[t]

        price = round(parse_float(quote.get("LTP") or labels.get("LTPLabel"), prices[-1] if prices else 0.0), 3)
        prev_close = round(parse_float(quote.get("PrevClose") or labels.get("PrevCloseLabel"), price), 3)
        open_price = round(parse_float(quote.get("OpenPrice") or labels.get("OpenLabel"), price), 3)
        day_high = round(parse_float(quote.get("High"), price), 3)
        day_low = round(parse_float(quote.get("Low"), price), 3)
        day_change = round(parse_float(quote.get("ChangeVal"), price - prev_close), 3)
        day_change_pct = round(parse_float(quote.get("Change"), ((price - prev_close) / prev_close * 100) if prev_close else 0.0), 2)
        volume_raw = parse_float(quote.get("Volume"), 0.0)
        turnover_raw = parse_float(quote.get("Turnover"), 0.0)

        # Ensure latest series point matches official MSX LTP
        if prices:
            prices[-1] = price

        low_52w = round(min(prices) if prices else price, 3)
        high_52w = round(max(prices) if prices else price, 3)

        # Benchmark indexed to ticker's starting price
        if full_dates and prices:
            b0 = bench_map.get(full_dates[0], 1.0)
            bench_indexed = [round(prices[0] * (bench_map.get(d, b0) / b0), 3) for d in full_dates]
        else:
            bench_indexed = []

        # Annualized 1Y volatility
        daily_rets = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices)) if prices[i - 1] > 0]
        if len(daily_rets) > 10:
            mean_r = sum(daily_rets) / len(daily_rets)
            var_r = sum((r - mean_r) ** 2 for r in daily_rets) / (len(daily_rets) - 1)
            ann_vol = f"{math.sqrt(var_r) * math.sqrt(252) * 100:.1f}%"
        else:
            ann_vol = "N/A"

        # Shares & Market Cap (OMR) from MCD + MSX LTP
        shares_out_raw = int(round(parse_float(listed.get("TotalNoOfShares") or labels.get("IssuedSharesLabel"), 0)))
        mcap_omr = shares_out_raw * price
        paid_up_cap = parse_float(listed.get("CapitalPaid") or labels.get("PaidupcapitalLabel"), 0)
        face_val = parse_float(listed.get("FaceValue") or labels.get("ParValueLabel"), 0.1)
        participants = int(round(parse_float(listed.get("NoOfParticipant"), 0)))
        isin = listed.get("ISINCode") or labels.get("ISINLabel") or ""

        # Employees from MSX
        emp_om = int(parse_float(labels.get("OmaniLabel"), 0))
        emp_non = int(parse_float(labels.get("NonOmaniLabel"), 0))
        emp_tot = emp_om + emp_non
        employees_str = f"{emp_tot:,} ({emp_om:,} Omani)" if emp_tot > 0 else "Not disclosed on MSX profile"

        founded_str = labels.get("EstablishedInLabel") or "N/A"
        listed_date_str = labels.get("ListedDateLabel") or "N/A"
        auditor_str = (labels.get("AuditorLabel") or "N/A").replace("&amp;", "&")
        ceo_name = cfg["governance"]["management"][0]["name"] if cfg["governance"]["management"] else labels.get("ChiefExecutivePresidentLabel", "N/A")

        # Financials & Sector-specific Valuation Cards (12 cards matching frontend grid!)
        ann_list = cfg["annuals"]
        fy25 = ann_list[-1]
        fy24 = ann_list[-2] if len(ann_list) >= 2 else fy25
        q3 = cfg["q3_2026"]
        dps = cfg["dps"]
        div_yield_pct = (dps / price * 100) if price > 0 else 0.0

        is_bank = cfg["sectorGroup"] == "Banking"
        pe_fy25 = (mcap_omr / fy25["netProfit"]) if fy25["netProfit"] > 0 else None
        nav_latest = q3["nav"]
        pb_ratio = (price / nav_latest) if nav_latest and nav_latest > 0 else None
        roe_fy25 = (fy25["netProfit"] / fy25["equity"] * 100) if fy25["equity"] > 0 else None
        roa_fy25 = (fy25["netProfit"] / fy25["assets"] * 100) if fy25["assets"] > 0 else None
        np_yoy_9m = ((q3["netProfit"] - q3["netProfitPrior"]) / q3["netProfitPrior"] * 100) if q3["netProfitPrior"] else 0.0

        if is_bank:
            ldr_q3 = (q3["loans"] / q3["deposits"] * 100) if q3["deposits"] > 0 else 0.0
            cir_fy25 = (fy25["opEx"] / fy25["opInc"] * 100) if fy25["opInc"] > 0 else 0.0
            opinc_yoy_9m = ((q3["opInc"] - q3["opIncPrior"]) / q3["opIncPrior"] * 100) if q3["opIncPrior"] else 0.0
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
                {"label": "Return on Equity (FY25)", "value": f"{roe_fy25:.1f}%" if roe_fy25 else "N/A", "sub": f"ROA: {roa_fy25:.2f}% • CAR: {cfg.get('car','N/A')}", "status": "positive"},
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
                    {
                        "period": a["year"],
                        "primaryMetric": a["opInc"],
                        "secondaryMetric": a["nii"],
                        "netProfit": a["netProfit"],
                        "dps": a.get("dps"),
                        "assets": a["assets"],
                        "loans": a["loans"],
                        "deposits": a["deposits"],
                        "equity": a["equity"],
                    } for a in ann_list
                ] + [
                    {
                        "period": "9M 2026",
                        "primaryMetric": q3["opInc"],
                        "secondaryMetric": q3["nii"],
                        "netProfit": q3["netProfit"],
                        "dps": q3.get("dps"),
                        "assets": q3["assets"],
                        "loans": q3["loans"],
                        "deposits": q3["deposits"],
                        "equity": q3["equity"],
                    }
                ],
            }
            quarterly_bars = [
                {"label": "9M 2026 Operating Income", "current": fmt_omr_large(q3["opInc"]), "prior": fmt_omr_large(q3["opIncPrior"]), "currentOmr": q3["opInc"], "priorOmr": q3["opIncPrior"], "yoyPct": round(opinc_yoy_9m, 1)},
                {"label": "9M 2026 Net Interest & Islamic", "current": fmt_omr_large(q3["nii"]), "prior": fmt_omr_large(q3["niiPrior"]), "currentOmr": q3["nii"], "priorOmr": q3["niiPrior"], "yoyPct": round((q3["nii"] - q3["niiPrior"]) / q3["niiPrior"] * 100, 1)},
                {"label": "9M 2026 Net Profit", "current": fmt_omr_large(q3["netProfit"]), "prior": fmt_omr_large(q3["netProfitPrior"]), "currentOmr": q3["netProfit"], "priorOmr": q3["netProfitPrior"], "yoyPct": round(np_yoy_9m, 1)},
            ]
            highlights = [
                f"Live MSX Market Cap of {fmt_omr_large(mcap_omr)} across {fmt_shares(shares_out_raw)} issued shares (OMR {price:.3f}/sh, ISIN {isin}).",
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
            rev_yoy_9m = ((q3["revenue"] - q3["revenuePrior"]) / q3["revenuePrior"] * 100) if q3["revenuePrior"] else 0.0
            ebitda_yoy_9m = ((q3["ebitda"] - q3["ebitdaPrior"]) / q3["ebitdaPrior"] * 100) if q3["ebitdaPrior"] else 0.0

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
                    {
                        "period": a["year"],
                        "primaryMetric": a["revenue"],
                        "secondaryMetric": a["ebitda"],
                        "netProfit": a["netProfit"],
                        "dps": a.get("dps"),
                        "ocf": a["ocf"],
                        "capex": a["capex"],
                        "assets": a["assets"],
                        "debt": a["debt"],
                        "equity": a["equity"],
                    } for a in ann_list
                ] + [
                    {
                        "period": "9M 2026",
                        "primaryMetric": q3["revenue"],
                        "secondaryMetric": q3["ebitda"],
                        "netProfit": q3["netProfit"],
                        "dps": q3.get("dps"),
                        "ocf": q3["ocf"],
                        "capex": q3["capex"],
                        "assets": q3["assets"],
                        "debt": q3["debt"],
                        "equity": q3["equity"],
                    }
                ],
            }
            quarterly_bars = [
                {"label": "9M 2026 Revenue", "current": fmt_omr_large(q3["revenue"]), "prior": fmt_omr_large(q3["revenuePrior"]), "currentOmr": q3["revenue"], "priorOmr": q3["revenuePrior"], "yoyPct": round(rev_yoy_9m, 1)},
                {"label": "9M 2026 EBITDA", "current": fmt_omr_large(q3["ebitda"]), "prior": fmt_omr_large(q3["ebitdaPrior"]), "currentOmr": q3["ebitda"], "priorOmr": q3["ebitdaPrior"], "yoyPct": round(ebitda_yoy_9m, 1)},
                {"label": "9M 2026 Net Profit", "current": fmt_omr_large(q3["netProfit"]), "prior": fmt_omr_large(q3["netProfitPrior"]), "currentOmr": q3["netProfit"], "priorOmr": q3["netProfitPrior"], "yoyPct": round(np_yoy_9m, 1)},
            ]
            highlights = [
                f"Live MSX Market Cap of {fmt_omr_large(mcap_omr)} and Enterprise Value of {fmt_omr_large(ev_omr)} ({fmt_shares(shares_out_raw)} shares at OMR {price:.3f}/sh).",
                f"FY2025 Audited Results: Revenue of {fmt_omr_large(fy25['revenue'])}, EBITDA of {fmt_omr_large(fy25['ebitda'])} ({ebitda_margin:.1f}% margin), and Net Profit of {fmt_omr_large(fy25['netProfit'])}.",
                f"Cash Flow & CapEx: {fmt_omr_large(fy25['ocf'])} FY2025 Operating Cash Flow, {fmt_omr_large(fy25['capex'])} CapEx, and {fmt_omr_large(q3['netProfit'])} 9M 2026 Net Profit ({np_yoy_9m:+.1f}% YoY).",
            ]

        # Ownership & Nationality Breakdown from mcd.om
        omani_pct = round(parse_float(nat.get("omaniCurr"), 0.0), 2)
        gcc_pct = round(parse_float(nat.get("gulfCurr"), 0.0), 2)
        arab_pct = round(parse_float(nat.get("arabCurr"), 0.0), 2)
        foreign_pct = round(parse_float(nat.get("foreignerCurr"), 0.0), 2)
        other_non_omani = round(arab_pct + foreign_pct, 2)

        nationality_breakdown = [
            {
                "label": "Omani Investors",
                "pct": omani_pct,
                "shares": int(round(parse_float(nat.get("omaniCurrSe"), 0))),
                "limitPct": parse_float(nat.get("omaniMax"), 100.0),
                "color": "#6366f1",
            },
            {
                "label": "GCC Investors",
                "pct": gcc_pct,
                "shares": int(round(parse_float(nat.get("gulfCurrSe"), 0))),
                "limitPct": parse_float(nat.get("gulfMax"), 100.0),
                "color": "#06b6d4",
            },
            {
                "label": "Foreign & Arab Investors",
                "pct": other_non_omani,
                "arabPct": arab_pct,
                "foreignerPct": foreign_pct,
                "shares": int(round(parse_float(nat.get("arabCurrSe"), 0) + parse_float(nat.get("foreignerCurrSe"), 0))),
                "limitPct": parse_float(nat.get("foreignerMax"), 100.0),
                "color": "#10b981",
            },
        ]

        # Major Investors (5%+) from mcd.om
        majors_sorted = sorted(majors, key=lambda x: parse_float(x.get("OwnershipPercentage"), 0.0), reverse=True)
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

        # Official 2026 MSX Disclosures (top 9 key corporate disclosures)
        raw_news = json.load(open(f"{RAW_DIR}/news_{t}.json"))
        disclosures = []
        seen_titles = set()
        for item in raw_news:
            title = (item.get("TitleEn") or "").strip()
            if not title or title in seen_titles:
                continue
            # Filter out routine low-signal notices if we have plenty of key ones
            cat, sent = classify_disclosure(title)
            doc_file = item.get("Doc_News")
            pdf_link = f"https://www.msx.om/MSMDocs/Images/NewsDocs/{doc_file}" if doc_file else f"https://www.msx.om/snapshot.aspx?s={t}&se=4&ni={item.get('NewsID','')}"
            seen_titles.add(title)
            disclosures.append({
                "title": title,
                "source": "Muscat Stock Exchange (msx.om)",
                "date": item.get("DateTime", ""),
                "link": pdf_link,
                "category": cat,
                "sentiment": sent,
                "summary": f"Official MSX regulatory disclosure filed by {cfg['displayName']} ({t}) on {item.get('DateTime', '2026')}." + (f" Document: {doc_file}" if doc_file else ""),
            })
            if len(disclosures) >= 9:
                break

        # Filings list
        company_filings = manifest.get(t, [])

        # Direct Verification URLs for every section
        verification_urls = {
            "msxProfile": f"https://www.msx.om/snapshot.aspx?s={t}",
            "msxHistoricalPrices": f"https://www.msx.om/company-chart-data.aspx?s={t}",
            "msxDisclosures": f"https://www.msx.om/snapshot.aspx?s={t}#news",
            "msxFinancialReports": company_filings[0]["sourceUrl"] if company_filings else f"https://www.msx.om/snapshot.aspx?s={t}",
            "mcdListedInfo": MCD_URLS["listed"],
            "mcdMajorInvestors": MCD_URLS["major"],
            "mcdNationalityOwnership": MCD_URLS["nationality"],
            "corporateWebsite": cfg["website"],
            "boardOfDirectors": cfg["governance"]["boardSourceUrl"],
            "executiveManagement": cfg["governance"]["managementSourceUrl"],
        }

        company_obj = {
            "ticker": t,
            "exchange": "MSX",
            "isin": isin,
            "currency": "OMR",
            "currencySymbol": "OMR ",
            "name": cfg["displayName"],
            "sectorGroup": cfg["sectorGroup"],
            "sector": cfg["sectorLabel"],
            "ceo": ceo_name,
            "hq": cfg["hq"],
            "employees": employees_str,
            "founded": founded_str,
            "listedDate": listed_date_str,
            "auditor": auditor_str,
            "website": cfg["website"],
            "msxUrl": f"https://www.msx.om/snapshot.aspx?s={t}",
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
            "dpsNote": cfg["dpsNote"],
            "latestFiscalYear": "FY2025 Audited + Q3 2026",
            "description": cfg["description"],
            "highlights": highlights,
            "segments": cfg["segments"],
            "segmentNote": cfg["segmentNote"],
            "series": {
                "dates": disp_dates,
                "fullDates": full_dates,
                "prices": prices,
                "volumes": vols,
                "sma50": sma50,
                "sma200": sma200,
                "benchmarkIndexed": bench_indexed,
                "benchmarkLabel": "MSX Flagship 8 (Indexed OMR)",
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
                "period": q3["period"],
                "bars": quarterly_bars,
                "sourceFiling": company_filings[0]["file"] if company_filings else "",
                "sourceUrl": company_filings[0]["sourceUrl"] if company_filings else f"https://www.msx.om/snapshot.aspx?s={t}",
            },
            "governance": cfg["governance"],
            "filings": company_filings,
            "disclosures": disclosures,
            "verificationUrls": verification_urls,
            "lastUpdated": "2026-11-19 18:15 GST",
            "dataSource": f"MSX & MCD Official ({t}:MSX)",
        }

        out_path = os.path.join(DATA_DIR, f"{t}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(company_obj, f, indent=2, ensure_ascii=False)
        print(f"Wrote data/{t}.json | Price=OMR {price:.3f} | MCap={fmt_omr_large(mcap_omr)} | Majors={len(major_holders)} | BOD={len(cfg['governance']['board'])} | Exec={len(cfg['governance']['management'])}")

        index_companies.append({
            "ticker": t,
            "name": cfg["displayName"],
            "exchange": "MSX",
            "isin": isin,
            "sectorGroup": cfg["sectorGroup"],
            "sector": cfg["sectorLabel"],
            "price": price,
            "dayChange": day_change,
            "dayChangePct": day_change_pct,
            "marketCapFmt": fmt_omr_large(mcap_omr),
            "dataFile": f"data/{t}.json",
        })

    index_payload = {
        "exchange": "Muscat Stock Exchange (MSX)",
        "currency": "OMR",
        "generatedAt": "2026-11-19 18:15 GST",
        "sources": [
            {"name": "Muscat Stock Exchange (MSX)", "url": "https://www.msx.om/"},
            {"name": "Muscat Clearing & Depository (MCD)", "url": "https://www.mcd.om/"},
            {"name": "Official Corporate Websites", "url": "Verified per company"},
        ],
        "companies": index_companies,
    }
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index_payload, f, indent=2, ensure_ascii=False)
    print(f"\nWrote data/index.json with {len(index_companies)} MSX flagship companies.")


if __name__ == "__main__":
    main()
