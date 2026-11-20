#!/usr/bin/env python3
"""Static Cloudflare Pages builder (Python mirror of scripts/build.mjs for environments without Node)."""
import json
import os
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(ROOT, "dist")

if os.path.exists(DIST):
    shutil.rmtree(DIST)
os.makedirs(DIST, exist_ok=True)

index_html = os.path.join(ROOT, "index.html")
data_dir = os.path.join(ROOT, "data")

assert os.path.isfile(index_html), "Missing index.html"
with open(os.path.join(data_dir, "index.json"), encoding="utf-8") as f:
    index_json = json.load(f)

for c in index_json["companies"]:
    p = os.path.join(ROOT, c["dataFile"])
    assert os.path.isfile(p), f"Missing {c['dataFile']}"
    with open(p, encoding="utf-8") as cf:
        obj = json.load(cf)
    # Verify zero third-party aggregator URLs
    raw_str = json.dumps(obj)
    for forbidden in ["yahoo", "google.com/finance", "tradingview", "investing.com", "marketscreener", "simplywall"]:
        assert forbidden not in raw_str.lower(), f"Forbidden third-party reference '{forbidden}' found in {c['ticker']}"

# Copy pure static assets (filings link directly to msx.om — no PDFs bundled in dist/)
shutil.copy2(index_html, os.path.join(DIST, "index.html"))
shutil.copytree(data_dir, os.path.join(DIST, "data"), ignore=shutil.ignore_patterns("ai_profiles.json", ".*"))

headers = """/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin

/data/*
  Cache-Control: public, max-age=300
  Content-Type: application/json; charset=utf-8
"""
with open(os.path.join(DIST, "_headers"), "w", encoding="utf-8") as f:
    f.write(headers)

# Count output files & total size
file_count = 0
total_bytes = 0
for dp, _, fns in os.walk(DIST):
    for fn in fns:
        file_count += 1
        total_bytes += os.path.getsize(os.path.join(dp, fn))

print(
    f"Static dist/ built cleanly: {file_count} files ({total_bytes / (1024*1024):.2f} MB), "
    f"{len(index_json['companies'])} MSX companies verified, filings linked to msx.om, 0 server dependencies."
)
