#!/usr/bin/env python3
"""Static Cloudflare Pages builder (Python mirror of scripts/build.mjs)."""
import json
import os
import shutil
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(ROOT, "dist")
ASSETS = os.path.join(DIST, "assets")
VENDOR = os.path.join(ASSETS, "vendor")
FONTS = os.path.join(ASSETS, "fonts")

if os.path.exists(DIST):
    shutil.rmtree(DIST)
os.makedirs(VENDOR, exist_ok=True)
os.makedirs(FONTS, exist_ok=True)

index_html = os.path.join(ROOT, "index.html")
data_dir = os.path.join(ROOT, "data")
tailwind_config = os.path.join(ROOT, "tailwind.config.js")
styles_input = os.path.join(ROOT, "src", "styles.css")
tailwind_cli = os.path.join(ROOT, "node_modules", "tailwindcss", "lib", "cli.js")

assert os.path.isfile(index_html), "Missing index.html"
assert os.path.isfile(tailwind_config), "Missing tailwind.config.js"
assert os.path.isfile(styles_input), "Missing src/styles.css"
assert os.path.isfile(tailwind_cli), 'Missing tailwindcss CLI in node_modules. Run "npm install" first.'

with open(index_html, encoding="utf-8") as f:
    html_src = f.read()
for cdn in [
    "cdn.tailwindcss.com",
    "cdn.jsdelivr.net",
    "cdnjs.cloudflare.com",
    "unpkg.com",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
]:
    assert cdn not in html_src, f"Forbidden third-party CDN reference '{cdn}' found in index.html"

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

# Compile self-hosted minified Tailwind CSS
tailwind_out = os.path.join(ASSETS, "tailwind.css")
subprocess.run(
    ["node", tailwind_cli, "-c", tailwind_config, "-i", styles_input, "-o", tailwind_out, "--minify"],
    cwd=ROOT,
    check=True,
)
assert os.path.getsize(tailwind_out) > 0, "Compiled dist/assets/tailwind.css is empty"

# Copy self-hosted vendor JS libraries
vendor_files = [
    (os.path.join(ROOT, "node_modules", "chart.js", "dist", "chart.umd.js"), os.path.join(VENDOR, "chart.umd.js")),
    (os.path.join(ROOT, "node_modules", "lucide", "dist", "umd", "lucide.min.js"), os.path.join(VENDOR, "lucide.min.js")),
    (os.path.join(ROOT, "node_modules", "jspdf", "dist", "jspdf.umd.min.js"), os.path.join(VENDOR, "jspdf.umd.min.js")),
]
for src, dest in vendor_files:
    assert os.path.isfile(src), f"Missing vendor bundle: {src}. Run 'npm install' first."
    shutil.copy2(src, dest)
    assert os.path.getsize(dest) > 0, f"Copied vendor bundle is empty: {dest}"

# Copy self-hosted WOFF2 fonts
font_files = [
    ("@fontsource", "inter", "inter-latin-300-normal.woff2"),
    ("@fontsource", "inter", "inter-latin-400-normal.woff2"),
    ("@fontsource", "inter", "inter-latin-500-normal.woff2"),
    ("@fontsource", "inter", "inter-latin-600-normal.woff2"),
    ("@fontsource", "inter", "inter-latin-700-normal.woff2"),
    ("@fontsource", "jetbrains-mono", "jetbrains-mono-latin-400-normal.woff2"),
    ("@fontsource", "jetbrains-mono", "jetbrains-mono-latin-500-normal.woff2"),
    ("@fontsource", "jetbrains-mono", "jetbrains-mono-latin-600-normal.woff2"),
]
for scope, pkg, fn in font_files:
    src = os.path.join(ROOT, "node_modules", scope, pkg, "files", fn)
    dest = os.path.join(FONTS, fn)
    assert os.path.isfile(src), f"Missing font file: {src}. Run 'npm install' first."
    shutil.copy2(src, dest)
    assert os.path.getsize(dest) > 0, f"Copied font file is empty: {dest}"

# Copy pure static assets (filings link directly to msx.om — no PDFs bundled in dist/)
shutil.copy2(index_html, os.path.join(DIST, "index.html"))
shutil.copytree(data_dir, os.path.join(DIST, "data"), ignore=shutil.ignore_patterns("ai_profiles.json", "ipo_prospectus_financials.json", "corporate_governance_overrides.json", ".*"))

headers = """/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin

/assets/*
  Cache-Control: public, max-age=3600

/assets/fonts/*
  Cache-Control: public, max-age=31536000, immutable

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
    f"{len(index_json['companies'])} MSX companies verified, self-hosted CSS/JS/fonts bundled, 0 server dependencies."
)
