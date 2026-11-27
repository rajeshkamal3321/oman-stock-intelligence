import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");
const assetsDir = path.join(distDir, "assets");
const vendorDir = path.join(assetsDir, "vendor");
const fontsDir = path.join(assetsDir, "fonts");

function copyRecursive(src, dest) {
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    fs.mkdirSync(dest, { recursive: true });
    for (const entry of fs.readdirSync(src)) {
      if (
        entry.startsWith(".") ||
        entry === "ai_profiles.json" ||
        entry === "ipo_prospectus_financials.json" ||
        entry === "corporate_governance_overrides.json" ||
        entry === "shareholder_translations.json" ||
        entry === "historical_ocr_backfill.json"
      ) continue;
      copyRecursive(path.join(src, entry), path.join(dest, entry));
    }
  } else {
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(src, dest);
  }
}

function copyRequiredFile(src, dest, label) {
  if (!fs.existsSync(src)) {
    throw new Error(`Missing ${label}: ${src}. Run "npm install" before "npm run build".`);
  }
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
  const size = fs.statSync(dest).size;
  if (size <= 0) {
    throw new Error(`Copied ${label} is empty: ${dest}`);
  }
  return size;
}

// Clean & rebuild dist/
fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(assetsDir, { recursive: true });
fs.mkdirSync(vendorDir, { recursive: true });
fs.mkdirSync(fontsDir, { recursive: true });

// Validate required static inputs
const indexHtml = path.join(rootDir, "index.html");
const dataDir = path.join(rootDir, "data");
const tailwindConfig = path.join(rootDir, "tailwind.config.js");
const stylesInput = path.join(rootDir, "src", "styles.css");
const tailwindCli = path.join(rootDir, "node_modules", "tailwindcss", "lib", "cli.js");

if (!fs.existsSync(indexHtml)) throw new Error("Missing index.html");
if (!fs.existsSync(path.join(dataDir, "index.json"))) throw new Error("Missing data/index.json");
if (!fs.existsSync(tailwindConfig)) throw new Error("Missing tailwind.config.js");
if (!fs.existsSync(stylesInput)) throw new Error("Missing src/styles.css");

// Guard against regressions back to third-party CSS/JS CDNs
const htmlSource = fs.readFileSync(indexHtml, "utf-8");
const forbiddenCdns = [
  "cdn.tailwindcss.com",
  "cdn.jsdelivr.net",
  "cdnjs.cloudflare.com",
  "unpkg.com",
  "fonts.googleapis.com",
  "fonts.gstatic.com",
];
for (const cdn of forbiddenCdns) {
  if (htmlSource.includes(cdn)) {
    throw new Error(`Forbidden third-party CDN reference "${cdn}" found in index.html; self-host under ./assets/ instead.`);
  }
}

// Validate JSON integrity before emitting static bundle
const indexJson = JSON.parse(fs.readFileSync(path.join(dataDir, "index.json"), "utf-8"));
for (const c of indexJson.companies) {
  const cPath = path.join(rootDir, c.dataFile);
  if (!fs.existsSync(cPath)) throw new Error(`Missing company JSON: ${c.dataFile}`);
  JSON.parse(fs.readFileSync(cPath, "utf-8"));
}

// Self-hosted vendor JS libraries and WOFF2 fonts
const vendorFiles = [
  {
    src: path.join(rootDir, "node_modules", "chart.js", "dist", "chart.umd.js"),
    dest: path.join(vendorDir, "chart.umd.js"),
    label: "Chart.js UMD bundle",
  },
  {
    src: path.join(rootDir, "node_modules", "lucide", "dist", "umd", "lucide.min.js"),
    dest: path.join(vendorDir, "lucide.min.js"),
    label: "Lucide Icons UMD bundle",
  },
  {
    src: path.join(rootDir, "node_modules", "jspdf", "dist", "jspdf.umd.min.js"),
    dest: path.join(vendorDir, "jspdf.umd.min.js"),
    label: "jsPDF UMD bundle",
  },
];

const fontFiles = [
  ["@fontsource/inter", "inter-latin-300-normal.woff2"],
  ["@fontsource/inter", "inter-latin-400-normal.woff2"],
  ["@fontsource/inter", "inter-latin-500-normal.woff2"],
  ["@fontsource/inter", "inter-latin-600-normal.woff2"],
  ["@fontsource/inter", "inter-latin-700-normal.woff2"],
  ["@fontsource/jetbrains-mono", "jetbrains-mono-latin-400-normal.woff2"],
  ["@fontsource/jetbrains-mono", "jetbrains-mono-latin-500-normal.woff2"],
  ["@fontsource/jetbrains-mono", "jetbrains-mono-latin-600-normal.woff2"],
];

// Ensure pinned build dependencies are installed (supports CI pipelines that invoke `npm run build` directly)
const requiredNodeModuleFiles = [
  tailwindCli,
  ...vendorFiles.map((v) => v.src),
  ...fontFiles.map(([pkg, file]) => path.join(rootDir, "node_modules", ...pkg.split("/"), "files", file)),
];
if (requiredNodeModuleFiles.some((p) => !fs.existsSync(p))) {
  console.log("Installing self-hosted frontend build dependencies via npm install...");
  const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";
  execFileSync(npmCmd, ["install", "--no-audit", "--no-fund"], {
    cwd: rootDir,
    stdio: "inherit",
    shell: process.platform === "win32",
  });
}

if (!fs.existsSync(tailwindCli)) {
  throw new Error('Missing tailwindcss CLI in node_modules after npm install.');
}

// Compile self-hosted minified Tailwind CSS
const tailwindOutput = path.join(assetsDir, "tailwind.css");
execFileSync(
  process.execPath,
  [tailwindCli, "-c", tailwindConfig, "-i", stylesInput, "-o", tailwindOutput, "--minify"],
  { cwd: rootDir, stdio: "inherit" }
);
const cssBytes = fs.statSync(tailwindOutput).size;
if (cssBytes <= 0) throw new Error("Compiled dist/assets/tailwind.css is empty");

let vendorBytes = 0;
for (const v of vendorFiles) {
  vendorBytes += copyRequiredFile(v.src, v.dest, v.label);
}

let fontBytes = 0;
for (const [pkg, file] of fontFiles) {
  const src = path.join(rootDir, "node_modules", ...pkg.split("/"), "files", file);
  const dest = path.join(fontsDir, file);
  fontBytes += copyRequiredFile(src, dest, `font ${file}`);
}

// Copy pure static assets (filings link directly to msx.om — no PDFs bundled in dist/)
fs.copyFileSync(indexHtml, path.join(distDir, "index.html"));
copyRecursive(dataDir, path.join(distDir, "data"));

// Cloudflare Pages static headers
const headersContent = `/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin

/assets/*
  Cache-Control: public, max-age=3600

/assets/fonts/*
  Cache-Control: public, max-age=31536000, immutable

/data/*
  Cache-Control: public, max-age=300
  Content-Type: application/json; charset=utf-8
`;
fs.writeFileSync(path.join(distDir, "_headers"), headersContent, "utf-8");

console.log(
  `Built static Cloudflare Pages bundle -> dist/ (` +
  `${indexJson.companies.length} MSX companies verified, ` +
  `CSS ${(cssBytes / 1024).toFixed(1)} KB, ` +
  `JS ${(vendorBytes / 1024).toFixed(1)} KB, ` +
  `fonts ${(fontBytes / 1024).toFixed(1)} KB, ` +
  `0 third-party CSS/JS CDN dependencies).`
);
