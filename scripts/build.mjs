import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");

function copyRecursive(src, dest) {
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    fs.mkdirSync(dest, { recursive: true });
    for (const entry of fs.readdirSync(src)) {
      if (entry.startsWith(".") || entry === "ai_profiles.json") continue;
      copyRecursive(path.join(src, entry), path.join(dest, entry));
    }
  } else {
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(src, dest);
  }
}

// Clean & rebuild dist/
fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });

// Validate required static inputs
const indexHtml = path.join(rootDir, "index.html");
const dataDir = path.join(rootDir, "data");

if (!fs.existsSync(indexHtml)) throw new Error("Missing index.html");
if (!fs.existsSync(path.join(dataDir, "index.json"))) throw new Error("Missing data/index.json");

// Validate JSON integrity before emitting static bundle
const indexJson = JSON.parse(fs.readFileSync(path.join(dataDir, "index.json"), "utf-8"));
for (const c of indexJson.companies) {
  const cPath = path.join(rootDir, c.dataFile);
  if (!fs.existsSync(cPath)) throw new Error(`Missing company JSON: ${c.dataFile}`);
  JSON.parse(fs.readFileSync(cPath, "utf-8"));
}

// Copy pure static assets (filings link directly to msx.om — no PDFs bundled in dist/)
fs.copyFileSync(indexHtml, path.join(distDir, "index.html"));
copyRecursive(dataDir, path.join(distDir, "data"));

// Cloudflare Pages static headers
const headersContent = `/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin

/data/*
  Cache-Control: public, max-age=300
  Content-Type: application/json; charset=utf-8
`;
fs.writeFileSync(path.join(distDir, "_headers"), headersContent, "utf-8");

console.log(`Built static Cloudflare Pages bundle -> dist/ (${indexJson.companies.length} MSX companies verified, filings linked directly to msx.om).`);
