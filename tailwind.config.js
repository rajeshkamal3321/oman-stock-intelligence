/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./index.html",
  ],
  darkMode: "class",
  safelist: [
    // Dynamic business segment bar & legend colors loaded from data/*.json
    "bg-indigo-500",
    "bg-cyan-400",
    "bg-violet-400",
    "bg-emerald-400",
    "bg-amber-400",
    "bg-rose-400",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Inter",
          "-apple-system",
          "BlinkMacSystemFont",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
        mono: [
          "JetBrains Mono",
          "Consolas",
          "Monaco",
          "monospace",
        ],
      },
      colors: {
        slate: {
          850: "#151e2e",
          925: "#0b111d",
          950: "#060a12",
        },
      },
    },
  },
  plugins: [],
};
