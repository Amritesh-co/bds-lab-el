/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        primary:      "#adc6ff",
        "primary-container": "#4d8eff",
        "on-primary": "#002e6a",
        secondary:    "#4edea3",
        "secondary-container": "#00a572",
        tertiary:     "#ffb95f",
        "tertiary-container": "#ca8100",
        error:        "#ffb4ab",
        "error-container": "#93000a",
        surface:      "#051424",
        "surface-dim": "#051424",
        "surface-bright": "#2c3a4c",
        "surface-container-lowest": "#010f1f",
        "surface-container-low": "#0d1c2d",
        "surface-container": "#122131",
        "surface-container-high": "#1c2b3c",
        "surface-container-highest": "#273647",
        "surface-variant": "#273647",
        "on-surface": "#d4e4fa",
        "on-surface-variant": "#c2c6d6",
        outline:      "#8c909f",
        "outline-variant": "#424754",
        background:   "#051424",
      },
      fontFamily: {
        sans: ["Inter", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
    },
  },
  plugins: [],
};

