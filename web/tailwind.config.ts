import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        logo: ['var(--font-microscan)'] 
      },
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
        purdue: {
          aged: '#8E6F3E',
          black: '#000000',
          steel: '#555960',
          coolGray: '#6F727B',
          boilermakerGold: '#CFB991',
          rush: '#DAAA00',
          field: '#DDB945',
          dust: '#EBD99F',
          railwayGray: '#9D9795',
          steam: '#C4BFC0',
        },
      },
    },
  },
  plugins: [],
};
export default config;
