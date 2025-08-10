// src/pages/_app.tsx
import "@/styles/globals.css";
import "@/styles/spinner.css";
import type { AppProps } from "next/app";
import localFont from "next/font/local";
import RouterProviderWrapper from "@/components/RouterProviderWrapper";
import { DuckDBProvider } from "@/context/DuckDBContext";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const microscan = localFont({
  src: "./fonts/Microscan-A.woff",
  variable: "--font-microscan",
});

export default function App({ Component, pageProps }: AppProps) {
  return (
      <main className={`${microscan.variable} min-h-dvh`}>
        <DuckDBProvider>
          <RouterProviderWrapper>
            <Component {...pageProps} />
          </RouterProviderWrapper>
        </DuckDBProvider>
      </main>
  );
}