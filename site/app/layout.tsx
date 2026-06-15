import type { Metadata, Viewport } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Paper-trade analytics — historical vs live",
  description:
    "Per-wallet historical (ranker) vs live (paper) copy-trade stats from Supabase.",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen">
          <header className="border-b border-border bg-panelAlt">
            <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
              <Link href="/" className="text-sm font-semibold tracking-tight text-text">
                pe<span className="text-accent">·</span>analytics
              </Link>
              <span className="text-xs text-muted">historical vs live · paper</span>
            </div>
          </header>
          <main className="mx-auto max-w-6xl px-4 py-6">{children}</main>
          <footer className="mx-auto max-w-6xl px-4 py-8 text-xs text-muted">
            Reads the Supabase <code className="text-accent">wallet_live_stats</code> view
            via the anon key (read-only RLS). Local <code>paper_state.db</code> stays
            authoritative.
          </footer>
        </div>
      </body>
    </html>
  );
}
