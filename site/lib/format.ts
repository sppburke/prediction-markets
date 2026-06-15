// Single shared display formatter (issue #343 PR3, hard AC).
//
// `@supabase/supabase-js` returns Postgres NUMERIC columns as full-precision
// *strings* (e.g. "1579.97989245199679481500", or ls_edge as "0.02342341…") to
// avoid float precision loss. Rendering those raw is the failure mode this
// module exists to prevent: EVERY numeric value the site shows must pass through
// one of these helpers, so no raw full-precision NUMERIC string reaches the DOM.
//
// These precisions are PRESENTATION-ONLY. They are not strategy/risk thresholds
// (those live canonically in docs/_GLOSSARY.md / docs/19-); the stored columns
// and the wallet_live_stats view keep full precision so analytics stay exact.

/** Declared display precision (decimal places) per value class. */
export const PRECISION = {
  /** USD amounts — realized P&L, $/trade live edge. */
  usd: 2,
  /** Fractions rendered as a percentage (0..1 → "62.3%"). */
  pct: 1,
  /** Dimensionless ranker edge — ls_edge. */
  edge: 4,
  /** Ranker t-stat — ls_tstat. */
  tstat: 2,
  /** Mid prices in [0,1] — avg_price. */
  price: 3,
} as const;

export type Numeric = number | string | null | undefined;

/** Parse a supabase-js numeric (string | number) into a finite number, else null. */
export function toNum(v: Numeric): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

const EM_DASH = "—";

/** USD with a thousands separator and exactly PRECISION.usd decimals. */
export function formatUsd(v: Numeric, opts?: { sign?: boolean }): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  const s = n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: PRECISION.usd,
    maximumFractionDigits: PRECISION.usd,
  });
  return opts?.sign && n > 0 ? `+${s}` : s;
}

/** A fraction in [0,1] rendered as a percentage with PRECISION.pct decimals. */
export function formatPct(frac: Numeric): string {
  const n = toNum(frac);
  if (n === null) return EM_DASH;
  return `${(n * 100).toFixed(PRECISION.pct)}%`;
}

/** Dimensionless ranker edge (ls_edge) at PRECISION.edge. */
export function formatEdge(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  return n.toFixed(PRECISION.edge);
}

/** Ranker t-stat (ls_tstat) at PRECISION.tstat. */
export function formatTstat(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  return n.toFixed(PRECISION.tstat);
}

/** Mid price in [0,1] (avg_price) at PRECISION.price. */
export function formatPrice(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  return n.toFixed(PRECISION.price);
}

/** Counts / ranks — integers with a thousands separator. */
export function formatInt(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  return Math.round(n).toLocaleString("en-US");
}

/** Sign class for colouring P&L; 0 is treated as neutral. */
export function signClass(v: Numeric): "pos" | "neg" | "zero" {
  const n = toNum(v);
  if (n === null || n === 0) return "zero";
  return n > 0 ? "pos" : "neg";
}

/** 0x-abbreviated wallet for compact display (full value stays in links/titles). */
export function shortWallet(wallet: string): string {
  if (wallet.length <= 12) return wallet;
  return `${wallet.slice(0, 6)}…${wallet.slice(-4)}`;
}
