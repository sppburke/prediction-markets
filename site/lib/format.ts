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
  /** Share quantities (contracts) — fractional on Polymarket, e.g. 47.62. */
  qty: 2,
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

/** Share quantity (contracts) at PRECISION.qty — fractional, never rounded to int. */
export function formatQty(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  return n.toLocaleString("en-US", {
    minimumFractionDigits: PRECISION.qty,
    maximumFractionDigits: PRECISION.qty,
  });
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

/**
 * Epoch *seconds* → absolute UTC calendar date "YYYY-MM-DD". Deterministic
 * (depends only on the input), so it is safe to assert directly in tests. The
 * wallet's real last on-chain trade time (#357 `last_trade_unix`) renders here.
 */
export function formatDate(v: Numeric): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  const d = new Date(n * 1000);
  if (Number.isNaN(d.getTime())) return EM_DASH;
  return d.toISOString().slice(0, 10);
}

/**
 * Epoch *seconds* → coarse relative age ("just now", "5m ago", "3h ago",
 * "8d ago") — the at-a-glance inactivity signal for #357. `nowSec` (epoch
 * seconds) is injectable so the output is deterministic in tests; it defaults
 * to the wall clock for live rendering. Future timestamps (clock skew) clamp
 * to "just now".
 */
export function formatAge(v: Numeric, nowSec: number = Date.now() / 1000): string {
  const n = toNum(v);
  if (n === null) return EM_DASH;
  const secs = nowSec - n;
  if (secs < 60) return "just now";
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}
