// Request body for the current-mark proxy (`/api/marks`). The keys travel in a POST body because
// one key per open fill ("market:outcome", ~70 characters) outgrew nginx's 8 KB request line once
// about 110 positions were open: the GET answered 414 and every Unrealized cell showed "—".
import { toNum } from "./format";
import type { PaperFill } from "./types";

/** A fill's P&L at `price` (an open fill's current mark, a settled fill's resolved price), with the economics of
 * `wallet_live_stats`' realized P&L: with recorded principal and fee, BUY = price × contracts − principal − fee and
 * SELL = principal − price × contracts − fee; a fill without them keeps side_sign × (price − fill_price) × contracts.
 * `null` when an input does not parse. */
export function fillPnl(
  fill: Pick<PaperFill, "side" | "contracts" | "fill_price" | "principal" | "fee">,
  price: number,
): number | null {
  const contracts = toNum(fill.contracts);
  if (contracts === null || !Number.isFinite(price)) return null;
  const sideSign = fill.side === "buy" ? 1 : -1;
  const principal = toNum(fill.principal);
  const fee = toNum(fill.fee);
  if (principal !== null && fee !== null) return sideSign * (price * contracts - principal) - fee;
  const fillPrice = toNum(fill.fill_price);
  return fillPrice === null ? null : sideSign * (price - fillPrice) * contracts;
}

/** Matches `fetchOpenFills`' row cap, so one request never asks for more keys than the page loads. */
export const MAX_MARK_KEYS = 5000;

/** Trimmed, de-duplicated keys from `{ keys: string[] }`, or `null` when the body is malformed. */
export function parseMarkKeys(body: unknown): string[] | null {
  if (typeof body !== "object" || body === null) return null;
  const keys = (body as { keys?: unknown }).keys;
  if (!Array.isArray(keys) || keys.length > MAX_MARK_KEYS) return null;
  const parsed = new Set<string>();
  for (const key of keys) {
    if (typeof key !== "string") return null;
    const trimmed = key.trim();
    if (trimmed) parsed.add(trimmed);
  }
  return [...parsed];
}
