import { getSupabase } from "./supabase";
import type { PaperFill, ServiceRuntime, WalletLiveStats } from "./types";

export class NotConfiguredError extends Error {
  constructor() {
    super("Supabase is not configured. Copy .env.example to .env.local.");
    this.name = "NotConfiguredError";
  }
}

/** All wallets in the historical-vs-live view. Reads the `wallet_live_stats_mv` materialized
 * view (a periodically-refreshed cache of the `wallet_live_stats` aggregation) so the heavy
 * full-table aggregation runs on a schedule, not on every page view. */
export async function fetchWalletStats(): Promise<WalletLiveStats[]> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("wallet_live_stats_mv")
    .select("*")
    .order("live_realized_pnl", { ascending: false, nullsFirst: false });
  if (error) throw new Error(error.message);
  return (data ?? []) as WalletLiveStats[];
}

/** pe-service's current live watchlist size ("N watched"), or null if not yet published. */
export async function fetchServiceRuntime(): Promise<ServiceRuntime | null> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("service_runtime")
    .select("watchlist_size, updated_at")
    .eq("id", 1)
    .maybeSingle();
  if (error) throw new Error(error.message);
  return (data as ServiceRuntime | null) ?? null;
}

/** One wallet's historical-vs-live row (wallet keys are lowercase). Reads the
 * `wallet_live_stats_mv` materialized view (filter pushed down to the indexed `wallet`). */
export async function fetchWalletStat(wallet: string): Promise<WalletLiveStats | null> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("wallet_live_stats_mv")
    .select("*")
    .eq("wallet", wallet.toLowerCase())
    .maybeSingle();
  if (error) throw new Error(error.message);
  return (data as WalletLiveStats | null) ?? null;
}

/** Lowercase `wallet_hex` set pe-service is actively copying (#398 WS3 step 20). Soft-fails to an
 * empty set when the table is unreadable/empty (risk #10) so the Live tab degrades gracefully to
 * `live_open_fills>0` rather than erroring. */
export async function fetchWatchedSet(): Promise<Set<string>> {
  const sb = getSupabase();
  if (!sb) return new Set();
  const { data, error } = await sb.from("service_watchlist").select("wallet_hex");
  if (error || !data) return new Set();
  return new Set(data.map((r) => String(r.wallet_hex).toLowerCase()));
}

/** Market ids that have resolved (have a `settled_markets` row) — used to split open vs settled
 * fills for the unrealized-PnL feature. Fail-CLOSED (throws on read error, not soft-fail): a silent
 * empty set would mislabel already-settled fills as open and double-count realized P&L as
 * unrealized. The caller (`fetchOpenFills` → page `.catch`) degrades to "no unrealized" instead. */
export async function fetchSettledMarketIds(): Promise<Set<string>> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb.from("settled_markets").select("market_id");
  if (error) throw new Error(error.message);
  return new Set((data ?? []).map((r) => String(r.market_id)));
}

/** All OPEN (not-yet-settled) paper fills, newest first, capped — the unrealized-PnL basis
 * (#398 WS3 step 19). A fill is open when its market has no `settled_markets` row. */
export async function fetchOpenFills(limit = 5000): Promise<PaperFill[]> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const settled = await fetchSettledMarketIds();
  const { data, error } = await sb
    .from("paper_fills")
    .select("*")
    .order("inserted_at", { ascending: false })
    .limit(limit);
  if (error) throw new Error(error.message);
  return ((data ?? []) as PaperFill[]).filter((f) => !settled.has(f.market_id));
}

/** A wallet's paper-fill tape, newest first (capped for display). */
export async function fetchFills(wallet: string, limit = 200): Promise<PaperFill[]> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("paper_fills")
    .select("*")
    .ilike("leader_wallet", wallet)
    .order("inserted_at", { ascending: false })
    .limit(limit);
  if (error) throw new Error(error.message);
  return (data ?? []) as PaperFill[];
}
