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
