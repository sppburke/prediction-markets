import { getSupabase } from "./supabase";
import type { PaperFill, WalletLiveStats } from "./types";

export class NotConfiguredError extends Error {
  constructor() {
    super("Supabase is not configured. Copy .env.example to .env.local.");
    this.name = "NotConfiguredError";
  }
}

/** All wallets in the historical-vs-live view. */
export async function fetchWalletStats(): Promise<WalletLiveStats[]> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("wallet_live_stats")
    .select("*")
    .order("live_realized_pnl", { ascending: false, nullsFirst: false });
  if (error) throw new Error(error.message);
  return (data ?? []) as WalletLiveStats[];
}

/** One wallet's historical-vs-live row (wallet keys are lowercase in the view). */
export async function fetchWalletStat(wallet: string): Promise<WalletLiveStats | null> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const { data, error } = await sb
    .from("wallet_live_stats")
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
