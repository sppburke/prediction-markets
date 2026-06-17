import type { Numeric } from "./format";

// One row of the Supabase `wallet_live_stats` view (scripts/supabase_schema.sql).
// NUMERIC columns arrive from supabase-js as strings; counts as numbers. Both are
// modelled as `Numeric` and rendered through lib/format.ts.
export interface WalletLiveStats {
  wallet: string;
  // ── live (paper) side ──
  live_total_fills: Numeric;
  live_settled_count: Numeric;
  live_open_fills: Numeric;
  live_wins: Numeric;
  live_win_rate: Numeric; // fraction 0..1, null until any settled
  live_realized_pnl: Numeric; // USD
  live_edge: Numeric; // USD per settled trade, null until any settled
  // ── historical (ranker) side, from the current latest_ranking batch ──
  rank: Numeric; // null if aged out of the current batch
  ls_edge: Numeric;
  ls_tstat: Numeric;
  fill_rate: Numeric; // fraction 0..1
  n_trades: Numeric;
  hit_rate: Numeric; // fraction 0..1 — historical win-rate (no win_rate_bps column)
  avg_price: Numeric; // mid price 0..1
  last_trade_unix: Numeric; // epoch s of the wallet's real last on-chain trade at the last rank push (#357); null if aged out / pre-#357 batch
}

// The single `service_runtime` row: pe-service's current live watchlist size (the wallets
// it actually copies). Published by the service because the count is in-memory only and the
// site cannot derive it from the ranking. Absent/0 until the service has published once.
export interface ServiceRuntime {
  watchlist_size: Numeric;
  updated_at: string;
}

// One row of the anon-readable `paper_fills` table (per-wallet detail tape).
export interface PaperFill {
  idempotency_key: string;
  leader_wallet: string;
  source_trade_id: string | null;
  market_id: string;
  outcome_id: number;
  side: string; // 'buy' | 'sell'
  contracts: Numeric;
  fill_price: Numeric;
  entry_unix: number | null;
  event_seq: number;
  inserted_at: string;
}
