import type { Numeric } from "./format";

// One row of the Supabase `wallet_live_stats` view (scripts/supabase_schema.sql).
// NUMERIC columns arrive from supabase-js as strings; counts as numbers. Both are
// modelled as `Numeric` and rendered through lib/format.ts.
export interface WalletLiveStats {
  wallet: string;
  // ── watched (paper) side; legacy DB field prefix stays `live_` ──
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
  fill_rate: Numeric; // repricing coverage 0..1 (#536: fraction of positions repriced at the minute reference; wire name kept)
  n_trades: Numeric; // repriced positions in the eval window
  hit_rate: Numeric; // fraction 0..1 — outcome rate among repriced positions (no win_rate_bps column)
  avg_price: Numeric; // mid price 0..1
  last_trade_unix: Numeric; // epoch s of the wallet's real last on-chain trade at the last rank push (#357); null if aged out / pre-#357 batch
}

// The single `service_runtime` row: pe-service's current paper watchlist size (the wallets
// it actually copies). Published by the service because the count is in-memory only and the
// site cannot derive it from the ranking. Absent/0 until the service has published once.
export interface ServiceRuntime {
  watchlist_size: Numeric;
  updated_at: string;
}

// One `service_config` KV row (#398 WS1). `value_type` drives the admin panel's typed input.
export type ConfigValueType = "bool" | "integer" | "decimal" | "text";
export interface ServiceConfigRow {
  key: string;
  value: string;
  value_type: ConfigValueType;
  description: string | null;
  updated_by: string | null;
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

// Display-safe #508 account control state. Credential ciphertext is intentionally absent.
export interface AccountAdminRow {
  account_id: string;
  is_primary: boolean;
  login_email: string | null;
  enabled: boolean;
  execution_order: number;
  requested_live_mode: "off" | "live_tiny";
  effective_live_mode: "off" | "live_tiny";
  live_sizing_mode: "kelly" | "dollar" | "contract" | null;
  live_sizing_dollar_usd: Numeric;
  live_sizing_contracts: Numeric;
  live_price_impact_cap_bps: number;
  created_at: string;
  updated_at: string;
  credentials: AccountCredentialMetadata | null;
}

export interface AccountCredentialMetadata {
  bundle_version: number;
  key_id: string;
  fingerprint: string;
  updated_at: string;
}

export interface LiveFill extends PaperFill {
  account_id: string;
}

export interface LivePosition {
  account_id: string;
  market_id: string;
  outcome_id: number;
  long_contracts: Numeric;
  short_contracts: Numeric;
  cost_basis: Numeric;
}

export interface LiveAccountState {
  account_id: string;
  free_collateral: Numeric;
  reserved: Numeric;
  unredeemed_value: Numeric;
  last_reconciled_at: string | null;
  admission_closed_reason: string | null;
}
