import type { SupabaseClient } from "@supabase/supabase-js";

import { getSupabase } from "./supabase";
import type { PaperFill, ServiceRuntime, WalletLiveStats } from "./types";

export class NotConfiguredError extends Error {
  constructor() {
    super("Supabase is not configured. Copy .env.example to .env.local.");
    this.name = "NotConfiguredError";
  }
}

const PAGE_SIZE = 1000;

/** Traverse a unique key so refreshed values cannot move rows across page boundaries. */
async function fetchPagedRows<Row>(
  sb: SupabaseClient,
  table: string,
  columns: string,
  key: keyof Row & string,
): Promise<Row[]> {
  const rows: Row[] = [];
  let cursor: string | null = null;
  for (;;) {
    let query = sb.from(table).select(columns).order(key, { ascending: true }).limit(PAGE_SIZE);
    if (cursor !== null) query = query.gt(key, cursor);
    const { data, error } = await query;
    if (error) throw new Error(error.message);
    if (!data) throw new Error(`The ${table} query returned no data.`);
    const page = data as Row[];
    rows.push(...page);
    if (page.length < PAGE_SIZE) return rows;
    cursor = String(page[page.length - 1][key]);
  }
}

/** All wallets in the historical-vs-watched view. Reads the `wallet_live_stats_mv` materialized
 * view (a periodically-refreshed cache of the `wallet_live_stats` aggregation) so the heavy
 * full-table aggregation runs on a schedule, not on every page view. */
export async function fetchWalletStats(): Promise<WalletLiveStats[]> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  return fetchPagedRows<WalletLiveStats>(sb, "wallet_live_stats_mv", "*", "wallet");
}

/** pe-service's current paper watchlist size ("N watched"), or null if not yet published. */
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

/** One wallet's historical-vs-watched row (wallet keys are lowercase). Reads the
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

export type WatchedUnavailableReason =
  | "not_configured"
  | "read_error"
  | "runtime_missing"
  | "token_changed"
  | "count_mismatch";

export type WatchedSetResult =
  | {
      status: "available";
      wallets: Set<string>;
      count: number;
      token: string;
    }
  | {
      status: "unavailable";
      reason: WatchedUnavailableReason;
      message: string;
    };

type WatchlistRow = { wallet_hex: unknown };

/** Validate the two-token read around the full watchlist traversal (#544). Exported for deterministic
 * unit proof; callers must never convert an unavailable result into an empty watchlist. */
export function guardWatchedSnapshot(
  before: ServiceRuntime,
  rows: WatchlistRow[],
  after: ServiceRuntime,
): WatchedSetResult {
  if (before.updated_at !== after.updated_at) {
    return {
      status: "unavailable",
      reason: "token_changed",
      message: "Watchlist changed while it was being read.",
    };
  }
  const beforeCount = Number(before.watchlist_size);
  const afterCount = Number(after.watchlist_size);
  if (
    !Number.isSafeInteger(beforeCount) ||
    beforeCount < 0 ||
    beforeCount !== afterCount ||
    rows.length !== afterCount
  ) {
    return {
      status: "unavailable",
      reason: "count_mismatch",
      message: "Watchlist row count does not match the service runtime snapshot.",
    };
  }
  return {
    status: "available",
    wallets: new Set(rows.map((row) => String(row.wallet_hex).toLowerCase())),
    count: afterCount,
    token: after.updated_at,
  };
}

type RuntimeRead = { data: ServiceRuntime | null; error: string | null };
type WatchlistRead = { data: WatchlistRow[] | null; error: string | null };

/** Execute runtime → rows → runtime with one retry when the tokens expose a concurrent replace. */
export async function readConsistentWatchedSet(
  readRuntime: () => Promise<RuntimeRead>,
  readWatchlist: () => Promise<WatchlistRead>,
): Promise<WatchedSetResult> {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const before = await readRuntime();
    if (before.error) {
      return { status: "unavailable", reason: "read_error", message: before.error };
    }
    if (!before.data) {
      return {
        status: "unavailable",
        reason: "runtime_missing",
        message: "The service runtime row is missing.",
      };
    }

    const watchlist = await readWatchlist();
    if (watchlist.error || !watchlist.data) {
      return {
        status: "unavailable",
        reason: "read_error",
        message: watchlist.error ?? "The watchlist query returned no data.",
      };
    }

    const after = await readRuntime();
    if (after.error) {
      return { status: "unavailable", reason: "read_error", message: after.error };
    }
    if (!after.data) {
      return {
        status: "unavailable",
        reason: "runtime_missing",
        message: "The service runtime row is missing.",
      };
    }

    const guarded = guardWatchedSnapshot(before.data, watchlist.data, after.data);
    if (guarded.status === "unavailable" && guarded.reason === "token_changed" && attempt === 0) {
      continue;
    }
    return guarded;
  }
  return {
    status: "unavailable",
    reason: "token_changed",
    message: "Watchlist changed during both read attempts.",
  };
}

/** Lowercase `wallet_hex` set pe-service is actively copying. Reads the runtime token before and
 * after the rows and retries one observed race. Read failures and inconsistent snapshots return a
 * typed unavailable state, distinct from a valid empty projection (#544). */
export async function fetchWatchedSet(): Promise<WatchedSetResult> {
  const sb = getSupabase();
  if (!sb) {
    return {
      status: "unavailable",
      reason: "not_configured",
      message: "Supabase is not configured.",
    };
  }
  try {
    return await readConsistentWatchedSet(
      async () => {
        const result = await sb
          .from("service_runtime")
          .select("watchlist_size, updated_at")
          .eq("id", 1)
          .maybeSingle();
        return {
          data: (result.data as ServiceRuntime | null) ?? null,
          error: result.error?.message ?? null,
        };
      },
      async () => ({
        data: await fetchPagedRows<WatchlistRow>(sb, "service_watchlist", "wallet_hex", "wallet_hex"),
        error: null,
      }),
    );
  } catch (error) {
    return {
      status: "unavailable",
      reason: "read_error",
      message: error instanceof Error ? error.message : "Watchlist read failed.",
    };
  }
}

/** Market ids that have resolved (have a `settled_markets` row) — used to split open vs settled
 * fills for the unrealized-PnL feature. Fail-CLOSED (throws on read error, not soft-fail): a silent
 * empty set would mislabel already-settled fills as open and double-count realized P&L as
 * unrealized. The caller (`fetchOpenFills` → page `.catch`) degrades to "no unrealized" instead. */
export async function fetchSettledMarketIds(): Promise<Set<string>> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const rows = await fetchPagedRows<{ market_id: string }>(sb, "settled_markets", "market_id", "market_id");
  return new Set(rows.map((row) => row.market_id));
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
