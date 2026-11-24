import type { SupabaseClient } from "@supabase/supabase-js";

import { type DailyPnl, type SettledMarket, dailyPnl } from "./daily-pnl";
import { getSupabase } from "./supabase";
import type { OpenFill, PaperFill, ServiceRuntime, WalletLiveStats } from "./types";

export class NotConfiguredError extends Error {
  constructor() {
    super("Supabase is not configured. Copy .env.example to .env.local.");
    this.name = "NotConfiguredError";
  }
}

const PAGE_SIZE = 1000;

/** Traverse a unique key so refreshed values cannot move rows across page boundaries. Each page carries a
 * fresh signal, which opts it out of Next's per-render fetch deduplication: a repeated traversal (the watched
 * read's retry) must read current rows, not replay the first. `from` (inclusive) and `to` (exclusive) limit the
 * traversal to one range of the key. */
async function fetchPagedRows<Row>(
  sb: SupabaseClient,
  table: string,
  columns: string,
  key: keyof Row & string,
  from?: string,
  to?: string,
): Promise<Row[]> {
  const rows: Row[] = [];
  let cursor: string | null = null;
  for (;;) {
    let query = sb.from(table).select(columns).order(key, { ascending: true }).limit(PAGE_SIZE);
    if (cursor !== null) query = query.gt(key, cursor);
    else if (from !== undefined) query = query.gte(key, from);
    if (to !== undefined) query = query.lt(key, to);
    const { data, error } = await query.abortSignal(new AbortController().signal);
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
  try {
    const sb = getSupabase();
    if (!sb) {
      return {
        status: "unavailable",
        reason: "not_configured",
        message: "Supabase is not configured.",
      };
    }
    return await readConsistentWatchedSet(
      async () => {
        // Like every page read, the token read carries a signal so its repeats reach the database.
        const result = await sb
          .from("service_runtime")
          .select("watchlist_size, updated_at")
          .eq("id", 1)
          .abortSignal(new AbortController().signal)
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

/** Fill keys are `wf|{wallet}|…`: these bounds split them by the wallet's first hex digit into four ranges that
 * are read side by side. The outer ranges are unbounded, so every key is in exactly one. */
const FILL_KEY_BOUNDS = [undefined, "wf|0x4", "wf|0x8", "wf|0xc", undefined];

/** The overview's paper results from one read of every fill and every settlement: realized P&L per
 * Central Time day, and the open (not-yet-settled) fills that are the unrealized-P&L basis (#398 WS3
 * step 19). Every read pages a unique key, so neither table is truncated at the API's row cap. */
export async function fetchPaperResults(): Promise<{ daily: DailyPnl[]; openFills: OpenFill[] }> {
  const sb = getSupabase();
  if (!sb) throw new NotConfiguredError();
  const [fillRanges, settled] = await Promise.all([
    Promise.all(
      FILL_KEY_BOUNDS.slice(1).map((to, i) =>
        fetchPagedRows<OpenFill & Pick<PaperFill, "idempotency_key">>(
          sb,
          "paper_fills",
          "idempotency_key,leader_wallet,market_id,outcome_id,side,contracts,fill_price,principal,fee",
          "idempotency_key",
          FILL_KEY_BOUNDS[i],
          to,
        ),
      ),
    ),
    fetchPagedRows<SettledMarket>(sb, "settled_markets", "market_id,outcome_prices,settled_at_unix", "market_id"),
  ]);
  const fills = fillRanges.flat();
  const settledIds = new Set(settled.map((market) => market.market_id));
  return {
    daily: dailyPnl(fills, settled),
    openFills: fills.filter((fill) => !settledIds.has(fill.market_id)),
  };
}
