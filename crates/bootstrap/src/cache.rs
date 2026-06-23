//! Permanent wallet trade-history cache — SQLite (WAL mode), no TTL.
//!
//! Key tables:
//! - `trades` — append-only per-trade rows, indexed by `(wallet_hex, timestamp_unix)`.
//! - `leaderboard_snapshots` — `(snapshot_at_unix, wallet_hex)` rows, one row-set per
//!   `pe-bootstrap` run, written from the post-filtered watchlist. Read by
//!   `pe-backtest` to constrain the candidate pool at each simulated week boundary.
//! - `market_resolutions` — one row per resolved market from the Gamma API.
//!   `winning_outcome_id NULL` means voided/non-binary — the backtest skips these.
//! - `market_schedules` — one row per market whose scheduled `endDate` has been
//!   fetched from Gamma. `end_date_unix NULL` means Gamma had no `endDate` for this
//!   market (it is still in the skip-set to avoid re-fetching).
//! - `wallets` — canonical wallet pile (issue #166). One row per known wallet across
//!   every discovery source (`wallet_set.json`, `trades`, Dune CSV, Dune incremental,
//!   Polymarket leaderboard, Radion, 502-gap). `is_active` is sticky (0→1 only) and
//!   controls which wallets the `backfill` subcommand processes.
//!
//! WAL mode provides per-commit durability — no atomic-rename or checkpoint batching
//! is needed. Per-wallet streaming reads keep peak memory bounded.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;

use const_format::concatcp;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rusqlite::{Connection, OpenFlags, params};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::error::BootstrapError;

/// Row tuple for [`WalletCache::upsert_wallets_bulk`].
///
/// Fields (in order): `wallet_hex`, `source_bits`, `is_infra`, `dune_first_seen_unix`,
/// `dune_closed_markets`, `dune_win_rate_bps`, `polymarket_contracts_seen` (issue #186 —
/// a V1/V2 CTF-exchange attribution bitmask: bit0 = V1, bit1 = V2). The 7th field is
/// `0` for every caller now that on-chain enumeration was removed (#326); Dune
/// enumeration leaves it unset.
pub type WalletUpsertRow = (
    String,
    i64,
    bool,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    i64,
);

/// Number of consecutive known `source_trade_id`s that signals the incremental fetch is done.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const INCREMENTAL_STOP_THRESHOLD: usize = 3;

/// Wallets deleted per transaction in [`WalletCache::purge_wallets`] (issue #385).
/// Bounds WAL growth + lock-hold time per commit; a chunk-boundary crash leaves a
/// consistent partial state a re-run completes idempotently.
const PURGE_CHUNK: usize = 1_000;

/// DDL for the two non-lookup `trades` secondary indexes that an armed
/// `pe-bootstrap purge` drops before its bulk delete and rebuilds after VACUUM
/// (issue #401). Shared by `SCHEMA` (assembled via `concatcp!` below) and
/// [`WalletCache::create_trades_bulk_delete_indexes`] so the on-open definition
/// and the rebuild physically cannot diverge — the `CREATE INDEX IF NOT EXISTS`
/// SCHEMA-on-open backstop heals an *absent* index, never a *divergent* one.
const IDX_TRADES_MARKET_ID_DDL: &str =
    "CREATE INDEX IF NOT EXISTS idx_trades_market_id ON trades(market_id);";
const IDX_TRADES_BUY_MARKET_OUTCOME_WALLET_TS_DDL: &str =
    "CREATE INDEX IF NOT EXISTS idx_trades_buy_market_outcome_wallet_ts
    ON trades(side, market_id, outcome_id, wallet_hex, timestamp_unix);";

const SCHEMA: &str = concatcp!(
    "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS trades (
    source_trade_id TEXT    PRIMARY KEY NOT NULL,
    wallet_hex      TEXT    NOT NULL,
    market_id       TEXT    NOT NULL,
    outcome_id      INTEGER NOT NULL,
    side            TEXT    NOT NULL CHECK(side IN ('buy', 'sell')),
    price_str       TEXT    NOT NULL,
    contracts       INTEGER NOT NULL,
    timestamp_unix  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet_hex, timestamp_unix);
-- Issue #197 follow-up: index on market_id alone so `all_market_ids`
-- (`SELECT DISTINCT market_id FROM trades ORDER BY market_id`, the only
-- full-trades scan in the resolutions pipeline) becomes an index-only
-- DISTINCT scan instead of a ~100 GB heap scan. Costs one extra B-tree
-- update per trade insert; worth it given the scan runs every resolutions
-- and `all` invocation.
",
    IDX_TRADES_MARKET_ID_DDL,
    "
-- Sub-task #3 (first_mover_percentile_bps): covering index for the
-- cross-wallet rank-index build query (`SELECT market_id, outcome_id,
-- wallet_hex, MIN(timestamp_unix) FROM trades WHERE side='buy' AND
-- timestamp_unix<=? GROUP BY market_id, outcome_id, wallet_hex`).
-- Without this, the planner uses `idx_trades_market_id` + TEMP B-TREE
-- for GROUP BY (>22 min on the live 269M-row trades table). With it
-- the planner does an index-only scan in the proper order, finding
-- each group's MIN(timestamp_unix) from the first matching entry.
",
    IDX_TRADES_BUY_MARKET_OUTCOME_WALLET_TS_DDL,
    "

CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
    snapshot_at_unix INTEGER NOT NULL,
    wallet_hex       TEXT    NOT NULL,
    PRIMARY KEY (snapshot_at_unix, wallet_hex)
);
CREATE INDEX IF NOT EXISTS idx_snapshots_at ON leaderboard_snapshots(snapshot_at_unix);

CREATE TABLE IF NOT EXISTS market_resolutions (
    market_id           TEXT    PRIMARY KEY NOT NULL,
    winning_outcome_id  INTEGER NULL,
    resolved_at_unix    INTEGER NOT NULL,
    fetched_at_unix     INTEGER NOT NULL,
    source              TEXT    NOT NULL DEFAULT 'gamma'
);
CREATE INDEX IF NOT EXISTS idx_resolutions_resolved_at
    ON market_resolutions(resolved_at_unix);

CREATE TABLE IF NOT EXISTS market_schedules (
    market_id       TEXT    PRIMARY KEY NOT NULL,
    end_date_unix   INTEGER NULL,
    fetched_at_unix INTEGER NOT NULL,
    source          TEXT    NOT NULL DEFAULT 'gamma'
);

-- Coarse pre-resolution CLOB price series per (market, token) for the ranker CLV bake-off
-- (issue #421 PR4). `market_id` is the `0x` condition id (join key to trades.market_id); `token_id`
-- is the decimal-string CLOB asset id (the `prices-history?market=` query param); `t` is the sample
-- unix second; `price` is the mid stored as a decimal string (TEXT, like trades.price_str — no f64
-- round-trip). Populated by the `prices-history` subcommand; `INSERT OR IGNORE` on the PK makes the
-- backfill resumable. The PK's (market_id, token_id) prefix also serves the per-token resume check,
-- so no secondary index is needed.
CREATE TABLE IF NOT EXISTS market_price_history (
    market_id TEXT    NOT NULL,
    token_id  TEXT    NOT NULL,
    t         INTEGER NOT NULL,
    price     TEXT    NOT NULL,
    PRIMARY KEY (market_id, token_id, t)
);

-- Maps Polymarket conditionId → Gamma event (issue #206). One condition belongs
-- to exactly one event; events group multiple markets (e.g. neg-risk bundles),
-- which is the unit the sign-randomization skill test randomizes over. Populated
-- by the `events` subcommand (Gamma /events bulk sweep). Orphan markets with no
-- Gamma event match self-map: event_id = condition_id, event_slug = NULL.
-- condition_id is the same `0x`-prefixed form as trades.market_id (join key).
CREATE TABLE IF NOT EXISTS market_events (
    condition_id    TEXT    PRIMARY KEY NOT NULL,
    event_id        TEXT    NOT NULL,
    event_slug      TEXT    NULL,
    fetched_at_unix INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_events_event_id ON market_events(event_id);

-- ERC-1155 position-token id -> conditionId map (issue #207, Slice 0).
-- token_id is the decimal-string uint256 form Gamma returns in `clobTokenIds`;
-- on-chain OrderFilled logs carry the same id as a 32-byte word (consumers
-- normalise to decimal before joining). condition_id is the `0x`-prefixed form
-- shared with trades.market_id / market_events. Lets the on-chain OrderFilled
-- legs (keyed by token id) be resolved to a market.
-- `outcome_index` (0-based positional outcome ordinal: 0=YES,1=NO for binary)
-- is added by migration in `open()` (issue #429); legacy rows stay NULL.
CREATE TABLE IF NOT EXISTS token_conditions (
    token_id        TEXT    PRIMARY KEY NOT NULL,
    condition_id    TEXT    NOT NULL,
    fetched_at_unix INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_token_conditions_condition ON token_conditions(condition_id);

-- Per-market Polymarket taker/maker fee schedule (issue #23, PR 1).
-- Populated by the `events` subcommand from Gamma takerBaseFee / makerBaseFee.
-- Fees are stored in basis points (0..=10_000) so all comparisons are integer.
-- `fee_active_from_unix` is NULL until PR 4 backfills it from counterparty_edges.
CREATE TABLE IF NOT EXISTS market_fees (
    condition_id         TEXT    PRIMARY KEY NOT NULL,
    taker_base_fee_bps   INTEGER NOT NULL,
    maker_base_fee_bps   INTEGER NOT NULL,
    fee_active_from_unix INTEGER,
    fetched_at_unix      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS market_liquidity (
    market_id        TEXT    PRIMARY KEY NOT NULL,
    liquidity_usd_str TEXT   NOT NULL,
    fetched_at_unix  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cursor (
    key        TEXT    PRIMARY KEY NOT NULL,
    value      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Wallet pile (issue #166). `wallet_hex` is the canonical form produced by
-- `WalletAddress::Display`: `\"0x\" + 40 lowercase hex chars`.
-- `source_bits`: bit0=wallet_set_json, bit1=trades, bit4=leaderboard,
-- bit5=radion, bit6=gap502. (bit2/bit3 were dune_csv/dune_incr, removed in
-- #335; the gap is intentional — `source_bits` is persisted, do not renumber.)
-- `is_active` is sticky 0→1; `is_infra` is also sticky once set.
CREATE TABLE IF NOT EXISTS wallets (
    wallet_hex               TEXT    PRIMARY KEY NOT NULL,
    is_active                INTEGER NOT NULL DEFAULT 0,
    trade_count              INTEGER NOT NULL DEFAULT 0,
    is_infra                 INTEGER NOT NULL DEFAULT 0,
    source_bits              INTEGER NOT NULL DEFAULT 0,
    last_polymarket_fetch_at INTEGER NULL,
    last_funder_fetch_at     INTEGER NULL,
    dune_first_seen_unix     INTEGER NULL,
    dune_closed_markets      INTEGER NULL,
    dune_win_rate_bps        INTEGER NULL,
    discovered_at_unix       INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
    polymarket_contracts_seen INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_wallets_is_active ON wallets(is_active);
CREATE INDEX IF NOT EXISTS idx_wallets_backfill
    ON wallets(is_active, last_polymarket_fetch_at) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_wallets_weekly
    ON wallets(is_active, last_funder_fetch_at) WHERE is_active = 1;

-- Issue #197: centralised filter for wallets that are simultaneously active
-- AND not flagged as infrastructure. All wallet-selection queries should
-- query this view instead of filtering ad-hoc, so future queries cannot
-- accidentally include infra-flagged wallets.
CREATE VIEW IF NOT EXISTS active_tradeable_wallets AS
    SELECT * FROM wallets WHERE is_active = 1 AND is_infra = 0;

-- Cached result of `load_first_mover_rank_index` keyed by `cutoff_unix`.
-- One row per (cutoff_unix, market_id, outcome_id) group; `ts_json` is a
-- JSON array of i64 first-buy timestamps (one per wallet, sorted ascending).
-- Storing the whole group in one row avoids PK collisions when two wallets
-- share the same first-buy timestamp on the same (market, outcome) pair.
-- Populated by the extract pipeline after the first build; subsequent
-- re-extracts at the same cutoff skip the full GROUP BY scan on `trades`.
-- LIMITATION: the cache is valid for a fixed `trades` table state. If
-- bootstrap backfills pre-cutoff trades after the cache is built, the
-- cached timestamps will be stale. Normal delta-mode bootstrap only appends
-- trades with `timestamp_unix` beyond the latest processed block, so
-- historical cutoffs are unaffected in the standard workflow.
CREATE TABLE IF NOT EXISTS first_mover_rank_cache (
    cutoff_unix  INTEGER NOT NULL,
    market_id    TEXT    NOT NULL,
    outcome_id   INTEGER NOT NULL,
    ts_json      TEXT    NOT NULL,
    PRIMARY KEY (cutoff_unix, market_id, outcome_id)
);
CREATE INDEX IF NOT EXISTS idx_fmrc_cutoff
    ON first_mover_rank_cache(cutoff_unix);

-- Tombstones for wallets hard-deleted by `pe-bootstrap purge` (issue #385).
-- Only rule-A `proven_loser` deletions write a row here; rule-B `dead_weight`
-- deletions write NO row (discovery may freely re-find them). The
-- `upsert_wallets_bulk` gate skips re-inserting any wallet listed here UNLESS the
-- incoming row carries an override bit (leaderboard/radion = `source_bits & 48`),
-- which DELETEs the row (lifts the tombstone) and re-admits the wallet. `reason`
-- is kept as TEXT for forward-compat / auditing.
CREATE TABLE IF NOT EXISTS purged_wallets (
    wallet_hex     TEXT    PRIMARY KEY NOT NULL,
    purged_at_unix INTEGER NOT NULL,
    reason         TEXT    NOT NULL
);
"
);

/// Result of `classify_infra_retroactive` (issue #197).
#[derive(Debug, Default, Clone, Copy)]
pub struct ClassifyInfraReport {
    /// Eligibility set: wallets with ≥500 trades in cache (the denominator).
    pub scanned: usize,
    /// Wallets meeting the infra threshold. Equal in dry-run and apply mode
    /// — `dry_run` only controls whether the `mark_infra` UPDATE ran.
    pub flagged: usize,
    /// Whether this run was a preview (no writes).
    pub dry_run: bool,
}

/// Backtest-readiness coverage counts (issue #208).
///
/// Every field is a *gap* count: `0` everywhere means the cache is ready for a
/// backtest. Produced by [`WalletCache::coverage_counts`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CoverageReport {
    /// Active, non-infra wallets never fetched from Polymarket
    /// (`last_polymarket_fetch_at IS NULL`).
    pub fetch_incomplete: usize,
    /// Distinct traded markets with no `market_resolutions` row.
    pub missing_resolution: usize,
    /// Distinct traded markets with no `market_schedules` row.
    pub missing_schedule: usize,
}

impl CoverageReport {
    /// True when every gap count is zero — the cache is backtest-ready.
    pub fn is_clean(&self) -> bool {
        self.fetch_incomplete == 0 && self.missing_resolution == 0 && self.missing_schedule == 0
    }
}

/// Why a wallet was selected for deletion by `pe-bootstrap purge` (issue #385).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeReason {
    /// Eligible per the ranker CSV but a proven money-loser (rule A). Deleted
    /// **and tombstoned** so a non-override discovery source cannot silently
    /// re-ingest it.
    ProvenLoser,
    /// Active, not eligible, and long-dormant dead weight (rule B). Deleted with
    /// **no tombstone** — discovery may re-find it.
    DeadWeight,
}

impl PurgeReason {
    /// Tag string stored in `purged_wallets.reason` (proven losers only).
    const fn as_str(self) -> &'static str {
        match self {
            PurgeReason::ProvenLoser => "proven_loser",
            PurgeReason::DeadWeight => "dead_weight",
        }
    }

    /// Whether a `purged_wallets` tombstone is written for this reason.
    const fn tombstoned(self) -> bool {
        matches!(self, PurgeReason::ProvenLoser)
    }
}

/// One wallet selected for deletion by [`WalletCache::purge_wallets`] (issue #385).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeRow {
    pub wallet_hex: String,
    pub reason: PurgeReason,
}

/// Outcome of [`WalletCache::purge_wallets`] (issue #385). In `dry_run` mode the
/// `*_deleted` counts are estimates (trades via `wallets.trade_count`, snapshots
/// via `COUNT`) of what an armed run *would* remove; nothing is written.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PurgeReport {
    /// Rule-A (proven-loser) wallets deleted + tombstoned (would-delete in dry-run).
    pub proven_losers_deleted: usize,
    /// Rule-B (dead-weight) wallets deleted, no tombstone (would-delete in dry-run).
    pub dead_weight_deleted: usize,
    /// `trades` rows deleted across all purged wallets (estimate in dry-run).
    pub trades_deleted: usize,
    /// `leaderboard_snapshots` rows deleted across all purged wallets.
    pub snapshots_deleted: usize,
    /// Tombstones written to `purged_wallets` (== `proven_losers_deleted`).
    pub tombstones_written: usize,
    /// True when this was a preview (`dry_run`) — nothing was written.
    pub dry_run: bool,
}

/// Per-market fee schedule row loaded from `market_fees` (issue #23).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketFeeRow {
    pub taker_base_fee_bps: i32,
    pub maker_base_fee_bps: i32,
}

/// Permanent wallet trade-history cache backed by SQLite.
/// One `(market, token)` work item for the `prices-history` CLOB backfill (issue #421 PR4), with the
/// per-market close reference the pre-resolution window is anchored on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriceBackfillTarget {
    /// `0x` condition id (= `trades.market_id`).
    pub market_id: String,
    /// CLOB token/asset id (the `prices-history?market=` query param).
    pub token_id: String,
    /// Close reference, unix seconds: `market_schedules.end_date_unix` when known, else
    /// `market_resolutions.resolved_at_unix`. The fetch window is `[close_ref − window, close_ref]`.
    pub close_ref_unix: i64,
}

pub struct WalletCache {
    conn: Connection,
}

impl WalletCache {
    /// Open or create the SQLite database at `path`. Runs schema migrations.
    pub fn open(path: &Path) -> Result<Self, BootstrapError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.execute_batch(SCHEMA)?;
        // Migration (#326 PR4): drop the operator/funder/delta tables. They fed
        // only the deleted operator-graph machinery; dropping reclaims the bulk of
        // the cache (`counterparty_edges` alone was ~275M rows). Idempotent — a
        // no-op once dropped, and the tables are no longer in SCHEMA so fresh DBs
        // never recreate them. `token_conditions` + `market_fees` are KEPT (still
        // written by the surviving `events` sweep).
        conn.execute_batch(
            "DROP TABLE IF EXISTS counterparty_edges; \
             DROP TABLE IF EXISTS funder_edges; \
             DROP TABLE IF EXISTS funder_lookup_done; \
             DROP TABLE IF EXISTS delta_audit;",
        )?;
        // Migration: add `source` column to market_resolutions / market_schedules. DBs
        // created before this change keep `DEFAULT 'gamma'` — correct since every
        // pre-migration row was inserted by the Gamma fetcher.
        add_column_if_missing(
            &conn,
            "market_resolutions",
            "source",
            "TEXT NOT NULL DEFAULT 'gamma'",
        )?;
        add_column_if_missing(
            &conn,
            "market_schedules",
            "source",
            "TEXT NOT NULL DEFAULT 'gamma'",
        )?;
        // Migration (issue #421 PR4): add `start_date_unix` (Gamma `createdAt`) to market_schedules
        // for the entry-timing CLV feature. Existing rows stay NULL until the `prices-history`
        // subcommand backfills them; an unfetched market remaining NULL is the honest "unknown
        // creation time" sentinel (no downstream gate treats NULL as eligible).
        add_column_if_missing(&conn, "market_schedules", "start_date_unix", "INTEGER NULL")?;
        // Migration (issue #429): add `outcome_index` (0-based positional outcome
        // ordinal: 0=YES,1=NO for binary) to `token_conditions` so the trades /
        // price-series join can map `trades.outcome_id` → token. Existing rows
        // (events-sourced before this change) stay NULL until a re-run; the
        // downstream true_clv join skips NULL. Mirrors the `start_date_unix`
        // nullable-migration precedent above.
        add_column_if_missing(&conn, "token_conditions", "outcome_index", "INTEGER NULL")?;
        // Migration (issue #176): add `last_polymarket_full_at` if absent. DBs
        // created before this change keep NULL — picked up by the weekly
        // paranoia full-fetch on first delta-mode run, which then stamps each
        // successfully-fetched wallet. Mirrors the `event_at_unix` migration
        // pattern (no DEFAULT clause; NULL means "needs full fetch").
        let last_polymarket_full_at_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('wallets') WHERE name='last_polymarket_full_at'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        if !last_polymarket_full_at_exists {
            conn.execute_batch(
                "ALTER TABLE wallets ADD COLUMN last_polymarket_full_at INTEGER NULL",
            )?;
        }

        // Migration (issue #186): add `polymarket_contracts_seen` bitmask if
        // absent. Pre-migration rows default to 0 ("no V1/V2 attribution
        // available"); enumeration populates via UPSERT OR-merge on each
        // (wallet, topic) discovery. Live-execution callers must treat 0 as
        // "unknown, route by liquidity" rather than "neither V1 nor V2."
        let polymarket_contracts_seen_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('wallets') WHERE name='polymarket_contracts_seen'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        if !polymarket_contracts_seen_exists {
            conn.execute_batch(
                "ALTER TABLE wallets ADD COLUMN polymarket_contracts_seen INTEGER NOT NULL DEFAULT 0",
            )?;
        }

        Ok(Self { conn })
    }

    /// Open the cache **read-only**, skipping schema creation and migrations.
    ///
    /// Used by the read-only `coverage` probe (issue #208): it must not create
    /// the file, must not run DDL (a `SQLITE_OPEN_READ_ONLY` connection cannot),
    /// and must not take the `CacheMutationLock`. The mutating subcommands
    /// create and migrate the database via [`Self::open`]; this opener only
    /// reads existing tables and views.
    ///
    /// # Precondition
    /// The database at `path` must already exist and have been migrated (opened
    /// at least once via [`Self::open`]). A nonexistent path errors rather than
    /// being created.
    pub fn open_read_only(path: &Path) -> Result<Self, BootstrapError> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self { conn })
    }

    /// Insert trades not already present for `wallet_hex`. Idempotent on `source_trade_id`.
    ///
    /// Returns the count of rows actually inserted — `INSERT OR IGNORE`
    /// returns 0 for duplicates and 1 for new rows, so the sum across the
    /// batch is exactly the number of new trades persisted. Issue #176 uses
    /// this count to populate `FetchOutcome::new_trades` for the delta-audit
    /// `new_trades_fetched` column without a pre/post `MAX(timestamp_unix)`
    /// snapshot pass.
    ///
    /// `trades` ordering is unimportant. All inserts run in a single
    /// transaction for atomicity and write batching.
    pub fn insert_new(
        &mut self,
        wallet_hex: &str,
        trades: Vec<RawTrade>,
    ) -> Result<usize, BootstrapError> {
        if trades.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut inserted: usize = 0;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO trades \
                 (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for t in &trades {
                let contracts_i64 =
                    i64::try_from(t.contracts.0).map_err(|_| BootstrapError::Internal)?;
                let rows = stmt.execute(params![
                    t.source_trade_id.0,
                    wallet_hex,
                    t.market_id.0.0,
                    i64::from(t.outcome_id),
                    side_to_str(&t.side),
                    t.price.0.to_string(),
                    contracts_i64,
                    t.timestamp.0.unix_timestamp(),
                ])?;
                inserted = inserted.saturating_add(rows);
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Return all `source_trade_id`s known for `wallet_hex`, newest-first.
    ///
    /// # Precondition
    /// Returns an empty `Vec` if the wallet has never been seen.
    pub fn known_trade_ids(&self, wallet_hex: &str) -> Vec<SourceTradeId> {
        let mut stmt = match self.conn.prepare(
            "SELECT source_trade_id FROM trades \
             WHERE wallet_hex = ?1 ORDER BY timestamp_unix DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map(params![wallet_hex], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).map(SourceTradeId).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Return all trades for `wallet_hex` sorted by timestamp ascending (FIFO order).
    ///
    /// # Precondition
    /// Returns an empty `Vec` if the wallet has never been seen.
    pub fn trades_for(&self, wallet_hex: &str) -> Vec<RawTrade> {
        let Ok(wallet) = WalletAddress::from_hex(wallet_hex) else {
            return Vec::new();
        };
        let mut stmt = match self.conn.prepare(
            "SELECT source_trade_id, market_id, outcome_id, side, price_str, contracts, timestamp_unix \
             FROM trades WHERE wallet_hex = ?1 ORDER BY timestamp_unix ASC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map(params![wallet_hex], |r| {
            let id: String = r.get(0)?;
            let market_id: String = r.get(1)?;
            let outcome_id: i64 = r.get(2)?;
            let side: String = r.get(3)?;
            let price_str: String = r.get(4)?;
            let contracts: i64 = r.get(5)?;
            let ts: i64 = r.get(6)?;
            Ok((id, market_id, outcome_id, side, price_str, contracts, ts))
        });
        let Ok(iter) = rows else {
            return Vec::new();
        };
        iter.filter_map(Result::ok)
            .filter_map(
                |(id, market_id, outcome_id, side, price_str, contracts, ts)| {
                    row_to_trade(
                        wallet, id, market_id, outcome_id, &side, &price_str, contracts, ts,
                    )
                },
            )
            .collect()
    }

    /// Return the hex addresses of every wallet present in the cache, lexicographically sorted.
    pub fn all_wallet_addresses(&self) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT DISTINCT wallet_hex FROM trades ORDER BY wallet_hex")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Return every trade in the cache, sorted by timestamp ascending. Used by the
    /// backtest binary's walk-forward simulation, which requires global timestamp order.
    ///
    /// Memory: O(total_trades). For per-wallet streaming, use [`Self::trades_for`].
    pub fn all_trades(&self) -> Vec<RawTrade> {
        let mut stmt = match self.conn.prepare(
            "SELECT source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix \
             FROM trades ORDER BY timestamp_unix ASC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |r| {
            let id: String = r.get(0)?;
            let wallet_hex: String = r.get(1)?;
            let market_id: String = r.get(2)?;
            let outcome_id: i64 = r.get(3)?;
            let side: String = r.get(4)?;
            let price_str: String = r.get(5)?;
            let contracts: i64 = r.get(6)?;
            let ts: i64 = r.get(7)?;
            Ok((
                id, wallet_hex, market_id, outcome_id, side, price_str, contracts, ts,
            ))
        });
        let Ok(iter) = rows else {
            return Vec::new();
        };
        iter.filter_map(Result::ok)
            .filter_map(
                |(id, wallet_hex, market_id, outcome_id, side, price_str, contracts, ts)| {
                    let wallet = WalletAddress::from_hex(&wallet_hex).ok()?;
                    row_to_trade(
                        wallet, id, market_id, outcome_id, &side, &price_str, contracts, ts,
                    )
                },
            )
            .collect()
    }

    /// Total number of unique trades stored across all wallets.
    pub fn trade_count(&self) -> usize {
        self.conn
            .query_row("SELECT COUNT(*) FROM trades", [], |r| r.get::<_, i64>(0))
            .ok()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0)
    }

    /// Number of distinct wallets present in the cache.
    #[cfg(any(test, feature = "scenario"))]
    pub fn wallet_count(&self) -> usize {
        self.conn
            .query_row("SELECT COUNT(DISTINCT wallet_hex) FROM trades", [], |r| {
                r.get::<_, i64>(0)
            })
            .ok()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0)
    }

    /// Sentinel `wallet_hex` written when a snapshot is inserted with no qualifying wallets.
    /// Reads filter it out via `WalletAddress::from_hex` parse failure (empty string is not
    /// a valid hex address), so the date appears in `all_snapshot_dates` and `snapshot_for_date`
    /// returns `Some((at, []))` rather than falling through to the prior week.
    const EMPTY_SNAPSHOT_SENTINEL: &str = "";

    /// Insert a leaderboard snapshot: for a given `snapshot_at_unix`, record every wallet
    /// in `wallets`. Idempotent on `(snapshot_at_unix, wallet_hex)` — re-running with the
    /// same inputs is a no-op via `INSERT OR IGNORE`.
    ///
    /// When `wallets` is empty, a sentinel row with `wallet_hex = ""` is written so the
    /// date is still recorded as "seeded but empty" — distinct from "never seeded". This
    /// prevents the historical-seed driver from re-querying Dune for empty weeks on every
    /// re-run, and lets the backtest distinguish "no wallets qualified that week" (apply
    /// empty-pool semantics) from "no snapshot for this week" (fall back to prior).
    pub fn insert_snapshot(
        &mut self,
        snapshot_at_unix: i64,
        wallets: &[WalletAddress],
    ) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO leaderboard_snapshots \
                 (snapshot_at_unix, wallet_hex) VALUES (?1, ?2)",
            )?;
            if wallets.is_empty() {
                stmt.execute(params![snapshot_at_unix, Self::EMPTY_SNAPSHOT_SENTINEL])?;
            } else {
                for wallet in wallets {
                    stmt.execute(params![snapshot_at_unix, wallet.to_string()])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Return the most-recent snapshot at or before `sim_date_unix`.
    ///
    /// Returns `Ok(None)` when no snapshot exists at or before that date (i.e. the
    /// simulation date precedes the first seeded snapshot, or the table is empty).
    /// Returns `Ok(Some((snapshot_at_unix, wallets)))` otherwise.
    pub fn snapshot_for_date(
        &self,
        sim_date_unix: i64,
    ) -> Result<Option<(i64, Vec<WalletAddress>)>, BootstrapError> {
        // Resolve to the most-recent snapshot timestamp ≤ sim_date_unix.
        let snapshot_at: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(snapshot_at_unix) FROM leaderboard_snapshots \
                 WHERE snapshot_at_unix <= ?1",
                params![sim_date_unix],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten();
        let Some(at) = snapshot_at else {
            return Ok(None);
        };

        let mut stmt = self.conn.prepare(
            "SELECT wallet_hex FROM leaderboard_snapshots \
             WHERE snapshot_at_unix = ?1 ORDER BY wallet_hex",
        )?;
        let mut wallets = Vec::new();
        let rows = stmt.query_map(params![at], |r| r.get::<_, String>(0))?;
        for row in rows {
            let hex = row?;
            // Skip unparseable rows defensively; insertion path validates, so this is
            // only reachable through external DB tampering.
            if let Ok(addr) = WalletAddress::from_hex(&hex) {
                wallets.push(addr);
            }
        }
        Ok(Some((at, wallets)))
    }

    /// Return every snapshot timestamp present in the cache, ascending.
    /// Useful for diagnostics and for the historical seed driver to detect already-seeded dates.
    pub fn all_snapshot_dates(&self) -> Result<Vec<i64>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT snapshot_at_unix FROM leaderboard_snapshots \
             ORDER BY snapshot_at_unix ASC",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, i64>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Load every snapshot row in the cache into an in-memory [`LeaderboardSnapshots`].
    ///
    /// Used by `pe-backtest` at simulation startup so the per-day filter can be
    /// applied without round-tripping to SQLite on every iteration. Returns an
    /// empty index when the table has no rows.
    ///
    /// Empty-week anchors (sentinel rows, see [`Self::EMPTY_SNAPSHOT_SENTINEL`])
    /// produce an entry with an empty wallet set so the backtest can correctly
    /// apply "no candidates this week" semantics rather than falling through.
    pub fn load_all_snapshots(&self) -> Result<LeaderboardSnapshots, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT snapshot_at_unix, wallet_hex FROM leaderboard_snapshots \
             ORDER BY snapshot_at_unix ASC, wallet_hex ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            let at: i64 = r.get(0)?;
            let hex: String = r.get(1)?;
            Ok((at, hex))
        })?;
        let mut entries: Vec<(i64, HashSet<WalletAddress>)> = Vec::new();
        for row in rows {
            let (at, hex) = row?;
            // Ensure an entry exists for `at` even when the only row is the empty-week
            // sentinel (or any unparseable hex, defensively).
            if entries.last().is_none_or(|(last_at, _)| *last_at != at) {
                entries.push((at, HashSet::new()));
            }
            if let Ok(addr) = WalletAddress::from_hex(&hex)
                && let Some((_, set)) = entries.last_mut()
            {
                set.insert(addr);
            }
        }
        Ok(LeaderboardSnapshots { entries })
    }

    // ── market_resolutions ────────────────────────────────────────────────────

    /// Insert a single market resolution. Idempotent: `INSERT OR IGNORE` silently
    /// skips if `market_id` is already present (first fetch wins).
    ///
    /// `winning_outcome_id = None` means voided/non-binary — backtest will not
    /// attempt to close positions on this market.
    pub fn insert_resolution(
        &mut self,
        market_id: &str,
        winning_outcome_id: Option<u16>,
        resolved_at_unix: i64,
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        let winner_i64: Option<i64> = winning_outcome_id.map(i64::from);
        self.conn.execute(
            "INSERT OR IGNORE INTO market_resolutions \
             (market_id, winning_outcome_id, resolved_at_unix, fetched_at_unix) \
             VALUES (?1, ?2, ?3, ?4)",
            params![market_id, winner_i64, resolved_at_unix, fetched_at_unix],
        )?;
        Ok(())
    }

    /// Insert a single market resolution, explicitly tagged with `source`.
    ///
    /// Same idempotency contract as [`Self::insert_resolution`] (`INSERT OR IGNORE` on
    /// `market_id`). The `source` column was added in the multi-source pipeline migration
    /// (issue #149) and lets the cache distinguish rows by their origin:
    /// `"polygon"` (on-chain `eth_getLogs`) and `"clob"` (Polymarket CLOB).
    /// (`"dune"` rows written before #335 may still exist in deployed caches; the
    /// source is no longer produced.) Gamma-sourced rows continue to flow through the
    /// existing [`Self::insert_resolution`] method, which omits `source` from the
    /// INSERT and lets the schema-level `DEFAULT 'gamma'` tag the row.
    pub fn insert_resolution_with_source(
        &mut self,
        market_id: &str,
        winning_outcome_id: Option<u16>,
        resolved_at_unix: i64,
        fetched_at_unix: i64,
        source: &str,
    ) -> Result<(), BootstrapError> {
        let winner_i64: Option<i64> = winning_outcome_id.map(i64::from);
        self.conn.execute(
            "INSERT OR IGNORE INTO market_resolutions \
             (market_id, winning_outcome_id, resolved_at_unix, fetched_at_unix, source) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                market_id,
                winner_i64,
                resolved_at_unix,
                fetched_at_unix,
                source,
            ],
        )?;
        Ok(())
    }

    /// Delete every `market_resolutions` row whose `source` matches one of
    /// the supplied tags. Returns the count of deleted rows.
    ///
    /// Designed for the `PE_BOOTSTRAP_REBUILD_RESOLUTIONS` rebuild flow: the
    /// pre-issue-#149 Gamma path wrote rows with `closedTime`
    /// (oracle-settlement-time approximation), and the CLOB path writes rows
    /// with `end_date_iso` (scheduled-close-time approximation). Deleting these
    /// imprecise rows lets the CLOB stage re-populate the `'clob'` rows on the
    /// next stage-6 pass. The rebuild caller passes only `['gamma', 'clob']`
    /// (`lib.rs`), so retained `source='polygon'` rows — which keep their exact
    /// block-timestamp `resolved_at_unix` — are never deleted (#369: the on-chain
    /// scan is gone, so polygon rows are the only exact-timestamp corpus left).
    ///
    /// # Precondition
    /// Returns `Ok(0)` when `sources` is empty — caller-provided empty input
    /// must not produce a no-WHERE DELETE that wipes the entire table.
    pub fn delete_resolutions_by_sources(
        &mut self,
        sources: &[&str],
    ) -> Result<usize, BootstrapError> {
        if sources.is_empty() {
            return Ok(0);
        }
        // SQLite requires explicit placeholders; build "?1, ?2, ..." for the
        // IN clause. Source strings travel through bound params, so callers
        // passing user-tainted tags here still cannot inject SQL.
        let placeholders = (1..=sources.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("DELETE FROM market_resolutions WHERE source IN ({placeholders})");
        let params_vec: Vec<&dyn rusqlite::ToSql> =
            sources.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let affected = self.conn.execute(&sql, &params_vec[..])?;
        Ok(affected)
    }

    /// Return the set of market IDs already present in `market_resolutions`.
    ///
    /// Used by [`GammaFetcher`] to skip markets that have already been fetched.
    pub fn resolved_market_ids(&self) -> HashSet<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT market_id FROM market_resolutions")
        {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Resolved markets with a **decided** outcome (`winning_outcome_id IS NOT NULL`) — i.e.
    /// excluding voided/non-binary markets (issue #421 PR4). This is the universe the createdAt
    /// backfill (pass 1) scopes to, so it matches the price-series backfill (pass 2, which filters
    /// the same way in [`Self::price_history_backfill_targets`]): a voided market yields no
    /// qualifying first-buy positions, so its `start_date_unix` would never be consumed.
    ///
    /// # Precondition
    /// Returns an empty set when no resolutions have been fetched.
    pub fn resolved_market_ids_with_winner(&self) -> HashSet<String> {
        let mut stmt = match self.conn.prepare(
            "SELECT market_id FROM market_resolutions WHERE winning_outcome_id IS NOT NULL",
        ) {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    // ── market_schedules ──────────────────────────────────────────────────────

    /// Insert a scheduled endDate for a market. Idempotent: `INSERT OR IGNORE` silently
    /// skips if `market_id` is already present (first fetch wins).
    ///
    /// `end_date_unix = None` means Gamma returned no `endDate` for this market — the
    /// market still enters the skip-set so it is not re-fetched on subsequent runs.
    pub fn insert_schedule(
        &mut self,
        market_id: &str,
        end_date_unix: Option<i64>,
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO market_schedules \
             (market_id, end_date_unix, fetched_at_unix) \
             VALUES (?1, ?2, ?3)",
            params![market_id, end_date_unix, fetched_at_unix],
        )?;
        Ok(())
    }

    /// Insert a scheduled endDate for a market, explicitly tagged with `source`.
    ///
    /// Same idempotency contract as [`Self::insert_schedule`] (`INSERT OR IGNORE` on
    /// `market_id`). The `source` column was added in the multi-source pipeline migration
    /// (issue #149) and lets the cache distinguish rows by their origin:
    /// `"clob"` (Polymarket CLOB closed-market listing) is the only non-Gamma writer
    /// today; future sources slot in here. Gamma-sourced rows continue to flow through
    /// the existing [`Self::insert_schedule`] method, which omits `source` from the
    /// INSERT and lets the schema-level `DEFAULT 'gamma'` tag the row.
    pub fn insert_schedule_with_source(
        &mut self,
        market_id: &str,
        end_date_unix: Option<i64>,
        fetched_at_unix: i64,
        source: &str,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO market_schedules \
             (market_id, end_date_unix, fetched_at_unix, source) \
             VALUES (?1, ?2, ?3, ?4)",
            params![market_id, end_date_unix, fetched_at_unix, source],
        )?;
        Ok(())
    }

    /// Return the set of market IDs already present in `market_schedules`.
    ///
    /// Includes markets where `end_date_unix IS NULL` — a NULL row means "we checked
    /// Gamma and it had no endDate" and should not be re-fetched.
    ///
    /// # Precondition
    /// Returns an empty set when no schedules have been fetched.
    pub fn scheduled_market_ids(&self) -> HashSet<String> {
        let mut stmt = match self.conn.prepare("SELECT market_id FROM market_schedules") {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Return market IDs in `market_schedules` whose `end_date_unix` is NULL.
    ///
    /// These are the candidate set for the null-rewrite pass (issue #137 Sub-PR 2):
    /// rows where the initial Gamma fetch returned no `endDate`. Empirically, pre-PR
    /// this was 98% of `source='gamma'` rows because the plain `?condition_ids=` URL
    /// returns an empty list for closed markets; the `&closed=true` URL surfaces them.
    ///
    /// Caller is responsible for filtering this set to the trade-set scope before
    /// issuing network requests.
    ///
    /// # Precondition
    /// Returns an empty set when no schedules have been fetched.
    pub fn null_schedule_market_ids(&self) -> HashSet<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT market_id FROM market_schedules WHERE end_date_unix IS NULL")
        {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Rewrite the `end_date_unix` of an existing schedule row, only if it was NULL.
    ///
    /// The `WHERE end_date_unix IS NULL` clause is a hard guard against accidentally
    /// overwriting good data: if a future caller passes a wrong value for a market
    /// that already has a populated `end_date_unix`, this method is a no-op. Returns
    /// `Ok(true)` when a row was updated, `Ok(false)` when no row matched (either the
    /// market is absent from `market_schedules`, or its `end_date_unix` was already
    /// populated by an earlier source).
    pub fn update_schedule_end_date(
        &mut self,
        market_id: &str,
        end_date_unix: i64,
        fetched_at_unix: i64,
    ) -> Result<bool, BootstrapError> {
        let changes = self.conn.execute(
            "UPDATE market_schedules \
             SET end_date_unix = ?1, fetched_at_unix = ?2 \
             WHERE market_id = ?3 AND end_date_unix IS NULL",
            params![end_date_unix, fetched_at_unix, market_id],
        )?;
        Ok(changes > 0)
    }

    /// Set `start_date_unix` (Gamma `createdAt`) on an existing schedule row, only if it was NULL
    /// (issue #421 PR4). The `WHERE start_date_unix IS NULL` guard makes the createdAt backfill
    /// idempotent and never overwrites a populated value — the same hard guard as
    /// [`Self::update_schedule_end_date`]. Returns `Ok(true)` when a row was updated, `Ok(false)`
    /// when none matched (market absent from `market_schedules`, or its `start_date_unix` already
    /// populated). `fetched_at_unix` is left untouched — it tracks the endDate fetch, not this pass.
    pub fn update_schedule_start_date(
        &mut self,
        market_id: &str,
        start_date_unix: i64,
    ) -> Result<bool, BootstrapError> {
        let changes = self.conn.execute(
            "UPDATE market_schedules \
             SET start_date_unix = ?1 \
             WHERE market_id = ?2 AND start_date_unix IS NULL",
            params![start_date_unix, market_id],
        )?;
        Ok(changes > 0)
    }

    /// Market IDs in `market_schedules` whose `start_date_unix` is NULL — the candidate set for the
    /// Gamma `createdAt` backfill (issue #421 PR4). The caller scopes this to the decided-outcome
    /// universe (∩ [`Self::resolved_market_ids_with_winner`]) before issuing network requests,
    /// mirroring how the null-endDate rewrite scopes [`Self::null_schedule_market_ids`] to the
    /// trade-set. Markets with no schedule row at all are not covered (they also lack
    /// `end_date_unix`), consistent with the endDate handling.
    ///
    /// # Precondition
    /// Returns an empty set when no schedules have been fetched.
    pub fn market_ids_missing_start_date(&self) -> HashSet<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT market_id FROM market_schedules WHERE start_date_unix IS NULL")
        {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Load all schedule rows into a [`ScheduleIndex`] keyed by [`MarketId`].
    ///
    /// Includes rows where `end_date_unix IS NULL` (Gamma had no `endDate`). The
    /// backtest uses presence in the index to distinguish "checked, no date → allow"
    /// from "never fetched → fall back to `resolved_at_unix`".
    ///
    /// # Precondition
    /// Returns an empty index when no schedules have been fetched.
    pub fn load_all_schedules(&self) -> Result<ScheduleIndex, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT market_id, end_date_unix FROM market_schedules ORDER BY market_id")?;
        let rows = stmt.query_map([], |r| {
            let market_id: String = r.get(0)?;
            let end_date_unix: Option<i64> = r.get(1)?;
            Ok((market_id, end_date_unix))
        })?;
        let mut index = ScheduleIndex::new();
        for row in rows {
            let (market_id, end_date_unix) = row?;
            index.insert(
                MarketId(VenueMarketId(market_id)),
                MarketSchedule { end_date_unix },
            );
        }
        Ok(index)
    }

    // ── market_price_history (issue #421 PR4 — CLV bake-off) ─────────────────────

    /// Insert a batch of coarse pre-resolution price points into `market_price_history` in one
    /// transaction. Rows are `(market_id, token_id, t, price_str)` where `price_str` is the decimal
    /// mid as produced by [`Decimal::to_string`] (TEXT — no `f64` round-trip). `INSERT OR IGNORE` on
    /// the `(market_id, token_id, t)` PK makes re-runs idempotent and the backfill resumable.
    ///
    /// # Precondition
    /// Returns immediately without writing when `rows` is empty.
    pub fn insert_price_history_batch(
        &mut self,
        rows: &[(String, String, i64, String)],
    ) -> Result<(), BootstrapError> {
        if rows.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO market_price_history \
                 (market_id, token_id, t, price) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (market_id, token_id, t, price) in rows {
                stmt.execute(params![market_id, token_id, t, price])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The `(market, token)` price-history backfill targets (issue #421 PR4): every resolved
    /// market's mapped CLOB tokens that have no `market_price_history` row yet, paired with the
    /// per-market close reference (`end_date_unix` when known, else `resolved_at_unix`). The
    /// `NOT EXISTS` guard keys on the `(market_id, token_id)` PK prefix, so a re-run after a partial
    /// backfill cheaply skips already-fetched tokens (resumable). `limit = 0` means unbounded; a
    /// positive `limit` bounds one run's memory/time (re-run to continue).
    ///
    /// Only markets present in `token_conditions` (the token→condition map written by the CLOB
    /// closed-markets sweep over the full resolved universe, and the Gamma `events` sweep over its
    /// curated slice — issue #429) are returned — a token id is required to query CLOB
    /// `/prices-history`.
    ///
    /// # Precondition
    /// Returns an empty vec when no resolved markets have mapped tokens.
    pub fn price_history_backfill_targets(
        &self,
        limit: usize,
    ) -> Result<Vec<PriceBackfillTarget>, BootstrapError> {
        let base = "SELECT tc.condition_id, tc.token_id, \
                    COALESCE(ms.end_date_unix, mr.resolved_at_unix) AS close_ref \
             FROM token_conditions tc \
             JOIN market_resolutions mr ON mr.market_id = tc.condition_id \
             LEFT JOIN market_schedules ms ON ms.market_id = tc.condition_id \
             WHERE mr.winning_outcome_id IS NOT NULL \
               AND NOT EXISTS ( \
                   SELECT 1 FROM market_price_history mph \
                   WHERE mph.market_id = tc.condition_id AND mph.token_id = tc.token_id \
               )";
        let sql = if limit > 0 {
            format!("{base} LIMIT {limit}")
        } else {
            base.to_owned()
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |r| {
            Ok(PriceBackfillTarget {
                market_id: r.get::<_, String>(0)?,
                token_id: r.get::<_, String>(1)?,
                close_ref_unix: r.get::<_, i64>(2)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    // ── market_liquidity ───────────────────────────────────────────────────────

    /// Upsert the Gamma `liquidity` value for `market_id`.
    ///
    /// `liquidity_usd` is the current order-book depth indicator reported by
    /// Gamma's `/markets` endpoint. Stored as TEXT to preserve `Decimal`
    /// precision (no `f64` round-trip). Idempotent via `INSERT OR REPLACE` —
    /// later refreshes overwrite earlier values since depth changes over time.
    pub fn upsert_market_liquidity(
        &mut self,
        market_id: &str,
        liquidity_usd: Decimal,
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO market_liquidity \
             (market_id, liquidity_usd_str, fetched_at_unix) \
             VALUES (?1, ?2, ?3)",
            params![market_id, liquidity_usd.to_string(), fetched_at_unix],
        )?;
        Ok(())
    }

    /// Return the set of market IDs that already have a cached liquidity row.
    ///
    /// Used by the bootstrap walk to skip already-fetched markets.
    ///
    /// # Precondition
    /// Returns an empty set when no liquidity values have been fetched.
    pub fn liquid_market_ids(&self) -> HashSet<String> {
        let mut stmt = match self.conn.prepare("SELECT market_id FROM market_liquidity") {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Load all liquidity rows into a [`LiquidityIndex`] keyed by [`MarketId`].
    ///
    /// Rows where the stored decimal fails to parse are skipped defensively (this is
    /// theoretically unreachable since the only writer goes through
    /// [`Decimal::to_string`], but we don't want a single corrupt row to fail the
    /// whole load).
    ///
    /// # Precondition
    /// Returns an empty index when no liquidity values have been fetched.
    pub fn load_all_liquidity(&self) -> Result<LiquidityIndex, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT market_id, liquidity_usd_str FROM market_liquidity ORDER BY market_id",
        )?;
        let rows = stmt.query_map([], |r| {
            let market_id: String = r.get(0)?;
            let liquidity_str: String = r.get(1)?;
            Ok((market_id, liquidity_str))
        })?;
        let mut index = LiquidityIndex::new();
        for row in rows {
            let (market_id, liquidity_str) = row?;
            let Ok(liquidity_usd) = liquidity_str.parse::<Decimal>() else {
                continue; // corrupt row; skip defensively
            };
            index.insert(MarketId(VenueMarketId(market_id)), liquidity_usd);
        }
        Ok(index)
    }

    // ── market_events (issue #206) ────────────────────────────────────────────

    /// Upsert a `conditionId → event` mapping.
    ///
    /// `INSERT OR REPLACE` because an event's market list can grow (neg-risk
    /// bundles gain markets), so a later sweep overwrites an earlier row. The
    /// caller must pass `condition_id` already normalized to the
    /// `trades.market_id` form (via `chain::normalise_condition_id`).
    pub fn upsert_market_events(
        &mut self,
        condition_id: &str,
        event_id: &str,
        event_slug: Option<&str>,
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO market_events \
             (condition_id, event_id, event_slug, fetched_at_unix) \
             VALUES (?1, ?2, ?3, ?4)",
            params![condition_id, event_id, event_slug, fetched_at_unix],
        )?;
        Ok(())
    }

    /// Upsert a batch of `(token_id, condition_id, outcome_index)` rows into
    /// `token_conditions` in one transaction (issue #207, Slice 0; `outcome_index`
    /// added in issue #429).
    ///
    /// `token_id` is the decimal-string uint256 form from Gamma `clobTokenIds` (or
    /// CLOB `/markets` `tokens[].token_id` — the same on-chain CTF positionId);
    /// `condition_id` is the normalised `0x`-prefixed market id; `outcome_index`
    /// is the 0-based positional outcome ordinal (0=YES,1=NO for binary). `INSERT
    /// OR REPLACE` keyed on `token_id` — a token belongs to exactly one condition,
    /// and a re-sweep refreshes `fetched_at_unix`/`outcome_index` without
    /// duplicating rows.
    pub fn upsert_token_conditions_batch(
        &mut self,
        rows: &[(String, String, u16)],
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO token_conditions \
                 (token_id, condition_id, outcome_index, fetched_at_unix) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (token_id, condition_id, outcome_index) in rows {
                stmt.execute(params![
                    token_id,
                    condition_id,
                    outcome_index,
                    fetched_at_unix
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Count of rows in `token_conditions` (distinct mapped token ids).
    ///
    /// # Precondition
    /// Returns `0` when no token map has been swept.
    pub fn token_condition_count(&self) -> i64 {
        self.conn
            .query_row("SELECT COUNT(*) FROM token_conditions", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// Resolve a token id to its stored `(condition_id, outcome_index)` row.
    ///
    /// `outcome_index` is `None` for legacy/events-sourced rows written before the
    /// issue #429 migration and never repopulated. The CLOB closed-markets
    /// ingestion cross-check (issue #429) uses this to detect a CLOB `tokens[]`
    /// array order that diverges from the authoritative Gamma `clob_token_ids`
    /// order *before* `upsert_token_conditions_batch`'s `INSERT OR REPLACE`
    /// overwrites the prior row.
    ///
    /// # Precondition
    /// Returns `None` when the token has not been mapped.
    pub fn token_condition_outcome(&self, token_id: &str) -> Option<(String, Option<i64>)> {
        self.conn
            .query_row(
                "SELECT condition_id, outcome_index FROM token_conditions WHERE token_id = ?1",
                params![token_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?)),
            )
            .ok()
    }

    /// Coverage of the CLOB token→condition map over resolved-with-winner markets
    /// (issue #429): `(resolved_with_winner, mapped)` where `mapped` counts those
    /// markets carrying at least one `token_conditions` row. The downstream
    /// `price_history_backfill_targets` join requires both a winner and a token
    /// map, so this is the realistic ceiling for that backfill.
    ///
    /// # Precondition
    /// Returns `(0, 0)` on an empty cache.
    pub fn token_coverage_report(&self) -> (i64, i64) {
        let total: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM market_resolutions WHERE winning_outcome_id IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let mapped: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM market_resolutions mr \
                 WHERE mr.winning_outcome_id IS NOT NULL \
                   AND EXISTS ( \
                       SELECT 1 FROM token_conditions tc WHERE tc.condition_id = mr.market_id \
                   )",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        (total, mapped)
    }

    /// Resolve a single ERC-1155 `token_id` (decimal string) to its `condition_id`.
    ///
    /// The on-chain `OrderFilled` consumer (issue #207, Slice 1) normalises each
    /// leg's token id to decimal and calls this to attribute the leg to a market.
    ///
    /// # Precondition
    /// Returns `None` when the token has not been mapped (unswept, or a token
    /// from a market Gamma did not return).
    pub fn condition_for_token(&self, token_id: &str) -> Option<String> {
        self.conn
            .query_row(
                "SELECT condition_id FROM token_conditions WHERE token_id = ?1",
                params![token_id],
                |r| r.get::<_, String>(0),
            )
            .ok()
    }

    /// Upsert a batch of per-market fee rows into `market_fees` in one transaction.
    ///
    /// Rows are `(condition_id, taker_base_fee_bps, maker_base_fee_bps, fetched_at_unix)`.
    /// `INSERT OR REPLACE` makes re-sweeps idempotent; `fee_active_from_unix` is always
    /// written as `NULL` in PR 1 (PR 4 backfills it from `counterparty_edges`).
    ///
    /// # Precondition
    /// Returns immediately without writing when `rows` is empty.
    pub fn upsert_market_fees_batch(
        &mut self,
        rows: &[(String, i32, i32, i64)],
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        if rows.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO market_fees \
                 (condition_id, taker_base_fee_bps, maker_base_fee_bps, \
                  fee_active_from_unix, fetched_at_unix) \
                 VALUES (?1, ?2, ?3, NULL, ?4)",
            )?;
            for (condition_id, taker_bps, maker_bps, _fetched) in rows {
                stmt.execute(params![condition_id, taker_bps, maker_bps, fetched_at_unix])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Load all rows from `market_fees` as a `HashMap<condition_id, MarketFeeRow>`.
    ///
    /// # Precondition
    /// Returns an empty map when the table has not been swept yet.
    pub fn load_market_fees(
        &self,
    ) -> Result<std::collections::HashMap<String, MarketFeeRow>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT condition_id, taker_base_fee_bps, maker_base_fee_bps FROM market_fees",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                MarketFeeRow {
                    taker_base_fee_bps: row.get::<_, i32>(1)?,
                    maker_base_fee_bps: row.get::<_, i32>(2)?,
                },
            ))
        })?;
        let mut map = std::collections::HashMap::new();
        for result in rows {
            let (condition_id, fee_row) = result?;
            map.insert(condition_id, fee_row);
        }
        Ok(map)
    }

    /// Stream per-trade `(market_id, price_str, contracts)` from the `trades`
    /// table, skipping rows with empty `market_id` (issue #207 Slice 1c).
    ///
    /// Read-only. The callback aggregates per market in caller-provided state
    /// to avoid materialising ~269M rows.
    pub fn for_each_trade_volume(
        &self,
        mut f: impl FnMut(String, String, i64) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        let sql = "SELECT market_id, price_str, contracts FROM trades WHERE market_id != ''";
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            f(row.get(0)?, row.get(1)?, row.get(2)?)?;
        }
        Ok(())
    }

    /// Return the set of `condition_id`s already present in `market_events`.
    ///
    /// Used to compute the orphan set (traded markets with no event row) without
    /// reloading the full map.
    ///
    /// # Precondition
    /// Returns an empty set when no events have been swept.
    pub fn mapped_condition_ids(&self) -> HashSet<String> {
        let mut stmt = match self.conn.prepare("SELECT condition_id FROM market_events") {
            Ok(s) => s,
            Err(_) => return HashSet::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => HashSet::new(),
        }
    }

    /// Self-map a traded market with no Gamma event as its own singleton event:
    /// `event_id = condition_id`, `event_slug = NULL`.
    ///
    /// `INSERT OR IGNORE` so a real mapping written by the sweep is never
    /// clobbered by the orphan pass.
    pub fn self_map_orphan(
        &mut self,
        condition_id: &str,
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO market_events \
             (condition_id, event_id, event_slug, fetched_at_unix) \
             VALUES (?1, ?1, NULL, ?2)",
            params![condition_id, fetched_at_unix],
        )?;
        Ok(())
    }

    /// Load the full `market_events` table as `condition_id → event_id`.
    ///
    /// The downstream skill engine calls this once at startup to group trades by
    /// event; a `condition_id` absent from the map falls back to itself (matching
    /// the orphan self-map).
    ///
    /// # Precondition
    /// Returns an empty map when no events have been swept.
    pub fn load_market_event_map(&self) -> Result<HashMap<String, String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT condition_id, event_id FROM market_events")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut map = HashMap::new();
        for row in rows {
            let (condition_id, event_id) = row?;
            map.insert(condition_id, event_id);
        }
        Ok(map)
    }

    /// Build the cross-wallet first-buy rank index used by
    /// `first_mover_percentile_bps` (sub-task #3 of #248).
    ///
    /// For each `(market_id, outcome_id)` group across every wallet, returns
    /// the sorted vector of *first-buy* timestamps — one per wallet that has at
    /// least one buy on that outcome at or before `cutoff_unix`. A wallet's
    /// per-position percentile is the fraction of group members whose first
    /// entry is strictly earlier (computed downstream via `slice::partition_point`).
    ///
    /// # Look-ahead invariant
    /// The `WHERE timestamp_unix <= ?` clause is mandatory: including post-cutoff
    /// trades would leak future information into a training feature. The SQL
    /// below filters strictly, and callers must pass the same `cutoff_unix`
    /// they use elsewhere in the extract pipeline.
    pub fn load_first_mover_rank_index(
        &self,
        cutoff_unix: i64,
    ) -> Result<HashMap<(String, u16), Vec<i64>>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            // The covering index `idx_trades_buy_market_outcome_wallet_ts`
            // (see SCHEMA) matches this query exactly: side equality first,
            // then GROUP BY columns in order, then timestamp_unix last so
            // MIN() is found from the first matching entry per group.
            "SELECT market_id, outcome_id, MIN(timestamp_unix) AS first_buy_ts
             FROM trades
             WHERE side = 'buy' AND timestamp_unix <= ?1
             GROUP BY market_id, outcome_id, wallet_hex",
        )?;
        let rows = stmt.query_map(params![cutoff_unix], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, u16>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        let mut map: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        for row in rows {
            let (market_id, outcome_id, first_buy_ts) = row?;
            map.entry((market_id, outcome_id))
                .or_default()
                .push(first_buy_ts);
        }
        // Sort each group's timestamps ascending so callers can binary-search
        // via `partition_point` to find count_ahead in O(log N).
        for v in map.values_mut() {
            v.sort_unstable();
        }
        Ok(map)
    }

    /// Return `true` if a cached rank index exists for `cutoff_unix`.
    ///
    /// Used by the extract pipeline to skip the expensive `load_first_mover_rank_index`
    /// scan on re-extracts at an already-seen cutoff.
    pub fn rank_index_cache_exists(&self, cutoff_unix: i64) -> Result<bool, BootstrapError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM first_mover_rank_cache WHERE cutoff_unix = ?1 LIMIT 1",
            params![cutoff_unix],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// Load the cached rank index for `cutoff_unix`.
    ///
    /// Returns the same structure as `load_first_mover_rank_index`: a map from
    /// `(market_id, outcome_id)` to a sorted ascending list of first-buy timestamps
    /// (one per wallet). Returns an empty map if no cache rows exist (callers should
    /// check `rank_index_cache_exists` first to distinguish "cached empty result"
    /// from "no cache entry").
    ///
    /// # Precondition
    /// `rank_index_cache_exists(cutoff_unix)` should return `true`; otherwise the
    /// result is indistinguishable from a cutoff with no buy-side trades.
    pub fn load_rank_index_cache(
        &self,
        cutoff_unix: i64,
    ) -> Result<HashMap<(String, u16), Vec<i64>>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT market_id, outcome_id, ts_json
             FROM first_mover_rank_cache
             WHERE cutoff_unix = ?1",
        )?;
        let rows = stmt.query_map(params![cutoff_unix], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, u16>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut map: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        for row in rows {
            let (market_id, outcome_id, ts_json) = row?;
            let timestamps: Vec<i64> =
                serde_json::from_str(&ts_json).map_err(|e| BootstrapError::Cache {
                    message: format!("rank cache decode ({market_id},{outcome_id}): {e}"),
                })?;
            map.insert((market_id, outcome_id), timestamps);
        }
        Ok(map)
    }

    /// Persist a rank index so future re-extracts at `cutoff_unix` can skip
    /// the full `trades` GROUP BY scan. Each `(market_id, outcome_id)` group's
    /// sorted timestamp list is stored as a JSON array — one row per group, so
    /// two wallets sharing the same first-buy timestamp are never collapsed.
    /// Idempotent: uses `INSERT OR REPLACE` so calling twice at the same cutoff
    /// is safe (the later call wins, which is correct since the input is identical).
    pub fn save_rank_index_cache(
        &mut self,
        cutoff_unix: i64,
        index: &HashMap<(String, u16), Vec<i64>>,
    ) -> Result<usize, BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO first_mover_rank_cache
                 (cutoff_unix, market_id, outcome_id, ts_json)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for ((market_id, outcome_id), timestamps) in index {
                let ts_json =
                    serde_json::to_string(timestamps).map_err(|e| BootstrapError::Cache {
                        message: format!("rank cache encode: {e}"),
                    })?;
                stmt.execute(params![
                    cutoff_unix,
                    market_id,
                    i64::from(*outcome_id),
                    ts_json
                ])?;
            }
        }
        tx.commit()?;
        Ok(index.len())
    }

    /// Return all distinct `market_id` values present in the `trades` table, sorted.
    ///
    /// Used by the Gamma fetch step to enumerate the full set of markets to resolve.
    pub fn all_market_ids(&self) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT DISTINCT market_id FROM trades ORDER BY market_id")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |r| r.get::<_, String>(0));
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Compute the four backtest-readiness gap counts (issue #208) in one read pass.
    ///
    /// Each count is a dedicated `COUNT(*)` or set-difference — never a `.len()`
    /// over a list-materialising helper. `fetch_incomplete` mirrors the `IS NULL`
    /// branch of [`Self::select_backfill_due`] without materialising the wallet
    /// list. The `missing_*` counts difference [`Self::all_market_ids`] against
    /// [`Self::resolved_market_ids`] / [`Self::scheduled_market_ids`], inheriting
    /// the `0x`-prefixed join-key invariant the write paths maintain (see the
    /// `market_events` schema note) — no fresh `LEFT JOIN`.
    pub fn coverage_counts(&self) -> Result<CoverageReport, BootstrapError> {
        let fetch_incomplete: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM active_tradeable_wallets \
             WHERE last_polymarket_fetch_at IS NULL",
            [],
            |r| r.get(0),
        )?;
        let traded = self.all_market_ids();
        let resolved = self.resolved_market_ids();
        let scheduled = self.scheduled_market_ids();
        let missing_resolution = traded.iter().filter(|&m| !resolved.contains(m)).count();
        let missing_schedule = traded.iter().filter(|&m| !scheduled.contains(m)).count();
        Ok(CoverageReport {
            // COUNT(*) is non-negative, so the conversion never saturates in
            // practice; `unwrap_or(0)` keeps the lint happy without an `as` cast.
            fetch_incomplete: usize::try_from(fetch_incomplete).unwrap_or(0),
            missing_resolution,
            missing_schedule,
        })
    }

    /// Returns `(oldest_ts, newest_ts)` for `wallet_hex`, or `None` if no trades cached.
    ///
    /// # Precondition
    /// Returns `None` when called before any trades have been ingested for this wallet.
    pub fn trade_ts_bounds(&self, wallet_hex: &str) -> Result<Option<(i64, i64)>, BootstrapError> {
        let result: (Option<i64>, Option<i64>) = self.conn.query_row(
            "SELECT MIN(timestamp_unix), MAX(timestamp_unix) FROM trades WHERE wallet_hex = ?1",
            params![wallet_hex],
            |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?)),
        )?;
        match result {
            (Some(min), Some(max)) => Ok(Some((min, max))),
            _ => Ok(None),
        }
    }

    /// Minimum `timestamp_unix` across all rows in `trades`.
    /// Returns 0 when the table is empty.
    pub fn min_trade_unix(&self) -> Result<i64, BootstrapError> {
        let ts: i64 = self.conn.query_row(
            "SELECT COALESCE(MIN(timestamp_unix), 0) FROM trades",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(ts)
    }

    /// Maximum `resolved_at_unix` across all rows in `market_resolutions`.
    /// Returns 0 when the table is empty — the first run then queries the full
    /// on-chain history by passing 0 to `FROM_UNIXTIME`.
    pub fn max_resolved_at_unix(&self) -> Result<i64, BootstrapError> {
        let ts: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(resolved_at_unix), 0) FROM market_resolutions",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        Ok(ts)
    }

    /// Load all resolved markets into a `ResolutionIndex` keyed by `MarketId`.
    ///
    /// Excludes rows where `winning_outcome_id IS NULL` (voided/non-binary markets).
    /// Used by `pe-backtest` at startup for the per-day resolution sweep.
    pub fn load_all_resolutions(&self) -> Result<ResolutionIndex, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT market_id, winning_outcome_id, resolved_at_unix \
             FROM market_resolutions \
             WHERE winning_outcome_id IS NOT NULL \
             ORDER BY market_id",
        )?;
        let rows = stmt.query_map([], |r| {
            let market_id: String = r.get(0)?;
            let winner_i64: i64 = r.get(1)?;
            let resolved_at: i64 = r.get(2)?;
            Ok((market_id, winner_i64, resolved_at))
        })?;
        let mut index = ResolutionIndex::new();
        for row in rows {
            let (market_id, winner_i64, resolved_at_unix) = row?;
            let Ok(winning_outcome_id) = OutcomeId::try_from(winner_i64) else {
                continue; // out-of-range value; skip defensively
            };
            index.insert(
                MarketId(VenueMarketId(market_id)),
                MarketResolution {
                    winning_outcome_id,
                    resolved_at_unix,
                },
            );
        }
        Ok(index)
    }

    /// Return the full resolution record for `market_id` if present, including
    /// the `source` tag. Intended for diagnostics and scenario tests; the
    /// backtest reads via [`Self::load_all_resolutions`] which does not
    /// surface the source column.
    pub fn resolution_record(&self, market_id: &str) -> Option<(Option<u16>, i64, i64, String)> {
        self.conn
            .query_row(
                "SELECT winning_outcome_id, resolved_at_unix, fetched_at_unix, source \
                 FROM market_resolutions WHERE market_id = ?1",
                params![market_id],
                |r| {
                    let winner_i64: Option<i64> = r.get(0)?;
                    let resolved_at: i64 = r.get(1)?;
                    let fetched_at: i64 = r.get(2)?;
                    let source: String = r.get(3)?;
                    let winner = winner_i64.and_then(|v| u16::try_from(v).ok());
                    Ok((winner, resolved_at, fetched_at, source))
                },
            )
            .ok()
    }

    /// Return the full schedule record for `market_id` if present, including
    /// the `source` tag. Companion to [`Self::resolution_record`].
    pub fn schedule_record(&self, market_id: &str) -> Option<(Option<i64>, i64, String)> {
        self.conn
            .query_row(
                "SELECT end_date_unix, fetched_at_unix, source FROM market_schedules \
                 WHERE market_id = ?1",
                params![market_id],
                |r| {
                    let end_date: Option<i64> = r.get(0)?;
                    let fetched_at: i64 = r.get(1)?;
                    let source: String = r.get(2)?;
                    Ok((end_date, fetched_at, source))
                },
            )
            .ok()
    }

    // ── source_cursor ─────────────────────────────────────────────────────────

    /// Read a checkpoint value previously written by [`Self::set_source_cursor`].
    ///
    /// Returns `None` when the key has never been written. The `source_cursor`
    /// table holds opaque string values keyed by `key` so different daily-run
    /// resumes (CLOB pagination, Polygon block-scan checkpoint, …) can share
    /// a single table without colliding. Callers parse the returned string as
    /// needed (e.g. `s.parse::<u64>().ok()` for a block number).
    ///
    /// # Precondition
    /// Returns `None` if no row exists for `key` or if the underlying query
    /// fails — callers should treat absent-or-broken identically (start fresh).
    pub fn get_source_cursor(&self, key: &str) -> Option<String> {
        self.conn
            .query_row(
                "SELECT value FROM source_cursor WHERE key = ?1",
                params![key],
                |row| row.get::<_, String>(0),
            )
            .ok()
    }

    /// Write or replace the checkpoint value for `key`.
    ///
    /// Uses `INSERT OR REPLACE` so subsequent calls overwrite the prior value;
    /// `updated_at` is set to the current UTC unix timestamp on every write so
    /// operators can audit how recently each cursor advanced.
    pub fn set_source_cursor(&mut self, key: &str, value: &str) -> Result<(), BootstrapError> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        self.conn.execute(
            "INSERT OR REPLACE INTO source_cursor (key, value, updated_at) \
             VALUES (?1, ?2, ?3)",
            params![key, value, now],
        )?;
        Ok(())
    }

    // ── wallets pile (issue #166) ─────────────────────────────────────────────

    /// UPSERT a wallet row by `wallet_hex`, OR-ing `source_bits` into any existing row.
    ///
    /// Wallets present in multiple sources accumulate all their bits. New rows are
    /// inserted with the provided fields; conflicting rows update `source_bits`,
    /// `is_infra` (sticky 0→1), and Dune fields only when the new value is non-NULL.
    ///
    /// Issue #186: this 6-arg API passes `polymarket_contracts_seen = 0`
    /// internally (no V1/V2 attribution available). Callers that need to set
    /// the bit use [`Self::upsert_wallets_bulk`] with the 7-tuple shape.
    pub fn upsert_wallet(
        &mut self,
        wallet_hex: &str,
        source_bits: i64,
        is_infra: bool,
        dune_first_seen_unix: Option<i64>,
        dune_closed_markets: Option<i64>,
        dune_win_rate_bps: Option<i64>,
    ) -> Result<(), BootstrapError> {
        let is_infra_int: i64 = i64::from(is_infra);
        self.conn.execute(
            "INSERT INTO wallets (\
                wallet_hex, source_bits, is_infra, \
                dune_first_seen_unix, dune_closed_markets, dune_win_rate_bps, \
                polymarket_contracts_seen\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0) \
             ON CONFLICT(wallet_hex) DO UPDATE SET \
                source_bits = source_bits | excluded.source_bits, \
                is_infra = MAX(is_infra, excluded.is_infra), \
                dune_first_seen_unix = COALESCE(excluded.dune_first_seen_unix, dune_first_seen_unix), \
                dune_closed_markets = COALESCE(excluded.dune_closed_markets, dune_closed_markets), \
                dune_win_rate_bps = COALESCE(excluded.dune_win_rate_bps, dune_win_rate_bps)",
            params![
                wallet_hex,
                source_bits,
                is_infra_int,
                dune_first_seen_unix,
                dune_closed_markets,
                dune_win_rate_bps,
            ],
        )?;
        Ok(())
    }

    /// UPSERT many wallets in a single transaction. See [`Self::upsert_wallet`].
    ///
    /// Issue #186: the 7th tuple field (`polymarket_contracts_seen`) is OR-merged
    /// with any existing value, matching the `source_bits` semantics. Callers
    /// pass `0` when V1/V2 attribution is unavailable; the enumeration path
    /// passes the bit from `topic_to_contract_version_bit`.
    pub fn upsert_wallets_bulk(&mut self, rows: &[WalletUpsertRow]) -> Result<(), BootstrapError> {
        // Issue #385 tombstone gate. Load the purged set once; for a tombstoned
        // wallet, an incoming row carrying an override bit (leaderboard/radion =
        // `source_bits & TOMBSTONE_OVERRIDE_SOURCES`) LIFTS the tombstone (DELETE
        // the `purged_wallets` row, then upsert normally — re-admit); any other
        // source bit (datadash/trades/wallet-set-json) SKIPS the row, leaving the
        // tombstone intact and the wallet un-inserted. An empty `purged_wallets`
        // makes this a no-op, so behaviour is identical to pre-#385.
        let mut purged = self.load_purged_set()?;
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO wallets (\
                    wallet_hex, source_bits, is_infra, \
                    dune_first_seen_unix, dune_closed_markets, dune_win_rate_bps, \
                    polymarket_contracts_seen\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(wallet_hex) DO UPDATE SET \
                    source_bits = source_bits | excluded.source_bits, \
                    is_infra = MAX(is_infra, excluded.is_infra), \
                    dune_first_seen_unix = COALESCE(excluded.dune_first_seen_unix, dune_first_seen_unix), \
                    dune_closed_markets = COALESCE(excluded.dune_closed_markets, dune_closed_markets), \
                    dune_win_rate_bps = COALESCE(excluded.dune_win_rate_bps, dune_win_rate_bps), \
                    polymarket_contracts_seen = polymarket_contracts_seen | excluded.polymarket_contracts_seen",
            )?;
            let mut lift = tx.prepare("DELETE FROM purged_wallets WHERE wallet_hex = ?1")?;
            for (wallet, bits, infra, first_seen, closed, win_rate, version_bits) in rows {
                if purged.contains(wallet) {
                    if (bits & crate::pile::TOMBSTONE_OVERRIDE_SOURCES) != 0 {
                        // Override source (leaderboard/radion) → lift + re-admit.
                        lift.execute(params![wallet])?;
                        purged.remove(wallet);
                    } else {
                        // Non-override source → keep the tombstone, skip the row.
                        continue;
                    }
                }
                let is_infra_int: i64 = i64::from(*infra);
                stmt.execute(params![
                    wallet,
                    bits,
                    is_infra_int,
                    first_seen,
                    closed,
                    win_rate,
                    version_bits,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Load the tombstone set (`purged_wallets.wallet_hex`) into memory (issue #385).
    pub fn load_purged_set(&self) -> Result<std::collections::HashSet<String>, BootstrapError> {
        let mut stmt = self.conn.prepare("SELECT wallet_hex FROM purged_wallets")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<std::collections::HashSet<_>, _>>()?)
    }

    /// Whether a wallet is currently tombstoned (issue #385).
    pub fn is_purged(&self, wallet_hex: &str) -> Result<bool, BootstrapError> {
        let exists: i64 = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM purged_wallets WHERE wallet_hex = ?1)",
            params![wallet_hex],
            |r| r.get(0),
        )?;
        Ok(exists != 0)
    }

    /// Global `MAX(timestamp_unix)` over the whole `trades` table — the cache
    /// freshness probe (issue #385). `None` when the cache holds no trades.
    pub fn newest_trade_unix(&self) -> Result<Option<i64>, BootstrapError> {
        let ts: Option<i64> =
            self.conn
                .query_row("SELECT MAX(timestamp_unix) FROM trades", [], |r| r.get(0))?;
        Ok(ts)
    }

    /// Rule-B dead-weight candidates (issue #385): `is_active = 1`, non-infra
    /// wallets that were **refreshed this run** (`last_polymarket_fetch_at` within
    /// `staleness_secs` — so a soft-failed backfill's stale recency cannot trigger
    /// a delete) whose newest trade is older than `inactivity_secs`. A wallet with
    /// no trades (`MAX(timestamp_unix) IS NULL`) is NOT matched (`NULL < x` is
    /// NULL) — zero trades means zero disk to reclaim. Eligibility filtering is the
    /// caller's job (the eligible set comes from the ranker CSV, not the cache).
    pub fn select_dead_weight_candidates(
        &self,
        now_unix: i64,
        inactivity_secs: i64,
        staleness_secs: i64,
    ) -> Result<Vec<String>, BootstrapError> {
        let fresh_cutoff = now_unix - staleness_secs;
        let inactivity_cutoff = now_unix - inactivity_secs;
        let mut stmt = self.conn.prepare(
            "SELECT v.wallet_hex FROM active_tradeable_wallets v \
             WHERE v.last_polymarket_fetch_at IS NOT NULL \
               AND v.last_polymarket_fetch_at >= ?1 \
               AND (SELECT MAX(t.timestamp_unix) FROM trades t \
                    WHERE t.wallet_hex = v.wallet_hex) < ?2",
        )?;
        let rows = stmt.query_map(params![fresh_cutoff, inactivity_cutoff], |r| {
            r.get::<_, String>(0)
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Hard-delete `rows` from `trades` + `leaderboard_snapshots` + `wallets`
    /// (issue #385). Rule-A (`ProvenLoser`) rows additionally get a `purged_wallets`
    /// tombstone written **in the same chunk transaction** as their row deletions,
    /// so a mid-purge crash can never leave a proven loser deleted-but-untombstoned
    /// (which a non-override source would then silently re-ingest). Rule-B
    /// (`DeadWeight`) rows are deleted with no tombstone.
    ///
    /// In `dry_run` mode nothing is written: the report's `*_deleted` counts are
    /// estimates (trades via `wallets.trade_count`, snapshots via `COUNT`).
    ///
    /// # Precondition
    /// Empty `rows` returns a zero [`PurgeReport`] (no no-WHERE mass delete).
    pub fn purge_wallets(
        &mut self,
        rows: &[PurgeRow],
        now_unix: i64,
        dry_run: bool,
    ) -> Result<PurgeReport, BootstrapError> {
        let proven = rows.iter().filter(|r| r.reason.tombstoned()).count();
        let dead = rows.len() - proven;
        let mut report = PurgeReport {
            dry_run,
            ..PurgeReport::default()
        };
        if rows.is_empty() {
            return Ok(report);
        }

        if dry_run {
            let mut trades_est: i64 = 0;
            let mut snaps: i64 = 0;
            for chunk in rows.chunks(PURGE_CHUNK) {
                let placeholders = (1..=chunk.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let params_vec: Vec<&dyn rusqlite::ToSql> = chunk
                    .iter()
                    .map(|r| &r.wallet_hex as &dyn rusqlite::ToSql)
                    .collect();
                let tq = format!(
                    "SELECT COALESCE(SUM(trade_count), 0) FROM wallets WHERE wallet_hex IN ({placeholders})"
                );
                trades_est += self
                    .conn
                    .query_row(&tq, &params_vec[..], |r| r.get::<_, i64>(0))?;
                let sq = format!(
                    "SELECT COUNT(*) FROM leaderboard_snapshots WHERE wallet_hex IN ({placeholders})"
                );
                snaps += self
                    .conn
                    .query_row(&sq, &params_vec[..], |r| r.get::<_, i64>(0))?;
            }
            report.proven_losers_deleted = proven;
            report.dead_weight_deleted = dead;
            report.tombstones_written = proven;
            report.trades_deleted = usize::try_from(trades_est).unwrap_or(usize::MAX);
            report.snapshots_deleted = usize::try_from(snaps).unwrap_or(usize::MAX);
            return Ok(report);
        }

        // Armed: delete per chunk in ONE transaction so each rule-A wallet's
        // tombstone and its row deletions commit atomically (crash-safety).
        for chunk in rows.chunks(PURGE_CHUNK) {
            let tx = self.conn.transaction()?;
            {
                let mut del_trades = tx.prepare("DELETE FROM trades WHERE wallet_hex = ?1")?;
                let mut del_snaps =
                    tx.prepare("DELETE FROM leaderboard_snapshots WHERE wallet_hex = ?1")?;
                let mut del_wallet = tx.prepare("DELETE FROM wallets WHERE wallet_hex = ?1")?;
                let mut tomb = tx.prepare(
                    "INSERT OR REPLACE INTO purged_wallets (wallet_hex, purged_at_unix, reason) \
                     VALUES (?1, ?2, ?3)",
                )?;
                for r in chunk {
                    report.trades_deleted += del_trades.execute(params![r.wallet_hex])?;
                    report.snapshots_deleted += del_snaps.execute(params![r.wallet_hex])?;
                    del_wallet.execute(params![r.wallet_hex])?;
                    if r.reason.tombstoned() {
                        tomb.execute(params![r.wallet_hex, now_unix, r.reason.as_str()])?;
                        report.tombstones_written += 1;
                        report.proven_losers_deleted += 1;
                    } else {
                        report.dead_weight_deleted += 1;
                    }
                }
            }
            tx.commit()?;
        }
        Ok(report)
    }

    /// `VACUUM` the database to reclaim freed pages (issue #385). Must run OUTSIDE
    /// any transaction — the purge orchestrator calls this after the chunked
    /// deletes commit.
    pub fn vacuum(&mut self) -> Result<(), BootstrapError> {
        self.conn.execute_batch("VACUUM")?;
        Ok(())
    }

    /// Drop the two non-lookup `trades` secondary indexes (`idx_trades_market_id`
    /// and `idx_trades_buy_market_outcome_wallet_ts`) before an armed bulk delete
    /// (issue #401). The per-wallet `DELETE FROM trades WHERE wallet_hex = ?`
    /// then maintains only `idx_trades_wallet_ts` (the lookup index it needs) and
    /// the `source_trade_id` PK — turning ~5 random B-tree writes per row into 3,
    /// so the dropped indexes are rebuilt once on the compacted table afterward
    /// instead of being churned per row. Idempotent (`DROP INDEX IF EXISTS`).
    ///
    /// # Precondition
    /// Pair with [`Self::create_trades_bulk_delete_indexes`] (`run_purge` calls
    /// both around the delete). A hard crash between drop and rebuild leaves the
    /// two indexes *absent*, and the next [`Self::open`] recreates them from the
    /// shared `SCHEMA` DDL — never *divergent* (see [`IDX_TRADES_MARKET_ID_DDL`]).
    pub fn drop_trades_bulk_delete_indexes(&mut self) -> Result<(), BootstrapError> {
        self.conn.execute_batch(
            "DROP INDEX IF EXISTS idx_trades_market_id;\n\
             DROP INDEX IF EXISTS idx_trades_buy_market_outcome_wallet_ts;",
        )?;
        Ok(())
    }

    /// Rebuild the two indexes dropped by [`Self::drop_trades_bulk_delete_indexes`]
    /// from the SAME shared `const` DDL that `SCHEMA` uses (issue #401), so the
    /// on-open definition and this rebuild cannot diverge. Idempotent
    /// (`CREATE INDEX IF NOT EXISTS`); each build is a single sequential scan +
    /// external sort over the post-delete `trades` table.
    pub fn create_trades_bulk_delete_indexes(&mut self) -> Result<(), BootstrapError> {
        self.conn.execute_batch(IDX_TRADES_MARKET_ID_DDL)?;
        self.conn
            .execute_batch(IDX_TRADES_BUY_MARKET_OUTCOME_WALLET_TS_DDL)?;
        Ok(())
    }

    /// Backfill `trade_count` for every wallet from the `trades` table.
    ///
    /// Idempotent. Run after migrate ingests trades and after each `run_backfill`
    /// pass so the activation rule sees up-to-date counts.
    pub fn refresh_trade_counts(&mut self) -> Result<usize, BootstrapError> {
        let affected = self.conn.execute(
            "UPDATE wallets SET trade_count = COALESCE((\
                SELECT COUNT(*) FROM trades WHERE trades.wallet_hex = wallets.wallet_hex\
             ), 0)",
            [],
        )?;
        Ok(affected)
    }

    /// Refresh `trade_count` for a single wallet — used after a per-wallet backfill.
    pub fn refresh_trade_count_for(&mut self, wallet_hex: &str) -> Result<(), BootstrapError> {
        self.conn.execute(
            "UPDATE wallets SET trade_count = COALESCE((\
                SELECT COUNT(*) FROM trades WHERE trades.wallet_hex = wallets.wallet_hex\
             ), 0) WHERE wallet_hex = ?1",
            params![wallet_hex],
        )?;
        Ok(())
    }

    /// Seed `last_polymarket_fetch_at = MAX(trades.timestamp_unix)` for wallets with trades.
    ///
    /// Run during migrate so existing wallets don't enter the backfill queue
    /// with NULL timestamps — that would trigger a redundant Polymarket re-fetch.
    pub fn seed_last_polymarket_fetch_from_trades(&mut self) -> Result<usize, BootstrapError> {
        let affected = self.conn.execute(
            "UPDATE wallets SET last_polymarket_fetch_at = (\
                SELECT MAX(timestamp_unix) FROM trades WHERE trades.wallet_hex = wallets.wallet_hex\
             ) WHERE EXISTS (\
                SELECT 1 FROM trades WHERE trades.wallet_hex = wallets.wallet_hex\
             )",
            [],
        )?;
        Ok(affected)
    }

    /// Set `last_polymarket_fetch_at = now_unix` for a single wallet.
    pub fn update_last_polymarket_fetch(
        &mut self,
        wallet_hex: &str,
        now_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "UPDATE wallets SET last_polymarket_fetch_at = ?2 WHERE wallet_hex = ?1",
            params![wallet_hex, now_unix],
        )?;
        Ok(())
    }

    /// Set `last_funder_fetch_at = now_unix` for a single wallet.
    pub fn update_last_funder_fetch(
        &mut self,
        wallet_hex: &str,
        now_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "UPDATE wallets SET last_funder_fetch_at = ?2 WHERE wallet_hex = ?1",
            params![wallet_hex, now_unix],
        )?;
        Ok(())
    }

    /// Set `last_polymarket_full_at = now_unix` for a single wallet.
    ///
    /// Written by `backfill::run_backfill` after a Polymarket full-fetch
    /// succeeds (the wallet was in the legacy due set or paranoia set, not
    /// only in `delta_set`). Resets the 7-day paranoia clock for the wallet.
    pub fn update_last_polymarket_full_at(
        &mut self,
        wallet_hex: &str,
        now_unix: i64,
    ) -> Result<(), BootstrapError> {
        self.conn.execute(
            "UPDATE wallets SET last_polymarket_full_at = ?2 WHERE wallet_hex = ?1",
            params![wallet_hex, now_unix],
        )?;
        Ok(())
    }

    /// Select active wallets due for a Polymarket full-fetch (issue #176
    /// paranoia backstop). Filters `is_active = 1` AND
    /// (`last_polymarket_full_at IS NULL` OR `< now_unix - staleness_secs`).
    /// Same NULLS-FIRST ordering as [`Self::select_backfill_due`].
    pub fn wallets_due_for_full_fetch(
        &self,
        now_unix: i64,
        staleness_secs: i64,
    ) -> Result<Vec<String>, BootstrapError> {
        let cutoff = now_unix - staleness_secs;
        let mut stmt = self.conn.prepare(
            "SELECT wallet_hex FROM active_tradeable_wallets \
             WHERE (last_polymarket_full_at IS NULL OR last_polymarket_full_at < ?1) \
             ORDER BY last_polymarket_full_at ASC NULLS FIRST, \
                      dune_win_rate_bps DESC NULLS LAST, \
                      dune_closed_markets DESC NULLS LAST",
        )?;
        let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Mark a wallet as infrastructure (sticky 0→1).
    pub fn mark_infra(&mut self, wallet_hex: &str) -> Result<(), BootstrapError> {
        self.conn.execute(
            "UPDATE wallets SET is_infra = 1 WHERE wallet_hex = ?1",
            params![wallet_hex],
        )?;
        Ok(())
    }

    /// Bulk-mark wallets as infrastructure (sticky). Idempotent.
    pub fn mark_infra_bulk(&mut self, wallet_hexes: &[String]) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE wallets SET is_infra = 1 WHERE wallet_hex = ?1")?;
            for w in wallet_hexes {
                stmt.execute(params![w])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Retroactively classify already-cached wallets as infra (issue #197).
    ///
    /// Mirrors the cold-start probe semantics over cached trades: for each
    /// wallet that has ≥500 trades, compute the timestamp span over the
    /// OLDEST 500. If `(newest - oldest) < threshold_secs`, the wallet is
    /// infra. The window-function SQL uses the `idx_trades_wallet_ts` index
    /// for a direct partition+order scan.
    ///
    /// A SINGLE pass over the index produces both counts: every row
    /// contributes to `scanned`, rows where the span is below threshold
    /// also contribute to `flagged`. (Earlier two-pass version doubled the
    /// I/O cost — ~40 GB read per pass on a 100 GB DB.)
    ///
    /// When `dry_run` is `true`, the report is computed but no `UPDATE`
    /// runs — use this to preview before applying.
    pub fn classify_infra_retroactive(
        &mut self,
        threshold_secs: i64,
        dry_run: bool,
    ) -> Result<ClassifyInfraReport, BootstrapError> {
        // Single scan: every wallet with ≥500 trades contributes one row;
        // `is_infra` = 1 when the OLDEST 500 span is below threshold.
        let (scanned, candidates): (usize, Vec<String>) = {
            let mut stmt = self.conn.prepare(
                "WITH ranked AS ( \
                    SELECT wallet_hex, timestamp_unix, \
                           ROW_NUMBER() OVER ( \
                               PARTITION BY wallet_hex ORDER BY timestamp_unix ASC \
                           ) AS rn \
                    FROM trades \
                 ), \
                 page AS ( \
                    SELECT wallet_hex, \
                           MIN(timestamp_unix) AS oldest, \
                           MAX(timestamp_unix) AS newest, \
                           COUNT(*) AS cnt \
                    FROM ranked WHERE rn <= 500 \
                    GROUP BY wallet_hex \
                 ) \
                 SELECT wallet_hex, ((newest - oldest) < ?1) AS is_infra FROM page \
                 WHERE cnt = 500",
            )?;
            let rows = stmt.query_map(params![threshold_secs], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?;
            let mut scanned = 0usize;
            let mut candidates: Vec<String> = Vec::new();
            for row in rows {
                let (wallet_hex, is_infra) = row?;
                scanned += 1;
                if is_infra != 0 {
                    candidates.push(wallet_hex);
                }
            }
            (scanned, candidates)
        };

        let flagged = candidates.len();
        if !dry_run && !candidates.is_empty() {
            self.mark_infra_bulk(&candidates)?;
        }
        Ok(ClassifyInfraReport {
            scanned,
            flagged,
            dry_run,
        })
    }

    /// Select active wallets due for Polymarket backfill.
    ///
    /// Filters `is_active = 1` AND (`last_polymarket_fetch_at IS NULL` OR
    /// `last_polymarket_fetch_at < now_unix - staleness_secs`). Orders by
    /// `last_polymarket_fetch_at ASC NULLS FIRST` then by Dune quality signals
    /// (`dune_win_rate_bps DESC NULLS LAST, dune_closed_markets DESC NULLS LAST`).
    /// `limit = 0` returns all due wallets (no LIMIT clause).
    pub fn select_backfill_due(
        &self,
        now_unix: i64,
        staleness_secs: i64,
        limit: usize,
    ) -> Result<Vec<String>, BootstrapError> {
        let cutoff = now_unix - staleness_secs;
        let base = "SELECT wallet_hex FROM active_tradeable_wallets \
                    WHERE (last_polymarket_fetch_at IS NULL OR last_polymarket_fetch_at < ?1) \
                    ORDER BY last_polymarket_fetch_at ASC NULLS FIRST, \
                             dune_win_rate_bps DESC NULLS LAST, \
                             dune_closed_markets DESC NULLS LAST";
        let result = if limit == 0 {
            let mut stmt = self.conn.prepare(base)?;
            let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let limited = format!("{base} LIMIT ?2");
            let mut stmt = self.conn.prepare(&limited)?;
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows = stmt.query_map(params![cutoff, limit_i64], |r| r.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        Ok(result)
    }

    /// Select active wallets due for weekly funder refresh.
    ///
    /// Same shape as [`Self::select_backfill_due`] but keyed on
    /// `last_funder_fetch_at` with a 7-day staleness window by default.
    pub fn select_weekly_due(
        &self,
        now_unix: i64,
        staleness_secs: i64,
        limit: usize,
    ) -> Result<Vec<String>, BootstrapError> {
        let cutoff = now_unix - staleness_secs;
        let base = "SELECT wallet_hex FROM active_tradeable_wallets \
                    WHERE (last_funder_fetch_at IS NULL OR last_funder_fetch_at < ?1) \
                    ORDER BY last_funder_fetch_at ASC NULLS FIRST, \
                             dune_win_rate_bps DESC NULLS LAST, \
                             dune_closed_markets DESC NULLS LAST";
        let result = if limit == 0 {
            let mut stmt = self.conn.prepare(base)?;
            let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let limited = format!("{base} LIMIT ?2");
            let mut stmt = self.conn.prepare(&limited)?;
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows = stmt.query_map(params![cutoff, limit_i64], |r| r.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        Ok(result)
    }

    /// Apply the activation rule. Returns the number of newly-activated wallets.
    ///
    /// Sticky semantics: only `is_active = 0` rows are considered. `is_infra = 0`
    /// gates **every** activation branch — a wallet listed in both the infra CSV
    /// and a curation list (leaderboard/radion/502-gap) stays inactive.
    ///
    /// Issue #385 defense-in-depth: a tombstoned wallet (`purged_wallets`) is
    /// never activated even if a stray row exists. The primary guard is the
    /// `upsert_wallets_bulk` gate (a tombstoned wallet has no row to activate
    /// unless re-admitted by an override source, which deletes the tombstone
    /// first); this clause blocks the activation path regardless.
    pub fn apply_activation_rules(&mut self, min_trades: i64) -> Result<usize, BootstrapError> {
        let affected = self.conn.execute(
            "UPDATE wallets SET is_active = 1 \
             WHERE is_active = 0 AND is_infra = 0 \
             AND wallet_hex NOT IN (SELECT wallet_hex FROM purged_wallets) AND (\
                COALESCE(trade_count, 0) >= ?1 \
             OR COALESCE(dune_closed_markets, 0) >= ?1 \
             OR (source_bits & 16) != 0 \
             OR (source_bits & 32) != 0 \
             OR (source_bits & 64) != 0 \
             OR (source_bits & 128) != 0\
             )",
            params![min_trades],
        )?;
        Ok(affected)
    }

    /// Number of wallets currently in the pile (any status).
    pub fn wallet_pile_size(&self) -> Result<usize, BootstrapError> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM wallets", [], |r| r.get(0))?;
        usize::try_from(n).map_err(|_| BootstrapError::Internal)
    }

    /// Number of `is_active = 1` wallets in the pile.
    pub fn active_wallet_count(&self) -> Result<usize, BootstrapError> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM wallets WHERE is_active = 1",
            [],
            |r| r.get(0),
        )?;
        usize::try_from(n).map_err(|_| BootstrapError::Internal)
    }

    /// Return every `wallet_hex` in the pile (full `wallets`-table scan).
    pub fn all_pile_wallet_hexes(&self) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self.conn.prepare("SELECT wallet_hex FROM wallets")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let out: Result<Vec<_>, _> = rows.collect();
        Ok(out?)
    }

    /// Return every `wallet_hex` in the `active_tradeable_wallets` view
    /// (`is_active = 1 AND is_infra = 0`) — the active candidate universe.
    /// Unfiltered, unlike `select_backfill_due` which adds staleness/limit
    /// clauses; mirrors [`Self::all_pile_wallet_hexes`].
    pub fn active_tradeable_wallet_hexes(&self) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_hex FROM active_tradeable_wallets")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let out: Result<Vec<_>, _> = rows.collect();
        Ok(out?)
    }

    /// Return every `wallet_hex` whose `source_bits` has at least one bit in
    /// common with `bit_mask`. Issue #181: used by `run()` to scope per-wallet
    /// trade fetch to the discovered-wallet subset (typically `SRC_WALLET_SET_JSON`),
    /// avoiding the ~2.7M-row blowup of [`Self::all_pile_wallet_hexes`].
    pub fn wallets_with_source_bit(&self, bit_mask: i64) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT wallet_hex FROM wallets \
                 WHERE (source_bits & ?1) != 0 AND is_infra = 0",
        )?;
        let rows = stmt.query_map(params![bit_mask], |r| r.get::<_, String>(0))?;
        let out: Result<Vec<_>, _> = rows.collect();
        Ok(out?)
    }

    // ── test-only escape hatches (issue #166 pile tests) ─────────────────────
    //
    // Gated on `cfg(test)` for unit tests and `feature = "scenario"` for
    // integration tests under `tests/scenario_*.rs`. `expect` is allowed here
    // because these helpers are only used in test fixtures with controlled
    // inputs; a failure indicates a bug in the test itself.

    /// Test/scenario-only raw `Connection` accessor for ad-hoc queries
    /// against the cache (e.g. delta-audit assertions). Hidden behind the
    /// same feature gate as the other test helpers.
    #[cfg(any(test, feature = "scenario"))]
    pub fn raw_conn_for_test(&self) -> &Connection {
        &self.conn
    }

    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_set_trade_count(&mut self, wallet_hex: &str, count: i64) {
        self.conn
            .execute(
                "UPDATE wallets SET trade_count = ?2 WHERE wallet_hex = ?1",
                params![wallet_hex, count],
            )
            .expect("test-only direct SQL must succeed");
    }

    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_set_active(&mut self, wallet_hex: &str, active: i64) {
        self.conn
            .execute(
                "UPDATE wallets SET is_active = ?2 WHERE wallet_hex = ?1",
                params![wallet_hex, active],
            )
            .expect("test-only direct SQL must succeed");
    }

    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_source_bits(&self, wallet_hex: &str) -> i64 {
        self.conn
            .query_row(
                "SELECT source_bits FROM wallets WHERE wallet_hex = ?1",
                params![wallet_hex],
                |r| r.get(0),
            )
            .expect("test-only direct SQL must succeed")
    }

    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_is_infra(&self, wallet_hex: &str) -> bool {
        let n: i64 = self
            .conn
            .query_row(
                "SELECT is_infra FROM wallets WHERE wallet_hex = ?1",
                params![wallet_hex],
                |r| r.get(0),
            )
            .expect("test-only direct SQL must succeed");
        n != 0
    }

    /// Test-only accessor for `polymarket_contracts_seen` (issue #186 V1/V2 bitmask).
    ///
    /// Returns 0 for a wallet that has not yet been observed via on-chain
    /// enumeration (e.g. ingested via Dune-only or trade-fetch paths).
    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_contracts_seen(&self, wallet_hex: &str) -> i64 {
        self.conn
            .query_row(
                "SELECT polymarket_contracts_seen FROM wallets WHERE wallet_hex = ?1",
                params![wallet_hex],
                |r| r.get(0),
            )
            .expect("test-only direct SQL must succeed")
    }

    /// Insert one minimal `trades` row (issue #385 scenarios) so a wallet has a
    /// controllable newest-trade timestamp without going through the fetch path.
    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_insert_trade(
        &mut self,
        wallet_hex: &str,
        source_trade_id: &str,
        timestamp_unix: i64,
    ) {
        self.conn
            .execute(
                "INSERT INTO trades \
                 (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, ?2, 'm', 0, 'buy', '0.5', 1, ?3)",
                params![source_trade_id, wallet_hex, timestamp_unix],
            )
            .expect("test-only direct SQL must succeed");
    }

    /// Insert one `leaderboard_snapshots` row (issue #385 scenarios).
    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_insert_snapshot(&mut self, wallet_hex: &str, snapshot_at_unix: i64) {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO leaderboard_snapshots (snapshot_at_unix, wallet_hex) \
                 VALUES (?1, ?2)",
                params![snapshot_at_unix, wallet_hex],
            )
            .expect("test-only direct SQL must succeed");
    }

    /// Whether a `wallets` row exists for `wallet_hex` (issue #385 scenarios).
    #[cfg(any(test, feature = "scenario"))]
    #[allow(clippy::expect_used)]
    pub fn conn_for_test_wallet_exists(&self, wallet_hex: &str) -> bool {
        let n: i64 = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM wallets WHERE wallet_hex = ?1)",
                params![wallet_hex],
                |r| r.get(0),
            )
            .expect("test-only direct SQL must succeed");
        n != 0
    }
}

/// Resolved-market record loaded from the `market_resolutions` table.
///
/// Only rows with `winning_outcome_id IS NOT NULL` are materialised in the
/// [`ResolutionIndex`]; voided markets are filtered at load time.
#[derive(Debug, Clone)]
pub struct MarketResolution {
    /// 0-based index of the winning outcome (e.g. `OutcomeId(0)` = YES, `OutcomeId(1)` = NO).
    /// Promoted from raw `u8` to `OutcomeId` (issue #159) to eliminate the `.0` deref footgun.
    pub winning_outcome_id: OutcomeId,
    /// Unix seconds when the market was closed (from Gamma `closedTime`).
    pub resolved_at_unix: i64,
}

/// In-memory map from [`MarketId`] to [`MarketResolution`].
///
/// Built once at backtest startup via [`WalletCache::load_all_resolutions`].
pub type ResolutionIndex = HashMap<MarketId, MarketResolution>;

/// Scheduled-endDate record loaded from the `market_schedules` table.
///
/// `end_date_unix = None` means Gamma had no `endDate` for this market — the backtest
/// treats this as "allow through" (no suppression) rather than falling back to
/// `resolved_at_unix`, which would leak future state.
#[derive(Debug, Clone)]
pub struct MarketSchedule {
    /// Unix seconds of the market's scheduled `endDate`, or `None` if Gamma had none.
    pub end_date_unix: Option<i64>,
}

/// In-memory map from [`MarketId`] to [`MarketSchedule`].
///
/// Built once at backtest startup via [`WalletCache::load_all_schedules`].
/// Presence in this index means the market has been queried; absence means it has
/// not yet been fetched and the buy-filter falls back to [`ResolutionIndex`].
pub type ScheduleIndex = HashMap<MarketId, MarketSchedule>;

/// In-memory map from [`MarketId`] to its Gamma `liquidity` USD value (current
/// order-book depth indicator).
///
/// Built once at backtest startup via [`WalletCache::load_all_liquidity`]. Used by
/// the simulation's liquidity-aware sizing clamp ([`pe_risk_engine::clamp_contracts_to_liquidity`]).
/// Absence of a market from this index means "no data" — the caller supplies
/// `Decimal::ZERO` to the clamp, which falls below the `min_required_usd` floor
/// and passes through.
pub type LiquidityIndex = HashMap<MarketId, Decimal>;

/// In-memory index of every leaderboard snapshot in the cache, sorted ascending.
///
/// Built once via [`WalletCache::load_all_snapshots`]; the backtest then queries
/// [`LeaderboardSnapshots::for_date`] per simulated day. The internal layout is a
/// sorted `Vec` rather than a `BTreeMap` because lookups are sequential by date
/// in the simulation and a binary search is sufficient at current snapshot counts
/// (typically 8–52 per cache).
#[derive(Debug, Default, Clone)]
pub struct LeaderboardSnapshots {
    /// `(snapshot_at_unix, wallet set)` ascending by `snapshot_at_unix`.
    entries: Vec<(i64, HashSet<WalletAddress>)>,
}

impl LeaderboardSnapshots {
    /// True when no snapshots exist (the backtest's fallback path applies).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the wallet set for the most-recent snapshot at or before `sim_date_unix`,
    /// or `None` when the simulation date precedes the first seeded snapshot.
    pub fn for_date(&self, sim_date_unix: i64) -> Option<&HashSet<WalletAddress>> {
        // partition_point finds the index of the first entry strictly greater than
        // sim_date_unix; the most-recent snapshot ≤ sim_date is at idx-1.
        let idx = self.entries.partition_point(|(at, _)| *at <= sim_date_unix);
        if idx == 0 {
            None
        } else {
            self.entries.get(idx - 1).map(|(_, set)| set)
        }
    }

    /// Return the `snapshot_at_unix` of the most-recent leaderboard snapshot
    /// at-or-before `sim_date_unix`. Used by the simulation to detect pool
    /// transitions for the log-gate.
    pub fn snapshot_at_for_date(&self, sim_date_unix: i64) -> Option<i64> {
        let idx = self.entries.partition_point(|(at, _)| *at <= sim_date_unix);
        if idx == 0 {
            None
        } else {
            self.entries.get(idx - 1).map(|(at, _)| *at)
        }
    }

    /// Count of snapshots up to and including `sim_date_unix` in which `wallet`
    /// was a leaderboard member.
    ///
    /// Used by the backtest's snapshot-aware win-rate prior to measure how long
    /// a leader has been on the candidate pool: more appearances → more
    /// evidence → weaker added shrinkage. Inclusive upper-bound semantics match
    /// [`Self::for_date`] (`*at <= sim_date_unix`) — a wallet present on the
    /// snapshot whose timestamp exactly equals `sim_date_unix` counts as 1.
    ///
    /// O(S) linear scan from the start, capped at the inclusive upper bound via
    /// `partition_point`. At realistic snapshot counts (≤ 200) this is
    /// negligible — see issue #129 for the amortisation analysis.
    ///
    /// # Precondition
    /// Returns 0 when `sim_date_unix` precedes the first snapshot.
    pub fn snapshot_appearances_up_to(&self, wallet: WalletAddress, sim_date_unix: i64) -> u32 {
        let idx = self.entries.partition_point(|(at, _)| *at <= sim_date_unix);
        let count = self.entries[..idx]
            .iter()
            .filter(|(_, set)| set.contains(&wallet))
            .count();
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// Number of snapshots in the index. For diagnostics and tests.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Construct from raw `(unix, wallets)` pairs. Used by tests and the backtest's
    /// in-memory fallback paths so the filter can be exercised without a SQLite file.
    /// Pairs are sorted internally and duplicate timestamps are merged.
    pub fn from_pairs(mut pairs: Vec<(i64, Vec<WalletAddress>)>) -> Self {
        pairs.sort_by_key(|(at, _)| *at);
        let mut entries: Vec<(i64, HashSet<WalletAddress>)> = Vec::with_capacity(pairs.len());
        for (at, wallets) in pairs {
            match entries.last_mut() {
                Some((last_at, set)) if *last_at == at => {
                    set.extend(wallets);
                }
                _ => {
                    entries.push((at, wallets.into_iter().collect()));
                }
            }
        }
        Self { entries }
    }
}

#[allow(clippy::too_many_arguments)]
fn row_to_trade(
    wallet: WalletAddress,
    id: String,
    market_id: String,
    outcome_id: i64,
    side: &str,
    price_str: &str,
    contracts: i64,
    ts: i64,
) -> Option<RawTrade> {
    let outcome_id = OutcomeId::try_from(outcome_id).ok()?;
    let side = str_to_side(side)?;
    let price_dec = Decimal::from_str(price_str).ok()?;
    let price = Price::new(price_dec).ok()?;
    let contracts_u64 = u64::try_from(contracts).ok()?;
    let dt = OffsetDateTime::from_unix_timestamp(ts).ok()?;
    Some(RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId(market_id)),
        outcome_id,
        side,
        price,
        contracts: ContractQty(contracts_u64),
        timestamp: SourceTimestamp(dt),
        source_trade_id: SourceTradeId(id),
    })
}

/// Add column `col` with full SQL declaration `decl` (e.g. `"INTEGER NULL"` or
/// `"TEXT NOT NULL DEFAULT 'gamma'"`) to `table` if it is not already present. Idempotent: safe to
/// call on every [`WalletCache::open`].
///
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so existence is probed via `pragma_table_info`;
/// `ALTER TABLE … ADD COLUMN` with a constant DEFAULT back-fills existing rows. Used for the additive
/// migrations: the issue #149 `source` column on `market_resolutions`/`market_schedules` (every
/// pre-migration row was Gamma-written, so `'gamma'` is the correct retroactive tag), and the issue
/// #421 PR4 `start_date_unix` column on `market_schedules`.
fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    col: &str,
    decl: &str,
) -> Result<(), BootstrapError> {
    let col_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
            params![table, col],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;
    if !col_exists {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {col} {decl}");
        conn.execute_batch(&sql)?;
    }
    Ok(())
}

fn side_to_str(s: &Side) -> &'static str {
    match s {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn str_to_side(s: &str) -> Option<Side> {
    match s {
        "buy" => Some(Side::Buy),
        "sell" => Some(Side::Sell),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tempfile::TempDir;

    fn addr(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).unwrap()
    }

    fn make_trade(id: &str, wallet: WalletAddress, ts: i64) -> RawTrade {
        RawTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.60)).unwrap(),
            contracts: ContractQty(1),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
            source_trade_id: SourceTradeId(id.to_owned()),
        }
    }

    // ── market_events (issue #206) ────────────────────────────────────────────

    #[test]
    fn market_events_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .upsert_market_events("0xa", "evt1", Some("slug-1"), 100)
            .unwrap();
        cache
            .upsert_market_events("0xb", "evt1", Some("slug-1"), 100)
            .unwrap();
        let map = cache.load_market_event_map().unwrap();
        assert_eq!(map.get("0xa").map(String::as_str), Some("evt1"));
        assert_eq!(map.get("0xb").map(String::as_str), Some("evt1"));
        // Two conditions, one event.
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn market_events_replace_overwrites_on_conflict() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.upsert_market_events("0xa", "old", None, 100).unwrap();
        cache
            .upsert_market_events("0xa", "new", Some("s"), 200)
            .unwrap();
        let map = cache.load_market_event_map().unwrap();
        assert_eq!(map.get("0xa").map(String::as_str), Some("new"));
    }

    #[test]
    fn token_conditions_batch_round_trips_and_replaces() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        assert_eq!(cache.token_condition_count(), 0);
        cache
            .upsert_token_conditions_batch(
                &[
                    ("111".to_string(), "0xaa".to_string(), 0),
                    ("222".to_string(), "0xaa".to_string(), 1),
                    ("333".to_string(), "0xbb".to_string(), 0),
                ],
                100,
            )
            .unwrap();
        assert_eq!(cache.token_condition_count(), 3);
        // outcome_index round-trips (issue #429): 222 is the NO leg (index 1).
        assert_eq!(
            cache.token_condition_outcome("222"),
            Some(("0xaa".to_string(), Some(1)))
        );
        // INSERT OR REPLACE keyed on token_id: re-mapping a token updates, not dupes.
        cache
            .upsert_token_conditions_batch(&[("111".to_string(), "0xcc".to_string(), 0)], 200)
            .unwrap();
        assert_eq!(cache.token_condition_count(), 3);
        let cond: String = cache
            .conn
            .query_row(
                "SELECT condition_id FROM token_conditions WHERE token_id = '111'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cond, "0xcc");
    }

    #[test]
    fn token_conditions_empty_batch_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.upsert_token_conditions_batch(&[], 100).unwrap();
        assert_eq!(cache.token_condition_count(), 0);
    }

    #[test]
    fn token_condition_outcome_reads_back_and_misses_cleanly() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .upsert_token_conditions_batch(&[("t0".to_string(), "0xc".to_string(), 0)], 9)
            .unwrap();
        assert_eq!(
            cache.token_condition_outcome("t0"),
            Some(("0xc".to_string(), Some(0)))
        );
        // Unmapped token → None (the cross-check treats this as "nothing to compare").
        assert_eq!(cache.token_condition_outcome("missing"), None);
    }

    #[test]
    fn token_condition_outcome_surfaces_null_index() {
        // A legacy/events-sourced row written before the issue #429 migration has
        // a NULL outcome_index. The accessor must surface it as `Some((cond, None))`
        // so the CLOB cross-check's `stored_idx.is_some_and(..)` skips it (no
        // divergence verdict) and overwrites it with the CLOB position on re-walk.
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .conn
            .execute(
                "INSERT INTO token_conditions (token_id, condition_id, outcome_index, fetched_at_unix) \
                 VALUES ('legacy', '0xc', NULL, 9)",
                [],
            )
            .unwrap();
        assert_eq!(
            cache.token_condition_outcome("legacy"),
            Some(("0xc".to_string(), None))
        );
    }

    #[test]
    fn token_coverage_report_counts_resolved_with_winner_and_mapped() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        assert_eq!(cache.token_coverage_report(), (0, 0));
        // Two resolved-with-winner markets; one voided (winner NULL) → excluded.
        cache.insert_resolution("0xm1", Some(0), 2000, 9).unwrap();
        cache.insert_resolution("0xm2", Some(1), 2000, 9).unwrap();
        cache.insert_resolution("0xvoid", None, 2000, 9).unwrap();
        // Only 0xm1 has a token map.
        cache
            .upsert_token_conditions_batch(&[("t1".to_string(), "0xm1".to_string(), 0)], 9)
            .unwrap();
        // total = 2 winner markets; mapped = 1.
        assert_eq!(cache.token_coverage_report(), (2, 1));
    }

    #[test]
    fn self_map_orphan_inserts_singleton_when_absent() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.self_map_orphan("0xorphan", 100).unwrap();
        let map = cache.load_market_event_map().unwrap();
        // Orphan maps to itself.
        assert_eq!(map.get("0xorphan").map(String::as_str), Some("0xorphan"));
    }

    #[test]
    fn self_map_orphan_does_not_overwrite_real_mapping() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .upsert_market_events("0xa", "realevt", Some("s"), 100)
            .unwrap();
        // Orphan pass must not clobber the real event mapping.
        cache.self_map_orphan("0xa", 200).unwrap();
        let map = cache.load_market_event_map().unwrap();
        assert_eq!(map.get("0xa").map(String::as_str), Some("realevt"));
    }

    #[test]
    fn mapped_condition_ids_returns_present_set() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.upsert_market_events("0xa", "e", None, 100).unwrap();
        cache.upsert_market_events("0xb", "e", None, 100).unwrap();
        let set = cache.mapped_condition_ids();
        assert!(set.contains("0xa") && set.contains("0xb"));
        assert_eq!(set.len(), 2);
    }

    fn tmp_cache(dir: &TempDir) -> WalletCache {
        WalletCache::open(&dir.path().join("cache.db")).unwrap()
    }

    #[test]
    fn fresh_cache_is_empty() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert_eq!(cache.trade_count(), 0);
        assert!(
            cache
                .known_trade_ids("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .is_empty()
        );
        assert!(
            cache
                .trades_for("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .is_empty()
        );
    }

    #[test]
    fn insert_new_stores_trades() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        let trades = vec![
            make_trade("0xtx2", wallet, 1_704_067_200),
            make_trade("0xtx1", wallet, 1_704_067_100),
        ];
        cache.insert_new(&hex, trades).unwrap();

        assert_eq!(cache.trade_count(), 2);
        let ids = cache.known_trade_ids(&hex);
        assert_eq!(ids.len(), 2);
        // newest-first order
        assert_eq!(ids[0], SourceTradeId("0xtx2".to_owned()));
        assert_eq!(ids[1], SourceTradeId("0xtx1".to_owned()));
        // trades_for returns ASC by timestamp
        let trades = cache.trades_for(&hex);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].source_trade_id, SourceTradeId("0xtx1".to_owned()));
        assert_eq!(trades[1].source_trade_id, SourceTradeId("0xtx2".to_owned()));
    }

    #[test]
    fn insert_new_is_idempotent_on_duplicate_ids() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        let trades = vec![make_trade("0xtx1", wallet, 1_704_067_100)];
        cache.insert_new(&hex, trades.clone()).unwrap();
        cache.insert_new(&hex, trades).unwrap();

        assert_eq!(cache.trade_count(), 1, "duplicate must not be stored twice");
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();
        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache
                .insert_new(&hex, vec![make_trade("0xtx1", wallet, 1_704_067_100)])
                .unwrap();
        }
        let cache2 = WalletCache::open(&path).unwrap();
        assert_eq!(cache2.trade_count(), 1);
        assert_eq!(cache2.trades_for(&hex).len(), 1);
    }

    #[test]
    fn all_trades_aggregates_across_wallets_sorted_by_timestamp() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache
            .insert_new(&w1.to_string(), vec![make_trade("0xtx1", w1, 1_000_000)])
            .unwrap();
        cache
            .insert_new(&w2.to_string(), vec![make_trade("0xtx2", w2, 2_000_000)])
            .unwrap();
        let all = cache.all_trades();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].source_trade_id, SourceTradeId("0xtx1".to_owned()));
        assert_eq!(all[1].source_trade_id, SourceTradeId("0xtx2".to_owned()));
    }

    #[test]
    fn all_wallet_addresses_enumerates_all() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache
            .insert_new(&w1.to_string(), vec![make_trade("0xtx1", w1, 1_000_000)])
            .unwrap();
        cache
            .insert_new(&w2.to_string(), vec![make_trade("0xtx2", w2, 2_000_000)])
            .unwrap();
        let addrs = cache.all_wallet_addresses();
        assert_eq!(addrs.len(), 2);
        assert!(addrs.contains(&w1.to_string()));
        assert!(addrs.contains(&w2.to_string()));
    }

    #[test]
    fn empty_insert_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache.insert_new(&wallet.to_string(), vec![]).unwrap();
        assert_eq!(cache.trade_count(), 0);
        assert_eq!(cache.wallet_count(), 0);
    }

    // ── leaderboard_snapshots tests ───────────────────────────────────────────

    #[test]
    fn fresh_cache_has_no_snapshots() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert!(cache.all_snapshot_dates().unwrap().is_empty());
        assert!(cache.snapshot_for_date(1_704_067_200).unwrap().is_none());
    }

    #[test]
    fn insert_snapshot_persists_and_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        cache.insert_snapshot(1_700_000_000, &[w1, w2]).unwrap();
        let dates = cache.all_snapshot_dates().unwrap();
        assert_eq!(dates, vec![1_700_000_000]);

        let (at, wallets) = cache.snapshot_for_date(1_700_000_000).unwrap().unwrap();
        assert_eq!(at, 1_700_000_000);
        assert_eq!(wallets.len(), 2);
        assert!(wallets.contains(&w1));
        assert!(wallets.contains(&w2));
    }

    #[test]
    fn insert_snapshot_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        cache.insert_snapshot(1_700_000_000, &[w1]).unwrap();
        cache.insert_snapshot(1_700_000_000, &[w1]).unwrap();
        let (_, wallets) = cache.snapshot_for_date(1_700_000_000).unwrap().unwrap();
        assert_eq!(wallets.len(), 1, "duplicate insert must not double-count");
    }

    #[test]
    fn snapshot_for_date_returns_most_recent_at_or_before() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let w3 = addr("0xcccccccccccccccccccccccccccccccccccccccc");

        cache.insert_snapshot(1_700_000_000, &[w1]).unwrap(); // week 1
        cache.insert_snapshot(1_700_604_800, &[w2]).unwrap(); // week 2 (+7 days)
        cache.insert_snapshot(1_701_209_600, &[w3]).unwrap(); // week 3 (+14 days)

        // Exactly on a boundary returns that snapshot.
        let (at, w) = cache.snapshot_for_date(1_700_604_800).unwrap().unwrap();
        assert_eq!(at, 1_700_604_800);
        assert_eq!(w, vec![w2]);

        // Between week 2 and week 3 returns week 2.
        let mid = 1_700_604_800 + 86_400; // +1 day after week 2
        let (at, w) = cache.snapshot_for_date(mid).unwrap().unwrap();
        assert_eq!(at, 1_700_604_800);
        assert_eq!(w, vec![w2]);

        // After all snapshots returns the latest.
        let after = 1_701_209_600 + 86_400 * 30;
        let (at, w) = cache.snapshot_for_date(after).unwrap().unwrap();
        assert_eq!(at, 1_701_209_600);
        assert_eq!(w, vec![w3]);

        // Before the first snapshot returns None.
        let before = 1_700_000_000 - 1;
        assert!(cache.snapshot_for_date(before).unwrap().is_none());
    }

    #[test]
    fn snapshot_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache.insert_snapshot(1_700_000_000, &[w1]).unwrap();
        }
        let cache2 = WalletCache::open(&path).unwrap();
        assert_eq!(cache2.all_snapshot_dates().unwrap(), vec![1_700_000_000]);
    }

    #[test]
    fn empty_snapshot_insert_records_anchor_with_no_wallets() {
        // Distinguishing "no qualifying wallets that week" from "never seeded" is
        // load-bearing for the historical-seed driver and the backtest filter — see
        // EMPTY_SNAPSHOT_SENTINEL doc above. An empty insert must persist the date.
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.insert_snapshot(1_700_000_000, &[]).unwrap();
        assert_eq!(
            cache.all_snapshot_dates().unwrap(),
            vec![1_700_000_000],
            "empty-week anchor must appear in all_snapshot_dates"
        );
        let (at, wallets) = cache.snapshot_for_date(1_700_000_000).unwrap().unwrap();
        assert_eq!(at, 1_700_000_000);
        assert!(
            wallets.is_empty(),
            "empty-week wallet set must be empty (sentinel filtered out)"
        );
    }

    #[test]
    fn empty_snapshot_anchor_blocks_fallthrough_to_prior_week() {
        // Without the empty-anchor design, a Dune-empty week W2 would silently let
        // for_date(W2) fall through to W1's pool. The sentinel row prevents that.
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        cache.insert_snapshot(1_700_000_000, &[w1]).unwrap();
        cache.insert_snapshot(1_700_604_800, &[]).unwrap(); // empty week W2

        let (at, wallets) = cache.snapshot_for_date(1_700_604_800).unwrap().unwrap();
        assert_eq!(at, 1_700_604_800, "must return W2's anchor, not W1's");
        assert!(wallets.is_empty(), "W2 was empty — must return empty set");
    }

    #[test]
    fn empty_snapshot_load_all_includes_anchor() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.insert_snapshot(1_700_000_000, &[]).unwrap();
        let snaps = cache.load_all_snapshots().unwrap();
        assert_eq!(snaps.len(), 1, "empty-week anchor must appear in index");
        let pool = snaps.for_date(1_700_000_000);
        assert!(pool.is_some(), "for_date must resolve the anchor");
        assert!(pool.unwrap().is_empty(), "wallet set must be empty");
    }

    #[test]
    fn snapshot_table_is_independent_of_trades() {
        // Adding snapshots must not alter trade rows or wallet counts.
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache
            .insert_new(&w1.to_string(), vec![make_trade("0xtx1", w1, 1_000_000)])
            .unwrap();
        cache.insert_snapshot(1_700_000_000, &[w1]).unwrap();
        assert_eq!(cache.trade_count(), 1);
        assert_eq!(cache.wallet_count(), 1);
        assert_eq!(cache.all_snapshot_dates().unwrap().len(), 1);
    }

    #[test]
    fn price_round_trips_through_decimal_text() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();
        let trade = RawTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(1),
            side: Side::Sell,
            price: Price::new(dec!(0.123456789)).unwrap(),
            contracts: ContractQty(7),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap()),
            source_trade_id: SourceTradeId("0xfoo".to_owned()),
        };
        cache.insert_new(&hex, vec![trade]).unwrap();
        let trades = cache.trades_for(&hex);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price.0, dec!(0.123456789));
        assert_eq!(trades[0].outcome_id, OutcomeId(1));
        assert_eq!(trades[0].side, Side::Sell);
        assert_eq!(trades[0].contracts, ContractQty(7));
    }

    // ── market_resolutions tests ──────────────────────────────────────────────

    #[test]
    fn insert_resolution_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache
                .insert_resolution("0xcond_yes", Some(0), 1_700_000_100, 1_700_000_200)
                .unwrap();
        }
        let cache2 = WalletCache::open(&path).unwrap();
        let idx = cache2.load_all_resolutions().unwrap();
        assert_eq!(idx.len(), 1);
        let m = idx
            .get(&MarketId(VenueMarketId("0xcond_yes".to_owned())))
            .unwrap();
        assert_eq!(m.winning_outcome_id, OutcomeId(0));
        assert_eq!(m.resolved_at_unix, 1_700_000_100);
    }

    #[test]
    fn insert_resolution_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond", Some(1), 1_700_000_000, 1_700_000_001)
            .unwrap();
        // Second insert with different data — INSERT OR IGNORE should keep first.
        cache
            .insert_resolution("0xcond", Some(0), 1_700_000_999, 1_700_001_000)
            .unwrap();
        let idx = cache.load_all_resolutions().unwrap();
        assert_eq!(idx.len(), 1);
        let m = idx
            .get(&MarketId(VenueMarketId("0xcond".to_owned())))
            .unwrap();
        assert_eq!(m.winning_outcome_id, OutcomeId(1), "first insert must win");
    }

    // Issue #159: ensure a multi-outcome market with winner index > 255
    // round-trips intact through SQLite, the OutcomeId::try_from path, and the
    // ResolutionIndex. Pre-#159 this would have lost data at the load_all step.
    #[test]
    fn resolution_round_trips_large_outcome_id() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond_multi", Some(999), 1_700_000_100, 1_700_000_200)
            .unwrap();
        let idx = cache.load_all_resolutions().unwrap();
        let m = idx
            .get(&MarketId(VenueMarketId("0xcond_multi".to_owned())))
            .expect("market with outcome 999 must be present in resolution index");
        assert_eq!(m.winning_outcome_id, OutcomeId(999));
    }

    #[test]
    fn resolution_round_trips_u16_max_outcome_id() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond_max", Some(u16::MAX), 1_700_000_100, 1_700_000_200)
            .unwrap();
        let idx = cache.load_all_resolutions().unwrap();
        let m = idx
            .get(&MarketId(VenueMarketId("0xcond_max".to_owned())))
            .expect("u16::MAX outcome must round-trip");
        assert_eq!(m.winning_outcome_id, OutcomeId(u16::MAX));
    }

    #[test]
    fn load_all_resolutions_filters_null_winners() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond_voided", None, 1_700_000_000, 1_700_000_001)
            .unwrap();
        cache
            .insert_resolution("0xcond_resolved", Some(0), 1_700_000_100, 1_700_000_101)
            .unwrap();
        let idx = cache.load_all_resolutions().unwrap();
        assert_eq!(idx.len(), 1, "voided market must be excluded");
        assert!(idx.contains_key(&MarketId(VenueMarketId("0xcond_resolved".to_owned()))));
    }

    #[test]
    fn resolved_market_ids_returns_complete_set() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xa", Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();
        cache
            .insert_resolution("0xb", None, 1_700_000_100, 1_700_000_101)
            .unwrap();
        let ids = cache.resolved_market_ids();
        // Both resolved and voided rows count as "already fetched".
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("0xa"));
        assert!(ids.contains("0xb"));
    }

    #[test]
    fn all_market_ids_returns_distinct_from_trades() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut trade1 = make_trade("0xtx1", wallet, 1_000_000);
        trade1.market_id = MarketId(VenueMarketId("0xmkt_a".to_owned()));
        let mut trade2 = make_trade("0xtx2", wallet, 1_000_001);
        trade2.market_id = MarketId(VenueMarketId("0xmkt_a".to_owned())); // duplicate market
        let mut trade3 = make_trade("0xtx3", wallet, 1_000_002);
        trade3.market_id = MarketId(VenueMarketId("0xmkt_b".to_owned()));
        cache
            .insert_new(&wallet.to_string(), vec![trade1, trade2, trade3])
            .unwrap();
        let ids = cache.all_market_ids();
        assert_eq!(ids.len(), 2, "must deduplicate market_ids");
        assert!(ids.contains(&"0xmkt_a".to_owned()));
        assert!(ids.contains(&"0xmkt_b".to_owned()));
    }

    #[test]
    fn all_market_ids_query_uses_market_id_index() {
        // Regression guard for the #197-follow-up index: the DISTINCT market_id
        // enumeration must be served by idx_trades_market_id, not a full heap
        // scan of the (production: ~100 GB) trades table.
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        let mut stmt = cache
            .conn
            .prepare("EXPLAIN QUERY PLAN SELECT DISTINCT market_id FROM trades ORDER BY market_id")
            .unwrap();
        // EXPLAIN QUERY PLAN row shape: (id, parent, notused, detail); detail is col 3.
        let plan: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter().any(|d| d.contains("idx_trades_market_id")),
            "all_market_ids must use idx_trades_market_id; plan was: {plan:?}"
        );
    }

    #[test]
    fn max_resolved_at_unix_empty_returns_zero() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert_eq!(cache.max_resolved_at_unix().unwrap(), 0);
    }

    #[test]
    fn max_resolved_at_unix_returns_maximum() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xa", Some(0), 1_700_000_100, 1_700_000_200)
            .unwrap();
        cache
            .insert_resolution("0xb", Some(1), 1_700_000_500, 1_700_000_600)
            .unwrap();
        cache
            .insert_resolution("0xc", None, 1_700_000_300, 1_700_000_400)
            .unwrap();
        assert_eq!(cache.max_resolved_at_unix().unwrap(), 1_700_000_500);
    }

    #[test]
    fn resolution_table_independent_of_trades_and_snapshots() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache
            .insert_new(
                &wallet.to_string(),
                vec![make_trade("0xtx1", wallet, 1_000_000)],
            )
            .unwrap();
        cache.insert_snapshot(1_700_000_000, &[wallet]).unwrap();
        cache
            .insert_resolution("0xcond", Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();
        assert_eq!(cache.trade_count(), 1);
        assert_eq!(cache.wallet_count(), 1);
        assert_eq!(cache.all_snapshot_dates().unwrap().len(), 1);
        assert_eq!(cache.load_all_resolutions().unwrap().len(), 1);
    }

    // ── market_schedules unit tests ───────────────────────────────────────────

    #[test]
    fn insert_schedule_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xcond", Some(1_700_000_100), 1_700_000_200)
            .unwrap();
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1);
        let s = idx
            .get(&MarketId(VenueMarketId("0xcond".to_owned())))
            .unwrap();
        assert_eq!(s.end_date_unix, Some(1_700_000_100));
    }

    #[test]
    fn insert_schedule_null_end_date_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xcond", None, 1_700_000_200)
            .unwrap();
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1, "NULL end_date must still appear in the index");
        let s = idx
            .get(&MarketId(VenueMarketId("0xcond".to_owned())))
            .unwrap();
        assert_eq!(s.end_date_unix, None);
    }

    #[test]
    fn insert_schedule_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xcond", Some(1_700_000_100), 1_700_000_200)
            .unwrap();
        // Second insert with different data — INSERT OR IGNORE keeps the first.
        cache
            .insert_schedule("0xcond", Some(9_999_999_999), 9_999_999_999)
            .unwrap();
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1);
        let s = idx
            .get(&MarketId(VenueMarketId("0xcond".to_owned())))
            .unwrap();
        assert_eq!(
            s.end_date_unix,
            Some(1_700_000_100),
            "first insert must win"
        );
    }

    #[test]
    fn scheduled_market_ids_returns_complete_set() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
            .unwrap();
        cache.insert_schedule("0xb", None, 1_700_000_100).unwrap();
        let ids = cache.scheduled_market_ids();
        assert_eq!(
            ids.len(),
            2,
            "both NULL and non-NULL rows must be in the skip-set"
        );
        assert!(ids.contains("0xa"));
        assert!(ids.contains("0xb"));
    }

    #[test]
    fn null_schedule_market_ids_returns_only_null_rows() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xpopulated", Some(1_700_000_000), 1_700_000_001)
            .unwrap();
        cache
            .insert_schedule("0xnull_a", None, 1_700_000_002)
            .unwrap();
        cache
            .insert_schedule("0xnull_b", None, 1_700_000_003)
            .unwrap();
        let ids = cache.null_schedule_market_ids();
        assert_eq!(
            ids.len(),
            2,
            "only NULL rows belong in the null-rewrite candidate set"
        );
        assert!(ids.contains("0xnull_a"));
        assert!(ids.contains("0xnull_b"));
        assert!(
            !ids.contains("0xpopulated"),
            "populated rows must not appear"
        );
    }

    #[test]
    fn update_schedule_end_date_rewrites_null_row() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.insert_schedule("0xa", None, 1_700_000_000).unwrap();
        let changed = cache
            .update_schedule_end_date("0xa", 1_700_500_000, 1_700_500_001)
            .unwrap();
        assert!(changed, "rewrite of a NULL row must return Ok(true)");
        let mut sched = cache.load_all_schedules().unwrap();
        let row = sched
            .remove(&MarketId(VenueMarketId("0xa".to_owned())))
            .unwrap();
        assert_eq!(row.end_date_unix, Some(1_700_500_000));
    }

    #[test]
    fn update_schedule_end_date_preserves_non_null_row() {
        // Guards the `WHERE end_date_unix IS NULL` clause from being dropped in a
        // future refactor; without it, a source returning a wrong value could
        // silently corrupt good data.
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
            .unwrap();
        let changed = cache
            .update_schedule_end_date("0xa", 1_700_999_999, 1_700_999_999)
            .unwrap();
        assert!(!changed, "rewrite of a populated row must return Ok(false)");
        let mut sched = cache.load_all_schedules().unwrap();
        let row = sched
            .remove(&MarketId(VenueMarketId("0xa".to_owned())))
            .unwrap();
        assert_eq!(
            row.end_date_unix,
            Some(1_700_000_000),
            "original populated value must be preserved"
        );
    }

    #[test]
    fn update_schedule_end_date_returns_false_for_missing_row() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let changed = cache
            .update_schedule_end_date("0xnonexistent", 1_700_500_000, 1_700_500_001)
            .unwrap();
        assert!(!changed, "no row to update → Ok(false), not an error");
    }

    #[test]
    fn schedule_table_independent_of_resolutions() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond", Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();
        cache
            .insert_schedule("0xcond", Some(1_699_999_000), 1_700_000_001)
            .unwrap();
        // Both tables can have the same market_id independently.
        assert_eq!(cache.load_all_resolutions().unwrap().len(), 1);
        assert_eq!(cache.load_all_schedules().unwrap().len(), 1);
        // The schedule value (1_699_999_000) is preserved independent of the resolution.
        let sched = cache
            .load_all_schedules()
            .unwrap()
            .remove(&MarketId(VenueMarketId("0xcond".to_owned())))
            .unwrap();
        assert_eq!(sched.end_date_unix, Some(1_699_999_000));
    }

    // ── source-column migration + source_cursor unit tests ───────────────────

    #[test]
    fn fresh_cache_resolution_default_source_is_gamma() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xcond", Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();
        let source: String = cache
            .conn
            .query_row(
                "SELECT source FROM market_resolutions WHERE market_id = ?1",
                params!["0xcond"],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            source, "gamma",
            "insert_resolution without explicit source must use DEFAULT 'gamma'"
        );
    }

    #[test]
    fn fresh_cache_schedule_default_source_is_gamma() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule("0xcond", Some(1_699_999_000), 1_700_000_001)
            .unwrap();
        let source: String = cache
            .conn
            .query_row(
                "SELECT source FROM market_schedules WHERE market_id = ?1",
                params!["0xcond"],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            source, "gamma",
            "insert_schedule without explicit source must use DEFAULT 'gamma'"
        );
    }

    #[test]
    fn insert_resolution_with_source_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution_with_source(
                "0xpoly",
                Some(0),
                1_700_000_000,
                1_700_000_001,
                "polygon",
            )
            .unwrap();
        cache
            .insert_resolution_with_source("0xclob", Some(1), 1_700_000_010, 1_700_000_011, "clob")
            .unwrap();
        cache
            .insert_resolution_with_source("0xdune", None, 1_700_000_020, 1_700_000_021, "dune")
            .unwrap();
        let mut stmt = cache
            .conn
            .prepare("SELECT market_id, source FROM market_resolutions ORDER BY market_id")
            .unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![
                ("0xclob".to_owned(), "clob".to_owned()),
                ("0xdune".to_owned(), "dune".to_owned()),
                ("0xpoly".to_owned(), "polygon".to_owned()),
            ]
        );
    }

    #[test]
    fn insert_schedule_with_source_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_schedule_with_source("0xclob", Some(1_700_000_000), 1_700_000_001, "clob")
            .unwrap();
        let source: String = cache
            .conn
            .query_row(
                "SELECT source FROM market_schedules WHERE market_id = ?1",
                params!["0xclob"],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(source, "clob");
    }

    #[test]
    fn insert_with_source_is_idempotent_first_writer_wins() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        // First write: polygon claims the row.
        cache
            .insert_resolution_with_source(
                "0xcond",
                Some(0),
                1_700_000_000,
                1_700_000_001,
                "polygon",
            )
            .unwrap();
        // Second write: clob tries to overwrite — INSERT OR IGNORE swallows it.
        cache
            .insert_resolution_with_source("0xcond", Some(1), 1_700_000_010, 1_700_000_011, "clob")
            .unwrap();
        let (winner, source): (i64, String) = cache
            .conn
            .query_row(
                "SELECT winning_outcome_id, source FROM market_resolutions WHERE market_id = ?1",
                params!["0xcond"],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(winner, 0, "first writer's winner must survive");
        assert_eq!(source, "polygon", "first writer's source must survive");
    }

    #[test]
    fn source_column_migration_back_fills_legacy_rows() {
        // Simulate a pre-migration DB by opening on a file path, dropping the
        // `source` column, inserting raw rows, then re-opening — the migration
        // must add the column with DEFAULT 'gamma' for the legacy rows.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        {
            // Step 1: open normally, then DROP the source columns we just added
            // to mimic a DB that pre-dates this migration.
            let cache = WalletCache::open(&path).unwrap();
            cache
                .conn
                .execute_batch(
                    "CREATE TABLE legacy_res AS SELECT market_id, winning_outcome_id, \
                     resolved_at_unix, fetched_at_unix FROM market_resolutions; \
                     DROP TABLE market_resolutions; \
                     ALTER TABLE legacy_res RENAME TO market_resolutions; \
                     CREATE TABLE legacy_sch AS SELECT market_id, end_date_unix, \
                     fetched_at_unix FROM market_schedules; \
                     DROP TABLE market_schedules; \
                     ALTER TABLE legacy_sch RENAME TO market_schedules;",
                )
                .unwrap();
            cache
                .conn
                .execute(
                    "INSERT INTO market_resolutions (market_id, winning_outcome_id, \
                     resolved_at_unix, fetched_at_unix) VALUES (?1, ?2, ?3, ?4)",
                    params!["0xlegacy", 0_i64, 1_700_000_000_i64, 1_700_000_001_i64],
                )
                .unwrap();
            cache
                .conn
                .execute(
                    "INSERT INTO market_schedules (market_id, end_date_unix, fetched_at_unix) \
                     VALUES (?1, ?2, ?3)",
                    params!["0xlegacy", 1_699_999_000_i64, 1_700_000_001_i64],
                )
                .unwrap();
        }
        // Step 2: re-open — migration must add `source` columns with DEFAULT 'gamma'.
        let cache = WalletCache::open(&path).unwrap();
        let res_source: String = cache
            .conn
            .query_row(
                "SELECT source FROM market_resolutions WHERE market_id = ?1",
                params!["0xlegacy"],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        let sch_source: String = cache
            .conn
            .query_row(
                "SELECT source FROM market_schedules WHERE market_id = ?1",
                params!["0xlegacy"],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            res_source, "gamma",
            "legacy resolution must be tagged gamma"
        );
        assert_eq!(sch_source, "gamma", "legacy schedule must be tagged gamma");
    }

    #[test]
    fn migration_is_idempotent_across_reopens() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let _ = WalletCache::open(&path).unwrap();
        // Re-opening repeatedly must not error or duplicate columns.
        let _ = WalletCache::open(&path).unwrap();
        let cache = WalletCache::open(&path).unwrap();
        // pragma_table_info should show exactly one `source` column.
        let count: i64 = cache
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('market_resolutions') WHERE name='source'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(count, 1, "source column must exist exactly once");
    }

    #[test]
    fn source_cursor_get_returns_none_when_absent() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert!(cache.get_source_cursor("clob_closed").is_none());
        assert!(cache.get_source_cursor("other_source").is_none());
    }

    #[test]
    fn source_cursor_set_then_get_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("clob_closed", "LTE=").unwrap();
        cache.set_source_cursor("other_source", "55000000").unwrap();
        assert_eq!(
            cache.get_source_cursor("clob_closed").as_deref(),
            Some("LTE=")
        );
        assert_eq!(
            cache.get_source_cursor("other_source").as_deref(),
            Some("55000000")
        );
    }

    #[test]
    fn source_cursor_set_overwrites_prior_value() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("other_source", "33605403").unwrap();
        cache.set_source_cursor("other_source", "33615403").unwrap();
        assert_eq!(
            cache.get_source_cursor("other_source").as_deref(),
            Some("33615403"),
            "later set must replace earlier value"
        );
    }

    #[test]
    fn source_cursor_keys_are_independent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("clob_closed", "abc").unwrap();
        cache.set_source_cursor("other_source", "1").unwrap();
        // Updating one key must not affect the other.
        cache.set_source_cursor("clob_closed", "def").unwrap();
        assert_eq!(
            cache.get_source_cursor("clob_closed").as_deref(),
            Some("def")
        );
        assert_eq!(
            cache.get_source_cursor("other_source").as_deref(),
            Some("1")
        );
    }

    // ── delete_resolutions_by_sources unit tests ──────────────────────────────

    #[test]
    fn delete_resolutions_by_sources_keeps_non_matching_rows() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        // Mix of imprecise and precise sources.
        cache
            .insert_resolution_with_source("0xpoly", Some(0), 1_700_000_000, 1, "polygon")
            .unwrap();
        cache
            .insert_resolution_with_source("0xdune", Some(1), 1_700_000_001, 2, "dune")
            .unwrap();
        cache
            .insert_resolution("0xgamma", Some(0), 1_700_000_002, 3)
            .unwrap(); // DEFAULT 'gamma'
        cache
            .insert_resolution_with_source("0xclob", Some(1), 1_700_000_003, 4, "clob")
            .unwrap();
        assert_eq!(cache.resolved_market_ids().len(), 4);

        let deleted = cache
            .delete_resolutions_by_sources(&["gamma", "clob"])
            .unwrap();
        assert_eq!(deleted, 2, "exactly 2 imprecise rows must be deleted");

        let remaining = cache.resolved_market_ids();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.contains("0xpoly"));
        assert!(remaining.contains("0xdune"));
        assert!(!remaining.contains("0xgamma"));
        assert!(!remaining.contains("0xclob"));
    }

    #[test]
    fn delete_resolutions_by_sources_empty_input_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution("0xa", Some(0), 1_700_000_000, 1)
            .unwrap();
        // Empty `sources` must NOT execute "DELETE FROM market_resolutions"
        // (which would wipe everything). Defensive against accidental call sites.
        let deleted = cache.delete_resolutions_by_sources(&[]).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(
            cache.resolved_market_ids().len(),
            1,
            "row must survive empty-sources delete"
        );
    }

    #[test]
    fn delete_resolutions_by_sources_no_matches_returns_zero() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .insert_resolution_with_source("0xpoly", Some(0), 1_700_000_000, 1, "polygon")
            .unwrap();
        // Asking to delete a source tag that doesn't exist in the cache must
        // succeed silently with zero affected rows.
        let deleted = cache
            .delete_resolutions_by_sources(&["gamma", "clob", "dune"])
            .unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(cache.resolved_market_ids().len(), 1);
    }

    /// PASS (issue #369, Resolved Q2 — "CLOB primary-for-new"): a `source='clob'`
    /// insert for a market polygon already resolved is dropped by `INSERT OR
    /// IGNORE`, so the original polygon row (source tag, winner, exact block
    /// timestamp) survives untouched. This is why retaining the ~1.19M legacy
    /// polygon rows keeps the historical corpus exact while CLOB owns new markets.
    /// FAIL: the CLOB insert overwrites any field of the existing polygon row.
    #[test]
    fn existing_polygon_rows_survive_clob_insert() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);

        // Polygon resolves the market first, with an exact block timestamp.
        let polygon_resolved_at = 1_700_000_000;
        cache
            .insert_resolution_with_source("0xmkt", Some(0), polygon_resolved_at, 10, "polygon")
            .unwrap();

        // CLOB later attempts the same market with a DIFFERENT winner and a
        // coarser `end_date_iso` timestamp — INSERT OR IGNORE must drop it.
        cache
            .insert_resolution_with_source(
                "0xmkt",
                Some(1),
                polygon_resolved_at + 999_999,
                20,
                "clob",
            )
            .unwrap();

        let (winner, resolved_at, _fetched, source) =
            cache.resolution_record("0xmkt").expect("row must exist");
        assert_eq!(
            source, "polygon",
            "CLOB insert must not overwrite the polygon source tag"
        );
        assert_eq!(winner, Some(0), "original polygon winner must be preserved");
        assert_eq!(
            resolved_at, polygon_resolved_at,
            "exact polygon block timestamp must be preserved"
        );
    }

    // ── snapshot_appearances_up_to ───────────────────────────────────────────

    fn snap_fixture() -> LeaderboardSnapshots {
        let alice = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let bob = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let carol = addr("0xcccccccccccccccccccccccccccccccccccccccc");
        // Three snapshots at unix 100, 200, 300:
        //   t=100 : { alice }                    — alice's first appearance
        //   t=200 : { alice, bob }               — bob joins
        //   t=300 : { alice, bob, carol }        — carol joins
        // alice is in every snapshot; bob in the last two; carol only in the last.
        LeaderboardSnapshots::from_pairs(vec![
            (100, vec![alice]),
            (200, vec![alice, bob]),
            (300, vec![alice, bob, carol]),
        ])
    }

    #[test]
    fn snapshot_appearances_zero_before_first_snapshot() {
        let snaps = snap_fixture();
        let alice = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 50), 0);
        // Boundary just below first snapshot.
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 99), 0);
    }

    #[test]
    fn snapshot_appearances_zero_when_wallet_never_present() {
        let snaps = snap_fixture();
        let dave = addr("0xdddddddddddddddddddddddddddddddddddddddd");
        assert_eq!(snaps.snapshot_appearances_up_to(dave, 1_000), 0);
    }

    #[test]
    fn snapshot_appearances_inclusive_cutoff_matches_for_date() {
        let snaps = snap_fixture();
        let alice = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        // `sim_date_unix == 100` — the timestamp of the first snapshot — must be
        // counted (inclusive). Matches `for_date(100)` returning `Some(...)`.
        assert!(snaps.for_date(100).is_some());
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 100), 1);
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 200), 2);
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 300), 3);
        // Past the last snapshot — full count.
        assert_eq!(snaps.snapshot_appearances_up_to(alice, 10_000), 3);
    }

    #[test]
    fn snapshot_appearances_partial_subset() {
        let snaps = snap_fixture();
        let bob = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        // Bob's first appearance is t=200. At t=100 → 0; at t=200 → 1; at t=300 → 2.
        assert_eq!(snaps.snapshot_appearances_up_to(bob, 100), 0);
        assert_eq!(snaps.snapshot_appearances_up_to(bob, 199), 0);
        assert_eq!(snaps.snapshot_appearances_up_to(bob, 200), 1);
        assert_eq!(snaps.snapshot_appearances_up_to(bob, 299), 1);
        assert_eq!(snaps.snapshot_appearances_up_to(bob, 300), 2);
    }

    #[test]
    fn snapshot_appearances_full_count_when_present_in_every_snapshot() {
        let snaps = snap_fixture();
        let alice = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(snaps.snapshot_appearances_up_to(alice, i64::MAX), 3);
    }

    // ── first_mover_rank_cache ────────────────────────────────────────────────

    #[test]
    fn rank_index_cache_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = WalletCache::open(&path).unwrap();

        let cutoff: i64 = 1_779_839_999;
        assert!(!cache.rank_index_cache_exists(cutoff).unwrap());

        let mut index: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        index
            .entry(("market-a".to_owned(), 0u16))
            .or_default()
            .extend([100_i64, 200, 300]);
        index
            .entry(("market-a".to_owned(), 1u16))
            .or_default()
            .extend([150_i64, 250]);
        index
            .entry(("market-b".to_owned(), 0u16))
            .or_default()
            .push(50_i64);

        let saved = cache.save_rank_index_cache(cutoff, &index).unwrap();
        assert_eq!(saved, 3, "3 groups");

        assert!(cache.rank_index_cache_exists(cutoff).unwrap());

        let loaded = cache.load_rank_index_cache(cutoff).unwrap();
        assert_eq!(loaded.len(), index.len());
        for ((market, outcome), mut expected_ts) in index {
            let mut got = loaded[&(market, outcome)].clone();
            expected_ts.sort_unstable();
            got.sort_unstable();
            assert_eq!(got, expected_ts);
        }
    }

    #[test]
    fn rank_index_cache_save_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = WalletCache::open(&path).unwrap();
        let cutoff: i64 = 1_000;

        let mut index: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        index
            .entry(("mkt".to_owned(), 0u16))
            .or_default()
            .push(42_i64);

        cache.save_rank_index_cache(cutoff, &index).unwrap();
        cache.save_rank_index_cache(cutoff, &index).unwrap();

        let loaded = cache.load_rank_index_cache(cutoff).unwrap();
        assert_eq!(
            loaded[&("mkt".to_owned(), 0u16)],
            vec![42_i64],
            "duplicate save must not duplicate rows"
        );
    }

    #[test]
    fn rank_index_cache_scoped_to_cutoff() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = WalletCache::open(&path).unwrap();

        let mut a: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        a.entry(("m".to_owned(), 0u16)).or_default().push(1_i64);
        let mut b: HashMap<(String, u16), Vec<i64>> = HashMap::new();
        b.entry(("m".to_owned(), 0u16)).or_default().push(2_i64);

        cache.save_rank_index_cache(100, &a).unwrap();
        cache.save_rank_index_cache(200, &b).unwrap();

        assert!(!cache.rank_index_cache_exists(999).unwrap());
        let la = cache.load_rank_index_cache(100).unwrap();
        let lb = cache.load_rank_index_cache(200).unwrap();
        assert_eq!(la[&("m".to_owned(), 0u16)], vec![1_i64]);
        assert_eq!(lb[&("m".to_owned(), 0u16)], vec![2_i64]);
    }
}
