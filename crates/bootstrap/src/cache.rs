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
//!   every discovery source (`wallet_set.json`, `trades`, Polymarket leaderboard,
//!   502-gap, datadash). `is_active` is sticky (0→1 only) and controls which wallets
//!   the `backfill` subcommand processes.
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
use pe_source_polymarket_public::{
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, ClobCoverageManifest,
    ClobCoveragePage, ClobPayoutResolution, ClobResolutionEvidence,
};
use pe_trader_index::snapshot::RawTrade;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::{
    config::{BootstrapConfig, CacheTuning},
    error::BootstrapError,
};

/// Legacy wallet-cache generation understood by the pre-#544 trade readers.
pub const CACHE_SCHEMA_VERSION_V1: i64 = 1;
/// Trustworthy activity/payout wallet-cache generation introduced by #544.
pub const CACHE_SCHEMA_VERSION_V2: i64 = 2;

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

/// Wallets deleted per transaction in [`WalletCache::purge_wallets`] (issue #385).
/// Bounds WAL growth + lock-hold time per commit; a chunk-boundary crash leaves a
/// consistent partial state a re-run completes idempotently.
const PURGE_CHUNK: usize = 1_000;

/// DDL for the two non-lookup `trades` secondary indexes that an armed
/// `pe-bootstrap purge` drops before its bulk delete and rebuilds after
/// free-page reclamation (#401; conversion `VACUUM` or `incremental_vacuum`
/// per #538). Shared by `SCHEMA` (assembled via `concatcp!` below) and
/// [`WalletCache::create_trades_bulk_delete_indexes`] so the on-open definition
/// and the rebuild physically cannot diverge — the `CREATE INDEX IF NOT EXISTS`
/// SCHEMA-on-open backstop heals an *absent* index, never a *divergent* one.
const IDX_TRADES_MARKET_ID_DDL: &str =
    "CREATE INDEX IF NOT EXISTS idx_trades_market_id ON trades(market_id);";
const IDX_TRADES_BUY_MARKET_OUTCOME_WALLET_TS_DDL: &str =
    "CREATE INDEX IF NOT EXISTS idx_trades_buy_market_outcome_wallet_ts
    ON trades(side, market_id, outcome_id, wallet_hex, timestamp_unix);";

/// Explicit indexes required before Forge cache activation (#544). The trades
/// primary-key auto-index is additionally reported in the complete inventory.
pub const REQUIRED_TRADES_INDEXES: [&str; 3] = [
    "idx_trades_wallet_ts",
    "idx_trades_market_id",
    "idx_trades_buy_market_outcome_wallet_ts",
];

const WRITABLE_PRAGMAS: &str = "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;";

const SCHEMA: &str = concatcp!(
    "
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
-- round-trip). `source` is the series provenance (issue #429 PR3): `'clob'` for the CLOB
-- `/prices-history` backfill, the only writer today (PR2's `clob_only` verdict dropped the trades
-- pass). Populated by the `prices-history` subcommand; `INSERT OR IGNORE` on the PK makes the
-- backfill resumable and **write-once** — a later re-run, or a future second source, never overwrites
-- a captured point, so a `purge` that hard-deletes trades cannot shift an already-written series. The
-- PK's (market_id, token_id) prefix also serves the per-token resume check, so no secondary index is
-- needed.
CREATE TABLE IF NOT EXISTS market_price_history (
    market_id TEXT    NOT NULL,
    token_id  TEXT    NOT NULL,
    t         INTEGER NOT NULL,
    price     TEXT    NOT NULL,
    source    TEXT    NOT NULL DEFAULT 'clob',
    PRIMARY KEY (market_id, token_id, t)
);

-- Ranker fill-oracle minute price reference (#536), ISOLATED from market_price_history
-- because true-CLV (`_TRUE_CLV_SQL`) and the mark index read every `source='clob'` row
-- there — dense minute rows would silently change both — and its write-once PK omits
-- `source`, so coarse and minute points would collide. `token_id` alone keys points
-- (`token_conditions.token_id` is globally unique); market/outcome mapping lives in the
-- per-run targets and retained outcome artifacts. Points are write-once; a conflicting
-- duplicate (same token+t, different price) rolls back its whole page (commit_ranker_price_page).
CREATE TABLE IF NOT EXISTS ranker_price_points (
    token_id        TEXT    NOT NULL,
    t               INTEGER NOT NULL,
    price           TEXT    NOT NULL,
    fetched_at_unix INTEGER NOT NULL,
    PRIMARY KEY (token_id, t)
);

-- Append-only ledger of VALIDATED fetched pages for ranker_price_points (#536). One row per
-- bounded request page, written in the SAME transaction as its points — or neither. Coverage
-- is computed by range algebra over terminal rows at the active fidelity (union-subtract), so
-- shifting target-merge boundaries can never orphan or double-count coverage, and a lone
-- pre-existing point can never masquerade as completeness (the legacy presence-based-resume
-- failure mode). `status`: 'complete' (points > 0) or 'empty' (valid HTTP 200 with zero points
-- — durable no-series truth; a 4xx is NEVER recorded here). Full external-input provenance per
-- page; `parser_version` is provenance only — deliberately OUTSIDE the coverage identity so a
-- code deploy never invalidates fetched truth, while a fidelity change (in the PK) does.
CREATE TABLE IF NOT EXISTS ranker_price_pages (
    token_id         TEXT    NOT NULL,
    start_ts         INTEGER NOT NULL,
    end_ts           INTEGER NOT NULL,
    fidelity_minutes INTEGER NOT NULL,
    status           TEXT    NOT NULL CHECK (status IN ('complete','empty')),
    point_count      INTEGER NOT NULL,
    raw_sha256       TEXT    NOT NULL,
    source_id        TEXT    NOT NULL,
    schema_version   INTEGER NOT NULL,
    parser_version   INTEGER NOT NULL,
    observed_at_unix INTEGER NOT NULL,
    fetched_at_unix  INTEGER NOT NULL,
    request_envelope TEXT    NOT NULL,
    PRIMARY KEY (token_id, start_ts, end_ts, fidelity_minutes)
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

CREATE TABLE IF NOT EXISTS market_liquidity (
    market_id        TEXT    PRIMARY KEY NOT NULL,
    liquidity_usd_str TEXT   NOT NULL,
    fetched_at_unix  INTEGER NOT NULL
);

-- Version-two CLOB payout evidence (#544). The final table is installed only
-- from a complete page-one-to-terminal staging walk in `complete_clob_payout_walk_v2`.
-- It is intentionally separate from legacy `market_resolutions` and
-- `source_cursor`: neither legacy rows nor their cursor can satisfy these
-- non-null coverage-generation/origin constraints or seed the v2 walk state.
CREATE TABLE IF NOT EXISTS clob_payout_coverage_manifests_v2 (
    generation                  INTEGER PRIMARY KEY NOT NULL,
    manifest_json               TEXT    NOT NULL,
    walked_start_cursor         TEXT    NULL,
    walked_end_cursor           TEXT    NULL,
    page_count                  INTEGER NOT NULL,
    market_count                INTEGER NOT NULL,
    closed_market_count         INTEGER NOT NULL,
    resolved_payout_count       INTEGER NOT NULL,
    unresolved_payout_count     INTEGER NOT NULL,
    explicit_fifty_fifty_count  INTEGER NOT NULL,
    terminal_kind               TEXT    NOT NULL CHECK (
        terminal_kind IN ('end_cursor','empty_cursor','missing_cursor')
    ),
    terminal_page_sha256        TEXT    NOT NULL,
    schema_version              INTEGER NOT NULL CHECK (schema_version = 2),
    parser_version              INTEGER NOT NULL CHECK (parser_version = 2),
    completed_at_unix           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clob_payout_evidence_v2 (
    market_id              TEXT    PRIMARY KEY NOT NULL,
    end_date_unix          INTEGER NULL,
    is_50_50_outcome       INTEGER NULL CHECK (is_50_50_outcome IN (0,1)),
    payout_status          TEXT    NOT NULL CHECK (payout_status IN (
        'resolved','unresolved_open','unresolved_incomplete',
        'unresolved_conflicting','unresolved_malformed_price'
    )),
    payout_vector_json     TEXT    NULL,
    closed                 INTEGER NULL CHECK (closed IN (0,1)),
    tokens_json            TEXT    NOT NULL,
    raw_page_sha256        TEXT    NOT NULL,
    coverage_generation    INTEGER NOT NULL,
    page_ordinal           INTEGER NOT NULL,
    schema_version         INTEGER NOT NULL CHECK (schema_version = 2),
    parser_version         INTEGER NOT NULL CHECK (parser_version = 2),
    fetched_at_unix        INTEGER NOT NULL,
    origin                 TEXT    NOT NULL CHECK (origin = 'clob_closed_walk_v2'),
    CHECK (
        (payout_status = 'resolved' AND payout_vector_json IN (
            '[\"1\",\"0\"]','[\"0\",\"1\"]','[\"0.5\",\"0.5\"]'
        )) OR
        (payout_status != 'resolved' AND payout_vector_json IS NULL)
    ),
    FOREIGN KEY (coverage_generation)
        REFERENCES clob_payout_coverage_manifests_v2(generation)
);

-- One active v2 walk. `next_cursor` is independently derived from v2 page
-- commits; it is never copied from `source_cursor.clob_closed`.
CREATE TABLE IF NOT EXISTS clob_payout_walk_state_v2 (
    singleton          INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    generation         INTEGER NOT NULL,
    next_cursor        TEXT    NULL,
    next_page_ordinal  INTEGER NOT NULL,
    started_at_unix    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clob_payout_walk_pages_v2 (
    generation                  INTEGER NOT NULL,
    page_ordinal                INTEGER NOT NULL,
    request_cursor              TEXT    NULL,
    returned_next_cursor        TEXT    NULL,
    raw_sha256                  TEXT    NOT NULL,
    market_count                INTEGER NOT NULL,
    closed_market_count         INTEGER NOT NULL,
    resolved_payout_count       INTEGER NOT NULL,
    unresolved_payout_count     INTEGER NOT NULL,
    explicit_fifty_fifty_count  INTEGER NOT NULL,
    PRIMARY KEY (generation, page_ordinal)
);

CREATE TABLE IF NOT EXISTS clob_payout_evidence_staging_v2 (
    generation             INTEGER NOT NULL,
    market_id              TEXT    NOT NULL,
    end_date_unix          INTEGER NULL,
    is_50_50_outcome       INTEGER NULL CHECK (is_50_50_outcome IN (0,1)),
    payout_status          TEXT    NOT NULL CHECK (payout_status IN (
        'resolved','unresolved_open','unresolved_incomplete',
        'unresolved_conflicting','unresolved_malformed_price'
    )),
    payout_vector_json     TEXT    NULL,
    closed                 INTEGER NULL CHECK (closed IN (0,1)),
    tokens_json            TEXT    NOT NULL,
    raw_page_sha256        TEXT    NOT NULL,
    page_ordinal           INTEGER NOT NULL,
    schema_version         INTEGER NOT NULL CHECK (schema_version = 2),
    parser_version         INTEGER NOT NULL CHECK (parser_version = 2),
    fetched_at_unix        INTEGER NOT NULL,
    origin                 TEXT    NOT NULL CHECK (origin = 'clob_closed_walk_v2'),
    CHECK (
        (payout_status = 'resolved' AND payout_vector_json IN (
            '[\"1\",\"0\"]','[\"0\",\"1\"]','[\"0.5\",\"0.5\"]'
        )) OR
        (payout_status != 'resolved' AND payout_vector_json IS NULL)
    ),
    PRIMARY KEY (generation, market_id)
);

CREATE TABLE IF NOT EXISTS source_cursor (
    key        TEXT    PRIMARY KEY NOT NULL,
    value      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Cache-level maintenance markers (#538). Distinct from `source_cursor` (whose
-- contract is source-resume checkpoints): rows here are fail-closed invariants —
-- a read error must surface, never read as absent. Sole key today:
-- `reclamation_pending` (set before a bulk purge's index drop, cleared only after
-- free-page reclamation AND index recreation both succeed).
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

-- Wallet pile (issue #166). `wallet_hex` is the canonical form produced by
-- `WalletAddress::Display`: `\"0x\" + 40 lowercase hex chars`.
-- `source_bits`: bit0=wallet_set_json, bit1=trades, bit4=leaderboard,
-- bit6=gap502, bit7=datadash. (bit2/bit3 were dune_csv/dune_incr, removed in
-- #335; bit5 was radion, removed on Radion retirement; the gaps are intentional
-- — `source_bits` is persisted, do not renumber or reuse.)
-- `is_active` is sticky 0→1; `is_infra` is also sticky once set.
CREATE TABLE IF NOT EXISTS wallets (
    wallet_hex               TEXT    PRIMARY KEY NOT NULL,
    is_active                INTEGER NOT NULL DEFAULT 0,
    trade_count              INTEGER NOT NULL DEFAULT 0,
    is_infra                 INTEGER NOT NULL DEFAULT 0,
    source_bits              INTEGER NOT NULL DEFAULT 0,
    last_polymarket_fetch_at INTEGER NULL,
    backfill_partial        INTEGER NOT NULL DEFAULT 0,
    forward_frontier_unix   INTEGER,
    backward_floor_unix     INTEGER,
    -- Written by nothing since the funder-graph removal (#326/#521); retained in
    -- fresh schema so a rolled-back binary can still create its
    -- idx_wallets_weekly index against it.
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

-- Tombstones for wallets hard-deleted by purge. Rule-A `proven_loser` and
-- `infra` deletions write a row; rule-B `dead_weight` writes none. Leaderboard
-- discovery may lift a proven-loser tombstone, but an infra tombstone is
-- non-liftable except through the explicit operator clearance command.
CREATE TABLE IF NOT EXISTS purged_wallets (
    wallet_hex     TEXT    PRIMARY KEY NOT NULL,
    purged_at_unix INTEGER NOT NULL,
    reason         TEXT    NOT NULL
);

-- Durable, idempotent audit for controlled wallet activation. The member table
-- deliberately has no foreign key to `wallets`: a later purge must not erase
-- the exact cohort activated by an earlier pipeline cycle.
CREATE TABLE IF NOT EXISTS wallet_activation_batches (
    batch_id           TEXT    PRIMARY KEY NOT NULL,
    activated_at_unix  INTEGER NOT NULL,
    requested_count    INTEGER NOT NULL,
    activated_count    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS wallet_activation_batch_wallets (
    batch_id    TEXT NOT NULL,
    wallet_hex  TEXT NOT NULL,
    PRIMARY KEY (batch_id, wallet_hex)
);
CREATE INDEX IF NOT EXISTS idx_activation_batch_wallet
    ON wallet_activation_batch_wallets(wallet_hex);
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
    /// Active, non-infra wallets with an incomplete walk or no successful fetch
    /// (`backfill_partial = 1 OR last_polymarket_fetch_at IS NULL`).
    pub fetch_incomplete: usize,
    /// Distinct traded markets with no `market_resolutions` row.
    pub missing_resolution: usize,
    /// Distinct traded markets with no `market_schedules` row.
    pub missing_schedule: usize,
}

/// Traded, scheduled, past-end markets that still have no terminal resolution.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolutionAuditMissing {
    /// Bounded repair population, ordered by market id for deterministic runs.
    pub market_ids: Vec<String>,
    /// Matching rows beyond the caller-provided repair cap.
    pub clipped: usize,
}

impl ResolutionAuditMissing {
    /// Total missing population before the repair cap was applied.
    pub fn total(&self) -> usize {
        self.market_ids.len().saturating_add(self.clipped)
    }
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
    /// Infrastructure wallet. Live wallet data is deleted, but a non-liftable
    /// tombstone permanently excludes ordinary discovery and activation.
    Infrastructure,
}

impl PurgeReason {
    /// Tag string stored in purge manifests and, for tombstoned reasons,
    /// `purged_wallets.reason`.
    const fn as_str(self) -> &'static str {
        match self {
            PurgeReason::ProvenLoser => "proven_loser",
            PurgeReason::DeadWeight => "dead_weight",
            PurgeReason::Infrastructure => "infra",
        }
    }

    /// Whether a `purged_wallets` tombstone is written for this reason.
    const fn tombstoned(self) -> bool {
        matches!(self, PurgeReason::ProvenLoser | PurgeReason::Infrastructure)
    }
}

/// One wallet selected for deletion by [`WalletCache::purge_wallets`] (issue #385).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeRow {
    pub wallet_hex: String,
    pub reason: PurgeReason,
}

/// Outcome of [`WalletCache::archive_wallets`] (archive-before-DELETE, item 3.7 of
/// the 2026-07-01 decision record on issue #417). Counts are rows copied into the
/// attached archive database this run (re-archiving a wallet replaces its prior
/// archive rows, so re-runs after a crash are idempotent, not additive).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveReport {
    /// `trades` rows copied into the archive.
    pub trades_archived: usize,
    /// `wallets` rows copied into the archive.
    pub wallets_archived: usize,
    /// `leaderboard_snapshots` rows copied into the archive.
    pub snapshots_archived: usize,
    /// `purge_manifest` census rows written (== delete-set size).
    pub manifest_written: usize,
}

/// Which maintenance statement [`WalletCache::reclaim_free_pages`] ran (#538).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclamationPath {
    /// `PRAGMA incremental_vacuum` on an already-incremental db — work
    /// proportional to the freelist.
    Incremental,
    /// Full `VACUUM` on a legacy mode-0 db — the one-time conversion to
    /// incremental auto-vacuum (whole-file rewrite).
    ConversionVacuum,
}

/// Measurements from one [`WalletCache::reclaim_free_pages`] run (#538). The
/// purge orchestrator logs this; the cache exposes no sibling pragma getters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclamationReport {
    /// `PRAGMA auto_vacuum` before the run (0 = legacy, 2 = incremental).
    pub auto_vacuum_before: i64,
    /// Statement selected from that mode.
    pub path: ReclamationPath,
    pub freelist_before: i64,
    pub freelist_after: i64,
    pub page_count_before: i64,
    pub page_count_after: i64,
}

/// Read-only cache evidence captured before Forge activation (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheReclamationEvidence {
    pub reclamation_pending: bool,
    pub freelist_pages: i64,
    pub trades_indexes: Vec<String>,
    pub missing_required_trades_indexes: Vec<String>,
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
    /// Infrastructure wallets deleted and durably excluded from rediscovery.
    pub infrastructure_deleted: usize,
    /// `trades` rows deleted across all purged wallets (estimate in dry-run).
    pub trades_deleted: usize,
    /// `leaderboard_snapshots` rows deleted across all purged wallets.
    pub snapshots_deleted: usize,
    /// Rows deleted from the optional legacy `wallet_features` table.
    pub wallet_features_deleted: usize,
    /// Tombstones written to `purged_wallets` (proven-loser + infrastructure).
    pub tombstones_written: usize,
    /// True when this was a preview (`dry_run`) — nothing was written.
    pub dry_run: bool,
}

/// Durable result of one controlled activation batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationBatch {
    pub batch_id: String,
    pub requested_count: usize,
    pub wallet_hexes: Vec<String>,
    /// True when this call reused an already-committed batch rather than
    /// activating another cohort.
    pub reused: bool,
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

/// Terminal status of one validated targeted price page (#536).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RankerPageStatus {
    /// A valid response with at least one point.
    Complete,
    /// A valid HTTP 200 with zero points — durable no-series truth. Never a 4xx.
    Empty,
}

impl RankerPageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Empty => "empty",
        }
    }
}

/// One validated targeted price page (#536): coverage identity + full external-input
/// provenance for `ranker_price_pages`. Written atomically with its points by
/// [`WalletCache::commit_ranker_price_page`].
#[derive(Clone, Debug)]
pub struct RankerPricePage {
    /// CLOB token/asset id (globally unique; `token_conditions.token_id`).
    pub token_id: String,
    /// Requested page bounds, unix seconds (padded beyond the decision window by the caller).
    pub start_ts: i64,
    pub end_ts: i64,
    /// Coverage identity: a fidelity change invalidates coverage; a code deploy does not.
    pub fidelity_minutes: u32,
    pub status: RankerPageStatus,
    pub point_count: usize,
    /// Hex sha256 of the raw response body.
    pub raw_sha256: String,
    pub source_id: String,
    pub schema_version: u32,
    /// Provenance only — deliberately outside the coverage identity.
    pub parser_version: u32,
    pub observed_at_unix: i64,
    pub fetched_at_unix: i64,
    /// The request URL (the full envelope for a parameterless GET).
    pub request_envelope: String,
}

/// Minimum points in a `(market, token)` series for it to count as "usable" in
/// [`WalletCache::price_series_coverage_report`] (issue #429 PR3). Matches the ≥3-point bar PR2's
/// `clv_source_comparison` memo measured, so the reported coverage is comparable to that memo's
/// ~63.6% CLOB usable-series ceiling. Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
const MIN_USABLE_SERIES_POINTS: i64 = 3;

/// CLOB price-series coverage over the resolved-with-winner universe (issue #429 PR3), returned by
/// [`WalletCache::price_series_coverage_report`] and logged after a `prices-history` backfill.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PriceSeriesCoverage {
    /// Resolved-with-winner markets — the coverage denominator.
    pub total: i64,
    /// Of `total`, markets with ≥1 `market_price_history` row (any series, even below the usable bar).
    pub with_series: i64,
    /// Of `total`, markets with ≥1 token carrying ≥ `MIN_USABLE_SERIES_POINTS` points (a usable series).
    pub usable: i64,
}

pub struct WalletCache {
    conn: Connection,
}

/// Independent resume state for the version-two CLOB payout walk (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobPayoutWalkStateV2 {
    pub generation: u64,
    pub next_cursor: Option<String>,
    pub next_page_ordinal: u64,
}

/// Canonical SQLite row shape shared with the Parquet/DuckDB/Python readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredClobPayoutEvidenceV2 {
    pub market_id: String,
    pub end_date_unix: Option<i64>,
    pub is_50_50_outcome: Option<bool>,
    pub payout: ClobPayoutResolution,
    pub closed: Option<bool>,
    pub tokens_json: String,
    pub raw_page_sha256: String,
    pub coverage_generation: u64,
    pub page_ordinal: u64,
    pub schema_version: u32,
    pub parser_version: u32,
    pub fetched_at_unix: i64,
    pub origin: String,
}

/// Exact version-two activity aggregate stored in the cache (#544).
///
/// The API has no version-one variant: sealed transaction-hash rows are read
/// only through the frozen-payload verifier in `cache_migration`, so a caller
/// cannot accidentally mix a legacy trade into v2 ranking/state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredActivityAggregateV2 {
    pub source_trade_id: SourceTradeId,
    pub generation: u64,
    pub semantic_revision: String,
    pub components_json: String,
    pub wallet_hex: String,
    pub transaction_hash: String,
    pub activity_type: String,
    pub condition_id: Option<String>,
    pub asset: Option<String>,
    pub outcome_id: Option<u16>,
    pub side: Option<Side>,
    pub row_count: u64,
    pub share_amount: Decimal,
    pub price_weighted_share_amount: Decimal,
    pub source_usdc_amount: Decimal,
    pub source_time_unix: i64,
    pub is_combo: bool,
    pub schema_version: u32,
    pub parser_version: u32,
}

impl WalletCache {
    /// Open or create the SQLite database at `path` with the bootstrap defaults.
    /// Runs schema migrations. Callers with operator config use [`Self::open_configured`].
    pub fn open(path: &Path) -> Result<Self, BootstrapError> {
        Self::open_with_tuning(path, &BootstrapConfig::default().cache_tuning()?)
    }

    /// Open with operator-configured connection tuning. Validation precedes all
    /// SQLite I/O, including for configs constructed without the loader.
    pub fn open_configured(config: &BootstrapConfig) -> Result<Self, BootstrapError> {
        Self::open_with_tuning(&config.cache_path, &config.cache_tuning()?)
    }

    fn open_with_tuning(path: &Path, tuning: &CacheTuning) -> Result<Self, BootstrapError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        let found: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found == CACHE_SCHEMA_VERSION_V2 {
            // A v2 cache deliberately has no legacy `trades`,
            // `market_resolutions`, or `source_cursor` table. Running the v1
            // CREATE-on-open batch would recreate generation-blind owners and
            // defeat sealing, so v2 opens only the already-installed schema.
            conn.execute_batch(WRITABLE_PRAGMAS)?;
            Self::apply_connection_tuning(&conn, tuning)?;
            return Ok(Self { conn });
        }
        if found != 0 && found != CACHE_SCHEMA_VERSION_V1 {
            return Err(BootstrapError::Cache {
                message: format!(
                    "wallet-cache schema version {found} is unsupported; expected {} or {}",
                    CACHE_SCHEMA_VERSION_V1, CACHE_SCHEMA_VERSION_V2
                ),
            });
        }
        // #538: request incremental auto-vacuum BEFORE any DDL. Three db states:
        // a FRESH db is created in incremental mode by this pragma alone; an
        // EXISTING mode-0 db is unaffected until a `VACUUM` on a connection
        // holding this pragma converts it (reclaim_free_pages' mode-0 path); an
        // already-incremental db is a no-op. Never inside a transaction.
        conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
        conn.execute_batch(WRITABLE_PRAGMAS)?;
        Self::apply_connection_tuning(&conn, tuning)?;
        conn.execute_batch(SCHEMA)?;
        add_column_if_missing(
            &conn,
            "wallets",
            "backfill_partial",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(&conn, "wallets", "forward_frontier_unix", "INTEGER")?;
        add_column_if_missing(&conn, "wallets", "backward_floor_unix", "INTEGER")?;
        // Migration (#326 PR4): drop the operator/funder/delta tables. They fed
        // only the deleted operator-graph machinery; dropping reclaims the bulk of
        // the cache (`counterparty_edges` alone was ~275M rows). Idempotent — a
        // no-op once dropped, and the tables are no longer in SCHEMA so fresh DBs
        // never recreate them. `token_conditions` is kept for the surviving
        // `events` sweep. #521 adds the orphan
        // `idx_wallets_weekly` drop: its selector went with the `weekly`
        // subcommand, and the `last_funder_fetch_at` column it covered stays in
        // SCHEMA only so a rolled-back binary can recreate the index.
        conn.execute_batch(
            "DROP TABLE IF EXISTS counterparty_edges; \
             DROP TABLE IF EXISTS funder_edges; \
             DROP TABLE IF EXISTS funder_lookup_done; \
             DROP TABLE IF EXISTS delta_audit; \
             DROP INDEX IF EXISTS idx_wallets_weekly;",
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
        // Migration (issue #429 PR3): add `source` provenance to market_price_history so the CLV
        // bake-off can distinguish CLOB-derived points from a future trades-derived series. DBs
        // created before this change keep `DEFAULT 'clob'` — correct since every pre-migration row
        // was written by the CLOB `/prices-history` backfill (the only writer). Mirrors the
        // market_resolutions / market_schedules `source` precedent above.
        add_column_if_missing(
            &conn,
            "market_price_history",
            "source",
            "TEXT NOT NULL DEFAULT 'clob'",
        )?;
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

        if found == 0 {
            conn.pragma_update(None, "user_version", CACHE_SCHEMA_VERSION_V1)?;
        }

        Ok(Self { conn })
    }

    fn apply_connection_tuning(
        conn: &Connection,
        tuning: &CacheTuning,
    ) -> Result<(), BootstrapError> {
        conn.pragma_update(None, "cache_size", tuning.cache_kib)?;
        conn.pragma_update(None, "mmap_size", tuning.mmap_bytes)?;
        let effective_cache_kib: i32 =
            conn.pragma_query_value(None, "cache_size", |row| row.get(0))?;
        // A VFS without mmap support can omit the result row; disabled builds
        // return zero. SQLite may also clamp the ceiling. None is a config error.
        let effective_mmap_bytes: i64 = conn
            .query_row("PRAGMA mmap_size", [], |row| row.get(0))
            .optional()?
            .unwrap_or(0);
        tracing::info!(
            requested_cache_kib = tuning.cache_kib,
            effective_cache_kib,
            requested_mmap_bytes = tuning.mmap_bytes,
            effective_mmap_bytes,
            "wallet cache: connection tuning applied"
        );
        Ok(())
    }

    /// Return the on-disk cache schema generation.
    pub fn schema_version(&self) -> Result<i64, BootstrapError> {
        self.conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(BootstrapError::from)
    }

    /// Load only version-two activity aggregates. Sealed v1 rows are
    /// structurally outside this query and therefore cannot seed a v2 consumer.
    pub fn activity_aggregates_v2(&self) -> Result<Vec<StoredActivityAggregateV2>, BootstrapError> {
        self.require_v2_schema()?;
        let mut statement = self.conn.prepare(
            "SELECT source_trade_id, coverage_generation, semantic_revision, components_json, \
                    wallet_hex, transaction_hash, activity_type, condition_id, asset, outcome_id, \
                    side, row_count, share_amount_str, price_weighted_share_amount_str, \
                    source_usdc_amount_str, source_time_unix, is_combo, schema_version, \
                    parser_version \
             FROM activity_groups_v2
             WHERE coverage_generation = (
                 SELECT MAX(generation) FROM activity_coverage_manifests_v2
             )
             ORDER BY source_time_unix, source_trade_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, i64>(15)?,
                row.get::<_, i64>(16)?,
                row.get::<_, i64>(17)?,
                row.get::<_, i64>(18)?,
            ))
        })?;
        let mut aggregates = Vec::new();
        for row in rows {
            let row = row?;
            if !row.0.starts_with("g2:") {
                return Err(BootstrapError::Cache {
                    message: format!("non-g2 identity in activity_groups_v2: {}", row.0),
                });
            }
            aggregates.push(StoredActivityAggregateV2 {
                source_trade_id: SourceTradeId(row.0),
                generation: to_u64_i64(row.1, "activity coverage generation")?,
                semantic_revision: row.2,
                components_json: row.3,
                wallet_hex: row.4,
                transaction_hash: row.5,
                activity_type: row.6,
                condition_id: row.7,
                asset: row.8,
                outcome_id: row.9.map(parse_u16_i64).transpose()?,
                side: row
                    .10
                    .map(|value| match value.as_str() {
                        "buy" => Ok(Side::Buy),
                        "sell" => Ok(Side::Sell),
                        _ => Err(BootstrapError::Cache {
                            message: format!("invalid v2 activity side {value}"),
                        }),
                    })
                    .transpose()?,
                row_count: to_u64_i64(row.11, "activity row count")?,
                share_amount: parse_decimal(&row.12, "activity share amount")?,
                price_weighted_share_amount: parse_decimal(
                    &row.13,
                    "activity price-weighted share amount",
                )?,
                source_usdc_amount: parse_decimal(&row.14, "activity source USDC amount")?,
                source_time_unix: row.15,
                is_combo: row.16 != 0,
                schema_version: u32::try_from(row.17).map_err(|_| BootstrapError::Cache {
                    message: "invalid v2 activity schema version".to_owned(),
                })?,
                parser_version: u32::try_from(row.18).map_err(|_| BootstrapError::Cache {
                    message: "invalid v2 activity parser version".to_owned(),
                })?,
            });
        }
        Ok(aggregates)
    }

    fn require_v2_schema(&self) -> Result<(), BootstrapError> {
        let found = self.schema_version()?;
        if found == CACHE_SCHEMA_VERSION_V2 {
            Ok(())
        } else {
            Err(BootstrapError::Cache {
                message: format!(
                    "version-two cache API requires schema {}, found {found}",
                    CACHE_SCHEMA_VERSION_V2
                ),
            })
        }
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

    /// Insert trades not already present for `wallet_hex`. Idempotent on
    /// `source_trade_id` (`INSERT OR IGNORE` skips duplicates).
    ///
    /// `trades` ordering is unimportant. All inserts run in a single
    /// transaction for atomicity and write batching. Returns the number of rows
    /// actually inserted, only after commit; duplicates contribute zero.
    pub fn insert_new(
        &mut self,
        wallet_hex: &str,
        trades: Vec<RawTrade>,
    ) -> Result<u64, BootstrapError> {
        if trades.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let inserted = insert_rows(&tx, wallet_hex, &trades)?;
        tx.commit()?;
        Ok(inserted)
    }

    /// Return all `source_trade_id`s known for `wallet_hex`, newest-first.
    ///
    /// # Precondition
    /// Returns an empty `Vec` if the wallet has never been seen.
    pub fn known_trade_ids(&self, wallet_hex: &str) -> Result<Vec<SourceTradeId>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT source_trade_id FROM trades \
             WHERE wallet_hex = ?1 ORDER BY timestamp_unix DESC",
        )?;
        let rows = stmt.query_map(params![wallet_hex], |r| {
            r.get::<_, String>(0).map(SourceTradeId)
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    /// Insert a single market resolution. Idempotent with one deliberate
    /// exception (issue #519 review): an existing row keeps its values (first
    /// fetch wins) UNLESS the stored `winning_outcome_id` is NULL and the new
    /// insert carries an explicit winner — then the row upgrades in place, so a
    /// row recorded while the venue's winner flag lagged can still heal.
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
            "INSERT INTO market_resolutions \
             (market_id, winning_outcome_id, resolved_at_unix, fetched_at_unix) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(market_id) DO UPDATE SET \
                 winning_outcome_id = excluded.winning_outcome_id, \
                 resolved_at_unix = excluded.resolved_at_unix, \
                 fetched_at_unix = excluded.fetched_at_unix \
             WHERE market_resolutions.winning_outcome_id IS NULL \
                 AND excluded.winning_outcome_id IS NOT NULL",
            params![market_id, winner_i64, resolved_at_unix, fetched_at_unix],
        )?;
        Ok(())
    }

    /// Insert a single market resolution, explicitly tagged with `source`.
    ///
    /// Same idempotency contract as [`Self::insert_resolution`] (first value wins,
    /// with the NULL-winner upgrade exception). The `source` column was added in the multi-source pipeline migration
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
            "INSERT INTO market_resolutions \
             (market_id, winning_outcome_id, resolved_at_unix, fetched_at_unix, source) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(market_id) DO UPDATE SET \
                 winning_outcome_id = excluded.winning_outcome_id, \
                 resolved_at_unix = excluded.resolved_at_unix, \
                 fetched_at_unix = excluded.fetched_at_unix, \
                 source = excluded.source \
             WHERE market_resolutions.winning_outcome_id IS NULL \
                 AND excluded.winning_outcome_id IS NOT NULL",
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

    /// Load schedules for only `markets` into a `ScheduleIndex` via per-market
    /// primary-key lookups (mirrors [`Self::load_resolutions_for_markets`] / the
    /// bounded [`Self::load_clob_marks`]). The injected-set bake-off path (#453) uses
    /// this to avoid the full-table scan in [`Self::load_all_schedules`].
    ///
    /// Mirrors [`Self::load_all_schedules`] exactly for the requested markets: a row
    /// with a NULL `end_date_unix` still enters the index (presence distinguishes
    /// "checked, no date" from "never fetched"), so the result is identical to
    /// [`Self::load_all_schedules`] restricted to `markets`.
    ///
    /// # Precondition
    /// Returns an empty index when `markets` is empty.
    pub fn load_schedules_for_markets(
        &self,
        markets: &HashSet<MarketId>,
    ) -> Result<ScheduleIndex, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT end_date_unix FROM market_schedules WHERE market_id = ?1")?;
        let mut index = ScheduleIndex::new();
        for market in markets {
            let rows = stmt.query_map(params![market.0.0], |r| {
                let end_date_unix: Option<i64> = r.get(0)?;
                Ok(end_date_unix)
            })?;
            for row in rows {
                let end_date_unix = row?;
                index.insert(market.clone(), MarketSchedule { end_date_unix });
            }
        }
        Ok(index)
    }

    // ── market_price_history (issue #421 PR4 — CLV bake-off) ─────────────────────

    /// Insert a batch of coarse pre-resolution price points into `market_price_history` in one
    /// transaction, all tagged with the same `source` provenance (issue #429 PR3: `'clob'` for the
    /// CLOB `/prices-history` backfill — the only writer today). Rows are
    /// `(market_id, token_id, t, price_str)` where `price_str` is the decimal mid as produced by
    /// [`Decimal::to_string`] (TEXT — no `f64` round-trip). `INSERT OR IGNORE` on the
    /// `(market_id, token_id, t)` PK makes re-runs idempotent, the backfill resumable, and the write
    /// **write-once**: a re-run, or a future second `source`, never overwrites a captured point, so a
    /// later `purge` (which hard-deletes `trades` by wallet) cannot shift an already-written series.
    ///
    /// # Precondition
    /// Returns immediately without writing when `rows` is empty.
    pub fn insert_price_history_batch(
        &mut self,
        rows: &[(String, String, i64, String)],
        source: &str,
    ) -> Result<(), BootstrapError> {
        if rows.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO market_price_history \
                 (market_id, token_id, t, price, source) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (market_id, token_id, t, price) in rows {
                stmt.execute(params![market_id, token_id, t, price, source])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Atomically commit one validated targeted price page and its points (#536): the point
    /// inserts and exactly one `ranker_price_pages` row happen in a single transaction — or
    /// neither (a crash, cap hit, or conflict leaves no ledger row, so range subtraction
    /// re-requests the page). Points are write-once: a duplicate `(token_id, t)` from an
    /// overlapping padded request is accepted only when its price string is byte-identical;
    /// a conflicting value rolls back the entire page and returns a typed error — upstream
    /// serving two values for one sample is data corruption, never silently resolved.
    ///
    /// # Precondition
    /// `page.status` must agree with `points` (`Complete` ⇔ non-empty); a mismatch is a
    /// caller bug and errors without writing.
    pub fn commit_ranker_price_page(
        &mut self,
        page: &RankerPricePage,
        points: &[(i64, String)],
    ) -> Result<(), BootstrapError> {
        let complete = matches!(page.status, RankerPageStatus::Complete);
        if complete == points.is_empty() {
            return Err(BootstrapError::Invalid {
                message: format!(
                    "ranker price page status {:?} disagrees with {} point(s)",
                    page.status,
                    points.len()
                ),
            });
        }
        let tx = self.conn.transaction()?;
        {
            let mut existing_stmt =
                tx.prepare("SELECT price FROM ranker_price_points WHERE token_id = ?1 AND t = ?2")?;
            let mut insert_stmt = tx.prepare(
                "INSERT INTO ranker_price_points (token_id, t, price, fetched_at_unix) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (t, price) in points {
                let existing: Option<String> = existing_stmt
                    .query_row(params![page.token_id, t], |row| row.get(0))
                    .optional()?;
                match existing {
                    Some(prior) if prior != *price => {
                        return Err(BootstrapError::Invalid {
                            message: format!(
                                "conflicting duplicate price point token={} t={t}: \
                                 stored {prior:?} vs fetched {price:?} — page rolled back",
                                page.token_id
                            ),
                        });
                    }
                    Some(_) => {} // byte-identical duplicate from an overlapping page: keep once
                    None => {
                        insert_stmt.execute(params![
                            page.token_id,
                            t,
                            price,
                            page.fetched_at_unix
                        ])?;
                    }
                }
            }
            tx.execute(
                "INSERT INTO ranker_price_pages \
                 (token_id, start_ts, end_ts, fidelity_minutes, status, point_count, raw_sha256, \
                  source_id, schema_version, parser_version, observed_at_unix, fetched_at_unix, \
                  request_envelope) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    page.token_id,
                    page.start_ts,
                    page.end_ts,
                    page.fidelity_minutes,
                    page.status.as_str(),
                    i64::try_from(page.point_count).map_err(|_| BootstrapError::Internal)?,
                    page.raw_sha256,
                    page.source_id,
                    page.schema_version,
                    page.parser_version,
                    page.observed_at_unix,
                    page.fetched_at_unix,
                    page.request_envelope,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// The union-ready validated ranges for one token at one fidelity (#536), ordered by
    /// `start_ts`. Range subtraction over these decides what still needs fetching —
    /// independent of how any cycle's targets happened to merge.
    pub fn ranker_price_covered_ranges(
        &self,
        token_id: &str,
        fidelity_minutes: u32,
    ) -> Result<Vec<(i64, i64)>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT start_ts, end_ts FROM ranker_price_pages \
             WHERE token_id = ?1 AND fidelity_minutes = ?2 ORDER BY start_ts",
        )?;
        let rows = stmt
            .query_map(params![token_id, fidelity_minutes], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Points for one token in `[lo, hi]` inclusive, ordered by `t` (#536; test/audit reader —
    /// pass-2 reads the table directly over its read-only connection).
    pub fn ranker_price_points_between(
        &self,
        token_id: &str,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<(i64, String)>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT t, price FROM ranker_price_points \
             WHERE token_id = ?1 AND t >= ?2 AND t <= ?3 ORDER BY t",
        )?;
        let rows = stmt
            .query_map(params![token_id, lo, hi], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
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
    /// Legacy `source = 'polygon'` resolutions are **excluded**: those markets predate the CLOB and
    /// never list on `/prices-history` (verified — every probe returns an empty series; issue #429's
    /// "do not re-chase the legacy polygon tail"), so fetching them is pure waste (they are the bulk
    /// of the resolved universe). Targets are ordered by `resolved_at_unix DESC` (most-recently-
    /// resolved first) so a bounded or interrupted run covers the recent markets — the ones the
    /// bake-off candidate wallets actually trade — first, and `market_price_history` grows from the
    /// start of the run rather than after the long non-listing tail.
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
               AND mr.source <> 'polygon' \
               AND NOT EXISTS ( \
                   SELECT 1 FROM market_price_history mph \
                   WHERE mph.market_id = tc.condition_id AND mph.token_id = tc.token_id \
               ) \
             ORDER BY mr.resolved_at_unix DESC";
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
    /// is the 0-based positional outcome ordinal (0=YES,1=NO for binary).
    /// Conflicts update only when the immutable mapping differs. This preserves
    /// the legacy-NULL `outcome_index` upgrade and a genuine remap while making
    /// an identical full-universe re-walk a write no-op (issue #519).
    pub fn upsert_token_conditions_batch(
        &mut self,
        rows: &[(String, String, u16)],
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO token_conditions \
                 (token_id, condition_id, outcome_index, fetched_at_unix) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(token_id) DO UPDATE SET \
                     condition_id = excluded.condition_id, \
                     outcome_index = excluded.outcome_index, \
                     fetched_at_unix = excluded.fetched_at_unix \
                 WHERE (token_conditions.condition_id, token_conditions.outcome_index) \
                     IS NOT (excluded.condition_id, excluded.outcome_index)",
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
    /// order before `upsert_token_conditions_batch`'s conditional upsert examines
    /// the prior row.
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
    /// `price_history_backfill_targets` join requires a winner and a token map, and
    /// now also excludes legacy `source='polygon'` markets, so `mapped` is an *upper*
    /// bound on that backfill's reach — the realistic ceiling is its non-`polygon`
    /// subset.
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

    /// Find traded markets whose known schedule ended before `past_end_before`
    /// and which still have no `market_resolutions` row (issue #519).
    ///
    /// NULL schedules are excluded because production ranking is
    /// `--scheduled-only`. The `EXISTS` probe is forced through
    /// `idx_trades_market_id`, avoiding a heap scan of the trades table. The
    /// returned repair set is bounded by the SQL `LIMIT`; `clipped` reports the
    /// exact remaining population so a cap can never silently pass.
    pub fn missing_resolution_audit(
        &self,
        past_end_before: i64,
        limit: usize,
    ) -> Result<ResolutionAuditMissing, BootstrapError> {
        const PREDICATE: &str = "ms.end_date_unix IS NOT NULL \
             AND ms.end_date_unix < ?1 \
             AND NOT EXISTS ( \
                 SELECT 1 FROM market_resolutions mr WHERE mr.market_id = ms.market_id \
             ) \
             AND EXISTS ( \
                 SELECT 1 FROM trades t INDEXED BY idx_trades_market_id \
                 WHERE t.market_id = ms.market_id \
             )";
        // ONE pass (issue #519 review): iterate every matching row, keep the
        // first `limit` ids and count the remainder as `clipped` — no separate
        // COUNT traversal of the 1.6M-row schedules table.
        let rows_sql = format!(
            "SELECT ms.market_id FROM market_schedules ms \
             WHERE {PREDICATE} ORDER BY ms.market_id"
        );
        let mut stmt = self.conn.prepare(&rows_sql)?;
        let rows = stmt.query_map(params![past_end_before], |row| row.get::<_, String>(0))?;
        let mut market_ids = Vec::new();
        let mut clipped = 0usize;
        for row in rows {
            let id = row?;
            if market_ids.len() < limit {
                market_ids.push(id);
            } else {
                clipped += 1;
            }
        }
        Ok(ResolutionAuditMissing {
            market_ids,
            clipped,
        })
    }

    /// Coverage of the CLOB price series over the resolved-with-winner universe (issue #429 PR3) —
    /// the operator health metric logged after a `prices-history` backfill, analogous to
    /// [`token_coverage_report`](Self::token_coverage_report). `total` = resolved-with-winner
    /// markets; `with_series` = those with ≥1 `source='clob'` row; `usable` = those with ≥1 token
    /// carrying ≥ `MIN_USABLE_SERIES_POINTS` `source='clob'` points (the bar PR2's source-comparison
    /// memo measured, so `usable / total` is comparable to that memo's ~63.6% CLOB ceiling). Both
    /// counts filter `source = 'clob'` so the metric stays CLOB-specific even if a second source is
    /// ever added. A market in the legacy-`polygon` gap (no CLOB token mapped → no series)
    /// contributes 0, by design.
    ///
    /// # Precondition
    /// Returns `PriceSeriesCoverage::default()` (all zero) before any resolution is ingested; the
    /// caller skips the warn when `total == 0`.
    pub fn price_series_coverage_report(&self) -> PriceSeriesCoverage {
        let total: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM market_resolutions WHERE winning_outcome_id IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let with_series: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(DISTINCT mph.market_id) FROM market_price_history mph \
                 JOIN market_resolutions mr ON mr.market_id = mph.market_id \
                 WHERE mr.winning_outcome_id IS NOT NULL AND mph.source = 'clob'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        // A market is "usable" when ≥1 of its tokens carries ≥ MIN_USABLE_SERIES_POINTS CLOB points.
        // The inner GROUP BY emits one row per qualifying (market, token); COUNT(DISTINCT market_id)
        // then collapses a 2-token market to a single usable market.
        let usable: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(DISTINCT market_id) FROM ( \
                     SELECT mph.market_id AS market_id \
                     FROM market_price_history mph \
                     JOIN market_resolutions mr ON mr.market_id = mph.market_id \
                     WHERE mr.winning_outcome_id IS NOT NULL AND mph.source = 'clob' \
                     GROUP BY mph.market_id, mph.token_id \
                     HAVING COUNT(*) >= ?1 \
                 )",
                params![MIN_USABLE_SERIES_POINTS],
                |r| r.get(0),
            )
            .unwrap_or(0);
        PriceSeriesCoverage {
            total,
            with_series,
            usable,
        }
    }

    /// Build a [`ClobMarkIndex`] for `markets` ONLY (bounded — mirrors the
    /// injected-set trade load), keeping `source = 'clob'` samples with
    /// `t <= max_t` (the latest horizon the caller will mark at; later samples
    /// are pruned to bound memory). Joins `market_price_history` to
    /// `token_conditions` on `(condition_id, token_id)` so each series is keyed
    /// by its 0-based `outcome_index` (= [`OutcomeId`]'s inner `u16`), exactly
    /// like the true-CLV close query (`suff_stats.py` `_TRUE_CLV_SQL`). Rows with
    /// a NULL `outcome_index` (legacy events-sourced tokens) or an unparseable
    /// price are skipped.
    ///
    /// One prepared statement is reused across `markets`; per market the rows
    /// arrive ordered by `(outcome_index, t)`, so each per-outcome `Vec` is built
    /// ascending by `t` (the [`ClobMarkIndex::mark_at_or_before`] binary-search
    /// precondition) without a re-sort.
    pub fn load_clob_marks(
        &self,
        markets: &HashSet<MarketId>,
        max_t: i64,
    ) -> Result<ClobMarkIndex, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT tc.outcome_index, mph.t, mph.price \
             FROM market_price_history mph \
             JOIN token_conditions tc \
               ON tc.condition_id = mph.market_id AND tc.token_id = mph.token_id \
             WHERE mph.market_id = ?1 AND mph.source = 'clob' \
               AND tc.outcome_index IS NOT NULL AND mph.t <= ?2 \
             ORDER BY tc.outcome_index, mph.t",
        )?;
        let mut series: HashMap<MarketId, HashMap<u16, Vec<(i64, Decimal)>>> = HashMap::new();
        for market in markets {
            let rows = stmt.query_map(params![market.0.0, max_t], |r| {
                let outcome_index: i64 = r.get(0)?;
                let t: i64 = r.get(1)?;
                let price_str: String = r.get(2)?;
                Ok((outcome_index, t, price_str))
            })?;
            for row in rows {
                let (outcome_index, t, price_str) = row?;
                let Ok(outcome) = u16::try_from(outcome_index) else {
                    continue; // out-of-range outcome ordinal; skip defensively
                };
                let Ok(price) = Decimal::from_str(&price_str) else {
                    continue; // unparseable price string; skip
                };
                series
                    .entry(market.clone())
                    .or_default()
                    .entry(outcome)
                    .or_default()
                    .push((t, price));
            }
        }
        Ok(ClobMarkIndex { series })
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
    /// over a list-materialising helper. `fetch_incomplete` counts marked or never-completed wallets, matching the incomplete
    /// branches of [`Self::select_backfill_due`] without materialising the wallet
    /// list. The `missing_*` counts difference [`Self::all_market_ids`] against
    /// [`Self::resolved_market_ids`] / [`Self::scheduled_market_ids`], inheriting
    /// the `0x`-prefixed join-key invariant the write paths maintain (see the
    /// `market_events` schema note) — no fresh `LEFT JOIN`.
    pub fn coverage_counts(&self) -> Result<CoverageReport, BootstrapError> {
        // One deferred read transaction so every count below observes the SAME
        // committed state. Without it a backfill committing mid-probe can leave
        // the marker count and the market-gap counts describing different states,
        // and the report can say CLEAN when no single state was. This is NOT the
        // `CacheMutationLock` the probe deliberately avoids (see coverage.rs): a
        // WAL read transaction does not block writers.
        let read = self.conn.unchecked_transaction()?;
        // Read-only coverage also supports schema-one caches not yet migrated.
        let has_partial: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('wallets') WHERE name = 'backfill_partial')",
            [], |r| r.get(0))?;
        let query = if has_partial {
            "SELECT COUNT(*) FROM active_tradeable_wallets WHERE backfill_partial = 1 OR last_polymarket_fetch_at IS NULL"
        } else {
            "SELECT COUNT(*) FROM active_tradeable_wallets WHERE last_polymarket_fetch_at IS NULL"
        };
        let fetch_incomplete: i64 = self.conn.query_row(query, [], |r| r.get(0))?;
        let traded = self.all_market_ids();
        let resolved = self.resolved_market_ids();
        let scheduled = self.scheduled_market_ids();
        let missing_resolution = traded.iter().filter(|&m| !resolved.contains(m)).count();
        let missing_schedule = traded.iter().filter(|&m| !scheduled.contains(m)).count();
        let report = CoverageReport {
            // COUNT(*) is non-negative, so the conversion never saturates in
            // practice; `unwrap_or(0)` keeps the lint happy without an `as` cast.
            fetch_incomplete: usize::try_from(fetch_incomplete).unwrap_or(0),
            missing_resolution,
            missing_schedule,
        };
        read.finish()?;
        Ok(report)
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

    /// Load resolutions for only `markets` into a `ResolutionIndex` via per-market
    /// primary-key lookups (one prepared statement reused across markets, mirroring
    /// [`Self::load_clob_marks`]). The injected-set bake-off path (#453) uses this to
    /// avoid the full-table scan in [`Self::load_all_resolutions`] on the 155 GB cache.
    ///
    /// The `winning_outcome_id IS NOT NULL` predicate is preserved verbatim, so voided
    /// markets are excluded exactly as in the full load; the returned index for the
    /// requested markets is therefore identical to [`Self::load_all_resolutions`]
    /// restricted to `markets` (the bit-identity property the bake-off relies on).
    ///
    /// # Precondition
    /// Returns an empty index when `markets` is empty.
    pub fn load_resolutions_for_markets(
        &self,
        markets: &HashSet<MarketId>,
    ) -> Result<ResolutionIndex, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT winning_outcome_id, resolved_at_unix \
             FROM market_resolutions \
             WHERE market_id = ?1 AND winning_outcome_id IS NOT NULL",
        )?;
        let mut index = ResolutionIndex::new();
        for market in markets {
            let rows = stmt.query_map(params![market.0.0], |r| {
                let winner_i64: i64 = r.get(0)?;
                let resolved_at: i64 = r.get(1)?;
                Ok((winner_i64, resolved_at))
            })?;
            for row in rows {
                let (winner_i64, resolved_at_unix) = row?;
                let Ok(winning_outcome_id) = OutcomeId::try_from(winner_i64) else {
                    continue; // out-of-range value; skip defensively
                };
                index.insert(
                    market.clone(),
                    MarketResolution {
                        winning_outcome_id,
                        resolved_at_unix,
                    },
                );
            }
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

    // ── version-two CLOB payout evidence ────────────────────────────────────

    /// Return the independent v2 CLOB walk state. This never reads the legacy
    /// `source_cursor` table, so a v1 cursor cannot seed v2 coverage (#544).
    pub fn clob_payout_walk_state_v2(
        &self,
    ) -> Result<Option<ClobPayoutWalkStateV2>, BootstrapError> {
        let row = self
            .conn
            .query_row(
                "SELECT generation, next_cursor, next_page_ordinal \
                 FROM clob_payout_walk_state_v2 WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(generation, next_cursor, next_page_ordinal)| {
            Ok(ClobPayoutWalkStateV2 {
                generation: u64::try_from(generation).map_err(|_| BootstrapError::Cache {
                    message: "negative clob payout walk generation".to_owned(),
                })?,
                next_cursor: normalize_stored_cursor(next_cursor),
                next_page_ordinal: u64::try_from(next_page_ordinal).map_err(|_| {
                    BootstrapError::Cache {
                        message: "negative clob payout page ordinal".to_owned(),
                    }
                })?,
            })
        })
        .transpose()
    }

    /// Start a new page-one v2 walk, or return its own durable resume state.
    /// Stale staging without an active state is discarded; installed evidence
    /// remains authoritative until a later complete manifest replaces it.
    pub fn begin_or_resume_clob_payout_walk_v2(
        &mut self,
        started_at_unix: i64,
    ) -> Result<ClobPayoutWalkStateV2, BootstrapError> {
        if let Some(state) = self.clob_payout_walk_state_v2()? {
            return Ok(state);
        }
        let tx = self.conn.transaction()?;
        let last_generation: i64 = tx.query_row(
            "SELECT COALESCE(MAX(generation), 0) FROM clob_payout_coverage_manifests_v2",
            [],
            |row| row.get(0),
        )?;
        let generation = last_generation
            .checked_add(1)
            .ok_or_else(|| BootstrapError::Cache {
                message: "clob payout generation overflow".to_owned(),
            })?;
        tx.execute("DELETE FROM clob_payout_evidence_staging_v2", [])?;
        tx.execute("DELETE FROM clob_payout_walk_pages_v2", [])?;
        tx.execute(
            "INSERT INTO clob_payout_walk_state_v2 \
             (singleton, generation, next_cursor, next_page_ordinal, started_at_unix) \
             VALUES (1, ?1, NULL, 0, ?2)",
            params![generation, started_at_unix],
        )?;
        tx.commit()?;
        Ok(ClobPayoutWalkStateV2 {
            generation: u64::try_from(generation).map_err(|_| BootstrapError::Cache {
                message: "negative clob payout walk generation".to_owned(),
            })?,
            next_cursor: None,
            next_page_ordinal: 0,
        })
    }

    /// Discard only an incomplete v2 staging walk. Installed evidence and its
    /// coverage manifest remain untouched until a later complete walk replaces
    /// them. Used by the explicit CLOB cursor-reset/rebuild controls.
    pub fn reset_clob_payout_walk_v2(&mut self) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM clob_payout_evidence_staging_v2", [])?;
        tx.execute("DELETE FROM clob_payout_walk_pages_v2", [])?;
        tx.execute("DELETE FROM clob_payout_walk_state_v2", [])?;
        tx.commit()?;
        Ok(())
    }

    /// Atomically stage one parsed page and advance only the v2 cursor.
    /// The page must match the active generation, ordinal, and expected cursor;
    /// callers cannot use a legacy cursor or an arbitrary evidence row here.
    pub fn commit_clob_payout_page_v2(
        &mut self,
        generation: u64,
        page: &ClobCoveragePage,
        evidence: &[ClobResolutionEvidence],
        fetched_at_unix: i64,
    ) -> Result<ClobPayoutWalkStateV2, BootstrapError> {
        let generation_i64 = checked_i64(generation, "clob payout generation")?;
        let page_ordinal_i64 = checked_i64(page.ordinal, "clob payout page ordinal")?;
        let mut stored_rows = Vec::with_capacity(evidence.len());
        for item in evidence {
            let Some(market_id) = item
                .condition_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let tokens_json =
                item.canonical_tokens_json()
                    .map_err(|error| BootstrapError::Clob {
                        message: error.to_string(),
                    })?;
            stored_rows.push((
                market_id.to_owned(),
                item.end_date_iso
                    .as_deref()
                    .and_then(pe_source_polymarket_public::parse_clob_end_date),
                bool_to_sql(item.is_50_50_outcome),
                item.payout.storage_status(),
                item.payout.payout_vector_json(),
                bool_to_sql(item.closed),
                tokens_json,
            ));
        }

        let tx = self.conn.transaction()?;
        let active = tx
            .query_row(
                "SELECT generation, next_cursor, next_page_ordinal \
                 FROM clob_payout_walk_state_v2 WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| BootstrapError::Cache {
                message: "clob payout page arrived without an active v2 walk".to_owned(),
            })?;
        if active.0 != generation_i64
            || active.2 != page_ordinal_i64
            || normalize_stored_cursor(active.1)
                != normalize_stored_cursor(page.request_cursor.clone())
        {
            return Err(BootstrapError::Cache {
                message: format!(
                    "clob payout page does not match active v2 walk: generation={generation}, \
                     ordinal={}, cursor={:?}",
                    page.ordinal, page.request_cursor
                ),
            });
        }

        tx.execute(
            "INSERT INTO clob_payout_walk_pages_v2 \
             (generation, page_ordinal, request_cursor, returned_next_cursor, raw_sha256, \
              market_count, closed_market_count, resolved_payout_count, \
              unresolved_payout_count, explicit_fifty_fifty_count) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                generation_i64,
                page_ordinal_i64,
                normalize_stored_cursor(page.request_cursor.clone()),
                page.returned_next_cursor,
                page.raw_sha256,
                checked_i64(page.market_count, "clob payout market count")?,
                checked_i64(page.closed_market_count, "clob payout closed count")?,
                checked_i64(page.resolved_payout_count, "clob payout resolved count")?,
                checked_i64(page.unresolved_payout_count, "clob payout unresolved count")?,
                checked_i64(
                    page.explicit_fifty_fifty_count,
                    "clob payout fifty-fifty count",
                )?,
            ],
        )?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO clob_payout_evidence_staging_v2 \
                 (generation, market_id, end_date_unix, is_50_50_outcome, payout_status, \
                  payout_vector_json, closed, tokens_json, raw_page_sha256, \
                  page_ordinal, schema_version, parser_version, fetched_at_unix, origin) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, \
                         'clob_closed_walk_v2') \
                 ON CONFLICT(generation, market_id) DO UPDATE SET \
                    end_date_unix = excluded.end_date_unix, \
                    is_50_50_outcome = excluded.is_50_50_outcome, \
                    payout_status = excluded.payout_status, \
                    payout_vector_json = excluded.payout_vector_json, \
                    closed = excluded.closed, \
                    tokens_json = excluded.tokens_json, \
                    raw_page_sha256 = excluded.raw_page_sha256, \
                    page_ordinal = excluded.page_ordinal, \
                    schema_version = excluded.schema_version, \
                    parser_version = excluded.parser_version, \
                    fetched_at_unix = excluded.fetched_at_unix, \
                    origin = excluded.origin",
            )?;
            for (
                market_id,
                end_date_unix,
                is_fifty_fifty,
                payout_status,
                payout_vector,
                closed,
                tokens,
            ) in stored_rows
            {
                statement.execute(params![
                    generation_i64,
                    market_id,
                    end_date_unix,
                    is_fifty_fifty,
                    payout_status,
                    payout_vector,
                    closed,
                    tokens,
                    page.raw_sha256,
                    page_ordinal_i64,
                    i64::from(CLOB_RESOLUTION_SCHEMA_VERSION),
                    i64::from(CLOB_RESOLUTION_PARSER_VERSION),
                    fetched_at_unix,
                ])?;
            }
        }
        let next_page_ordinal =
            page.ordinal
                .checked_add(1)
                .ok_or_else(|| BootstrapError::Cache {
                    message: "clob payout page ordinal overflow".to_owned(),
                })?;
        tx.execute(
            "UPDATE clob_payout_walk_state_v2 \
             SET next_cursor = ?1, next_page_ordinal = ?2 WHERE singleton = 1",
            params![
                normalize_stored_cursor(page.returned_next_cursor.clone()),
                checked_i64(next_page_ordinal, "clob payout next page ordinal")?,
            ],
        )?;
        tx.commit()?;
        Ok(ClobPayoutWalkStateV2 {
            generation,
            next_cursor: normalize_stored_cursor(page.returned_next_cursor.clone()),
            next_page_ordinal,
        })
    }

    /// Load the persisted page chain for the active generation.
    pub fn clob_payout_coverage_pages_v2(
        &self,
        generation: u64,
    ) -> Result<Vec<ClobCoveragePage>, BootstrapError> {
        let mut statement = self.conn.prepare(
            "SELECT page_ordinal, request_cursor, returned_next_cursor, raw_sha256, \
                    market_count, closed_market_count, resolved_payout_count, \
                    unresolved_payout_count, explicit_fifty_fifty_count \
             FROM clob_payout_walk_pages_v2 WHERE generation = ?1 \
             ORDER BY page_ordinal",
        )?;
        let rows = statement.query_map(
            params![checked_i64(generation, "clob payout generation")?],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )?;
        let mut pages = Vec::new();
        for row in rows {
            let row = row?;
            pages.push(ClobCoveragePage {
                ordinal: checked_u64(row.0, "clob payout page ordinal")?,
                request_cursor: normalize_stored_cursor(row.1),
                returned_next_cursor: row.2,
                raw_sha256: row.3,
                market_count: checked_u64(row.4, "clob payout market count")?,
                closed_market_count: checked_u64(row.5, "clob payout closed count")?,
                resolved_payout_count: checked_u64(row.6, "clob payout resolved count")?,
                unresolved_payout_count: checked_u64(row.7, "clob payout unresolved count")?,
                explicit_fifty_fifty_count: checked_u64(row.8, "clob payout fifty-fifty count")?,
            });
        }
        Ok(pages)
    }

    /// Atomically install staged evidence only after the persisted page chain
    /// validates as one complete page-one-to-terminal coverage manifest.
    pub fn complete_clob_payout_walk_v2(
        &mut self,
        manifest: &ClobCoverageManifest,
        completed_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        let pages = self.clob_payout_coverage_pages_v2(manifest.generation)?;
        let expected =
            ClobCoverageManifest::complete(manifest.generation, pages).map_err(|error| {
                BootstrapError::Cache {
                    message: error.to_string(),
                }
            })?;
        if &expected != manifest
            || manifest.schema_version != CLOB_RESOLUTION_SCHEMA_VERSION
            || manifest.parser_version != CLOB_RESOLUTION_PARSER_VERSION
        {
            return Err(BootstrapError::Cache {
                message: "clob payout manifest does not match persisted v2 page evidence"
                    .to_owned(),
            });
        }
        let manifest_json = serde_json::to_string(manifest)?;
        let terminal_kind = match manifest.terminal_proof.kind {
            pe_source_polymarket_public::ClobTerminalKind::EndCursor => "end_cursor",
            pe_source_polymarket_public::ClobTerminalKind::EmptyCursor => "empty_cursor",
            pe_source_polymarket_public::ClobTerminalKind::MissingCursor => "missing_cursor",
        };
        let generation = checked_i64(manifest.generation, "clob payout generation")?;
        let tx = self.conn.transaction()?;
        let active_generation = tx
            .query_row(
                "SELECT generation FROM clob_payout_walk_state_v2 WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| BootstrapError::Cache {
                message: "clob payout completion has no active v2 walk".to_owned(),
            })?;
        if active_generation != generation {
            return Err(BootstrapError::Cache {
                message: "clob payout completion generation is not active".to_owned(),
            });
        }
        tx.execute(
            "INSERT INTO clob_payout_coverage_manifests_v2 \
             (generation, manifest_json, walked_start_cursor, walked_end_cursor, page_count, \
              market_count, closed_market_count, resolved_payout_count, \
              unresolved_payout_count, explicit_fifty_fifty_count, terminal_kind, \
              terminal_page_sha256, schema_version, parser_version, completed_at_unix) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                generation,
                manifest_json,
                manifest.walked_start_cursor,
                manifest.walked_end_cursor,
                checked_i64(manifest.counts.pages, "clob payout page count")?,
                checked_i64(manifest.counts.markets, "clob payout market count")?,
                checked_i64(
                    manifest.counts.closed_markets,
                    "clob payout closed market count",
                )?,
                checked_i64(
                    manifest.counts.resolved_payouts,
                    "clob payout resolved count",
                )?,
                checked_i64(
                    manifest.counts.unresolved_payouts,
                    "clob payout unresolved count",
                )?,
                checked_i64(
                    manifest.counts.explicit_fifty_fifty,
                    "clob payout fifty-fifty count",
                )?,
                terminal_kind,
                manifest.terminal_proof.terminal_page_sha256,
                i64::from(manifest.schema_version),
                i64::from(manifest.parser_version),
                completed_at_unix,
            ],
        )?;
        tx.execute("DELETE FROM clob_payout_evidence_v2", [])?;
        tx.execute(
            "INSERT INTO clob_payout_evidence_v2 \
             (market_id, end_date_unix, is_50_50_outcome, payout_status, payout_vector_json, closed, \
              tokens_json, raw_page_sha256, coverage_generation, page_ordinal, \
              schema_version, parser_version, fetched_at_unix, origin) \
             SELECT market_id, end_date_unix, is_50_50_outcome, payout_status, payout_vector_json, closed, \
                    tokens_json, raw_page_sha256, generation, page_ordinal, schema_version, \
                    parser_version, fetched_at_unix, origin \
             FROM clob_payout_evidence_staging_v2 WHERE generation = ?1",
            params![generation],
        )?;
        tx.execute(
            "DELETE FROM clob_payout_evidence_staging_v2 WHERE generation = ?1",
            params![generation],
        )?;
        tx.execute(
            "DELETE FROM clob_payout_walk_pages_v2 WHERE generation = ?1",
            params![generation],
        )?;
        tx.execute(
            "DELETE FROM clob_payout_walk_state_v2 WHERE singleton = 1",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Read and revalidate the newest installed coverage manifest.
    pub fn latest_clob_payout_coverage_manifest_v2(
        &self,
    ) -> Result<Option<ClobCoverageManifest>, BootstrapError> {
        let stored = self
            .conn
            .query_row(
                "SELECT manifest_json FROM clob_payout_coverage_manifests_v2 \
                 ORDER BY generation DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let manifest: ClobCoverageManifest = serde_json::from_str(&stored)?;
        let validated = ClobCoverageManifest::complete(manifest.generation, manifest.pages.clone())
            .map_err(|error| BootstrapError::Cache {
                message: error.to_string(),
            })?;
        if validated != manifest {
            return Err(BootstrapError::Cache {
                message: "stored clob payout coverage manifest failed validation".to_owned(),
            });
        }
        Ok(Some(manifest))
    }

    /// Read one installed v2 payout row into the canonical Rust shape.
    pub fn clob_payout_evidence_v2(
        &self,
        market_id: &str,
    ) -> Result<Option<StoredClobPayoutEvidenceV2>, BootstrapError> {
        let stored = self
            .conn
            .query_row(
                "SELECT market_id, end_date_unix, is_50_50_outcome, payout_status, payout_vector_json, \
                        closed, tokens_json, raw_page_sha256, coverage_generation, \
                        page_ordinal, schema_version, parser_version, fetched_at_unix, origin \
                 FROM clob_payout_evidence_v2 WHERE market_id = ?1",
                params![market_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, i64>(12)?,
                        row.get::<_, String>(13)?,
                    ))
                },
            )
            .optional()?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let payout = ClobPayoutResolution::from_storage(&stored.3, stored.4.as_deref()).map_err(
            |error| BootstrapError::Cache {
                message: error.to_string(),
            },
        )?;
        Ok(Some(StoredClobPayoutEvidenceV2 {
            market_id: stored.0,
            end_date_unix: stored.1,
            is_50_50_outcome: sql_to_bool(stored.2, "is_50_50_outcome")?,
            payout,
            closed: sql_to_bool(stored.5, "closed")?,
            tokens_json: stored.6,
            raw_page_sha256: stored.7,
            coverage_generation: checked_u64(stored.8, "clob payout generation")?,
            page_ordinal: checked_u64(stored.9, "clob payout page ordinal")?,
            schema_version: u32::try_from(stored.10).map_err(|_| BootstrapError::Cache {
                message: "invalid clob payout schema version".to_owned(),
            })?,
            parser_version: u32::try_from(stored.11).map_err(|_| BootstrapError::Cache {
                message: "invalid clob payout parser version".to_owned(),
            })?,
            fetched_at_unix: stored.12,
            origin: stored.13,
        }))
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

    /// Delete the checkpoint row for `key`.
    ///
    /// Used before a CLOB rebuild or emergency reset so a failure before page 1
    /// leaves no forged completion marker (issue #519).
    pub fn delete_source_cursor(&mut self, key: &str) -> Result<(), BootstrapError> {
        self.conn
            .execute("DELETE FROM source_cursor WHERE key = ?1", params![key])?;
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
        // Issue #385 tombstone gate. Leaderboard may lift a non-infra tombstone;
        // all other sources skip it. Infrastructure tombstones are never lifted
        // here, regardless of source.
        let mut purged = self.load_purged_reasons()?;
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
                if let Some(reason) = purged.get(wallet) {
                    // Infrastructure exclusions are permanent under ordinary
                    // discovery. Only the explicit operator clearance command
                    // may remove them.
                    if reason != "infra" && (bits & crate::pile::TOMBSTONE_OVERRIDE_SOURCES) != 0 {
                        // Override source (leaderboard) → lift + re-admit.
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

    /// Load tombstoned wallets and their durable purge reason.
    pub fn load_purged_reasons(
        &self,
    ) -> Result<std::collections::HashMap<String, String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_hex, reason FROM purged_wallets")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        Ok(rows.collect::<Result<std::collections::HashMap<_, _>, _>>()?)
    }

    /// Remove one infrastructure exclusion in both shapes it takes: the durable
    /// `purged_wallets(reason='infra')` tombstone left by a historical
    /// `purge-infra`, and a live `wallets.is_infra = 1` flag (the only shape the
    /// cold probe writes since purge retirement, #544). Returns `true` when
    /// either existed. This does not recreate or activate the wallet: a
    /// tombstone-only wallet needs a later discovery to re-admit it, and a
    /// flagged wallet keeps its `is_active`; an active one with no fetch stamp
    /// becomes due for backfill at once, where the cold probe classifies it
    /// again on its newest page (`docs/37`).
    pub fn clear_infra_exclusion(&mut self, wallet_hex: &str) -> Result<bool, BootstrapError> {
        let tx = self.conn.transaction()?;
        let tombstones = tx.execute(
            "DELETE FROM purged_wallets WHERE wallet_hex = ?1 AND reason = 'infra'",
            params![wallet_hex],
        )?;
        let flags = tx.execute(
            "UPDATE wallets SET is_infra = 0 WHERE wallet_hex = ?1 AND is_infra = 1",
            params![wallet_hex],
        )?;
        tx.commit()?;
        Ok(tombstones == 1 || flags == 1)
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
             WHERE v.backfill_partial = 0 AND v.last_polymarket_fetch_at IS NOT NULL \
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
    /// (`DeadWeight`) rows are deleted with no tombstone. Infrastructure rows
    /// get a non-liftable tombstone. Every armed non-empty deletion clears the
    /// trade-derived `first_mover_rank_cache` in its first chunk transaction.
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
        let proven = rows
            .iter()
            .filter(|r| r.reason == PurgeReason::ProvenLoser)
            .count();
        let dead = rows
            .iter()
            .filter(|r| r.reason == PurgeReason::DeadWeight)
            .count();
        let infrastructure = rows
            .iter()
            .filter(|r| r.reason == PurgeReason::Infrastructure)
            .count();
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
            let mut features: i64 = 0;
            let has_wallet_features: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='wallet_features')",
                [],
                |row| row.get::<_, i64>(0),
            )? != 0;
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
                if has_wallet_features {
                    let fq = format!(
                        "SELECT COUNT(*) FROM wallet_features WHERE wallet_hex IN ({placeholders})"
                    );
                    features += self
                        .conn
                        .query_row(&fq, &params_vec[..], |r| r.get::<_, i64>(0))?;
                }
            }
            report.proven_losers_deleted = proven;
            report.dead_weight_deleted = dead;
            report.infrastructure_deleted = infrastructure;
            report.tombstones_written = proven + infrastructure;
            report.trades_deleted = usize::try_from(trades_est).unwrap_or(usize::MAX);
            report.snapshots_deleted = usize::try_from(snaps).unwrap_or(usize::MAX);
            report.wallet_features_deleted = usize::try_from(features).unwrap_or(usize::MAX);
            return Ok(report);
        }

        // Armed: delete per chunk in ONE transaction so each rule-A wallet's
        // tombstone and its row deletions commit atomically (crash-safety).
        let has_wallet_features: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='wallet_features')",
            [],
            |row| row.get::<_, i64>(0),
        )? != 0;
        for (chunk_index, chunk) in rows.chunks(PURGE_CHUNK).enumerate() {
            let tx = self.conn.transaction()?;
            {
                // The rank cache is valid only for a fixed trades table. Clear
                // it in the first committed delete transaction so an interrupted
                // multi-chunk purge can never leave stale derived rows behind.
                if chunk_index == 0 {
                    tx.execute("DELETE FROM first_mover_rank_cache", [])?;
                }
                let mut del_trades = tx.prepare("DELETE FROM trades WHERE wallet_hex = ?1")?;
                let mut del_snaps =
                    tx.prepare("DELETE FROM leaderboard_snapshots WHERE wallet_hex = ?1")?;
                let mut del_wallet = tx.prepare("DELETE FROM wallets WHERE wallet_hex = ?1")?;
                let mut del_features = if has_wallet_features {
                    Some(tx.prepare("DELETE FROM wallet_features WHERE wallet_hex = ?1")?)
                } else {
                    None
                };
                let mut tomb = tx.prepare(
                    "INSERT OR REPLACE INTO purged_wallets (wallet_hex, purged_at_unix, reason) \
                     VALUES (?1, ?2, ?3)",
                )?;
                for r in chunk {
                    report.trades_deleted += del_trades.execute(params![r.wallet_hex])?;
                    report.snapshots_deleted += del_snaps.execute(params![r.wallet_hex])?;
                    if let Some(stmt) = del_features.as_mut() {
                        report.wallet_features_deleted += stmt.execute(params![r.wallet_hex])?;
                    }
                    del_wallet.execute(params![r.wallet_hex])?;
                    if r.reason.tombstoned() {
                        tomb.execute(params![r.wallet_hex, now_unix, r.reason.as_str()])?;
                        report.tombstones_written += 1;
                    }
                    match r.reason {
                        PurgeReason::ProvenLoser => report.proven_losers_deleted += 1,
                        PurgeReason::DeadWeight => report.dead_weight_deleted += 1,
                        PurgeReason::Infrastructure => report.infrastructure_deleted += 1,
                    }
                }
            }
            tx.commit()?;
        }
        Ok(report)
    }

    /// Archive every doomed wallet's rows into a separate SQLite database at
    /// `archive_path` BEFORE any destructive purge step (archive-before-DELETE,
    /// item 3.7 of the 2026-07-01 decision record on issue #417). The #385 purge
    /// hard-deleted 85k wallets and made a point-in-time roster unreconstructable
    /// — an optimistic survivorship bias no later analysis could quantify. This
    /// ends that: when archiving is enabled (the default), the purge orchestrator
    /// calls this first and ABORTS the purge on any archive failure (fail-closed
    /// — never delete what was not archived; disabling the knob is an explicit
    /// operator opt-out of that guarantee).
    ///
    /// Mechanics: `ATTACH` the archive; create `trades` / `wallets` /
    /// `leaderboard_snapshots` mirrors (`CREATE TABLE … AS SELECT * … WHERE 0` on
    /// first use) and then **reconcile columns by name** — any `main` column
    /// missing from the mirror is `ALTER TABLE … ADD COLUMN`ed, and all inserts
    /// use explicit name lists — so an additive schema migration on `main` never
    /// breaks an existing archive (older archive rows read NULL in new columns).
    /// Rows are copied with `INSERT OR IGNORE` under UNIQUE indexes on the real
    /// row identities (`trades.source_trade_id`,
    /// `leaderboard_snapshots(snapshot_at_unix, wallet_hex)`,
    /// `wallets.wallet_hex`), so the archive is a **union across purges**: a
    /// crash-rerun is idempotent, and a wallet re-discovered and re-purged later
    /// ADDS its new rows without touching the originally archived ones. The
    /// `purge_manifest` census row (both rules, unlike the rule-A-only live
    /// tombstone) is upserted with the wallet's TOTAL archived trade count. The
    /// `DETACH` always runs; the first archive error wins.
    ///
    /// # Precondition
    /// Call while the `trades` lookup index (`idx_trades_wallet_ts`) is still
    /// present — i.e. before `drop_trades_bulk_delete_indexes` — or the per-wallet
    /// `SELECT` degrades to a full scan of a ~269M-row table.
    pub fn archive_wallets(
        &mut self,
        rows: &[PurgeRow],
        archive_path: &Path,
        now_unix: i64,
    ) -> Result<ArchiveReport, BootstrapError> {
        let mut report = ArchiveReport::default();
        if rows.is_empty() {
            return Ok(report);
        }
        let path_str = archive_path.to_string_lossy().into_owned();
        self.conn
            .execute("ATTACH DATABASE ?1 AS purge_archive", params![path_str])?;

        // Everything between ATTACH and DETACH is bound (never `?`) so the DETACH
        // always runs on this connection; the original error is surfaced after.
        let work = (|| -> Result<(), BootstrapError> {
            // Mirrors on first use + the manifest. CTAS is the bootstrap only;
            // steady-state schema safety is the name reconciliation below.
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS purge_archive.trades AS \
                     SELECT * FROM main.trades WHERE 0;\n\
                 CREATE TABLE IF NOT EXISTS purge_archive.wallets AS \
                     SELECT * FROM main.wallets WHERE 0;\n\
                 CREATE TABLE IF NOT EXISTS purge_archive.leaderboard_snapshots AS \
                     SELECT * FROM main.leaderboard_snapshots WHERE 0;\n\
                 CREATE TABLE IF NOT EXISTS purge_archive.purge_manifest (\n\
                     wallet_hex      TEXT PRIMARY KEY NOT NULL,\n\
                     purged_at_unix  INTEGER NOT NULL,\n\
                     reason          TEXT NOT NULL,\n\
                     trades_archived INTEGER NOT NULL\n\
                 );\n\
                 CREATE UNIQUE INDEX IF NOT EXISTS purge_archive.uidx_archive_trades_id \
                     ON trades (source_trade_id);\n\
                 CREATE INDEX IF NOT EXISTS purge_archive.idx_archive_trades_wallet \
                     ON trades (wallet_hex);\n\
                 CREATE UNIQUE INDEX IF NOT EXISTS purge_archive.uidx_archive_wallets_hex \
                     ON wallets (wallet_hex);\n\
                 CREATE UNIQUE INDEX IF NOT EXISTS purge_archive.uidx_archive_snaps \
                     ON leaderboard_snapshots (snapshot_at_unix, wallet_hex);",
            )?;

            // Name-based column reconciliation: append any main-only column to the
            // mirror (SQLite ADD COLUMN appends; old archive rows read NULL), and
            // return main's column-name list for the explicit-name INSERT.
            let cols_t = self.archive_sync_columns("trades")?;
            let cols_w = self.archive_sync_columns("wallets")?;
            let cols_s = self.archive_sync_columns("leaderboard_snapshots")?;

            let ins_sql = |table: &str, cols: &[String]| -> String {
                let quoted: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
                let list = quoted.join(", ");
                format!(
                    "INSERT OR IGNORE INTO purge_archive.{table} ({list}) \
                     SELECT {list} FROM main.{table} WHERE wallet_hex = ?1"
                )
            };
            let sql_t = ins_sql("trades", &cols_t);
            let sql_w = ins_sql("wallets", &cols_w);
            let sql_s = ins_sql("leaderboard_snapshots", &cols_s);

            for chunk in rows.chunks(PURGE_CHUNK) {
                let tx = self.conn.transaction()?;
                {
                    let mut ins_t = tx.prepare(&sql_t)?;
                    let mut ins_w = tx.prepare(&sql_w)?;
                    let mut ins_s = tx.prepare(&sql_s)?;
                    let mut total_t = tx.prepare(
                        "SELECT COUNT(*) FROM purge_archive.trades WHERE wallet_hex = ?1",
                    )?;
                    let mut man = tx.prepare(
                        "INSERT OR REPLACE INTO purge_archive.purge_manifest \
                         (wallet_hex, purged_at_unix, reason, trades_archived) \
                         VALUES (?1, ?2, ?3, ?4)",
                    )?;
                    for r in chunk {
                        report.trades_archived += ins_t.execute(params![r.wallet_hex])?;
                        report.wallets_archived += ins_w.execute(params![r.wallet_hex])?;
                        report.snapshots_archived += ins_s.execute(params![r.wallet_hex])?;
                        // Manifest carries the wallet's TOTAL archived trades (a
                        // union across purges), not just this run's inserts.
                        let total: i64 =
                            total_t.query_row(params![r.wallet_hex], |row| row.get(0))?;
                        man.execute(params![r.wallet_hex, now_unix, r.reason.as_str(), total])?;
                        report.manifest_written += 1;
                    }
                }
                tx.commit()?;
            }
            Ok(())
        })();
        let detach = self.conn.execute_batch("DETACH DATABASE purge_archive");
        work?;
        detach?;
        Ok(report)
    }

    /// Reconcile one archive mirror's columns with `main.<table>` by NAME: any
    /// column present in `main` but missing from `purge_archive` is appended via
    /// `ALTER TABLE … ADD COLUMN` (same declared type; existing archive rows read
    /// NULL). Returns `main`'s column names in order, for explicit-name inserts.
    /// Columns that only exist in the archive (a dropped `main` column) are left
    /// in place and simply not written. Requires `purge_archive` to be attached.
    fn archive_sync_columns(&self, table: &str) -> Result<Vec<String>, BootstrapError> {
        let read_cols = |schema: &str| -> Result<Vec<(String, String)>, BootstrapError> {
            let sql = format!("PRAGMA {schema}.table_info({table})");
            let mut stmt = self.conn.prepare(&sql)?;
            let cols = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(cols)
        };
        let main_cols = read_cols("main")?;
        let arch_names: HashSet<String> = read_cols("purge_archive")?
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        for (name, ty) in &main_cols {
            if !arch_names.contains(name) {
                // Additive migration on main → append to the mirror. Identifiers
                // come from PRAGMA table_info (not user input); quote defensively.
                let ddl = format!("ALTER TABLE purge_archive.{table} ADD COLUMN \"{name}\" {ty}");
                self.conn.execute_batch(&ddl)?;
            }
        }
        Ok(main_cols.into_iter().map(|(n, _)| n).collect())
    }

    /// Reclaim freed pages (#538) — the single maintenance owner (replaces the
    /// former unconditional `vacuum()`). Must run OUTSIDE any transaction — the
    /// purge orchestrator calls this after the chunked deletes commit.
    ///
    /// Path selection by the db's PERSISTED auto-vacuum mode:
    /// - mode 2 (incremental): `PRAGMA incremental_vacuum;` drains the whole
    ///   freelist — work proportional to the freelist, not the file.
    /// - mode 0 (legacy): full `VACUUM`, which doubles as the one-time
    ///   conversion to incremental mode (the open pragma is already set on this
    ///   connection).
    pub fn reclaim_free_pages(&mut self) -> Result<ReclamationReport, BootstrapError> {
        let read_i64 = |conn: &Connection, sql: &str| -> Result<i64, BootstrapError> {
            Ok(conn.query_row(sql, [], |r| r.get(0))?)
        };
        let auto_vacuum_before = read_i64(&self.conn, "PRAGMA auto_vacuum")?;
        let freelist_before = read_i64(&self.conn, "PRAGMA freelist_count")?;
        let page_count_before = read_i64(&self.conn, "PRAGMA page_count")?;
        let path = if auto_vacuum_before == 2 {
            // Stepped pragma: pages are freed as the cursor advances, so the
            // rows must be driven to exhaustion — execute_batch alone leaves
            // part of the freelist behind.
            let mut stmt = self.conn.prepare("PRAGMA incremental_vacuum")?;
            let mut rows = stmt.query([])?;
            while rows.next()?.is_some() {}
            drop(rows);
            drop(stmt);
            ReclamationPath::Incremental
        } else {
            self.conn.execute_batch("VACUUM")?;
            ReclamationPath::ConversionVacuum
        };
        Ok(ReclamationReport {
            auto_vacuum_before,
            path,
            freelist_before,
            freelist_after: read_i64(&self.conn, "PRAGMA freelist_count")?,
            page_count_before,
            page_count_after: read_i64(&self.conn, "PRAGMA page_count")?,
        })
    }

    /// Set the fail-closed `reclamation_pending` marker (#538). Committed BEFORE
    /// a bulk purge's index drop so a failed/interrupted reclamation is retried
    /// by the next purge invocation instead of silently persisting bloat.
    pub fn set_reclamation_pending(&mut self) -> Result<(), BootstrapError> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES ('reclamation_pending', '1')
             ON CONFLICT(key) DO UPDATE SET value = '1'",
            [],
        )?;
        Ok(())
    }

    /// Read the marker. Fail-closed contract: a query ERROR propagates (never
    /// coerced to absent); only a genuinely missing row reads as `false`.
    pub fn reclamation_pending(&self) -> Result<bool, BootstrapError> {
        let row: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'reclamation_pending'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.is_some())
    }

    /// Capture the marker, freelist, and complete trades-index inventory using
    /// the current read-only-compatible connection (#544).
    pub fn reclamation_evidence(&self) -> Result<CacheReclamationEvidence, BootstrapError> {
        let freelist_pages = self
            .conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'trades' ORDER BY name",
        )?;
        let trades_indexes = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let missing_required_trades_indexes = REQUIRED_TRADES_INDEXES
            .iter()
            .filter(|required| !trades_indexes.iter().any(|actual| actual == **required))
            .map(|name| (*name).to_owned())
            .collect();
        Ok(CacheReclamationEvidence {
            reclamation_pending: self.reclamation_pending()?,
            freelist_pages,
            trades_indexes,
            missing_required_trades_indexes,
        })
    }

    /// Clear the marker after reclamation AND index recreation both succeeded.
    /// A clear failure is returned — recovery stays pending.
    pub fn clear_reclamation_pending(&mut self) -> Result<(), BootstrapError> {
        self.conn
            .execute("DELETE FROM meta WHERE key = 'reclamation_pending'", [])?;
        Ok(())
    }

    /// Test-only (#538): mutable raw connection for in-crate tests (interrupt
    /// handle acquisition, fixture DDL). Mirrors `raw_conn_for_test`.
    #[cfg(test)]
    pub(crate) fn raw_conn_mut_for_test(&mut self) -> &mut Connection {
        &mut self.conn
    }

    /// Test-only (#538): install/remove a SQL trace so in-crate tests can prove
    /// which maintenance statement ran (`incremental_vacuum` vs bare `VACUUM`,
    /// and the absence of `DROP INDEX` on the recovery path).
    #[cfg(test)]
    pub(crate) fn install_sql_trace(&mut self, f: Option<fn(&str)>) {
        self.conn.trace(f);
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

    /// Quarantine incomplete schema-one history before any incremental trade write.
    /// Legacy fetch callers can supply wallets absent from the pile; create their
    /// default row too so retained partial history cannot become rankable.
    pub(crate) fn begin_walk(
        &mut self,
        wallet_hex: &str,
        anchor: Option<i64>,
    ) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO wallets (wallet_hex, backfill_partial, forward_frontier_unix) VALUES (?1, 1, ?2) \
             ON CONFLICT(wallet_hex) DO UPDATE SET backfill_partial = 1, \
             forward_frontier_unix = COALESCE(wallets.forward_frontier_unix, ?2)",
            params![wallet_hex, anchor],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn forward_frontier(&self, wallet_hex: &str) -> Result<Option<i64>, BootstrapError> {
        Ok(self
            .conn
            .query_row(
                "SELECT forward_frontier_unix FROM wallets WHERE wallet_hex = ?1",
                params![wallet_hex],
                |r| r.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten())
    }

    pub(crate) fn backward_floor(&self, wallet_hex: &str) -> Result<Option<i64>, BootstrapError> {
        Ok(self
            .conn
            .query_row(
                "SELECT backward_floor_unix FROM wallets WHERE wallet_hex = ?1",
                params![wallet_hex],
                |r| r.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten())
    }

    /// Commit a proved piece and its outward-only coverage bounds together, even
    /// when conversion or duplicate filtering leaves no rows to insert.
    pub(crate) fn commit_walk_piece(
        &mut self,
        wallet_hex: &str,
        rows: Vec<RawTrade>,
        frontier_unix: Option<i64>,
        floor_unix: Option<i64>,
    ) -> Result<u64, BootstrapError> {
        let tx = self.conn.transaction()?;
        let inserted = insert_rows(&tx, wallet_hex, &rows)?;
        if let Some(frontier) = frontier_unix {
            tx.execute(
                "UPDATE wallets SET forward_frontier_unix = ?2 WHERE wallet_hex = ?1 \
                AND (forward_frontier_unix IS NULL OR forward_frontier_unix < ?2)",
                params![wallet_hex, frontier],
            )?;
        }
        if let Some(floor) = floor_unix {
            tx.execute(
                "UPDATE wallets SET backward_floor_unix = ?2 WHERE wallet_hex = ?1 \
                AND (backward_floor_unix IS NULL OR backward_floor_unix > ?2)",
                params![wallet_hex, floor],
            )?;
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Finalize only after both phases completed. Failure preserves quarantine,
    /// the old stamp and all previously committed pieces.
    pub(crate) fn finish_walk(
        &mut self,
        wallet_hex: &str,
        stamp: Option<i64>,
        hi: i64,
    ) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE wallets SET backfill_partial = 0, \
             forward_frontier_unix = MAX(COALESCE(forward_frontier_unix, ?3), ?3), \
             last_polymarket_fetch_at = COALESCE(?2, last_polymarket_fetch_at) \
             WHERE wallet_hex = ?1",
            params![wallet_hex, stamp, hi],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// All quarantined wallets, including inactive and infrastructure rows.
    pub(crate) fn partial_backfill_wallet_hexes(&self) -> Result<HashSet<String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_hex FROM wallets WHERE backfill_partial = 1")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<HashSet<_>, _>>()?)
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
    /// Mirrors the cold-start probe semantics over completed cached histories:
    /// for each unmarked wallet with ≥500 trades, compute the timestamp span over the
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
        // Single scan: every unmarked wallet with ≥500 trades contributes one row;
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
                 WHERE cnt = 500 AND NOT EXISTS (SELECT 1 FROM wallets w \
                    WHERE w.wallet_hex = page.wallet_hex AND w.backfill_partial = 1)",
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
    /// Filters active non-infrastructure wallets AND (`backfill_partial = 1` OR
    /// `last_polymarket_fetch_at IS NULL` OR `last_polymarket_fetch_at < now_unix - staleness_secs`). Orders by
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
                    WHERE (backfill_partial = 1 OR last_polymarket_fetch_at IS NULL OR last_polymarket_fetch_at < ?1) \
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

    /// Apply the activation rule. Returns the number of newly-activated wallets.
    ///
    /// Sticky semantics: only `is_active = 0` rows are considered. `is_infra = 0`
    /// gates **every** activation branch — a wallet listed in both the infra CSV
    /// and a curation list (leaderboard/502-gap/datadash) stays inactive.
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
             OR (source_bits & 64) != 0 \
             OR (source_bits & 128) != 0\
             )",
            params![min_trades],
        )?;
        Ok(affected)
    }

    /// Activate at most `requested_count` inactive, non-infrastructure,
    /// non-tombstoned wallets and durably record the exact cohort in the same
    /// transaction. Reusing `batch_id` returns the committed cohort without
    /// activating another wallet.
    pub fn activate_next_batch(
        &mut self,
        batch_id: &str,
        requested_count: usize,
        min_trades: i64,
        now_unix: i64,
    ) -> Result<ActivationBatch, BootstrapError> {
        if batch_id.is_empty()
            || !batch_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(BootstrapError::Invalid {
                message:
                    "activation batch id must contain only ASCII letters, digits, '.', '_', or '-'"
                        .to_owned(),
            });
        }
        let requested_i64 =
            i64::try_from(requested_count).map_err(|_| BootstrapError::Invalid {
                message: "activation requested count exceeds SQLite integer range".to_owned(),
            })?;

        let tx = self.conn.transaction()?;
        let existing: Option<(i64, i64)> = tx
            .query_row(
                "SELECT requested_count, activated_count FROM wallet_activation_batches \
                 WHERE batch_id = ?1",
                params![batch_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((stored_requested, stored_activated)) = existing {
            if stored_requested != requested_i64 {
                return Err(BootstrapError::Invalid {
                    message: format!(
                        "activation batch {batch_id} requested_count mismatch: stored {stored_requested}, requested {requested_i64}"
                    ),
                });
            }
            let wallet_hexes = {
                let mut stmt = tx.prepare(
                    "SELECT wallet_hex FROM wallet_activation_batch_wallets \
                     WHERE batch_id = ?1 ORDER BY wallet_hex",
                )?;
                stmt.query_map(params![batch_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let stored_count =
                usize::try_from(stored_activated).map_err(|_| BootstrapError::Invalid {
                    message: format!("activation batch {batch_id} has an invalid stored count"),
                })?;
            if wallet_hexes.len() != stored_count {
                return Err(BootstrapError::Invalid {
                    message: format!(
                        "activation batch {batch_id} audit mismatch: {stored_count} recorded, {} members",
                        wallet_hexes.len()
                    ),
                });
            }
            tx.commit()?;
            return Ok(ActivationBatch {
                batch_id: batch_id.to_owned(),
                requested_count,
                wallet_hexes,
                reused: true,
            });
        }

        let wallet_hexes = {
            let mut stmt = tx.prepare(
                "SELECT w.wallet_hex FROM wallets w \
                 WHERE w.is_active = 0 AND w.is_infra = 0 \
                   AND NOT EXISTS (SELECT 1 FROM purged_wallets p WHERE p.wallet_hex = w.wallet_hex) \
                 ORDER BY CASE WHEN ( \
                              COALESCE(w.trade_count, 0) >= ?1 \
                           OR COALESCE(w.dune_closed_markets, 0) >= ?1 \
                           OR (w.source_bits & 16) != 0 \
                           OR (w.source_bits & 64) != 0 \
                           OR (w.source_bits & 128) != 0 \
                          ) THEN 0 ELSE 1 END, \
                          w.dune_win_rate_bps DESC NULLS LAST, \
                          w.dune_closed_markets DESC NULLS LAST, \
                          w.trade_count DESC, w.wallet_hex ASC \
                 LIMIT ?2",
            )?;
            stmt.query_map(params![min_trades, requested_i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        let activated_i64 =
            i64::try_from(wallet_hexes.len()).map_err(|_| BootstrapError::Invalid {
                message: "activation batch size exceeds SQLite integer range".to_owned(),
            })?;
        tx.execute(
            "INSERT INTO wallet_activation_batches \
             (batch_id, activated_at_unix, requested_count, activated_count) \
             VALUES (?1, ?2, ?3, ?4)",
            params![batch_id, now_unix, requested_i64, activated_i64],
        )?;
        {
            let mut audit = tx.prepare(
                "INSERT INTO wallet_activation_batch_wallets (batch_id, wallet_hex) \
                 VALUES (?1, ?2)",
            )?;
            let mut activate = tx.prepare(
                "UPDATE wallets SET is_active = 1 \
                 WHERE wallet_hex = ?1 AND is_active = 0 AND is_infra = 0 \
                   AND NOT EXISTS (SELECT 1 FROM purged_wallets p WHERE p.wallet_hex = wallets.wallet_hex)",
            )?;
            for wallet in &wallet_hexes {
                audit.execute(params![batch_id, wallet])?;
                if activate.execute(params![wallet])? != 1 {
                    return Err(BootstrapError::Invalid {
                        message: format!(
                            "activation batch {batch_id} lost eligibility for wallet {wallet}"
                        ),
                    });
                }
            }
        }
        tx.commit()?;
        Ok(ActivationBatch {
            batch_id: batch_id.to_owned(),
            requested_count,
            wallet_hexes,
            reused: false,
        })
    }

    /// Select every currently live infrastructure wallet for purge.
    pub fn infra_wallet_hexes(&self) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_hex FROM wallets WHERE is_infra = 1 ORDER BY wallet_hex")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    /// Mutable sibling of [`Self::raw_conn_for_test`], for helpers that need
    /// `&mut Connection` (rusqlite's `trace` hook). Same gate, so neither adds
    /// production surface.
    #[cfg(any(test, feature = "scenario"))]
    pub fn raw_conn_mut_for_test(&mut self) -> &mut Connection {
        &mut self.conn
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

fn to_u64_i64(value: i64, field: &str) -> Result<u64, BootstrapError> {
    u64::try_from(value).map_err(|_| BootstrapError::Cache {
        message: format!("{field} is negative"),
    })
}

fn parse_u16_i64(value: i64) -> Result<u16, BootstrapError> {
    u16::try_from(value).map_err(|_| BootstrapError::Cache {
        message: format!("v2 activity outcome {value} is outside u16 range"),
    })
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal, BootstrapError> {
    Decimal::from_str(value).map_err(|error| BootstrapError::Cache {
        message: format!("invalid {field} {value:?}: {error}"),
    })
}

/// Resolved-market record loaded from the `market_resolutions` table.
///
/// Only rows with `winning_outcome_id IS NOT NULL` are materialised in the
/// [`ResolutionIndex`]; voided markets are filtered at load time.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// CLOB mid-price marks for a bounded set of markets, used by the injected-set
/// backtest's forward mark-to-market (issue #436 Phase E). Built once from
/// `market_price_history ⋈ token_conditions` (`source = 'clob'`) for ONLY the
/// markets the injected wallets traded (mirrors [`WalletCache::load_clob_marks`],
/// itself bounded like the injected-set trade load), then queried by binary
/// search — no DB access in the simulation hot loop.
#[derive(Debug, Default, Clone)]
pub struct ClobMarkIndex {
    /// `condition market_id → outcome_index → ascending (t_unix, mid_price)`.
    series: HashMap<MarketId, HashMap<u16, Vec<(i64, Decimal)>>>,
}

impl ClobMarkIndex {
    /// Most-recent CLOB mid at-or-before `t_unix` for the `(market, outcome)`
    /// token, or `None` when no `source = 'clob'` sample exists at-or-before
    /// `t_unix` — the position is then *uncovered* (it contributes `0` to the
    /// mark and counts against MTM coverage, issue #436 E3).
    ///
    /// Matches the true-CLV close semantics (`suff_stats.py` `_TRUE_CLV_SQL`'s
    /// `arg_max(price, t WHERE t <= close)`): the latest sample is used however
    /// old it is — there is no staleness cap (operator decision, issue #436 E).
    pub fn mark_at_or_before(
        &self,
        market: &MarketId,
        outcome: OutcomeId,
        t_unix: i64,
    ) -> Option<Decimal> {
        let samples = self.series.get(market)?.get(&outcome.0)?;
        // partition_point: index of the first sample strictly after t_unix; the
        // latest sample at-or-before t_unix is at idx - 1.
        let idx = samples.partition_point(|(st, _)| *st <= t_unix);
        if idx == 0 {
            None
        } else {
            samples.get(idx - 1).map(|(_, p)| *p)
        }
    }

    /// True when the index holds at least one CLOB sample for `(market, outcome)`.
    pub fn covers(&self, market: &MarketId, outcome: OutcomeId) -> bool {
        self.series
            .get(market)
            .is_some_and(|by_outcome| by_outcome.contains_key(&outcome.0))
    }

    /// Number of `(market, outcome)` token series held. For diagnostics and tests.
    pub fn len(&self) -> usize {
        self.series.values().map(HashMap::len).sum()
    }

    /// True when no series are held (no CLOB coverage for any injected market).
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }
}

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

/// Schema-one row insertion shared by standalone and coverage transactions.
fn insert_rows(
    tx: &Transaction<'_>,
    wallet_hex: &str,
    trades: &[RawTrade],
) -> Result<u64, BootstrapError> {
    let mut inserted = 0_u64;
    {
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO trades \
                 (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for t in trades {
            let contracts_i64 =
                i64::try_from(t.contracts.0).map_err(|_| BootstrapError::Internal)?;
            let affected = stmt.execute(params![
                t.source_trade_id.0,
                wallet_hex,
                t.market_id.0.0,
                i64::from(t.outcome_id),
                side_to_str(&t.side),
                t.price.0.to_string(),
                contracts_i64,
                t.timestamp.0.unix_timestamp(),
            ])?;
            inserted += u64::try_from(affected).map_err(|_| BootstrapError::Internal)?;
        }
    }
    Ok(inserted)
}

/// Add column `col` with full SQL declaration `decl` (e.g. `"INTEGER NULL"` or
/// `"TEXT NOT NULL DEFAULT 'gamma'"`) to `table` if it is not already present. Idempotent: safe to
/// call on every [`WalletCache::open_with_tuning`].
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

fn normalize_stored_cursor(cursor: Option<String>) -> Option<String> {
    cursor.filter(|value| !value.is_empty())
}

const fn bool_to_sql(value: Option<bool>) -> Option<i64> {
    match value {
        Some(true) => Some(1),
        Some(false) => Some(0),
        None => None,
    }
}

fn sql_to_bool(value: Option<i64>, field: &str) -> Result<Option<bool>, BootstrapError> {
    match value {
        Some(1) => Ok(Some(true)),
        Some(0) => Ok(Some(false)),
        None => Ok(None),
        Some(other) => Err(BootstrapError::Cache {
            message: format!("invalid {field} boolean value {other}"),
        }),
    }
}

fn checked_i64(value: u64, field: &str) -> Result<i64, BootstrapError> {
    i64::try_from(value).map_err(|_| BootstrapError::Cache {
        message: format!("{field} exceeds SQLite INTEGER range"),
    })
}

fn checked_u64(value: i64, field: &str) -> Result<u64, BootstrapError> {
    u64::try_from(value).map_err(|_| BootstrapError::Cache {
        message: format!("{field} is negative"),
    })
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

    // ── ranker price pages (#536) ─────────────────────────────────────────────

    fn ranker_page(token: &str, start: i64, end: i64, n: usize) -> RankerPricePage {
        RankerPricePage {
            token_id: token.to_owned(),
            start_ts: start,
            end_ts: end,
            fidelity_minutes: 1,
            status: if n == 0 {
                RankerPageStatus::Empty
            } else {
                RankerPageStatus::Complete
            },
            point_count: n,
            raw_sha256: "ab".repeat(32),
            source_id: "polymarket-clob-prices-history".to_owned(),
            schema_version: 1,
            parser_version: 1,
            observed_at_unix: 1_700_000_000,
            fetched_at_unix: 1_700_000_000,
            request_envelope: "https://clob.example/prices-history?market=tok".to_owned(),
        }
    }

    #[test]
    fn ranker_price_page_commits_points_and_ledger_atomically() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let points = vec![(100_i64, "0.5".to_owned()), (160, "0.55".to_owned())];
        cache
            .commit_ranker_price_page(&ranker_page("tok", 90, 200, 2), &points)
            .unwrap();
        assert_eq!(
            cache.ranker_price_points_between("tok", 0, 300).unwrap(),
            points
        );
        assert_eq!(
            cache.ranker_price_covered_ranges("tok", 1).unwrap(),
            vec![(90, 200)]
        );
        // A valid-empty page records durable no-series truth with zero points.
        cache
            .commit_ranker_price_page(&ranker_page("tok", 200, 300, 0), &[])
            .unwrap();
        assert_eq!(
            cache.ranker_price_covered_ranges("tok", 1).unwrap(),
            vec![(90, 200), (200, 300)]
        );
        // Different fidelity → separate coverage identity.
        assert!(
            cache
                .ranker_price_covered_ranges("tok", 60)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ranker_price_page_conflicting_duplicate_rolls_back_whole_page() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .commit_ranker_price_page(
                &ranker_page("tok", 0, 100, 1),
                &[(50_i64, "0.5".to_owned())],
            )
            .unwrap();
        // An overlapping page with a byte-identical duplicate is accepted once…
        cache
            .commit_ranker_price_page(
                &ranker_page("tok", 40, 140, 2),
                &[(50_i64, "0.5".to_owned()), (110, "0.6".to_owned())],
            )
            .unwrap();
        // …but a conflicting value rolls back the ENTIRE page: no new point, no ledger row.
        let err = cache
            .commit_ranker_price_page(
                &ranker_page("tok", 100, 220, 2),
                &[(110_i64, "0.7".to_owned()), (170, "0.8".to_owned())],
            )
            .unwrap_err();
        assert!(matches!(err, BootstrapError::Invalid { .. }));
        assert_eq!(
            cache.ranker_price_points_between("tok", 0, 300).unwrap(),
            vec![(50, "0.5".to_owned()), (110, "0.6".to_owned())],
            "the conflicting page must leave no points behind"
        );
        assert_eq!(
            cache.ranker_price_covered_ranges("tok", 1).unwrap(),
            vec![(0, 100), (40, 140)],
            "the conflicting page must leave no coverage row"
        );
        // Retry after rollback with the corrected value succeeds.
        cache
            .commit_ranker_price_page(
                &ranker_page("tok", 100, 220, 2),
                &[(110_i64, "0.6".to_owned()), (170, "0.8".to_owned())],
            )
            .unwrap();
        assert_eq!(
            cache.ranker_price_covered_ranges("tok", 1).unwrap().len(),
            3
        );
    }

    #[test]
    fn ranker_price_page_status_must_agree_with_points() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let err = cache
            .commit_ranker_price_page(&ranker_page("tok", 0, 100, 0), &[(1, "0.5".to_owned())])
            .unwrap_err();
        assert!(matches!(err, BootstrapError::Invalid { .. }));
        let err = cache
            .commit_ranker_price_page(&ranker_page("tok", 0, 100, 1), &[])
            .unwrap_err();
        assert!(matches!(err, BootstrapError::Invalid { .. }));
        assert!(
            cache
                .ranker_price_covered_ranges("tok", 1)
                .unwrap()
                .is_empty()
        );
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
    fn token_conditions_batch_round_trips_and_remaps() {
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
        // A conflicting token mapping updates every mutable field without
        // changing the table cardinality.
        cache
            .upsert_token_conditions_batch(&[("111".to_string(), "0xcc".to_string(), 2)], 200)
            .unwrap();
        assert_eq!(cache.token_condition_count(), 3);
        let row: (String, i64, i64) = cache
            .conn
            .query_row(
                "SELECT condition_id, outcome_index, fetched_at_unix \
                 FROM token_conditions WHERE token_id = '111'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, ("0xcc".to_owned(), 2, 200));
    }

    #[test]
    fn null_winner_resolution_upgrades_but_real_values_are_first_write_wins() {
        // Issue #519 review: a row recorded while the venue's winner flag lagged
        // (NULL winner) must upgrade in place when the explicit winner arrives;
        // a row that already carries a winner must never be overwritten.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .insert_resolution_with_source("m-null", None, 100, 1, "clob")
            .unwrap();
        cache
            .insert_resolution_with_source("m-null", Some(1), 200, 2, "clob")
            .unwrap();
        let (winner, resolved_at): (Option<i64>, i64) = cache
            .conn
            .query_row(
                "SELECT winning_outcome_id, resolved_at_unix FROM market_resolutions \
                 WHERE market_id = 'm-null'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(winner, Some(1));
        assert_eq!(resolved_at, 200);

        cache
            .insert_resolution_with_source("m-set", Some(0), 100, 1, "clob")
            .unwrap();
        cache
            .insert_resolution_with_source("m-set", Some(1), 200, 2, "clob")
            .unwrap();
        let winner: Option<i64> = cache
            .conn
            .query_row(
                "SELECT winning_outcome_id FROM market_resolutions WHERE market_id = 'm-set'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(winner, Some(0), "explicit winners are first-write-wins");
        // A NULL re-insert over an existing winner must also be a no-op.
        cache
            .insert_resolution_with_source("m-set", None, 300, 3, "clob")
            .unwrap();
        let winner: Option<i64> = cache
            .conn
            .query_row(
                "SELECT winning_outcome_id FROM market_resolutions WHERE market_id = 'm-set'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(winner, Some(0));
    }

    #[test]
    fn token_conditions_empty_batch_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.upsert_token_conditions_batch(&[], 100).unwrap();
        assert_eq!(cache.token_condition_count(), 0);
    }

    // ── bounded resolution/schedule loaders (issue #453) ─────────────────────────
    //
    // The bake-off injected path loads resolutions/schedules for only the traded
    // markets. These prove the bounded loaders return exactly `load_all_*()`
    // restricted to the requested set — the bit-identity property the optimization
    // relies on — including the `winning_outcome_id IS NOT NULL` predicate.

    fn mkt(id: &str) -> MarketId {
        MarketId(VenueMarketId(id.to_owned()))
    }

    #[test]
    fn load_resolutions_for_markets_equals_full_restricted() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // Two resolved markets, a VOIDED market (NULL winner), plus two extra
        // unrelated markets that must never leak into a bounded load.
        cache.insert_resolution("m-a", Some(0), 100, 100).unwrap();
        cache.insert_resolution("m-b", Some(1), 110, 110).unwrap();
        cache.insert_resolution("m-void", None, 120, 120).unwrap();
        cache.insert_resolution("x-1", Some(0), 130, 130).unwrap();
        cache.insert_resolution("x-2", Some(1), 140, 140).unwrap();

        let full = cache.load_all_resolutions().unwrap();
        // The full load already drops the voided market.
        assert!(!full.contains_key(&mkt("m-void")));

        // Request the resolved pair, the voided market, and an absent market. The
        // bounded result must equal the full index restricted to that set.
        let requested: HashSet<MarketId> = ["m-a", "m-b", "m-void", "m-absent"]
            .into_iter()
            .map(mkt)
            .collect();
        let bounded = cache.load_resolutions_for_markets(&requested).unwrap();
        let expected: ResolutionIndex = full
            .iter()
            .filter(|(k, _)| requested.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(bounded, expected);
        // Concretely: only the two non-voided requested markets survive; the voided
        // and absent markets are excluded, and the extra x-* never appear.
        assert_eq!(bounded.len(), 2);
        assert!(bounded.contains_key(&mkt("m-a")));
        assert!(bounded.contains_key(&mkt("m-b")));
        assert!(!bounded.contains_key(&mkt("m-void"))); // predicate preserved
        assert!(!bounded.contains_key(&mkt("x-1")));
    }

    #[test]
    fn load_resolutions_for_markets_empty_set_is_empty() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.insert_resolution("m-a", Some(0), 100, 100).unwrap();
        let bounded = cache.load_resolutions_for_markets(&HashSet::new()).unwrap();
        assert!(bounded.is_empty());
    }

    #[test]
    fn load_schedules_for_markets_equals_full_restricted() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // A market with a concrete endDate, one with a NULL endDate (must still be
        // present in the index — presence semantics), plus an extra unrelated market.
        cache.insert_schedule("s-a", Some(200), 100).unwrap();
        cache.insert_schedule("s-null", None, 100).unwrap();
        cache.insert_schedule("x-1", Some(300), 100).unwrap();

        let full = cache.load_all_schedules().unwrap();
        // The NULL-endDate row is materialised (presence distinguishes "checked, no
        // date" from "never fetched").
        assert!(full.contains_key(&mkt("s-null")));

        let requested: HashSet<MarketId> =
            ["s-a", "s-null", "s-absent"].into_iter().map(mkt).collect();
        let bounded = cache.load_schedules_for_markets(&requested).unwrap();
        let expected: ScheduleIndex = full
            .iter()
            .filter(|(k, _)| requested.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(bounded, expected);
        // The NULL-endDate market is preserved; the extra x-1 never appears.
        assert_eq!(bounded.len(), 2);
        assert_eq!(
            bounded.get(&mkt("s-null")).map(|s| s.end_date_unix),
            Some(None)
        );
        assert!(!bounded.contains_key(&mkt("x-1")));
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
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
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
        cache
            .upsert_token_conditions_batch(&[("legacy".to_owned(), "0xc".to_owned(), 1)], 10)
            .unwrap();
        assert_eq!(
            cache.token_condition_outcome("legacy"),
            Some(("0xc".to_string(), Some(1)))
        );
        let fetched_at: i64 = cache
            .conn
            .query_row(
                "SELECT fetched_at_unix FROM token_conditions WHERE token_id = 'legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(fetched_at, 10);
    }

    #[test]
    fn identical_token_condition_reinsert_preserves_fetched_at() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let row = [("t0".to_owned(), "0xc".to_owned(), 0)];
        cache.upsert_token_conditions_batch(&row, 9).unwrap();
        cache.upsert_token_conditions_batch(&row, 99).unwrap();
        let fetched_at: i64 = cache
            .conn
            .query_row(
                "SELECT fetched_at_unix FROM token_conditions WHERE token_id = 't0'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fetched_at, 9);
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
    fn price_series_coverage_report_counts_total_with_series_and_usable() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        assert_eq!(
            cache.price_series_coverage_report(),
            PriceSeriesCoverage::default(),
            "empty cache → all zero"
        );
        // Two resolved-with-winner markets + one voided (excluded from the denominator).
        cache.insert_resolution("0xm1", Some(0), 2000, 9).unwrap();
        cache.insert_resolution("0xm2", Some(1), 2000, 9).unwrap();
        cache.insert_resolution("0xvoid", None, 2000, 9).unwrap();
        // 0xm1: one token with 3 points → usable. 0xm2: one token with 2 points → with_series, NOT usable.
        cache
            .insert_price_history_batch(
                &[
                    ("0xm1".to_owned(), "t1".to_owned(), 100, "0.4".to_owned()),
                    ("0xm1".to_owned(), "t1".to_owned(), 200, "0.5".to_owned()),
                    ("0xm1".to_owned(), "t1".to_owned(), 300, "0.6".to_owned()),
                    ("0xm2".to_owned(), "t2".to_owned(), 100, "0.7".to_owned()),
                    ("0xm2".to_owned(), "t2".to_owned(), 200, "0.8".to_owned()),
                ],
                "clob",
            )
            .unwrap();
        let cov = cache.price_series_coverage_report();
        assert_eq!(cov.total, 2, "two winner markets");
        assert_eq!(cov.with_series, 2, "both have ≥1 point");
        assert_eq!(cov.usable, 1, "only 0xm1 has a ≥3-point token");
    }

    #[test]
    fn load_clob_marks_binary_searches_at_or_before() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // 0xm1: outcome 0 (token t0, two samples) + outcome 1 (token t1); 0xm2: outcome 0 (token u0).
        cache
            .upsert_token_conditions_batch(
                &[
                    ("t0".to_string(), "0xm1".to_string(), 0),
                    ("t1".to_string(), "0xm1".to_string(), 1),
                    ("u0".to_string(), "0xm2".to_string(), 0),
                ],
                9,
            )
            .unwrap();
        cache
            .insert_price_history_batch(
                &[
                    ("0xm1".to_owned(), "t0".to_owned(), 100, "0.30".to_owned()),
                    ("0xm1".to_owned(), "t0".to_owned(), 200, "0.55".to_owned()),
                    ("0xm1".to_owned(), "t1".to_owned(), 150, "0.45".to_owned()),
                    ("0xm2".to_owned(), "u0".to_owned(), 100, "0.90".to_owned()),
                ],
                "clob",
            )
            .unwrap();

        let m1 = MarketId(VenueMarketId("0xm1".to_owned()));
        let m2 = MarketId(VenueMarketId("0xm2".to_owned()));
        let m_absent = MarketId(VenueMarketId("0xabsent".to_owned()));
        let markets: HashSet<MarketId> = [m1.clone(), m2.clone(), m_absent.clone()]
            .into_iter()
            .collect();

        // max_t = 199 prunes the t=200 sample on (0xm1, t0).
        let idx = cache.load_clob_marks(&markets, 199).unwrap();
        assert_eq!(
            idx.mark_at_or_before(&m1, OutcomeId(0), 250),
            Some(dec!(0.30))
        );
        assert_eq!(
            idx.mark_at_or_before(&m1, OutcomeId(0), 100),
            Some(dec!(0.30))
        );
        // Before the first sample → None (uncovered at that instant).
        assert_eq!(idx.mark_at_or_before(&m1, OutcomeId(0), 99), None);
        // Each outcome carries its own series.
        assert_eq!(
            idx.mark_at_or_before(&m1, OutcomeId(1), 160),
            Some(dec!(0.45))
        );
        assert_eq!(
            idx.mark_at_or_before(&m2, OutcomeId(0), 1000),
            Some(dec!(0.90))
        );
        // Absent market / unmapped outcome → None + covers() false.
        assert_eq!(idx.mark_at_or_before(&m_absent, OutcomeId(0), 1000), None);
        assert!(!idx.covers(&m_absent, OutcomeId(0)));
        assert!(idx.covers(&m1, OutcomeId(1)));
        assert!(!idx.covers(&m2, OutcomeId(1)));

        // Without the max_t prune, the later t=200 sample is the at-or-before mark.
        let idx_full = cache.load_clob_marks(&markets, 10_000).unwrap();
        assert_eq!(
            idx_full.mark_at_or_before(&m1, OutcomeId(0), 250),
            Some(dec!(0.55))
        );
        assert_eq!(
            idx_full.mark_at_or_before(&m1, OutcomeId(0), 150),
            Some(dec!(0.30))
        );
    }

    #[test]
    fn price_history_write_once_across_sources() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // A clob point is written first.
        cache
            .insert_price_history_batch(
                &[("0xm".to_owned(), "t".to_owned(), 100, "0.40".to_owned())],
                "clob",
            )
            .unwrap();
        // A later trades-sourced write at the SAME PK must be IGNORED (write-once): neither the price
        // nor the source changes. Guards the purge-durability invariant (#429 PR3 step 6).
        cache
            .insert_price_history_batch(
                &[("0xm".to_owned(), "t".to_owned(), 100, "0.99".to_owned())],
                "trades",
            )
            .unwrap();
        let (price, source): (String, String) = cache
            .conn
            .query_row(
                "SELECT price, source FROM market_price_history \
                 WHERE market_id = '0xm' AND token_id = 't' AND t = 100",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(price, "0.40", "the original clob price survives");
        assert_eq!(source, "clob", "the original clob provenance survives");
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

    fn tmp_cache_with_tuning(dir: &TempDir, cache_mib: u64, mmap_mib: u64) -> WalletCache {
        WalletCache::open_configured(&BootstrapConfig {
            cache_path: dir.path().join("cache.db"),
            cache_page_cache_mib: cache_mib,
            cache_mmap_mib: mmap_mib,
            ..BootstrapConfig::default()
        })
        .unwrap()
    }

    fn assert_connection_tuning(cache: &WalletCache, cache_kib: i32, mmap_bytes: i64) {
        let conn = cache.raw_conn_for_test();
        let effective_cache: i32 = conn
            .pragma_query_value(None, "cache_size", |row| row.get(0))
            .unwrap();
        assert_eq!(effective_cache, cache_kib);
        let effective_mmap: i64 = conn
            .query_row("PRAGMA mmap_size", [], |row| row.get(0))
            .optional()
            .unwrap()
            .unwrap_or(0);
        assert!((0..=mmap_bytes).contains(&effective_mmap));
        let mmap_disabled: bool = conn
            .query_row(
                "SELECT sqlite_compileoption_used('MAX_MMAP_SIZE=0')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if mmap_bytes > 0 && !mmap_disabled {
            assert!(effective_mmap > 0);
        }
        let journal: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let synchronous: i32 = conn
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(synchronous, 1);
    }

    #[test]
    fn fresh_cache_is_empty() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert_connection_tuning(&cache, -4096 * 1024, 2047 * (1 << 20));
        assert_eq!(cache.trade_count(), 0);
        assert!(
            cache
                .known_trade_ids("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .unwrap()
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
        assert_eq!(cache.insert_new(&hex, trades).unwrap(), 2);

        assert_eq!(cache.trade_count(), 2);
        let ids = cache.known_trade_ids(&hex).unwrap();
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
        assert_eq!(cache.insert_new(&hex, trades.clone()).unwrap(), 1);
        assert_eq!(cache.insert_new(&hex, trades).unwrap(), 0);
        assert_eq!(
            cache
                .insert_new(
                    &hex,
                    vec![
                        make_trade("0xnew", wallet, 1_704_067_101),
                        make_trade("0xtx1", wallet, 1_704_067_100)
                    ]
                )
                .unwrap(),
            1
        );
        assert_eq!(cache.insert_new(&hex, vec![]).unwrap(), 0);

        assert_eq!(cache.trade_count(), 2, "duplicate must not be stored twice");
    }

    #[test]
    fn insert_new_rollback_returns_error_and_no_partial_batch() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let first = make_trade("ok", wallet, 100);
        let mut bad = make_trade("overflow", wallet, 101);
        bad.contracts = ContractQty(u64::MAX);
        assert!(
            cache
                .insert_new(&wallet.to_string(), vec![first, bad])
                .is_err()
        );
        assert_eq!(cache.trade_count(), 0);
    }

    #[test]
    fn backfill_marker_is_durable_before_and_after_optional_stamp_finalization() {
        for stamp in [None, Some(2000)] {
            for old_stamp in [None, Some(1000)] {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("marker.db");
                let wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                let mut cache = WalletCache::open(&path).unwrap();
                cache.begin_walk(wallet, None).unwrap();
                if let Some(old) = old_stamp {
                    cache.update_last_polymarket_fetch(wallet, old).unwrap();
                }
                drop(cache);
                let mut cache = WalletCache::open(&path).unwrap();
                let read = |cache: &WalletCache| -> (i64, Option<i64>) {
                    cache.conn.query_row("SELECT backfill_partial, last_polymarket_fetch_at FROM wallets WHERE wallet_hex = ?1",
                        [wallet], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                };
                assert_eq!(read(&cache), (1, old_stamp));
                cache.finish_walk(wallet, stamp, 2000).unwrap();
                drop(cache);
                let cache = WalletCache::open(&path).unwrap();
                assert_eq!(read(&cache), (0, stamp.or(old_stamp)));
            }
        }
    }

    #[test]
    fn legacy_schema_gains_default_zero_marker_idempotently() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("legacy.db");
        let cache = WalletCache::open(&path).unwrap();
        cache.conn.execute_batch("ALTER TABLE wallets DROP COLUMN backfill_partial; ALTER TABLE wallets DROP COLUMN backward_floor_unix; ALTER TABLE wallets DROP COLUMN forward_frontier_unix; INSERT INTO wallets (wallet_hex) VALUES ('legacy');").unwrap();
        drop(cache);
        for _ in 0..2 {
            let config = BootstrapConfig {
                cache_path: path.clone(),
                cache_page_cache_mib: 8,
                cache_mmap_mib: 0,
                ..BootstrapConfig::default()
            };
            let cache = WalletCache::open_configured(&config).unwrap();
            assert_connection_tuning(&cache, -8 * 1024, 0);
            assert_eq!(cache.forward_frontier("legacy").unwrap(), None);
            assert_eq!(cache.backward_floor("legacy").unwrap(), None);
            let marker: i64 = cache
                .conn
                .query_row(
                    "SELECT backfill_partial FROM wallets WHERE wallet_hex = 'legacy'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(marker, 0);
        }
    }

    #[test]
    fn walk_bounds_are_atomic_outward_only_even_for_empty_pieces() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("bounds.db")).unwrap();
        cache.begin_walk("wallet", Some(100)).unwrap();
        cache
            .commit_walk_piece("wallet", vec![], Some(200), Some(50))
            .unwrap();
        cache
            .commit_walk_piece("wallet", vec![], Some(150), Some(75))
            .unwrap();
        assert_eq!(cache.forward_frontier("wallet").unwrap(), Some(200));
        assert_eq!(cache.backward_floor("wallet").unwrap(), Some(50));
        cache.conn.execute_batch("CREATE TRIGGER fail_floor BEFORE UPDATE OF backward_floor_unix ON wallets BEGIN SELECT RAISE(ABORT,'floor'); END").unwrap();
        assert!(
            cache
                .commit_walk_piece("wallet", vec![], Some(300), Some(1))
                .is_err()
        );
        assert_eq!(cache.forward_frontier("wallet").unwrap(), Some(200));
        assert_eq!(cache.backward_floor("wallet").unwrap(), Some(50));
    }

    #[test]
    fn fresh_partial_wallet_due_and_stamp_seed_does_not_clear_quarantine() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("due.db")).unwrap();
        cache.conn.execute_batch("INSERT INTO wallets(wallet_hex,is_active,last_polymarket_fetch_at,backfill_partial) VALUES ('partial',1,1000,1),('complete',1,1000,0),('inactive',0,1000,1),('infra',1,1000,1); UPDATE wallets SET is_infra=1 WHERE wallet_hex='infra'").unwrap();
        cache.seed_last_polymarket_fetch_from_trades().unwrap();
        assert_eq!(
            cache.select_backfill_due(1001, 100, 0).unwrap(),
            ["partial"]
        );
        assert_eq!(cache.partial_backfill_wallet_hexes().unwrap().len(), 3);
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = TempDir::new().unwrap();
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();
        {
            // Above the bundled SQLite mmap cap: clamping must not fail the open.
            let mut cache = tmp_cache_with_tuning(&dir, 8, 4096);
            assert_connection_tuning(&cache, -8 * 1024, 4096 * (1 << 20));
            cache
                .insert_new(&hex, vec![make_trade("0xtx1", wallet, 1_704_067_100)])
                .unwrap();
        }
        let cache2 = tmp_cache_with_tuning(&dir, 16, 0);
        assert_connection_tuning(&cache2, -16 * 1024, 0);
        assert_eq!(cache2.trade_count(), 1);
        assert_eq!(
            cache2.trades_for(&hex)[0].source_trade_id,
            SourceTradeId("0xtx1".to_owned())
        );
    }

    #[test]
    fn invalid_connection_tuning_never_creates_database() {
        let dir = TempDir::new().unwrap();
        let config = BootstrapConfig {
            cache_path: dir.path().join("cache.db"),
            cache_page_cache_mib: 0,
            ..BootstrapConfig::default()
        };
        assert!(matches!(
            WalletCache::open_configured(&config),
            Err(BootstrapError::Invalid { .. })
        ));
        assert!(!config.cache_path.exists());
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
    fn price_history_backfill_targets_skips_polygon_and_orders_recent_first() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        // Resolved markets with token mappings, distinct resolved_at + sources.
        cache
            .insert_resolution_with_source("0xclob-new", Some(0), 300, 301, "clob")
            .unwrap(); // target — newest
        cache
            .insert_resolution_with_source("0xgamma-mid", Some(0), 200, 201, "gamma")
            .unwrap(); // target — middle
        cache
            .insert_resolution_with_source("0xclob-old", Some(0), 100, 101, "clob")
            .unwrap(); // target — oldest
        cache
            .insert_resolution_with_source("0xpoly", Some(0), 250, 251, "polygon")
            .unwrap(); // EXCLUDED — legacy polygon never lists on CLOB
        cache
            .insert_resolution_with_source("0xvoid", None, 400, 401, "clob")
            .unwrap(); // EXCLUDED — no winner (voided)
        cache
            .insert_resolution_with_source("0xhasseries", Some(0), 500, 501, "clob")
            .unwrap(); // EXCLUDED — already has a series (NOT EXISTS)
        cache
            .upsert_token_conditions_batch(
                &[
                    ("t-clob-new".to_owned(), "0xclob-new".to_owned(), 0),
                    ("t-gamma".to_owned(), "0xgamma-mid".to_owned(), 0),
                    ("t-clob-old".to_owned(), "0xclob-old".to_owned(), 0),
                    ("t-poly".to_owned(), "0xpoly".to_owned(), 0),
                    ("t-void".to_owned(), "0xvoid".to_owned(), 0),
                    ("t-has".to_owned(), "0xhasseries".to_owned(), 0),
                ],
                1,
            )
            .unwrap();
        cache
            .insert_price_history_batch(
                &[(
                    "0xhasseries".to_owned(),
                    "t-has".to_owned(),
                    499,
                    "0.5".to_owned(),
                )],
                "clob",
            )
            .unwrap();

        let targets = cache.price_history_backfill_targets(0).unwrap();
        let got: Vec<(&str, &str, i64)> = targets
            .iter()
            .map(|t| (t.market_id.as_str(), t.token_id.as_str(), t.close_ref_unix))
            .collect();
        // Polygon, voided, and already-serialized markets are excluded; the rest are
        // ordered most-recently-resolved first (close_ref falls back to resolved_at).
        assert_eq!(
            got,
            vec![
                ("0xclob-new", "t-clob-new", 300),
                ("0xgamma-mid", "t-gamma", 200),
                ("0xclob-old", "t-clob-old", 100),
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

    #[test]
    fn source_cursor_delete_removes_only_requested_key() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
        cache.set_source_cursor("other_source", "1").unwrap();
        cache.delete_source_cursor("clob_closed").unwrap();
        assert!(cache.get_source_cursor("clob_closed").is_none());
        assert_eq!(
            cache.get_source_cursor("other_source").as_deref(),
            Some("1")
        );
        cache.delete_source_cursor("clob_closed").unwrap();
    }

    fn insert_audit_trade(cache: &WalletCache, id: &str, market_id: &str) {
        cache
            .conn
            .execute(
                "INSERT INTO trades (source_trade_id, wallet_hex, market_id, outcome_id, \
                 side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, '0x0000000000000000000000000000000000000001', ?2, 0, \
                         'buy', '0.5', 1, 1)",
                params![id, market_id],
            )
            .unwrap();
    }

    #[test]
    fn resolution_audit_selects_only_traded_scheduled_past_end_missing() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        for (id, end_date) in [
            ("past", Some(1_000)),
            ("null", None),
            ("future", Some(9_000)),
            ("resolved", Some(1_000)),
            ("untraded", Some(1_000)),
        ] {
            cache.insert_schedule(id, end_date, 1).unwrap();
        }
        for id in ["past", "null", "future", "resolved"] {
            insert_audit_trade(&cache, &format!("trade-{id}"), id);
        }
        cache
            .insert_resolution("resolved", Some(0), 1_000, 1)
            .unwrap();

        let missing = cache.missing_resolution_audit(6_400, 10).unwrap();
        assert_eq!(missing.market_ids, vec!["past"]);
        assert_eq!(missing.clipped, 0);
        assert_eq!(missing.total(), 1);
    }

    #[test]
    fn resolution_audit_reports_exact_clipped_count() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        for id in ["m3", "m1", "m2"] {
            cache.insert_schedule(id, Some(1_000), 1).unwrap();
            insert_audit_trade(&cache, &format!("trade-{id}"), id);
        }

        let missing = cache.missing_resolution_audit(6_400, 2).unwrap();
        assert_eq!(missing.market_ids, vec!["m1", "m2"]);
        assert_eq!(missing.clipped, 1);
        assert_eq!(missing.total(), 3);
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

    // ── #538: reclamation path selection (trace-proven) ───────────────────────

    static RECLAIM_TRACE: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    fn reclaim_trace_collect(sql: &str) {
        RECLAIM_TRACE.lock().unwrap().push(sql.to_owned());
    }

    #[test]
    fn reclaim_incremental_path_trace_proven() {
        // A fresh db is born incremental (open pragma), so reclamation must issue
        // `PRAGMA incremental_vacuum` and never a bare `VACUUM` (#538).
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // Build then drain a freelist: insert junk rows, delete them.
        cache
            .conn
            .execute_batch(
                "CREATE TABLE junk (x BLOB); \
                 INSERT INTO junk SELECT randomblob(4096) FROM \
                   (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 200) \
                    SELECT i FROM n); \
                 DROP TABLE junk;",
            )
            .unwrap();
        RECLAIM_TRACE.lock().unwrap().clear();
        cache.install_sql_trace(Some(reclaim_trace_collect));
        let report = cache.reclaim_free_pages().unwrap();
        cache.install_sql_trace(None);

        assert_eq!(report.auto_vacuum_before, 2, "fresh db is born incremental");
        assert_eq!(report.path, ReclamationPath::Incremental);
        assert_eq!(report.freelist_after, 0, "freelist drained");
        assert!(
            report.page_count_after < report.page_count_before,
            "incremental_vacuum truncates the file"
        );
        let stmts = RECLAIM_TRACE.lock().unwrap().clone();
        assert!(
            stmts.iter().any(|q| q.contains("incremental_vacuum")),
            "incremental_vacuum was issued: {stmts:?}"
        );
        assert!(
            !stmts
                .iter()
                .any(|q| q.trim_start().to_uppercase().starts_with("VACUUM")),
            "no bare VACUUM on the incremental path: {stmts:?}"
        );
    }

    #[test]
    fn reclamation_marker_roundtrip_fail_closed_shape() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        assert!(!cache.reclamation_pending().unwrap());
        cache.set_reclamation_pending().unwrap();
        assert!(cache.reclamation_pending().unwrap());
        cache.set_reclamation_pending().unwrap(); // idempotent upsert
        assert!(cache.reclamation_pending().unwrap());
        cache.clear_reclamation_pending().unwrap();
        assert!(!cache.reclamation_pending().unwrap());
    }
}
