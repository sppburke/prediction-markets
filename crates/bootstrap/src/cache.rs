//! Permanent wallet trade-history cache — SQLite (WAL mode), no TTL.
//!
//! Six tables:
//! - `trades` — append-only per-trade rows, indexed by `(wallet_hex, timestamp_unix)`.
//! - `leaderboard_snapshots` — `(snapshot_at_unix, wallet_hex)` rows, one row-set per
//!   `pe-bootstrap` run, written from the post-filtered watchlist. Read by
//!   `pe-backtest` to constrain the candidate pool at each simulated week boundary.
//! - `market_resolutions` — one row per resolved market from the Gamma API.
//!   `winning_outcome_id NULL` means voided/non-binary — the backtest skips these.
//! - `market_schedules` — one row per market whose scheduled `endDate` has been
//!   fetched from Gamma. `end_date_unix NULL` means Gamma had no `endDate` for this
//!   market (it is still in the skip-set to avoid re-fetching).
//! - `funder_edges` — one row per `(funder, funded)` pair discovered via Etherscan.
//!   Time-invariant once the block range is finalized; populated by `pe-bootstrap`
//!   when `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1`.
//! - `funder_lookup_done` — one row per wallet that has been queried for funders.
//!   Distinguishes "queried and found zero funders" from "not yet queried".
//! - `wallets` — canonical wallet pile (issue #166). One row per known wallet across
//!   every discovery source (`wallet_set.json`, `trades`, Dune CSV, Dune incremental,
//!   Polymarket leaderboard, Radion, 502-gap). `is_active` is sticky (0→1 only) and
//!   controls which wallets the `backfill`/`weekly` subcommands process.
//!
//! WAL mode provides per-commit durability — no atomic-rename or checkpoint batching
//! is needed. Per-wallet streaming reads keep peak memory bounded.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rusqlite::{Connection, OpenFlags, params};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::error::BootstrapError;

/// Row tuple for [`WalletCache::upsert_wallets_bulk`]:
/// `(wallet_hex, source_bits, is_infra, dune_first_seen_unix, dune_closed_markets, dune_win_rate_bps)`.
pub type WalletUpsertRow = (String, i64, bool, Option<i64>, Option<i64>, Option<i64>);

/// Number of consecutive known `source_trade_id`s that signals the incremental fetch is done.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const INCREMENTAL_STOP_THRESHOLD: usize = 3;

const SCHEMA: &str = "
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

CREATE TABLE IF NOT EXISTS funder_edges (
    funder_hex      TEXT    NOT NULL,
    funded_hex      TEXT    NOT NULL,
    fetched_at_unix INTEGER NOT NULL,
    event_at_unix   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (funder_hex, funded_hex)
);
CREATE INDEX IF NOT EXISTS idx_funder_edges_funded ON funder_edges(funded_hex);

CREATE TABLE IF NOT EXISTS funder_lookup_done (
    wallet_hex      TEXT    PRIMARY KEY NOT NULL,
    fetched_at_unix INTEGER NOT NULL
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

-- Delta-backfill audit table (issue #176). One row per (run, wallet) where
-- the wallet had at least one new trade OR appeared in the on-chain scan's
-- delta_set this run. True-negative wallets (no activity, not in delta_set)
-- produce no row so the audit table grows ~5–10% of the daily backfill size
-- instead of full ~105k/day.
CREATE TABLE IF NOT EXISTS delta_audit (
    run_at_unix        INTEGER NOT NULL,
    wallet_hex         TEXT    NOT NULL,
    classification     TEXT    NOT NULL,
    new_trades_fetched INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_at_unix, wallet_hex)
);
CREATE INDEX IF NOT EXISTS idx_delta_audit_classification
    ON delta_audit(classification, new_trades_fetched);

-- Wallet pile (issue #166). `wallet_hex` is the canonical form produced by
-- `WalletAddress::Display`: `\"0x\" + 40 lowercase hex chars`.
-- `source_bits`: bit0=wallet_set_json, bit1=trades, bit2=dune_csv,
-- bit3=dune_incr, bit4=leaderboard, bit5=radion, bit6=gap502.
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
    discovered_at_unix       INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
);
CREATE INDEX IF NOT EXISTS idx_wallets_is_active ON wallets(is_active);
CREATE INDEX IF NOT EXISTS idx_wallets_backfill
    ON wallets(is_active, last_polymarket_fetch_at) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_wallets_weekly
    ON wallets(is_active, last_funder_fetch_at) WHERE is_active = 1;
";

/// Permanent wallet trade-history cache backed by SQLite.
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
        // Migration: add event_at_unix if absent (DBs created before this change keep
        // the default 0, which makes existing edges always visible in walk-forward sims).
        let col_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('funder_edges') WHERE name='event_at_unix'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        if !col_exists {
            conn.execute_batch(
                "ALTER TABLE funder_edges ADD COLUMN event_at_unix INTEGER NOT NULL DEFAULT 0",
            )?;
        }
        // Migration: add `source` column to market_resolutions / market_schedules. DBs
        // created before this change keep `DEFAULT 'gamma'` — correct since every
        // pre-migration row was inserted by the Gamma fetcher.
        add_source_column_if_missing(&conn, "market_resolutions")?;
        add_source_column_if_missing(&conn, "market_schedules")?;
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
    #[cfg(any(test, feature = "scenario"))]
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
    /// `"polygon"` (on-chain `eth_getLogs`), `"clob"` (Polymarket CLOB), `"dune"`
    /// (Dune Analytics fallback). Gamma-sourced rows continue to flow through the
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
    /// (oracle-settlement-time approximation), and Cycle 2's CLOB path
    /// writes rows with `end_date_iso` (scheduled-close-time approximation).
    /// Both are imprecise relative to Dune's `evt_block_time` /
    /// Polygon RPC's block timestamp. Deleting them lets the precision
    /// sources (Dune, Polygon) re-populate on the next stage-6 pass.
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

    // ── funder_edges / funder_lookup_done ─────────────────────────────────────

    /// Return wallets that appear in `trades` but have not yet been queried for funders.
    ///
    /// # Precondition
    /// Returns all distinct trade wallets when no funder discovery has been run.
    pub fn wallets_needing_funder_lookup(&self) -> Result<Vec<WalletAddress>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT wallet_hex FROM trades \
             WHERE wallet_hex NOT IN (SELECT wallet_hex FROM funder_lookup_done)",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            let hex = row?;
            if let Ok(addr) = WalletAddress::from_hex(&hex) {
                result.push(addr);
            }
        }
        Ok(result)
    }

    /// Record funder edges for `wallet` and mark it as done.
    ///
    /// `funders` is a slice of `(funder_address, event_at_unix)` pairs where
    /// `event_at_unix` is the Polygon block timestamp of the earliest funding
    /// transaction. Use `0` as the sentinel for edges without a known on-chain
    /// timestamp — `0` is treated as epoch (Jan 1 1970) by `FunderGraphTimeline`,
    /// making those edges always visible in walk-forward simulations.
    ///
    /// Atomic per-wallet commit: N edge inserts + 1 done-mark in a single transaction.
    /// Idempotent: repeated calls for the same `(funder, funded)` pair are safe.
    /// A wallet with zero funders is still written to `funder_lookup_done` so it is
    /// not re-queried on subsequent runs.
    pub fn insert_funder_edges(
        &mut self,
        wallet: WalletAddress,
        funders: &[(WalletAddress, i64)],
        fetched_at_unix: i64,
    ) -> Result<(), BootstrapError> {
        let wallet_hex = wallet.to_string();
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO funder_edges \
                 (funder_hex, funded_hex, fetched_at_unix, event_at_unix) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (funder, event_at) in funders {
                stmt.execute(params![
                    funder.to_string(),
                    wallet_hex,
                    fetched_at_unix,
                    event_at
                ])?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO funder_lookup_done \
                 (wallet_hex, fetched_at_unix) VALUES (?1, ?2)",
                params![wallet_hex, fetched_at_unix],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Load all funder edges as `(funder, funded)` address pairs.
    ///
    /// # Precondition
    /// Returns an empty `Vec` if no funder discovery has been run yet.
    pub fn load_funder_edges(&self) -> Result<Vec<(WalletAddress, WalletAddress)>, BootstrapError> {
        Ok(self
            .load_funder_edges_with_timestamp()?
            .into_iter()
            .map(|(funder, funded, _)| (funder, funded))
            .collect())
    }

    /// Load all funder edges with their on-chain event timestamp, sorted ascending.
    ///
    /// Returns `(funder, funded, event_at_unix)` triples. Rows migrated from older
    /// DB versions have `event_at_unix = 0` (epoch sentinel — always visible in
    /// walk-forward simulations). Used by `FunderGraphTimeline`.
    pub fn load_funder_edges_with_timestamp(
        &self,
    ) -> Result<Vec<(WalletAddress, WalletAddress, i64)>, BootstrapError> {
        let mut stmt = self.conn.prepare(
            "SELECT funder_hex, funded_hex, event_at_unix \
             FROM funder_edges ORDER BY event_at_unix ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (funder_hex, funded_hex, event_at) = row?;
            if let (Ok(funder), Ok(funded)) = (
                WalletAddress::from_hex(&funder_hex),
                WalletAddress::from_hex(&funded_hex),
            ) {
                result.push((funder, funded, event_at));
            }
        }
        Ok(result)
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
                dune_first_seen_unix, dune_closed_markets, dune_win_rate_bps\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
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
    pub fn upsert_wallets_bulk(&mut self, rows: &[WalletUpsertRow]) -> Result<(), BootstrapError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO wallets (\
                    wallet_hex, source_bits, is_infra, \
                    dune_first_seen_unix, dune_closed_markets, dune_win_rate_bps\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(wallet_hex) DO UPDATE SET \
                    source_bits = source_bits | excluded.source_bits, \
                    is_infra = MAX(is_infra, excluded.is_infra), \
                    dune_first_seen_unix = COALESCE(excluded.dune_first_seen_unix, dune_first_seen_unix), \
                    dune_closed_markets = COALESCE(excluded.dune_closed_markets, dune_closed_markets), \
                    dune_win_rate_bps = COALESCE(excluded.dune_win_rate_bps, dune_win_rate_bps)",
            )?;
            for (wallet, bits, infra, first_seen, closed, win_rate) in rows {
                let is_infra_int: i64 = i64::from(*infra);
                stmt.execute(params![
                    wallet,
                    bits,
                    is_infra_int,
                    first_seen,
                    closed,
                    win_rate,
                ])?;
            }
        }
        tx.commit()?;
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

    /// Seed `last_funder_fetch_at = funder_lookup_done.fetched_at_unix` for already-discovered wallets.
    ///
    /// Run during migrate so the first `run_weekly` doesn't redundantly re-fetch
    /// funders for the existing wallet set via Etherscan.
    pub fn seed_last_funder_fetch_from_done(&mut self) -> Result<usize, BootstrapError> {
        let affected = self.conn.execute(
            "UPDATE wallets SET last_funder_fetch_at = (\
                SELECT fetched_at_unix FROM funder_lookup_done \
                WHERE funder_lookup_done.wallet_hex = wallets.wallet_hex\
             ) WHERE EXISTS (\
                SELECT 1 FROM funder_lookup_done WHERE funder_lookup_done.wallet_hex = wallets.wallet_hex\
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
            "SELECT wallet_hex FROM wallets \
             WHERE is_active = 1 \
               AND (last_polymarket_full_at IS NULL OR last_polymarket_full_at < ?1) \
             ORDER BY last_polymarket_full_at ASC NULLS FIRST, \
                      dune_win_rate_bps DESC NULLS LAST, \
                      dune_closed_markets DESC NULLS LAST",
        )?;
        let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Bulk-insert classification rows into the `delta_audit` table.
    ///
    /// Idempotent on `(run_at_unix, wallet_hex)` PK — re-running the same
    /// classification pass is a no-op. Issue #176.
    pub fn insert_delta_audit_rows(
        &mut self,
        rows: &[(i64, String, &'static str, i64)],
    ) -> Result<(), BootstrapError> {
        if rows.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO delta_audit \
                 (run_at_unix, wallet_hex, classification, new_trades_fetched) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (run_at, hex, class, count) in rows {
                stmt.execute(params![run_at, hex, class, count])?;
            }
        }
        tx.commit()?;
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
        let base = "SELECT wallet_hex FROM wallets \
                    WHERE is_active = 1 \
                      AND (last_polymarket_fetch_at IS NULL OR last_polymarket_fetch_at < ?1) \
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
        let base = "SELECT wallet_hex FROM wallets \
                    WHERE is_active = 1 \
                      AND (last_funder_fetch_at IS NULL OR last_funder_fetch_at < ?1) \
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
    pub fn apply_activation_rules(&mut self, min_trades: i64) -> Result<usize, BootstrapError> {
        let affected = self.conn.execute(
            "UPDATE wallets SET is_active = 1 \
             WHERE is_active = 0 AND is_infra = 0 AND (\
                COALESCE(trade_count, 0) >= ?1 \
             OR COALESCE(dune_closed_markets, 0) >= ?1 \
             OR (source_bits & 16) != 0 \
             OR (source_bits & 32) != 0 \
             OR (source_bits & 64) != 0\
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

    /// Return every `wallet_hex` in the pile — used by `run_discovery` for the
    /// Dune known-wallets upload (anti-join input).
    pub fn all_pile_wallet_hexes(&self) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self.conn.prepare("SELECT wallet_hex FROM wallets")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let out: Result<Vec<_>, _> = rows.collect();
        Ok(out?)
    }

    /// Return every `wallet_hex` whose `source_bits` has at least one bit in
    /// common with `bit_mask`. Issue #181: used by `run()` to scope per-wallet
    /// trade fetch to the discovered-wallet subset (typically `SRC_WALLET_SET_JSON`),
    /// avoiding the ~2.7M-row blowup of [`Self::all_pile_wallet_hexes`].
    pub fn wallets_with_source_bit(&self, bit_mask: i64) -> Result<Vec<String>, BootstrapError> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_hex FROM wallets WHERE (source_bits & ?1) != 0")?;
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

/// Add a `source TEXT NOT NULL DEFAULT 'gamma'` column to `table` if it doesn't
/// already exist. Idempotent: safe to call on every `WalletCache::open`.
///
/// Used for the issue #149 multi-source-pipeline migration. SQLite ALTER TABLE
/// ADD COLUMN with a constant DEFAULT is supported and back-fills the column
/// for existing rows — every pre-migration `market_resolutions` /
/// `market_schedules` row was written by Gamma, so `'gamma'` is the correct
/// retroactive tag.
fn add_source_column_if_missing(conn: &Connection, table: &str) -> Result<(), BootstrapError> {
    let col_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name='source'",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;
    if !col_exists {
        let sql = format!("ALTER TABLE {table} ADD COLUMN source TEXT NOT NULL DEFAULT 'gamma'");
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
        assert!(cache.get_source_cursor("polygon_ctf_last_block").is_none());
    }

    #[test]
    fn source_cursor_set_then_get_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("clob_closed", "LTE=").unwrap();
        cache
            .set_source_cursor("polygon_ctf_last_block", "55000000")
            .unwrap();
        assert_eq!(
            cache.get_source_cursor("clob_closed").as_deref(),
            Some("LTE=")
        );
        assert_eq!(
            cache.get_source_cursor("polygon_ctf_last_block").as_deref(),
            Some("55000000")
        );
    }

    #[test]
    fn source_cursor_set_overwrites_prior_value() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache
            .set_source_cursor("polygon_ctf_last_block", "33605403")
            .unwrap();
        cache
            .set_source_cursor("polygon_ctf_last_block", "33615403")
            .unwrap();
        assert_eq!(
            cache.get_source_cursor("polygon_ctf_last_block").as_deref(),
            Some("33615403"),
            "later set must replace earlier value"
        );
    }

    #[test]
    fn source_cursor_keys_are_independent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        cache.set_source_cursor("clob_closed", "abc").unwrap();
        cache
            .set_source_cursor("polygon_ctf_last_block", "1")
            .unwrap();
        // Updating one key must not affect the other.
        cache.set_source_cursor("clob_closed", "def").unwrap();
        assert_eq!(
            cache.get_source_cursor("clob_closed").as_deref(),
            Some("def")
        );
        assert_eq!(
            cache.get_source_cursor("polygon_ctf_last_block").as_deref(),
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

    // ── funder_edges / funder_lookup_done unit tests ──────────────────────────

    #[test]
    fn wallets_needing_lookup_returns_all_when_none_done() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache
            .insert_new(&w1.to_string(), vec![make_trade("t1", w1, 1_000)])
            .unwrap();
        cache
            .insert_new(&w2.to_string(), vec![make_trade("t2", w2, 2_000)])
            .unwrap();

        let pending = cache.wallets_needing_funder_lookup().unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&w1));
        assert!(pending.contains(&w2));
    }

    #[test]
    fn wallets_needing_lookup_returns_only_pending() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache
            .insert_new(&w1.to_string(), vec![make_trade("t1", w1, 1_000)])
            .unwrap();
        cache
            .insert_new(&w2.to_string(), vec![make_trade("t2", w2, 2_000)])
            .unwrap();

        // Mark w1 as done.
        cache.insert_funder_edges(w1, &[], 1_700_000_000).unwrap(); // zero funders

        let pending = cache.wallets_needing_funder_lookup().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], w2);
    }

    #[test]
    fn insert_funder_edges_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let funded = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let funder1 = addr("0x1111111111111111111111111111111111111111");
        let funder2 = addr("0x2222222222222222222222222222222222222222");
        let funder3 = addr("0x3333333333333333333333333333333333333333");

        cache
            .insert_funder_edges(
                funded,
                &[
                    (funder1, 1_600_000_000),
                    (funder2, 1_600_000_001),
                    (funder3, 1_600_000_002),
                ],
                1_700_000_000,
            )
            .unwrap();

        let edges = cache.load_funder_edges().unwrap();
        assert_eq!(edges.len(), 3);
        let funded_addrs: Vec<_> = edges.iter().map(|(_, f)| *f).collect();
        assert!(funded_addrs.iter().all(|f| *f == funded));
        let funder_addrs: Vec<_> = edges.iter().map(|(f, _)| *f).collect();
        assert!(funder_addrs.contains(&funder1));
        assert!(funder_addrs.contains(&funder2));
        assert!(funder_addrs.contains(&funder3));
    }

    #[test]
    fn insert_funder_edges_zero_funders_marks_done() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache
            .insert_new(&w.to_string(), vec![make_trade("t1", w, 1_000)])
            .unwrap();

        // Zero funders — wallet is still marked done.
        cache.insert_funder_edges(w, &[], 1_700_000_000).unwrap(); // no funder tuples

        let pending = cache.wallets_needing_funder_lookup().unwrap();
        assert!(
            pending.is_empty(),
            "wallet with zero funders must still be marked done"
        );

        let edges = cache.load_funder_edges().unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn insert_funder_edges_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let funded = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let funder = addr("0x1111111111111111111111111111111111111111");

        cache
            .insert_funder_edges(funded, &[(funder, 1_600_000_000)], 1_700_000_000)
            .unwrap();
        cache
            .insert_funder_edges(funded, &[(funder, 1_600_000_001)], 1_700_000_001)
            .unwrap();

        let edges = cache.load_funder_edges().unwrap();
        assert_eq!(
            edges.len(),
            1,
            "duplicate (funder, funded) must not double-insert"
        );
    }

    #[test]
    fn load_funder_edges_full_table() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let f1 = addr("0x1111111111111111111111111111111111111111");
        let f2 = addr("0x2222222222222222222222222222222222222222");
        let f3 = addr("0x3333333333333333333333333333333333333333");

        // w1 has 2 funders, w2 has 1.
        cache
            .insert_funder_edges(
                w1,
                &[(f1, 1_600_000_000), (f2, 1_600_000_001)],
                1_700_000_000,
            )
            .unwrap();
        cache
            .insert_funder_edges(w2, &[(f3, 1_600_000_002)], 1_700_000_001)
            .unwrap();

        let edges = cache.load_funder_edges().unwrap();
        assert_eq!(edges.len(), 3);
    }

    #[test]
    fn funder_edges_round_trip_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let funded = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let funder = addr("0x1111111111111111111111111111111111111111");

        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache
                .insert_funder_edges(funded, &[(funder, 1_600_000_000)], 1_700_000_000)
                .unwrap();
        }
        {
            let cache = WalletCache::open(&path).unwrap();
            let edges = cache.load_funder_edges().unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0], (funder, funded));
        }
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
}
