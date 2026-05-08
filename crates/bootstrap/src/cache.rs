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
    fetched_at_unix     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_resolutions_resolved_at
    ON market_resolutions(resolved_at_unix);

CREATE TABLE IF NOT EXISTS market_schedules (
    market_id       TEXT    PRIMARY KEY NOT NULL,
    end_date_unix   INTEGER NULL,
    fetched_at_unix INTEGER NOT NULL
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
        Ok(Self { conn })
    }

    /// Insert trades not already present for `wallet_hex`. Idempotent on `source_trade_id`.
    ///
    /// `trades` ordering is unimportant — `INSERT OR IGNORE` rejects duplicates by primary key.
    /// All inserts run in a single transaction for atomicity and write batching.
    pub fn insert_new(
        &mut self,
        wallet_hex: &str,
        trades: Vec<RawTrade>,
    ) -> Result<(), BootstrapError> {
        if trades.is_empty() {
            return Ok(());
        }
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO trades \
                 (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for t in &trades {
                let contracts_i64 =
                    i64::try_from(t.contracts.0).map_err(|_| BootstrapError::Internal)?;
                stmt.execute(params![
                    t.source_trade_id.0,
                    wallet_hex,
                    t.market_id.0.0,
                    i64::from(t.outcome_id.0),
                    side_to_str(&t.side),
                    t.price.0.to_string(),
                    contracts_i64,
                    t.timestamp.0.unix_timestamp(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
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
        winning_outcome_id: Option<u8>,
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
            let Ok(winning_outcome_id) = u8::try_from(winner_i64) else {
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
}

/// Resolved-market record loaded from the `market_resolutions` table.
///
/// Only rows with `winning_outcome_id IS NOT NULL` are materialised in the
/// [`ResolutionIndex`]; voided markets are filtered at load time.
#[derive(Debug, Clone)]
pub struct MarketResolution {
    /// 0-based index of the winning outcome (e.g. 0 = YES, 1 = NO).
    pub winning_outcome_id: u8,
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
    let outcome = u8::try_from(outcome_id).ok()?;
    let side = str_to_side(side)?;
    let price_dec = Decimal::from_str(price_str).ok()?;
    let price = Price::new(price_dec).ok()?;
    let contracts_u64 = u64::try_from(contracts).ok()?;
    let dt = OffsetDateTime::from_unix_timestamp(ts).ok()?;
    Some(RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId(market_id)),
        outcome_id: OutcomeId(outcome),
        side,
        price,
        contracts: ContractQty(contracts_u64),
        timestamp: SourceTimestamp(dt),
        source_trade_id: SourceTradeId(id),
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
        assert_eq!(m.winning_outcome_id, 0);
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
        assert_eq!(m.winning_outcome_id, 1, "first insert must win");
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
}
