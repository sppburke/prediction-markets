//! SQLite persistence for the shadow harness, mirroring `pe-paper-state`:
//! `Mutex<Connection>`, `PRAGMA user_version` schema versioning, Decimal-as-TEXT.
//!
//! Tables: `meta` (run provenance + vantage), `markets`, `raw_ticks` (every raw
//! frame, for offline recompute/replay), `observations` (computed edges).

use std::path::Path;
use std::str::FromStr as _;
use std::sync::{Mutex, PoisonError};

use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use rust_decimal::Decimal;

use std::collections::HashMap;

use crate::types::{
    BtcMarketMeta, BtcSeriesKind, ClobTrade, EdgeObservation, FeedSource, MoveDirection,
};

/// On-disk schema version, stamped into `PRAGMA user_version` on create.
/// Currently `4`. No migration is provided because no rows predate any of these
/// changes (production `observations = 0`); an older DB is rejected by the
/// version-mismatch guard in [`ShadowDb::open`] rather than silently mis-read.
///
/// History:
/// - `2` — move-trigger re-architecture (issue #300 Phase 2): `observations`
///   swaps `chainlink_value_str` → `signal_value_str` (the exchange-consensus
///   median, not the Chainlink settling value) and adds `move_magnitude_bps_str`
///   / `move_direction`.
/// - `3` — NO-side capture: `markets` adds `no_token_id` and `observations` adds
///   `no_best_ask_str` / `no_mid_str` (the real Down-buy entry price, so
///   down-moves are scored on the NO book rather than `1 − yes_bid`).
/// - `4` — trade-feed capture: adds `clob_trades` (`last_trade_price` prints,
///   the maker-fill-simulation input). `condition_id` is `NOT NULL` — taken from
///   the frame's authoritative `market` field so a trade is attributed even
///   before the join knows its token — and is indexed alongside `token_id` and
///   `traded_at_ms` for offline time-windowed strategy comparison.
pub const SCHEMA_VERSION: i64 = 4;

/// Provenance value stamped into `meta["lag_clock"]` so a recompute can tell
/// which clock basis `feed_to_book_lag_ms` was computed under. The column is
/// re-meaninged within `v1` (source-timestamp skew → receive-node-clock skew,
/// issue #300 fix 5); no migration is needed because no observation rows
/// predate the change.
pub const LAG_CLOCK: &str = "received_node_v2";

/// `meta` keys carrying the per-source frames-dropped tallies the runner stamps
/// at run end (issue #311). Shared by the runner's stamp and the #310 sweep's
/// tape-validity read so the two cannot drift. Order mirrors the runner's
/// `DropCounters` fields.
pub(crate) const FRAMES_DROPPED_META_KEYS: [&str; 5] = [
    "frames_dropped_chainlink",
    "frames_dropped_clob",
    "frames_dropped_bybit",
    "frames_dropped_okx",
    "frames_dropped_coinbase",
];

const SCHEMA: &str = "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    condition_id   TEXT PRIMARY KEY NOT NULL,
    yes_token_id   TEXT NOT NULL,
    no_token_id    TEXT NOT NULL,
    series         TEXT NOT NULL,
    range_start_ms INTEGER NOT NULL,
    range_end_ms   INTEGER NOT NULL,
    tick_str       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_ticks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,
    received_ms  INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id         TEXT NOT NULL,
    series               TEXT NOT NULL,
    observed_at_ms       INTEGER NOT NULL,
    signal_value_str     TEXT NOT NULL,
    range_start_str      TEXT,
    prob_up_str          TEXT,
    best_ask_str         TEXT,
    mid_str              TEXT,
    no_best_ask_str      TEXT,
    no_mid_str           TEXT,
    gross_edge_ask_str   TEXT,
    gross_edge_mid_str   TEXT,
    fee_cost_str         TEXT,
    net_edge_ask_str     TEXT,
    net_edge_mid_str     TEXT,
    feed_to_book_lag_ms  INTEGER,
    move_magnitude_bps_str TEXT NOT NULL,
    move_direction       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_obs_series ON observations(series);

CREATE TABLE IF NOT EXISTS resolutions (
    condition_id  TEXT PRIMARY KEY NOT NULL,
    yes_won       INTEGER NOT NULL,
    fetched_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clob_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id          TEXT NOT NULL,
    condition_id      TEXT NOT NULL,
    series            TEXT,
    price_str         TEXT NOT NULL,
    size_str          TEXT NOT NULL,
    taker_is_buy      INTEGER NOT NULL,
    traded_at_ms      INTEGER NOT NULL,
    received_at_ms    INTEGER NOT NULL,
    fee_rate_bps      INTEGER NOT NULL,
    transaction_hash  TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_clob_trades_token ON clob_trades(token_id);
CREATE INDEX IF NOT EXISTS idx_clob_trades_condition ON clob_trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_clob_trades_traded_at ON clob_trades(traded_at_ms);
";

/// Persistence error.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("schema version mismatch: db has user_version {found}, expected {expected}")]
    SchemaVersionMismatch { found: i64, expected: i64 },
    #[error("corrupt stored value: {0}")]
    Corrupt(String),
}

/// A subset of an `observations` row, read back for the report aggregation.
#[derive(Debug, Clone, PartialEq)]
pub struct ObsRow {
    pub series: String,
    pub best_ask: Option<Decimal>,
    pub net_edge_vs_ask: Option<Decimal>,
    pub net_edge_vs_mid: Option<Decimal>,
    pub feed_to_book_lag_ms: Option<i64>,
}

/// One observation joined to its market resolution (if resolved), for the
/// **realized**-edge report. `yes_won` is `None` while the market is still open
/// or unresolved — those rows are not scored. The realized layer picks the entry
/// side by `move_direction`: an up-move buys YES at `best_ask`; a down-move buys
/// NO at `no_best_ask`.
#[derive(Debug, Clone, PartialEq)]
pub struct RealizedRow {
    pub series: String,
    pub move_direction: String,
    pub best_ask: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub yes_won: Option<bool>,
}

/// One `raw_ticks` row streamed back in tape (`id`) order for the #310 offline
/// sweep. `id` is the canonical tape position (single-writer insertion order,
/// pinned by `frame_batch_persists_both_tables_in_order`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTickRow {
    pub id: i64,
    pub source: FeedSource,
    pub received_ms: i64,
    pub payload_json: String,
}

/// SQLite-backed store. Cheap to clone the handle is not supported; share via a
/// reference or `Arc`.
pub struct ShadowDb {
    conn: Mutex<Connection>,
}

impl ShadowDb {
    /// Open (creating if absent) and migrate the database at `path`.
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        let found: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found != 0 && found != SCHEMA_VERSION {
            return Err(DbError::SchemaVersionMismatch {
                found,
                expected: SCHEMA_VERSION,
            });
        }
        conn.execute_batch(SCHEMA)?;
        if found == 0 {
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Upsert a `meta` key/value.
    pub fn set_meta(&self, key: &str, value: &str) -> Result<(), DbError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Read a `meta` value.
    pub fn get_meta(&self, key: &str) -> Result<Option<String>, DbError> {
        let conn = self.lock();
        let v = conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get::<_, String>(0)
            })
            .optional()?;
        Ok(v)
    }

    /// Upsert a market's static metadata.
    pub fn upsert_market(&self, m: &BtcMarketMeta) -> Result<(), DbError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO markets
               (condition_id, yes_token_id, no_token_id, series, range_start_ms, range_end_ms, tick_str)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(condition_id) DO UPDATE SET
               yes_token_id = excluded.yes_token_id,
               no_token_id  = excluded.no_token_id,
               series       = excluded.series,
               range_start_ms = excluded.range_start_ms,
               range_end_ms   = excluded.range_end_ms,
               tick_str       = excluded.tick_str",
            params![
                m.condition_id,
                m.yes_token_id,
                m.no_token_id,
                m.series.as_str(),
                m.range_start_ms,
                m.range_end_ms,
                m.tick.to_string(),
            ],
        )?;
        Ok(())
    }

    /// Persist one raw inbound frame.
    pub fn insert_raw_tick(
        &self,
        source: FeedSource,
        received_ms: i64,
        payload_json: &str,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO raw_ticks (source, received_ms, payload_json) VALUES (?1, ?2, ?3)",
            params![source.as_str(), received_ms, payload_json],
        )?;
        Ok(())
    }

    /// Persist a decoded CLOB trade print. `condition_id` is taken from the trade
    /// itself (the frame's authoritative `market` field — always present), so a
    /// trade is attributed even before the join knows its token. `series`
    /// (5m/15m) comes from the join's token lookup and may be `None` for a trade
    /// that printed before its market was registered **or after it was pruned**
    /// (issue #311) — both recoverable offline from `markets`/Gamma. `INSERT OR
    /// IGNORE` on the unique `transaction_hash` dedups a re-delivered print
    /// without dropping any distinct trade (the hash is one-per-print on the
    /// live feed, verified 2026-06-09).
    pub fn insert_clob_trade(
        &self,
        trade: &ClobTrade,
        received_at_ms: i64,
        series: Option<&str>,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        conn.execute(
            "INSERT OR IGNORE INTO clob_trades
               (token_id, condition_id, series, price_str, size_str, taker_is_buy,
                traded_at_ms, received_at_ms, fee_rate_bps, transaction_hash)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                trade.token_id,
                trade.condition_id,
                series,
                trade.price.0.to_string(),
                trade.size.to_string(),
                i64::from(trade.taker_is_buy),
                trade.traded_at_ms,
                received_at_ms,
                i64::from(trade.fee_rate_bps),
                trade.transaction_hash,
            ],
        )?;
        Ok(())
    }

    /// Count rows in `clob_trades`.
    pub fn clob_trade_count(&self) -> Result<i64, DbError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM clob_trades", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Persist one flush window of buffered frames in a **single transaction**
    /// spanning `raw_ticks` and `clob_trades` (issue #311): per-frame
    /// auto-commits on the hot path could not keep up with CLOB book volume
    /// (~450–1800 ms/s of blocking at ~900 frames/s), saturating the bounded
    /// channel. One commit per flush window cuts that to ~10–20 ms/s.
    ///
    /// The tuples are the runner's buffer element types, passed as the buffers
    /// themselves. `raw_ticks.id` stays insertion-ordered (single writer
    /// connection; rows execute in slice order). Trades keep `INSERT OR IGNORE`
    /// semantics on the unique `transaction_hash` — a duplicate inside one batch
    /// is ignored, not an error.
    pub fn insert_frame_batch(
        &self,
        ticks: &[(FeedSource, i64, String)],
        trades: &[(ClobTrade, i64, Option<&'static str>)],
    ) -> Result<(), DbError> {
        if ticks.is_empty() && trades.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        {
            let mut tick_stmt = tx.prepare_cached(
                "INSERT INTO raw_ticks (source, received_ms, payload_json) VALUES (?1, ?2, ?3)",
            )?;
            for (source, received_ms, payload_json) in ticks {
                tick_stmt.execute(params![source.as_str(), received_ms, payload_json])?;
            }
            let mut trade_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO clob_trades
                   (token_id, condition_id, series, price_str, size_str, taker_is_buy,
                    traded_at_ms, received_at_ms, fee_rate_bps, transaction_hash)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            )?;
            for (trade, received_at_ms, series) in trades {
                trade_stmt.execute(params![
                    trade.token_id,
                    trade.condition_id,
                    series,
                    trade.price.0.to_string(),
                    trade.size.to_string(),
                    i64::from(trade.taker_is_buy),
                    trade.traded_at_ms,
                    received_at_ms,
                    i64::from(trade.fee_rate_bps),
                    trade.transaction_hash,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Batch-insert computed observations in a single transaction.
    pub fn insert_observations(&self, rows: &[EdgeObservation]) -> Result<(), DbError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        for o in rows {
            tx.execute(
                "INSERT INTO observations
                   (condition_id, series, observed_at_ms, signal_value_str,
                    range_start_str, prob_up_str, best_ask_str, mid_str,
                    no_best_ask_str, no_mid_str,
                    gross_edge_ask_str, gross_edge_mid_str, fee_cost_str,
                    net_edge_ask_str, net_edge_mid_str, feed_to_book_lag_ms,
                    move_magnitude_bps_str, move_direction)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
                params![
                    o.condition_id,
                    o.series.as_str(),
                    o.observed_at_ms,
                    o.signal_value.to_string(),
                    o.range_start_value.map(|d| d.to_string()),
                    o.instantaneous_prob_up.map(|d| d.to_string()),
                    o.best_ask.map(|d| d.to_string()),
                    o.mid.map(|d| d.to_string()),
                    o.no_best_ask.map(|d| d.to_string()),
                    o.no_mid.map(|d| d.to_string()),
                    o.gross_edge_vs_ask.map(|d| d.to_string()),
                    o.gross_edge_vs_mid.map(|d| d.to_string()),
                    o.fee_cost.map(|d| d.to_string()),
                    o.net_edge_vs_ask.map(|d| d.to_string()),
                    o.net_edge_vs_mid.map(|d| d.to_string()),
                    o.feed_to_book_lag_ms,
                    o.move_magnitude_bps.to_string(),
                    o.move_direction.as_str(),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Count rows in `observations`.
    pub fn observation_count(&self) -> Result<i64, DbError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM observations", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Count rows in `raw_ticks`.
    pub fn raw_tick_count(&self) -> Result<i64, DbError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM raw_ticks", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Upsert a market resolution (`yes_won` = the Up/YES token won).
    pub fn upsert_resolution(
        &self,
        condition_id: &str,
        yes_won: bool,
        fetched_at_ms: i64,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO resolutions (condition_id, yes_won, fetched_at_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(condition_id) DO UPDATE SET
               yes_won = excluded.yes_won,
               fetched_at_ms = excluded.fetched_at_ms",
            params![condition_id, i64::from(yes_won), fetched_at_ms],
        )?;
        Ok(())
    }

    /// Count rows in `resolutions`.
    pub fn resolution_count(&self) -> Result<i64, DbError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM resolutions", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Distinct `condition_id`s that have at least one observation — the markets
    /// the `resolve` step fetches outcomes for.
    pub fn distinct_observation_condition_ids(&self) -> Result<Vec<String>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT DISTINCT condition_id FROM observations")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Observations left-joined to their resolution, for the realized report.
    /// Unresolved markets yield `yes_won = None` (not scored).
    pub fn all_realized_rows(&self) -> Result<Vec<RealizedRow>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT o.series, o.move_direction, o.best_ask_str, o.no_best_ask_str, r.yes_won
             FROM observations o
             LEFT JOIN resolutions r ON o.condition_id = r.condition_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<i64>>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (series, move_direction, best_ask, no_best_ask, yes_won) = row?;
            out.push(RealizedRow {
                series,
                move_direction,
                best_ask: parse_opt_decimal(best_ask.as_deref())?,
                no_best_ask: parse_opt_decimal(no_best_ask.as_deref())?,
                yes_won: yes_won.map(|v| v != 0),
            });
        }
        Ok(out)
    }

    /// Read all observations needed for the report aggregation.
    pub fn all_observations_for_report(&self) -> Result<Vec<ObsRow>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT series, best_ask_str, net_edge_ask_str, net_edge_mid_str, feed_to_book_lag_ms
             FROM observations",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<i64>>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (series, ask, net_ask, net_mid, lag) = row?;
            out.push(ObsRow {
                series,
                best_ask: parse_opt_decimal(ask.as_deref())?,
                net_edge_vs_ask: parse_opt_decimal(net_ask.as_deref())?,
                net_edge_vs_mid: parse_opt_decimal(net_mid.as_deref())?,
                feed_to_book_lag_ms: lag,
            });
        }
        Ok(out)
    }

    /// Open an existing database **read-only** — the #310 sweep path (always a
    /// copy of a run DB, never the live one). Keeps the `user_version` guard;
    /// unlike [`Self::open`], version `0` (fresh/empty) is also rejected because
    /// a read-only connection cannot create the schema.
    pub fn open_readonly(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let found: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found != SCHEMA_VERSION {
            return Err(DbError::SchemaVersionMismatch {
                found,
                expected: SCHEMA_VERSION,
            });
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Stream every `raw_ticks` row in `id` (tape) order through `f` — the #310
    /// sweep's single decode pass. Streaming by design: one prepared statement,
    /// row-at-a-time; the full tape is never materialized.
    pub fn raw_ticks_ordered<F>(&self, mut f: F) -> Result<(), DbError>
    where
        F: FnMut(RawTickRow) -> Result<(), DbError>,
    {
        let conn = self.lock();
        let mut stmt = conn
            .prepare("SELECT id, source, received_ms, payload_json FROM raw_ticks ORDER BY id")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let source_label: String = row.get(1)?;
            let source = FeedSource::try_from(source_label.as_str()).map_err(|_| {
                DbError::Corrupt(format!("unknown raw_ticks.source {source_label:?}"))
            })?;
            f(RawTickRow {
                id: row.get(0)?,
                source,
                received_ms: row.get(2)?,
                payload_json: row.get(3)?,
            })?;
        }
        Ok(())
    }

    /// All markets the capture run registered (boot + delivery-gated refresh
    /// upserts) — the #310 sweep's market universe.
    pub fn all_markets(&self) -> Result<Vec<BtcMarketMeta>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT condition_id, yes_token_id, no_token_id, series,
                    range_start_ms, range_end_ms, tick_str
             FROM markets",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let series_label: String = row.get(3)?;
            let series = BtcSeriesKind::from_str_label(&series_label)
                .ok_or_else(|| DbError::Corrupt(format!("unknown series {series_label:?}")))?;
            let tick_str: String = row.get(6)?;
            let tick = Decimal::from_str(&tick_str)
                .map_err(|_| DbError::Corrupt(format!("bad tick {tick_str:?}")))?;
            out.push(BtcMarketMeta {
                condition_id: row.get(0)?,
                yes_token_id: row.get(1)?,
                no_token_id: row.get(2)?,
                series,
                range_start_ms: row.get(4)?,
                range_end_ms: row.get(5)?,
                tick,
            });
        }
        Ok(out)
    }

    /// `condition_id -> yes_won` for every stored resolution — the #310 buy-hold
    /// scorer's ground truth. Markets without a row are unresolved (unscored).
    pub fn all_resolutions_map(&self) -> Result<HashMap<String, bool>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT condition_id, yes_won FROM resolutions")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? != 0))
        })?;
        let mut out = HashMap::new();
        for row in rows {
            let (condition_id, yes_won) = row?;
            out.insert(condition_id, yes_won);
        }
        Ok(out)
    }

    /// Every `observations` row, fully decoded, in insertion (`id`) order — the
    /// live side of the #310 reference-cell fidelity compare. [`EdgeObservation`]
    /// derives `PartialEq`, so replay rows compare directly.
    pub fn all_observations_full(&self) -> Result<Vec<EdgeObservation>, DbError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT condition_id, series, observed_at_ms, signal_value_str,
                    range_start_str, prob_up_str, best_ask_str, mid_str,
                    no_best_ask_str, no_mid_str,
                    gross_edge_ask_str, gross_edge_mid_str, fee_cost_str,
                    net_edge_ask_str, net_edge_mid_str, feed_to_book_lag_ms,
                    move_magnitude_bps_str, move_direction
             FROM observations ORDER BY id",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let series_label: String = row.get(1)?;
            let series = BtcSeriesKind::from_str_label(&series_label)
                .ok_or_else(|| DbError::Corrupt(format!("unknown series {series_label:?}")))?;
            let signal_str: String = row.get(3)?;
            let signal_value = Decimal::from_str(&signal_str)
                .map_err(|_| DbError::Corrupt(format!("bad signal_value {signal_str:?}")))?;
            let magnitude_str: String = row.get(16)?;
            let move_magnitude_bps = Decimal::from_str(&magnitude_str)
                .map_err(|_| DbError::Corrupt(format!("bad move_magnitude {magnitude_str:?}")))?;
            let direction_label: String = row.get(17)?;
            let move_direction =
                MoveDirection::from_str_label(&direction_label).ok_or_else(|| {
                    DbError::Corrupt(format!("unknown direction {direction_label:?}"))
                })?;
            out.push(EdgeObservation {
                condition_id: row.get(0)?,
                series,
                observed_at_ms: row.get(2)?,
                signal_value,
                range_start_value: parse_opt_decimal(row.get::<_, Option<String>>(4)?.as_deref())?,
                instantaneous_prob_up: parse_opt_decimal(
                    row.get::<_, Option<String>>(5)?.as_deref(),
                )?,
                best_ask: parse_opt_decimal(row.get::<_, Option<String>>(6)?.as_deref())?,
                mid: parse_opt_decimal(row.get::<_, Option<String>>(7)?.as_deref())?,
                no_best_ask: parse_opt_decimal(row.get::<_, Option<String>>(8)?.as_deref())?,
                no_mid: parse_opt_decimal(row.get::<_, Option<String>>(9)?.as_deref())?,
                gross_edge_vs_ask: parse_opt_decimal(row.get::<_, Option<String>>(10)?.as_deref())?,
                gross_edge_vs_mid: parse_opt_decimal(row.get::<_, Option<String>>(11)?.as_deref())?,
                fee_cost: parse_opt_decimal(row.get::<_, Option<String>>(12)?.as_deref())?,
                net_edge_vs_ask: parse_opt_decimal(row.get::<_, Option<String>>(13)?.as_deref())?,
                net_edge_vs_mid: parse_opt_decimal(row.get::<_, Option<String>>(14)?.as_deref())?,
                feed_to_book_lag_ms: row.get(15)?,
                move_magnitude_bps,
                move_direction,
            });
        }
        Ok(out)
    }
}

fn parse_opt_decimal(s: Option<&str>) -> Result<Option<Decimal>, DbError> {
    match s {
        None => Ok(None),
        Some(raw) => Decimal::from_str(raw)
            .map(Some)
            .map_err(|_| DbError::Corrupt(format!("bad decimal {raw:?}"))),
    }
}

/// Series-kind label round-trip helper used by the report layer.
pub fn parse_series_label(s: &str) -> Option<BtcSeriesKind> {
    BtcSeriesKind::from_str_label(s)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::types::BtcSeriesKind;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

    fn sample_obs() -> EdgeObservation {
        EdgeObservation {
            condition_id: "0xcond".to_string(),
            series: BtcSeriesKind::Five,
            observed_at_ms: 1_000,
            signal_value: dec!(60000),
            range_start_value: Some(dec!(59000)),
            instantaneous_prob_up: Some(Decimal::ONE),
            best_ask: Some(dec!(0.52)),
            mid: Some(dec!(0.50)),
            no_best_ask: Some(dec!(0.49)),
            no_mid: Some(dec!(0.47)),
            gross_edge_vs_ask: Some(dec!(0.48)),
            gross_edge_vs_mid: Some(dec!(0.50)),
            fee_cost: Some(dec!(0.017472)),
            net_edge_vs_ask: Some(dec!(0.462528)),
            net_edge_vs_mid: Some(dec!(0.4825)),
            feed_to_book_lag_ms: Some(500),
            move_magnitude_bps: dec!(4),
            move_direction: crate::types::MoveDirection::Up,
        }
    }

    #[test]
    fn open_creates_and_stamps_version() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.db");
        let db = ShadowDb::open(&path).unwrap();
        assert_eq!(db.observation_count().unwrap(), 0);
        assert_eq!(db.raw_tick_count().unwrap(), 0);
    }

    #[test]
    fn observation_roundtrips_through_report_read() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        db.insert_observations(&[sample_obs()]).unwrap();
        assert_eq!(db.observation_count().unwrap(), 1);
        let rows = db.all_observations_for_report().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].series, "5m");
        assert_eq!(rows[0].best_ask, Some(dec!(0.52)));
        assert_eq!(rows[0].net_edge_vs_ask, Some(dec!(0.462528)));
        assert_eq!(rows[0].feed_to_book_lag_ms, Some(500));
    }

    #[test]
    fn realized_rows_carry_no_best_ask() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        db.insert_observations(&[sample_obs()]).unwrap();
        let rows = db.all_realized_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].move_direction, "up");
        assert_eq!(rows[0].best_ask, Some(dec!(0.52)));
        assert_eq!(rows[0].no_best_ask, Some(dec!(0.49)));
        assert_eq!(rows[0].yes_won, None, "no resolution upserted yet");
    }

    #[test]
    fn empty_insert_is_noop() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        db.insert_observations(&[]).unwrap();
        assert_eq!(db.observation_count().unwrap(), 0);
    }

    #[test]
    fn meta_and_raw_tick_persist() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        db.set_meta("fee_provenance", "x").unwrap();
        assert_eq!(db.get_meta("fee_provenance").unwrap().as_deref(), Some("x"));
        db.insert_raw_tick(FeedSource::Chainlink, 1, "{}").unwrap();
        assert_eq!(db.raw_tick_count().unwrap(), 1);
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "user_version", 999_i64).unwrap();
        }
        let result = ShadowDb::open(&path);
        assert!(matches!(
            result,
            Err(DbError::SchemaVersionMismatch { found: 999, .. })
        ));
    }

    fn sample_trade() -> ClobTrade {
        ClobTrade {
            token_id: "0xtok".to_string(),
            condition_id: "0xcond".to_string(),
            price: pe_core_types::Price(dec!(0.78)),
            size: dec!(5.166663),
            taker_is_buy: true,
            traded_at_ms: 1_781_032_143_544,
            fee_rate_bps: 0,
            transaction_hash: "0xdead".to_string(),
        }
    }

    #[test]
    fn clob_trade_dedups_on_transaction_hash() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let t = sample_trade();
        db.insert_clob_trade(&t, 1_781_032_143_600, Some("5m"))
            .unwrap();
        // Same transaction_hash (reconnect replay) -> ignored, not duplicated.
        db.insert_clob_trade(&t, 1_781_032_143_700, Some("5m"))
            .unwrap();
        assert_eq!(db.clob_trade_count().unwrap(), 1);
        // condition_id was taken from the trade's own `market` field.
        let conn = db.lock();
        let cond: String = conn
            .query_row("SELECT condition_id FROM clob_trades", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cond, "0xcond");
    }

    #[test]
    fn frame_batch_persists_both_tables_in_order() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let ticks = vec![
            (FeedSource::Clob, 10_i64, "{\"a\":1}".to_string()),
            (FeedSource::Bybit, 11_i64, "{\"b\":2}".to_string()),
            (FeedSource::Clob, 12_i64, "{\"c\":3}".to_string()),
        ];
        let trades = vec![(sample_trade(), 13_i64, Some("5m"))];
        db.insert_frame_batch(&ticks, &trades).unwrap();
        assert_eq!(db.raw_tick_count().unwrap(), 3);
        assert_eq!(db.clob_trade_count().unwrap(), 1);
        // `raw_ticks.id` is insertion-ordered (single writer; slice order) —
        // the #310 sweep depends on it.
        let conn = db.lock();
        let mut stmt = conn
            .prepare("SELECT received_ms FROM raw_ticks ORDER BY id")
            .unwrap();
        let got: Vec<i64> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(got, vec![10, 11, 12]);
    }

    #[test]
    fn frame_batch_dedups_duplicate_tx_hash_within_one_batch() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut other = sample_trade();
        other.transaction_hash = "0xbeef".to_string();
        // 3 trades, 1 in-batch duplicate hash => 2 rows, no error.
        let trades = vec![
            (sample_trade(), 1_i64, Some("5m")),
            (sample_trade(), 2_i64, Some("5m")),
            (other, 3_i64, None),
        ];
        db.insert_frame_batch(&[], &trades).unwrap();
        assert_eq!(db.clob_trade_count().unwrap(), 2);
    }

    #[test]
    fn empty_frame_batch_is_noop() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        db.insert_frame_batch(&[], &[]).unwrap();
        assert_eq!(db.raw_tick_count().unwrap(), 0);
        assert_eq!(db.clob_trade_count().unwrap(), 0);
    }

    #[test]
    fn schema_v3_rejected_after_bump() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "user_version", 3_i64).unwrap();
        }
        assert!(matches!(
            ShadowDb::open(&path),
            Err(DbError::SchemaVersionMismatch { found: 3, .. })
        ));
    }

    #[test]
    fn full_observation_round_trips_exactly() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut sparse = sample_obs();
        // A row with None book fields (the no-book live case) must round-trip too.
        sparse.best_ask = None;
        sparse.mid = None;
        sparse.fee_cost = None;
        sparse.net_edge_vs_ask = None;
        sparse.feed_to_book_lag_ms = None;
        db.insert_observations(&[sample_obs(), sparse.clone()])
            .unwrap();
        let rows = db.all_observations_full().unwrap();
        assert_eq!(
            rows,
            vec![sample_obs(), sparse],
            "insertion order, exact fields"
        );
    }

    #[test]
    fn markets_and_resolutions_round_trip() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let m = BtcMarketMeta {
            condition_id: "0xc".to_string(),
            yes_token_id: "y".to_string(),
            no_token_id: "n".to_string(),
            series: BtcSeriesKind::Fifteen,
            range_start_ms: 1_000,
            range_end_ms: 901_000,
            tick: dec!(0.01),
        };
        db.upsert_market(&m).unwrap();
        assert_eq!(db.all_markets().unwrap(), vec![m]);

        db.upsert_resolution("0xc", true, 5).unwrap();
        db.upsert_resolution("0xd", false, 6).unwrap();
        let map = db.all_resolutions_map().unwrap();
        assert_eq!(map.get("0xc"), Some(&true));
        assert_eq!(map.get("0xd"), Some(&false));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn raw_ticks_stream_in_tape_order_with_decoded_source() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let ticks = vec![
            (FeedSource::Clob, 10_i64, "a".to_string()),
            (FeedSource::Bybit, 11_i64, "b".to_string()),
            (FeedSource::Chainlink, 12_i64, "c".to_string()),
        ];
        db.insert_frame_batch(&ticks, &[]).unwrap();
        let mut seen = Vec::new();
        db.raw_ticks_ordered(|row| {
            seen.push((row.id, row.source, row.received_ms, row.payload_json));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            seen,
            vec![
                (1, FeedSource::Clob, 10, "a".to_string()),
                (2, FeedSource::Bybit, 11, "b".to_string()),
                (3, FeedSource::Chainlink, 12, "c".to_string()),
            ]
        );
    }

    #[test]
    fn raw_ticks_unknown_source_is_corrupt() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        {
            let conn = db.lock();
            conn.execute(
                "INSERT INTO raw_ticks (source, received_ms, payload_json) VALUES ('binance', 1, '{}')",
                [],
            )
            .unwrap();
        }
        let result = db.raw_ticks_ordered(|_| Ok(()));
        assert!(matches!(result, Err(DbError::Corrupt(_))));
    }

    #[test]
    fn open_readonly_reads_but_rejects_writes_and_fresh_dbs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.db");
        {
            let db = ShadowDb::open(&path).unwrap();
            db.insert_observations(&[sample_obs()]).unwrap();
        }
        let ro = ShadowDb::open_readonly(&path).unwrap();
        assert_eq!(ro.all_observations_full().unwrap().len(), 1);
        assert!(
            ro.set_meta("k", "v").is_err(),
            "read-only handle must reject writes"
        );

        // A fresh (version-0) file is rejected: read-only cannot create schema.
        let empty = dir.path().join("empty.db");
        {
            let conn = Connection::open(&empty).unwrap();
            conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
        }
        assert!(matches!(
            ShadowDb::open_readonly(&empty),
            Err(DbError::SchemaVersionMismatch { found: 0, .. })
        ));
    }
}
