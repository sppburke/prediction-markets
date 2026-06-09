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

use crate::types::{BtcMarketMeta, BtcSeriesKind, EdgeObservation, FeedSource};

/// On-disk schema version, stamped into `PRAGMA user_version` on create.
/// Currently `3`. No migration is provided because no rows predate any of these
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
pub const SCHEMA_VERSION: i64 = 3;

/// Provenance value stamped into `meta["lag_clock"]` so a recompute can tell
/// which clock basis `feed_to_book_lag_ms` was computed under. The column is
/// re-meaninged within `v1` (source-timestamp skew → receive-node-clock skew,
/// issue #300 fix 5); no migration is needed because no observation rows
/// predate the change.
pub const LAG_CLOCK: &str = "received_node_v2";

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
}
