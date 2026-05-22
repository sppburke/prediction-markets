//! `wallet_features` persistence: the per-wallet feature vector computed at a
//! given train/forward cutoff, stored in the same `wallet_cache.db` file the
//! bootstrap pipeline owns.
//!
//! Storage conventions follow the bootstrap cache (`crates/bootstrap/src/cache.rs`):
//! money is TEXT via `Decimal::to_string()` (no `f64` round-trip), ratios are
//! integer basis points, and `INSERT OR REPLACE` keyed on `wallet_hex` makes
//! re-extraction idempotent. A row is scoped to the `cutoff_unix` it was
//! computed at, so re-running at a different cutoff replaces in place and
//! [`SkillCache::load_features_for_cutoff`] selects one population.

use std::path::Path;
use std::str::FromStr;

use rusqlite::{Connection, OpenFlags, params};
use rust_decimal::Decimal;

use crate::error::SkillSelectError;

/// `wallet_features` schema. Created by [`SkillCache::open`]; the read-only
/// opener never runs DDL. Later columns are added with the
/// `add_column_if_missing` pattern from `cache.rs` rather than by editing this.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS wallet_features (
    wallet_hex              TEXT    PRIMARY KEY NOT NULL,
    cutoff_unix             INTEGER NOT NULL,
    extracted_at_unix       INTEGER NOT NULL,
    reconstruction_quality  INTEGER NOT NULL,
    closed_trades           INTEGER NOT NULL,
    distinct_markets        INTEGER NOT NULL,
    distinct_events         INTEGER NOT NULL,
    total_pnl_usd_str       TEXT    NOT NULL,
    roi_bps                 INTEGER NOT NULL,
    win_rate_bps            INTEGER NOT NULL,
    lcb_5pct_bps            INTEGER NOT NULL,
    sharpe_bps              INTEGER NOT NULL,
    skewness_bps            INTEGER NOT NULL,
    excess_kurtosis_bps     INTEGER NOT NULL,
    compound_return_bps     INTEGER NOT NULL,
    buy_hold_return_bps     INTEGER NOT NULL,
    calibration_bps         INTEGER NOT NULL,
    avg_hold_secs           INTEGER NOT NULL,
    skill_pnl_usd_str       TEXT    NOT NULL,
    skill_pvalue_bps        INTEGER NOT NULL,
    skill_permutations      INTEGER NOT NULL,
    deflated_sharpe_bps     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_features_cutoff ON wallet_features(cutoff_unix);
CREATE INDEX IF NOT EXISTS idx_wallet_features_skill ON wallet_features(skill_pvalue_bps, deflated_sharpe_bps);
";

/// One wallet's feature vector at a given cutoff. Money fields are exact
/// (`Decimal`); ratios/returns are integer basis points (× 10_000); the rest
/// are counts/seconds. Populated by the (later) extraction phase; this slice
/// only persists and reloads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletFeatures {
    /// `0x`-prefixed lowercase wallet address (the `trades.wallet_hex` form).
    pub wallet_hex: String,
    /// Train/forward split this row was computed at (unix seconds, UTC).
    pub cutoff_unix: i64,
    /// When this row was written (unix seconds, UTC).
    pub extracted_at_unix: i64,
    /// FIFO reconstruction quality, 0..=100.
    pub reconstruction_quality: u8,
    /// Closed trades in the train window.
    pub closed_trades: u32,
    /// Distinct traded markets in the train window.
    pub distinct_markets: u32,
    /// Distinct events (neg-risk bundles) in the train window.
    pub distinct_events: u32,
    /// Sum of realized PnL over the train window (USD).
    pub total_pnl_usd: Decimal,
    /// Return on cost, basis points.
    pub roi_bps: i64,
    /// Win rate, basis points.
    pub win_rate_bps: i32,
    /// Lower-confidence-bound (5th pct) daily return, basis points.
    pub lcb_5pct_bps: i32,
    /// Sharpe ratio × 10_000.
    pub sharpe_bps: i64,
    /// Fisher skewness of daily returns × 10_000.
    pub skewness_bps: i64,
    /// Excess kurtosis of daily returns × 10_000.
    pub excess_kurtosis_bps: i64,
    /// Compounded equity return, basis points.
    pub compound_return_bps: i64,
    /// Buy-and-hold-to-resolution benchmark return, basis points.
    pub buy_hold_return_bps: i64,
    /// Calibration score (mean |bucket win-rate − entry price|), basis points.
    pub calibration_bps: i64,
    /// Average hold duration, seconds.
    pub avg_hold_secs: i64,
    /// Observed PnL used as the sign-randomization skill statistic (USD).
    pub skill_pnl_usd: Decimal,
    /// Sign-randomization permutation p-value × 10_000.
    pub skill_pvalue_bps: u32,
    /// Permutation count used for the skill test.
    pub skill_permutations: u32,
    /// Deflated Sharpe Ratio t-stat × 10_000.
    pub deflated_sharpe_bps: i64,
}

/// Read/write accessor for the `wallet_features` table in `wallet_cache.db`.
///
/// Holds its own SQLite connection, independent of the bootstrap `WalletCache`:
/// the read pass opens [`SkillCache::open_read_only`]; only the extraction
/// write pass opens [`SkillCache::open`] (the sole DDL path).
pub struct SkillCache {
    conn: Connection,
}

impl SkillCache {
    /// Open (creating if absent) and ensure the `wallet_features` schema. Use
    /// this for the write/extraction path.
    pub fn open(path: &Path) -> Result<Self, SkillSelectError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Open **read-only**, skipping DDL. Use for the selection read path.
    ///
    /// # Precondition
    /// The database at `path` must already exist with the `wallet_features`
    /// table present (created by a prior [`Self::open`]); a nonexistent path or
    /// missing table errors rather than being created.
    pub fn open_read_only(path: &Path) -> Result<Self, SkillSelectError> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self { conn })
    }

    /// Upsert a batch of feature rows in one transaction. `INSERT OR REPLACE`
    /// on `wallet_hex` — re-extraction is idempotent. Returns the row count.
    pub fn upsert_features_batch(
        &mut self,
        rows: &[WalletFeatures],
    ) -> Result<usize, SkillSelectError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO wallet_features (\
                    wallet_hex, cutoff_unix, extracted_at_unix, reconstruction_quality, \
                    closed_trades, distinct_markets, distinct_events, total_pnl_usd_str, \
                    roi_bps, win_rate_bps, lcb_5pct_bps, sharpe_bps, skewness_bps, \
                    excess_kurtosis_bps, compound_return_bps, buy_hold_return_bps, \
                    calibration_bps, avg_hold_secs, skill_pnl_usd_str, skill_pvalue_bps, \
                    skill_permutations, deflated_sharpe_bps\
                 ) VALUES (\
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                    ?16, ?17, ?18, ?19, ?20, ?21, ?22\
                 )",
            )?;
            for f in rows {
                stmt.execute(params![
                    f.wallet_hex,
                    f.cutoff_unix,
                    f.extracted_at_unix,
                    i64::from(f.reconstruction_quality),
                    i64::from(f.closed_trades),
                    i64::from(f.distinct_markets),
                    i64::from(f.distinct_events),
                    f.total_pnl_usd.to_string(),
                    f.roi_bps,
                    f.win_rate_bps,
                    f.lcb_5pct_bps,
                    f.sharpe_bps,
                    f.skewness_bps,
                    f.excess_kurtosis_bps,
                    f.compound_return_bps,
                    f.buy_hold_return_bps,
                    f.calibration_bps,
                    f.avg_hold_secs,
                    f.skill_pnl_usd.to_string(),
                    i64::from(f.skill_pvalue_bps),
                    i64::from(f.skill_permutations),
                    f.deflated_sharpe_bps,
                ])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    /// Load all feature rows for a given cutoff, ordered by `wallet_hex`.
    pub fn load_features_for_cutoff(
        &self,
        cutoff_unix: i64,
    ) -> Result<Vec<WalletFeatures>, SkillSelectError> {
        let mut stmt = self.conn.prepare(
            "SELECT wallet_hex, cutoff_unix, extracted_at_unix, reconstruction_quality, \
                    closed_trades, distinct_markets, distinct_events, total_pnl_usd_str, \
                    roi_bps, win_rate_bps, lcb_5pct_bps, sharpe_bps, skewness_bps, \
                    excess_kurtosis_bps, compound_return_bps, buy_hold_return_bps, \
                    calibration_bps, avg_hold_secs, skill_pnl_usd_str, skill_pvalue_bps, \
                    skill_permutations, deflated_sharpe_bps \
             FROM wallet_features WHERE cutoff_unix = ?1 ORDER BY wallet_hex",
        )?;
        let rows = stmt.query_map(params![cutoff_unix], |r| {
            // rusqlite's row closure must yield rusqlite::Error; decode typed
            // forms after collection so we can map into SkillSelectError::Decode.
            Ok(RawRow {
                wallet_hex: r.get(0)?,
                cutoff_unix: r.get(1)?,
                extracted_at_unix: r.get(2)?,
                reconstruction_quality: r.get(3)?,
                closed_trades: r.get(4)?,
                distinct_markets: r.get(5)?,
                distinct_events: r.get(6)?,
                total_pnl_usd_str: r.get(7)?,
                roi_bps: r.get(8)?,
                win_rate_bps: r.get(9)?,
                lcb_5pct_bps: r.get(10)?,
                sharpe_bps: r.get(11)?,
                skewness_bps: r.get(12)?,
                excess_kurtosis_bps: r.get(13)?,
                compound_return_bps: r.get(14)?,
                buy_hold_return_bps: r.get(15)?,
                calibration_bps: r.get(16)?,
                avg_hold_secs: r.get(17)?,
                skill_pnl_usd_str: r.get(18)?,
                skill_pvalue_bps: r.get(19)?,
                skill_permutations: r.get(20)?,
                deflated_sharpe_bps: r.get(21)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?.decode()?);
        }
        Ok(out)
    }
}

/// Raw SQLite row (pre-decode). Integers come back as `i64`; narrowing and
/// `Decimal` parsing happen in [`RawRow::decode`] so failures surface as
/// [`SkillSelectError::Decode`] rather than being lost in the row closure.
struct RawRow {
    wallet_hex: String,
    cutoff_unix: i64,
    extracted_at_unix: i64,
    reconstruction_quality: i64,
    closed_trades: i64,
    distinct_markets: i64,
    distinct_events: i64,
    total_pnl_usd_str: String,
    roi_bps: i64,
    win_rate_bps: i64,
    lcb_5pct_bps: i64,
    sharpe_bps: i64,
    skewness_bps: i64,
    excess_kurtosis_bps: i64,
    compound_return_bps: i64,
    buy_hold_return_bps: i64,
    calibration_bps: i64,
    avg_hold_secs: i64,
    skill_pnl_usd_str: String,
    skill_pvalue_bps: i64,
    skill_permutations: i64,
    deflated_sharpe_bps: i64,
}

impl RawRow {
    fn decode(self) -> Result<WalletFeatures, SkillSelectError> {
        let decimal = |s: &str| {
            Decimal::from_str(s)
                .map_err(|e| SkillSelectError::Decode(format!("decimal '{s}': {e}")))
        };
        let narrow_u8 = |v: i64| {
            u8::try_from(v).map_err(|_| SkillSelectError::Decode(format!("u8 out of range: {v}")))
        };
        let narrow_u32 = |v: i64| {
            u32::try_from(v).map_err(|_| SkillSelectError::Decode(format!("u32 out of range: {v}")))
        };
        let narrow_i32 = |v: i64| {
            i32::try_from(v).map_err(|_| SkillSelectError::Decode(format!("i32 out of range: {v}")))
        };
        Ok(WalletFeatures {
            wallet_hex: self.wallet_hex,
            cutoff_unix: self.cutoff_unix,
            extracted_at_unix: self.extracted_at_unix,
            reconstruction_quality: narrow_u8(self.reconstruction_quality)?,
            closed_trades: narrow_u32(self.closed_trades)?,
            distinct_markets: narrow_u32(self.distinct_markets)?,
            distinct_events: narrow_u32(self.distinct_events)?,
            total_pnl_usd: decimal(&self.total_pnl_usd_str)?,
            roi_bps: self.roi_bps,
            win_rate_bps: narrow_i32(self.win_rate_bps)?,
            lcb_5pct_bps: narrow_i32(self.lcb_5pct_bps)?,
            sharpe_bps: self.sharpe_bps,
            skewness_bps: self.skewness_bps,
            excess_kurtosis_bps: self.excess_kurtosis_bps,
            compound_return_bps: self.compound_return_bps,
            buy_hold_return_bps: self.buy_hold_return_bps,
            calibration_bps: self.calibration_bps,
            avg_hold_secs: self.avg_hold_secs,
            skill_pnl_usd: decimal(&self.skill_pnl_usd_str)?,
            skill_pvalue_bps: narrow_u32(self.skill_pvalue_bps)?,
            skill_permutations: narrow_u32(self.skill_permutations)?,
            deflated_sharpe_bps: self.deflated_sharpe_bps,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tempfile::TempDir;

    fn sample(hex: &str, cutoff: i64, pnl: Decimal) -> WalletFeatures {
        WalletFeatures {
            wallet_hex: hex.to_owned(),
            cutoff_unix: cutoff,
            extracted_at_unix: 1_700_000_000,
            reconstruction_quality: 100,
            closed_trades: 42,
            distinct_markets: 30,
            distinct_events: 18,
            total_pnl_usd: pnl,
            roi_bps: 1_234,
            win_rate_bps: 6_700,
            lcb_5pct_bps: -250,
            sharpe_bps: 14_142,
            skewness_bps: -500,
            excess_kurtosis_bps: 9_000,
            compound_return_bps: 2_500,
            buy_hold_return_bps: 800,
            calibration_bps: 320,
            avg_hold_secs: 86_400,
            skill_pnl_usd: pnl,
            skill_pvalue_bps: 75,
            skill_permutations: 999,
            deflated_sharpe_bps: 3_300,
        }
    }

    #[test]
    fn upsert_then_load_round_trips_all_fields() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = SkillCache::open(&path).unwrap();

        let a = sample("0xaaaa", 1_743_465_599, dec!(1234.56));
        let b = sample("0xbbbb", 1_743_465_599, dec!(-78.90));
        let other_cutoff = sample("0xcccc", 1_700_000_000, dec!(5.0));
        let n = cache
            .upsert_features_batch(&[a.clone(), b.clone(), other_cutoff])
            .unwrap();
        assert_eq!(n, 3);

        let loaded = cache.load_features_for_cutoff(1_743_465_599).unwrap();
        assert_eq!(
            loaded,
            vec![a, b],
            "round-trip must preserve every field and scope by cutoff"
        );
    }

    #[test]
    fn upsert_is_idempotent_on_wallet_hex() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = SkillCache::open(&path).unwrap();

        cache
            .upsert_features_batch(&[sample("0xaaaa", 100, dec!(1.0))])
            .unwrap();
        cache
            .upsert_features_batch(&[sample("0xaaaa", 100, dec!(2.0))])
            .unwrap();

        let loaded = cache.load_features_for_cutoff(100).unwrap();
        assert_eq!(loaded.len(), 1, "re-upsert replaces, no duplicate row");
        assert_eq!(loaded[0].total_pnl_usd, dec!(2.0));
    }

    #[test]
    fn read_only_open_sees_committed_rows() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        {
            let mut cache = SkillCache::open(&path).unwrap();
            cache
                .upsert_features_batch(&[sample("0xaaaa", 100, dec!(9.99))])
                .unwrap();
        }
        let ro = SkillCache::open_read_only(&path).unwrap();
        let loaded = ro.load_features_for_cutoff(100).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].total_pnl_usd, dec!(9.99));
    }
}
