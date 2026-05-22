//! `wallet_features` persistence: the per-wallet feature vector computed at a
//! given train/forward cutoff, stored in the same `wallet_cache.db` file the
//! bootstrap pipeline owns.
//!
//! [`WalletFeatures`] embeds the [`DeterministicFeatures`] batch
//! ([`crate::features`]) plus the extraction timestamp and the sign-randomization
//! skill-test outputs ([`crate::skill_test`]). The Deflated Sharpe is *not*
//! persisted — it is a selection-time derivation (it depends on the
//! cross-section size `m`), computed in [`crate::selection`] from the stored
//! `sharpe_bps`. Compounding / buy-and-hold / calibration features are deferred
//! to a later slice and gain their columns then (via `add_column_if_missing`).
//!
//! Storage conventions follow `crates/bootstrap/src/cache.rs`: money is TEXT via
//! `Decimal::to_string()` (no `f64`), ratios are integer basis points, and
//! `INSERT OR REPLACE` on the composite `(cutoff_unix, wallet_hex)` key makes
//! re-extraction idempotent while letting a wallet coexist across cutoffs.

use std::path::Path;
use std::str::FromStr;

use rusqlite::{Connection, OpenFlags, params};
use rust_decimal::Decimal;

use crate::error::SkillSelectError;
use crate::features::DeterministicFeatures;

/// `wallet_features` schema. Created by [`SkillCache::open`]; the read-only
/// opener never runs DDL. Later columns are added with the
/// `add_column_if_missing` pattern from `cache.rs` rather than by editing this.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS wallet_features (
    wallet_hex              TEXT    NOT NULL,
    cutoff_unix             INTEGER NOT NULL,
    extracted_at_unix       INTEGER NOT NULL,
    reconstruction_quality  INTEGER NOT NULL,
    closed_trades           INTEGER NOT NULL,
    distinct_markets        INTEGER NOT NULL,
    distinct_events         INTEGER NOT NULL,
    total_pnl_usd_str       TEXT    NOT NULL,
    roi_bps                 INTEGER NOT NULL,
    win_rate_bps            INTEGER NOT NULL,
    avg_hold_secs           INTEGER NOT NULL,
    trading_days            INTEGER NOT NULL,
    mean_daily_return_bps   INTEGER NOT NULL,
    std_daily_return_bps    INTEGER NOT NULL,
    sharpe_bps              INTEGER NOT NULL,
    skewness_bps            INTEGER NOT NULL,
    excess_kurtosis_bps     INTEGER NOT NULL,
    lcb_5pct_bps            INTEGER NOT NULL,
    skill_pnl_usd_str       TEXT    NOT NULL,
    skill_pvalue_bps        INTEGER NOT NULL,
    skill_permutations      INTEGER NOT NULL,
    PRIMARY KEY (cutoff_unix, wallet_hex)
);
CREATE INDEX IF NOT EXISTS idx_wallet_features_cutoff ON wallet_features(cutoff_unix);
CREATE INDEX IF NOT EXISTS idx_wallet_features_skill ON wallet_features(skill_pvalue_bps, sharpe_bps);
";

/// One persisted wallet feature row: the deterministic feature batch plus
/// extraction metadata and the sign-randomization skill-test outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletFeatures {
    /// Deterministic per-wallet features (counts, PnL, ROI, win rate, moments).
    pub features: DeterministicFeatures,
    /// When this row was written (unix seconds, UTC).
    pub extracted_at_unix: i64,
    /// Observed total PnL used as the skill-test statistic (USD).
    pub skill_pnl_usd: Decimal,
    /// Sign-randomization permutation p-value × 10_000.
    pub skill_pvalue_bps: u32,
    /// Permutation count used for the skill test.
    pub skill_permutations: u32,
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
    /// on the `(cutoff_unix, wallet_hex)` key — re-extraction at the same cutoff
    /// is idempotent, and a wallet may hold rows at multiple cutoffs. Returns the
    /// row count.
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
                    roi_bps, win_rate_bps, avg_hold_secs, trading_days, \
                    mean_daily_return_bps, std_daily_return_bps, sharpe_bps, skewness_bps, \
                    excess_kurtosis_bps, lcb_5pct_bps, skill_pnl_usd_str, skill_pvalue_bps, \
                    skill_permutations\
                 ) VALUES (\
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                    ?16, ?17, ?18, ?19, ?20, ?21\
                 )",
            )?;
            for w in rows {
                let f = &w.features;
                stmt.execute(params![
                    f.wallet_hex,
                    f.cutoff_unix,
                    w.extracted_at_unix,
                    i64::from(f.reconstruction_quality),
                    i64::from(f.closed_trades),
                    i64::from(f.distinct_markets),
                    i64::from(f.distinct_events),
                    f.total_pnl_usd.to_string(),
                    f.roi_bps,
                    f.win_rate_bps,
                    f.avg_hold_secs,
                    i64::from(f.trading_days),
                    f.mean_daily_return_bps,
                    f.std_daily_return_bps,
                    f.sharpe_bps,
                    f.skewness_bps,
                    f.excess_kurtosis_bps,
                    f.lcb_5pct_bps,
                    w.skill_pnl_usd.to_string(),
                    i64::from(w.skill_pvalue_bps),
                    i64::from(w.skill_permutations),
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
                    roi_bps, win_rate_bps, avg_hold_secs, trading_days, \
                    mean_daily_return_bps, std_daily_return_bps, sharpe_bps, skewness_bps, \
                    excess_kurtosis_bps, lcb_5pct_bps, skill_pnl_usd_str, skill_pvalue_bps, \
                    skill_permutations \
             FROM wallet_features WHERE cutoff_unix = ?1 ORDER BY wallet_hex",
        )?;
        let rows = stmt.query_map(params![cutoff_unix], |r| {
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
                avg_hold_secs: r.get(10)?,
                trading_days: r.get(11)?,
                mean_daily_return_bps: r.get(12)?,
                std_daily_return_bps: r.get(13)?,
                sharpe_bps: r.get(14)?,
                skewness_bps: r.get(15)?,
                excess_kurtosis_bps: r.get(16)?,
                lcb_5pct_bps: r.get(17)?,
                skill_pnl_usd_str: r.get(18)?,
                skill_pvalue_bps: r.get(19)?,
                skill_permutations: r.get(20)?,
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
    avg_hold_secs: i64,
    trading_days: i64,
    mean_daily_return_bps: i64,
    std_daily_return_bps: i64,
    sharpe_bps: i64,
    skewness_bps: i64,
    excess_kurtosis_bps: i64,
    lcb_5pct_bps: i64,
    skill_pnl_usd_str: String,
    skill_pvalue_bps: i64,
    skill_permutations: i64,
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
        let features = DeterministicFeatures {
            wallet_hex: self.wallet_hex,
            cutoff_unix: self.cutoff_unix,
            reconstruction_quality: narrow_u8(self.reconstruction_quality)?,
            closed_trades: narrow_u32(self.closed_trades)?,
            distinct_markets: narrow_u32(self.distinct_markets)?,
            distinct_events: narrow_u32(self.distinct_events)?,
            total_pnl_usd: decimal(&self.total_pnl_usd_str)?,
            roi_bps: self.roi_bps,
            win_rate_bps: narrow_i32(self.win_rate_bps)?,
            avg_hold_secs: self.avg_hold_secs,
            trading_days: narrow_u32(self.trading_days)?,
            mean_daily_return_bps: self.mean_daily_return_bps,
            std_daily_return_bps: self.std_daily_return_bps,
            sharpe_bps: self.sharpe_bps,
            skewness_bps: self.skewness_bps,
            excess_kurtosis_bps: self.excess_kurtosis_bps,
            lcb_5pct_bps: narrow_i32(self.lcb_5pct_bps)?,
        };
        Ok(WalletFeatures {
            features,
            extracted_at_unix: self.extracted_at_unix,
            skill_pnl_usd: decimal(&self.skill_pnl_usd_str)?,
            skill_pvalue_bps: narrow_u32(self.skill_pvalue_bps)?,
            skill_permutations: narrow_u32(self.skill_permutations)?,
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
            features: DeterministicFeatures {
                wallet_hex: hex.to_owned(),
                cutoff_unix: cutoff,
                reconstruction_quality: 100,
                closed_trades: 42,
                distinct_markets: 30,
                distinct_events: 18,
                total_pnl_usd: pnl,
                roi_bps: 1_234,
                win_rate_bps: 6_700,
                avg_hold_secs: 86_400,
                trading_days: 12,
                mean_daily_return_bps: 500,
                std_daily_return_bps: 800,
                sharpe_bps: 6_250,
                skewness_bps: -500,
                excess_kurtosis_bps: 9_000,
                lcb_5pct_bps: -250,
            },
            extracted_at_unix: 1_700_000_000,
            skill_pnl_usd: pnl,
            skill_pvalue_bps: 75,
            skill_permutations: 999,
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
        assert_eq!(loaded[0].features.total_pnl_usd, dec!(2.0));
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
        assert_eq!(loaded[0].features.total_pnl_usd, dec!(9.99));
    }

    #[test]
    fn same_wallet_at_two_cutoffs_coexists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallet_cache.db");
        let mut cache = SkillCache::open(&path).unwrap();

        cache
            .upsert_features_batch(&[
                sample("0xaaaa", 1_700_000_000, dec!(1.0)),
                sample("0xaaaa", 1_743_465_599, dec!(2.0)),
            ])
            .unwrap();

        let early = cache.load_features_for_cutoff(1_700_000_000).unwrap();
        let late = cache.load_features_for_cutoff(1_743_465_599).unwrap();
        assert_eq!(early.len(), 1);
        assert_eq!(late.len(), 1);
        assert_eq!(early[0].features.total_pnl_usd, dec!(1.0));
        assert_eq!(late[0].features.total_pnl_usd, dec!(2.0));
    }
}
