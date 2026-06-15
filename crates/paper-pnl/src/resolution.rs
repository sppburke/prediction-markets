//! Resolution store: the double-credit guard for market settlement.
//!
//! Tracks which markets have been settled (their resolution prices applied to the
//! bankroll) so a still-closed market is never re-credited across restarts. As of
//! issue #343 step 0 the **authoritative** durable set lives in `paper_state.db`
//! (`settled_markets`); the in-memory map is hydrated from SQLite on load and
//! `is_settled` reads it. The legacy `paper_resolutions.json` sidecar is still
//! written in parallel (a rollback guard so the prior JSON-reading binary keeps a
//! current settled-set) and back-filled into SQLite on first load — both are
//! removed once the SQLite store has baked one release (PR4).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pe_core_types::{MarketId, VenueMarketId};
use pe_paper_state::{PaperStateDb, PaperStateError};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// One settled market record in the JSON sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledMarket {
    pub market_id: String,
    /// Resolution price per outcome_id index.
    pub outcome_prices: Vec<String>,
    /// Net bankroll credit applied (`long - short` × resolution_price sum over all outcomes).
    pub credit_applied: String,
    pub settled_at_unix: i64,
}

/// Sidecar file structure.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Sidecar {
    settlements: Vec<SettledMarket>,
}

#[derive(Debug)]
struct Entry {
    outcome_prices: Vec<Decimal>,
    credit_applied: Decimal,
    settled_at_unix: i64,
}

/// Per-market settlement detail, cloned out of the private [`Entry`] for callers
/// that need to value or mark settled positions (the dashboard valuation path).
#[derive(Debug, Clone)]
pub struct SettlementInfo {
    /// Resolution price per `outcome_id` index (YES-wins ≈ `[1, 0]`).
    pub outcome_prices: Vec<Decimal>,
    /// Net bankroll credit applied at settlement (clamped ≥ 0).
    pub credit_applied: Decimal,
    pub settled_at_unix: i64,
}

/// Resolution store: an in-memory settled-markets map backed by the durable
/// `settled_markets` SQLite table (authoritative) plus the legacy JSON sidecar
/// (rollback dual-write). Hydrated from SQLite on [`ResolutionStore::load`].
pub struct ResolutionStore {
    settled: HashMap<MarketId, Entry>,
    sidecar_path: PathBuf,
    total_credits: Decimal,
    /// Durable settled-set backing store; written by `mark_settled`, read by `load`.
    db: Arc<PaperStateDb>,
}

impl std::fmt::Debug for ResolutionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `db` (Arc<PaperStateDb>) is not Debug; surface only the derived state.
        f.debug_struct("ResolutionStore")
            .field("settled_count", &self.settled.len())
            .field("sidecar_path", &self.sidecar_path)
            .field("total_credits", &self.total_credits)
            .finish_non_exhaustive()
    }
}

impl ResolutionStore {
    /// Load the settled-set from `db`'s `settled_markets` table, after a one-time
    /// back-fill of any market that exists only in the legacy JSON sidecar at `path`.
    ///
    /// The back-fill is the PR1-cutover correctness fix: on the first startup after
    /// step 0 the `settled_markets` table is empty while the live JSON holds the real
    /// set, and settled markets keep their `positions` rows — so hydrating `is_settled`
    /// from an empty SQLite set would re-credit every already-settled market. Copying
    /// JSON→SQLite first (idempotent) keeps the guard non-empty at cutover. Once SQLite
    /// has caught up the back-fill is a no-op.
    ///
    /// Creates an empty store if neither the JSON sidecar nor any SQLite row exists.
    pub fn load(db: Arc<PaperStateDb>, path: &Path) -> Result<Self, ResolutionStoreError> {
        // Read the legacy JSON sidecar (still the rollback-safe write target in PR1).
        let sidecar: Sidecar = if path.exists() {
            let bytes = std::fs::read(path)?;
            serde_json::from_slice(&bytes)?
        } else {
            Sidecar::default()
        };

        // One-time JSON→SQLite back-fill (idempotent on `market_id`).
        for rec in &sidecar.settlements {
            let mid = MarketId(VenueMarketId(rec.market_id.clone()));
            let prices_json = serde_json::to_string(&rec.outcome_prices)?;
            let credit: Decimal = rec.credit_applied.parse().unwrap_or(Decimal::ZERO);
            db.record_settled_market(&mid, &prices_json, credit, rec.settled_at_unix)?;
        }

        // Hydrate the in-memory guard from SQLite (authoritative post-back-fill).
        let mut settled = HashMap::new();
        let mut total_credits = Decimal::ZERO;
        for row in db.list_settled_markets()? {
            let prices: Vec<Decimal> =
                serde_json::from_str::<Vec<String>>(&row.outcome_prices_json)?
                    .iter()
                    .map(|s| s.parse().unwrap_or(Decimal::ZERO))
                    .collect();
            total_credits = total_credits
                .checked_add(row.credit_applied)
                .unwrap_or(total_credits);
            settled.insert(
                row.market_id,
                Entry {
                    outcome_prices: prices,
                    credit_applied: row.credit_applied,
                    settled_at_unix: row.settled_at_unix,
                },
            );
        }

        Ok(Self {
            settled,
            sidecar_path: path.to_owned(),
            total_credits,
            db,
        })
    }

    /// Whether `market_id` has already been settled.
    pub fn is_settled(&self, market_id: &MarketId) -> bool {
        self.settled.contains_key(market_id)
    }

    /// Record a settlement: write the authoritative SQLite row, the in-memory map, and
    /// the legacy JSON sidecar (rollback dual-write). Idempotent — a market already in
    /// the in-memory set returns early without rewriting.
    ///
    /// The SQLite write lands before this returns, so a caller that credits the bankroll
    /// *after* `mark_settled` (the resolution tick does) under-credits — not double-credits —
    /// if it crashes in between: the market is already marked settled.
    pub fn mark_settled(
        &mut self,
        market_id: MarketId,
        outcome_prices: Vec<Decimal>,
        credit_applied: Decimal,
        settled_at_unix: i64,
    ) -> Result<(), ResolutionStoreError> {
        if self.settled.contains_key(&market_id) {
            return Ok(());
        }
        self.total_credits = self
            .total_credits
            .checked_add(credit_applied)
            .unwrap_or(self.total_credits);

        // Authoritative durable guard: write SQLite first (read back by `load` on restart).
        let prices_json = serde_json::to_string(
            &outcome_prices
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>(),
        )?;
        self.db
            .record_settled_market(&market_id, &prices_json, credit_applied, settled_at_unix)?;

        self.settled.insert(
            market_id,
            Entry {
                outcome_prices,
                credit_applied,
                settled_at_unix,
            },
        );

        // Rollback dual-write: keep the legacy JSON sidecar current for one release so a
        // rollback to the prior JSON-reading binary still sees the settled-set (PR4 drops this).
        // Rebuild from the in-memory map and persist atomically.
        let sidecar = Sidecar {
            settlements: self
                .settled
                .iter()
                .map(|(mid, e)| SettledMarket {
                    market_id: mid.to_string(),
                    outcome_prices: e.outcome_prices.iter().map(|d| d.to_string()).collect(),
                    credit_applied: e.credit_applied.to_string(),
                    settled_at_unix: e.settled_at_unix,
                })
                .collect(),
        };
        let bytes = serde_json::to_vec_pretty(&sidecar)?;
        let tmp = self.sidecar_path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &self.sidecar_path)?;
        Ok(())
    }

    /// Sum of all credits applied to the bankroll by settled markets.
    pub fn total_credits(&self) -> Decimal {
        self.total_credits
    }

    /// Count of settled markets.
    pub fn settled_count(&self) -> usize {
        self.settled.len()
    }

    /// Per-market settlement detail, or `None` if the market is not settled.
    pub fn settlement_info(&self, market_id: &MarketId) -> Option<SettlementInfo> {
        self.settled.get(market_id).map(|e| SettlementInfo {
            outcome_prices: e.outcome_prices.clone(),
            credit_applied: e.credit_applied,
            settled_at_unix: e.settled_at_unix,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolutionStoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("paper-state: {0}")]
    PaperState(#[from] PaperStateError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    /// A temp dir plus a SQLite-backed paper-state DB for the settled-set.
    fn db_and_dir() -> (tempfile::TempDir, Arc<PaperStateDb>) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
        (dir, db)
    }

    #[test]
    fn empty_on_missing_file() {
        let (dir, db) = db_and_dir();
        let store = ResolutionStore::load(db, &dir.path().join("res.json")).unwrap();
        assert_eq!(store.settled_count(), 0);
        assert_eq!(store.total_credits(), Decimal::ZERO);
    }

    #[test]
    fn mark_settled_persists_and_accumulates() {
        let (dir, db) = db_and_dir();
        let path = dir.path().join("res.json");
        let mut store = ResolutionStore::load(db.clone(), &path).unwrap();
        store
            .mark_settled(
                mid("0xcond1"),
                vec![dec!(1), dec!(0)],
                dec!(5.50),
                1_700_000_000,
            )
            .unwrap();
        assert!(store.is_settled(&mid("0xcond1")));
        assert_eq!(store.total_credits(), dec!(5.50));

        // Reload (same DB) and verify persistence via SQLite hydration.
        let store2 = ResolutionStore::load(db, &path).unwrap();
        assert!(store2.is_settled(&mid("0xcond1")));
        assert_eq!(store2.total_credits(), dec!(5.50));
    }

    #[test]
    fn settlement_info_exposes_detail_after_mark_settled() {
        let (dir, db) = db_and_dir();
        let path = dir.path().join("res.json");
        let mut store = ResolutionStore::load(db, &path).unwrap();
        assert!(store.settlement_info(&mid("0xcond1")).is_none());
        store
            .mark_settled(
                mid("0xcond1"),
                vec![dec!(1), dec!(0)],
                dec!(7.25),
                1_700_000_000,
            )
            .unwrap();
        let info = store.settlement_info(&mid("0xcond1")).unwrap();
        assert_eq!(info.outcome_prices, vec![dec!(1), dec!(0)]);
        assert_eq!(info.credit_applied, dec!(7.25));
        assert_eq!(info.settled_at_unix, 1_700_000_000);
        // Unsettled market still returns None.
        assert!(store.settlement_info(&mid("0xother")).is_none());
    }

    #[test]
    fn mark_settled_is_idempotent() {
        let (dir, db) = db_and_dir();
        let path = dir.path().join("res.json");
        let mut store = ResolutionStore::load(db, &path).unwrap();
        store
            .mark_settled(
                mid("0xcond"),
                vec![dec!(1), dec!(0)],
                dec!(10),
                1_700_000_000,
            )
            .unwrap();
        store
            .mark_settled(
                mid("0xcond"),
                vec![dec!(1), dec!(0)],
                dec!(10),
                1_700_000_001,
            )
            .unwrap();
        assert_eq!(store.total_credits(), dec!(10)); // no double-count
    }

    #[test]
    fn restart_hydrates_settled_set_from_sqlite_not_json() {
        // After settling, a fresh store reopened on the same DB still sees the market
        // as settled even with the JSON sidecar removed — SQLite is the durable guard.
        let (dir, db) = db_and_dir();
        let path = dir.path().join("res.json");
        {
            let mut store = ResolutionStore::load(db.clone(), &path).unwrap();
            store
                .mark_settled(
                    mid("0xcond1"),
                    vec![dec!(1), dec!(0)],
                    dec!(5.50),
                    1_700_000_000,
                )
                .unwrap();
        }
        // Simulate the PR4 end-state (JSON gone): SQLite must still guard.
        std::fs::remove_file(&path).ok();
        let store2 = ResolutionStore::load(db, &path).unwrap();
        assert!(store2.is_settled(&mid("0xcond1")));
        assert_eq!(store2.total_credits(), dec!(5.50));
        assert_eq!(store2.settled_count(), 1);
    }

    #[test]
    fn cutover_backfills_json_settled_set_into_sqlite() {
        // First PR1 startup on the live DB: `settled_markets` is empty, the JSON sidecar
        // holds the real settled-set. `load` must back-fill JSON→SQLite so `is_settled`
        // is non-empty at cutover (otherwise every already-settled market is re-credited).
        let (dir, db) = db_and_dir();
        let path = dir.path().join("res.json");
        // Pre-seed ONLY the JSON sidecar (the pre-step-0 binary's durable state).
        let sidecar = serde_json::json!({
            "settlements": [{
                "market_id": "0xcond1",
                "outcome_prices": ["1", "0"],
                "credit_applied": "12.00",
                "settled_at_unix": 1_700_000_000
            }]
        });
        std::fs::write(&path, serde_json::to_vec(&sidecar).unwrap()).unwrap();
        assert_eq!(
            db.list_settled_markets().unwrap().len(),
            0,
            "SQLite empty pre-load"
        );

        let store = ResolutionStore::load(db.clone(), &path).unwrap();
        assert!(
            store.is_settled(&mid("0xcond1")),
            "JSON-only settled market must be back-filled into the guard"
        );
        assert_eq!(store.total_credits(), dec!(12.00));
        // And durably persisted into SQLite, so it survives the JSON removal in PR4.
        let rows = db.list_settled_markets().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_id, mid("0xcond1"));
        assert_eq!(rows[0].credit_applied, dec!(12.00));
    }
}
