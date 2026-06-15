//! Resolution store: the double-credit guard for market settlement.
//!
//! Tracks which markets have been settled (their resolution prices applied to the
//! bankroll) so a still-closed market is never re-credited across restarts. The
//! **authoritative** durable set lives in `paper_state.db` (`settled_markets`); the
//! in-memory map is hydrated from SQLite on [`ResolutionStore::load`] and
//! `is_settled` reads it.

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::MarketId;
use pe_paper_state::{PaperStateDb, PaperStateError};
use rust_decimal::Decimal;

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
/// `settled_markets` SQLite table (the authoritative double-credit guard).
/// Hydrated from SQLite on [`ResolutionStore::load`].
pub struct ResolutionStore {
    settled: HashMap<MarketId, Entry>,
    total_credits: Decimal,
    /// Durable settled-set backing store; written by `mark_settled`, read by `load`.
    db: Arc<PaperStateDb>,
}

impl std::fmt::Debug for ResolutionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `db` (Arc<PaperStateDb>) is not Debug; surface only the derived state.
        f.debug_struct("ResolutionStore")
            .field("settled_count", &self.settled.len())
            .field("total_credits", &self.total_credits)
            .finish_non_exhaustive()
    }
}

impl ResolutionStore {
    /// Load the settled-set from `db`'s `settled_markets` table.
    ///
    /// `settled_markets` is the authoritative durable double-credit guard: the in-memory
    /// map is hydrated from it on load and `is_settled` reads the map. Creates an empty
    /// store if no SQLite row exists yet.
    pub fn load(db: Arc<PaperStateDb>) -> Result<Self, ResolutionStoreError> {
        // Hydrate the in-memory guard from SQLite (authoritative).
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
            total_credits,
            db,
        })
    }

    /// Whether `market_id` has already been settled.
    pub fn is_settled(&self, market_id: &MarketId) -> bool {
        self.settled.contains_key(market_id)
    }

    /// Record a settlement: write the authoritative SQLite row and the in-memory map.
    /// Idempotent — a market already in the in-memory set returns early without rewriting.
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

        // Authoritative durable guard: write SQLite (read back by `load` on restart).
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
    fn empty_on_fresh_db() {
        let (_dir, db) = db_and_dir();
        let store = ResolutionStore::load(db).unwrap();
        assert_eq!(store.settled_count(), 0);
        assert_eq!(store.total_credits(), Decimal::ZERO);
    }

    #[test]
    fn mark_settled_persists_and_accumulates() {
        let (_dir, db) = db_and_dir();
        let mut store = ResolutionStore::load(db.clone()).unwrap();
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
        let store2 = ResolutionStore::load(db).unwrap();
        assert!(store2.is_settled(&mid("0xcond1")));
        assert_eq!(store2.total_credits(), dec!(5.50));
    }

    #[test]
    fn settlement_info_exposes_detail_after_mark_settled() {
        let (_dir, db) = db_and_dir();
        let mut store = ResolutionStore::load(db).unwrap();
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
        let (_dir, db) = db_and_dir();
        let mut store = ResolutionStore::load(db).unwrap();
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
    fn restart_hydrates_settled_set_from_sqlite() {
        // After settling, a fresh store reopened on the same DB still sees the market as
        // settled — SQLite is the sole durable guard; no JSON sidecar exists post-PR4.
        let (_dir, db) = db_and_dir();
        {
            let mut store = ResolutionStore::load(db.clone()).unwrap();
            store
                .mark_settled(
                    mid("0xcond1"),
                    vec![dec!(1), dec!(0)],
                    dec!(5.50),
                    1_700_000_000,
                )
                .unwrap();
        }
        let store2 = ResolutionStore::load(db).unwrap();
        assert!(store2.is_settled(&mid("0xcond1")));
        assert_eq!(store2.total_credits(), dec!(5.50));
        assert_eq!(store2.settled_count(), 1);
    }
}
