//! In-memory resolution store backed by a JSON sidecar file.
//!
//! Tracks which markets have been settled (their resolution prices applied to the bankroll)
//! to prevent double-crediting across service restarts. The sidecar is the source of truth;
//! the in-memory map is derived from it on load.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pe_core_types::{MarketId, VenueMarketId};
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

/// In-memory resolution store. Load from a JSON sidecar path; persist on each new settlement.
#[derive(Debug)]
pub struct ResolutionStore {
    settled: HashMap<MarketId, Entry>,
    sidecar_path: PathBuf,
    total_credits: Decimal,
}

impl ResolutionStore {
    /// Load from `path` (creates an empty store if the file does not exist).
    pub fn load(path: &Path) -> Result<Self, ResolutionStoreError> {
        let sidecar: Sidecar = if path.exists() {
            let bytes = std::fs::read(path)?;
            serde_json::from_slice(&bytes)?
        } else {
            Sidecar::default()
        };

        let mut settled = HashMap::new();
        let mut total_credits = Decimal::ZERO;
        for rec in &sidecar.settlements {
            let mid = MarketId(VenueMarketId(rec.market_id.clone()));
            let prices: Vec<Decimal> = rec
                .outcome_prices
                .iter()
                .map(|s| s.parse().unwrap_or(Decimal::ZERO))
                .collect();
            let credit: Decimal = rec.credit_applied.parse().unwrap_or(Decimal::ZERO);
            total_credits = total_credits.checked_add(credit).unwrap_or(total_credits);
            settled.insert(
                mid,
                Entry {
                    outcome_prices: prices,
                    credit_applied: credit,
                    settled_at_unix: rec.settled_at_unix,
                },
            );
        }

        Ok(Self {
            settled,
            sidecar_path: path.to_owned(),
            total_credits,
        })
    }

    /// Whether `market_id` has already been settled.
    pub fn is_settled(&self, market_id: &MarketId) -> bool {
        self.settled.contains_key(market_id)
    }

    /// Record a settlement and persist to the sidecar.
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
        self.settled.insert(
            market_id,
            Entry {
                outcome_prices,
                credit_applied,
                settled_at_unix,
            },
        );

        // Rebuild sidecar from the in-memory map and persist atomically.
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
}

#[derive(Debug, thiserror::Error)]
pub enum ResolutionStoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    #[test]
    fn empty_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = ResolutionStore::load(&dir.path().join("res.json")).unwrap();
        assert_eq!(store.settled_count(), 0);
        assert_eq!(store.total_credits(), Decimal::ZERO);
    }

    #[test]
    fn mark_settled_persists_and_accumulates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("res.json");
        let mut store = ResolutionStore::load(&path).unwrap();
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

        // Reload and verify persistence.
        let store2 = ResolutionStore::load(&path).unwrap();
        assert!(store2.is_settled(&mid("0xcond1")));
        assert_eq!(store2.total_credits(), dec!(5.50));
    }

    #[test]
    fn mark_settled_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("res.json");
        let mut store = ResolutionStore::load(&path).unwrap();
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
}
