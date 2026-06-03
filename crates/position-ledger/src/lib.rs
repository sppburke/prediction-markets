//! Pure deterministic position and cluster-observation accumulators.
//!
//! Both types are stateful in-memory accumulators; they have no I/O and produce
//! deterministic output given the same ordered input stream.

use std::collections::HashMap;

use pe_copy_signal_engine::{ClusterEntry, ClusterObs, IncomingTrade, PositionSnapshot};
use pe_core_types::{
    MarketId, MarketOutcomeId, OperatorId, OutcomeId, Side, SourceTimestamp, WalletAddress,
};

// ── PositionLedger ────────────────────────────────────────────────────────────

/// Accumulates [`IncomingTrade`] events into per-wallet position snapshots.
///
/// Maintains net long/short exposure per `(wallet, market, outcome)`. Trades are
/// applied in arrival order; no settlement or expiry logic is included here.
///
/// # Precondition
///
/// `position()` returns the state after all trades ingested so far. Calling it
/// before any trade has been ingested returns `None` for every wallet — this is
/// the correct sentinel, not an error.
pub struct PositionLedger {
    snapshots: HashMap<WalletAddress, PositionSnapshot>,
}

impl PositionLedger {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    /// Rehydrate the ledger from previously persisted per-wallet snapshots (the
    /// `leader_positions` mirror in `paper-state`), so that classification after a
    /// restart sees each leader's existing position rather than treating the first
    /// post-restart trade as a fresh Entry. Reuses the existing `PositionSnapshot`
    /// type; no trades are replayed.
    pub fn from_snapshots(snapshots: HashMap<WalletAddress, PositionSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Apply one trade to the ledger, updating net exposure for the wallet.
    ///
    /// Buying reduces short contracts first (covering), then adds to long.
    /// Selling reduces long contracts first (trimming), then adds to short.
    pub fn ingest(&mut self, trade: &IncomingTrade) {
        let snap = self
            .snapshots
            .entry(trade.wallet)
            .or_insert_with(|| PositionSnapshot {
                wallet: trade.wallet,
                positions: HashMap::new(),
            });

        let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
        let state = snap.positions.entry(key).or_default();
        let qty = trade.contracts.0;

        match trade.side {
            Side::Buy => {
                let covered = state.short_contracts.min(qty);
                state.short_contracts -= covered;
                state.long_contracts = state.long_contracts.saturating_add(qty - covered);
            }
            Side::Sell => {
                let trimmed = state.long_contracts.min(qty);
                state.long_contracts -= trimmed;
                state.short_contracts = state.short_contracts.saturating_add(qty - trimmed);
            }
        }
    }

    /// Return the current position snapshot for a wallet, or `None` if the wallet
    /// has never been observed.
    pub fn position(&self, wallet: &WalletAddress) -> Option<&PositionSnapshot> {
        self.snapshots.get(wallet)
    }

    /// Overlay per-wallet snapshots from the live positions API, replacing each
    /// wallet's entry wholesale (API wins). Wallets absent from `updates` are
    /// untouched. A wallet present in `updates` with an empty `positions` map
    /// clears that wallet's entry (all positions closed per the API).
    pub fn overlay(&mut self, updates: HashMap<WalletAddress, PositionSnapshot>) {
        for (wallet, snap) in updates {
            self.snapshots.insert(wallet, snap);
        }
    }

    /// Required for replay reconciliation; deferred to Phase 1.
    pub fn rewind_to(&mut self, _ts: SourceTimestamp) {}
}

impl Default for PositionLedger {
    fn default() -> Self {
        Self::new()
    }
}

// ── ClusterObservationTracker ─────────────────────────────────────────────────

/// Tracks recent intra-cluster trade entries per `(operator, market, outcome, side)`.
///
/// Entries older than `window_secs` are pruned on each [`ingest`] call.
/// The tracker is keyed by operator — wallets with no known operator are never
/// recorded and will always produce `None` from [`cluster_obs_for`].
///
/// # Precondition
///
/// Call [`ingest`] with the current trade *before* calling [`cluster_obs_for`]
/// so that the current trade is included in the returned [`ClusterObs`]. The
/// classifier's `is_cluster_coordination` then applies its own member-count and
/// notional-aggregate gates on the full set.
///
/// [`ingest`]: ClusterObservationTracker::ingest
/// [`cluster_obs_for`]: ClusterObservationTracker::cluster_obs_for
pub struct ClusterObservationTracker {
    /// How long entries are retained. Should be ≥ `cluster_coord_window_seconds_W`
    /// (default 300 s) so the classifier always has the full window to evaluate.
    window_secs: u64,
    entries: HashMap<ClusterKey, Vec<ClusterEntry>>,
}

/// Internal key for one coordination bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClusterKey {
    operator_id: OperatorId,
    market_id: MarketId,
    outcome_id: OutcomeId,
    side: Side,
}

impl ClusterObservationTracker {
    /// `window_secs` — how long entries are retained in memory.
    /// Default from `_GLOSSARY.md`: `cluster_observation_window_secs = 300`.
    pub fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            entries: HashMap::new(),
        }
    }

    /// Record an entry for the trade. Prunes entries older than `window_secs`.
    pub fn ingest(&mut self, trade: &IncomingTrade, operator_id: OperatorId) {
        let key = ClusterKey {
            operator_id,
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            side: trade.side,
        };

        let trade_ts = trade.observed_at.unix_timestamp();
        let cutoff = trade_ts.saturating_sub(self.window_secs as i64);

        let bucket = self.entries.entry(key).or_default();
        bucket.push(ClusterEntry {
            wallet: trade.wallet,
            price: trade.price,
            contracts: trade.contracts,
            observed_at_unix: trade_ts,
        });
        bucket.retain(|e| e.observed_at_unix >= cutoff);
    }

    /// Return a [`ClusterObs`] for the given trade and operator if any entries
    /// exist in the bucket. Returns `None` if no entry has been recorded yet for
    /// this `(operator, market, outcome, side)`.
    ///
    /// The classifier applies its own member-count and notional-aggregate gates;
    /// this method returns `Some` whenever the bucket is non-empty.
    pub fn cluster_obs_for(
        &self,
        trade: &IncomingTrade,
        operator_id: OperatorId,
    ) -> Option<ClusterObs> {
        let key = ClusterKey {
            operator_id,
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            side: trade.side,
        };

        let entries = self.entries.get(&key)?;
        if entries.is_empty() {
            return None;
        }

        Some(ClusterObs {
            operator_id,
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            side: trade.side,
            wallet_entries: entries.clone(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use pe_core_types::{ContractQty, OutcomeId, Price, SourceTradeId};
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    use super::*;

    fn wallet(hex: &str) -> WalletAddress {
        serde_json::from_str(&format!("\"{hex}\"")).unwrap()
    }

    fn market() -> pe_core_types::MarketId {
        use pe_core_types::VenueMarketId;
        pe_core_types::MarketId(VenueMarketId("0xmarket1".to_string()))
    }

    fn trade(w: WalletAddress, side: Side, contracts: u64, ts_unix: i64) -> IncomingTrade {
        let ts = OffsetDateTime::from_unix_timestamp(ts_unix).unwrap();
        IncomingTrade {
            wallet: w,
            market_id: market(),
            outcome_id: OutcomeId(0),
            side,
            price: Price(dec!(0.5)),
            contracts: ContractQty(contracts),
            observed_at: ts,
            received_at: ts,
            source_trade_id: SourceTradeId("t1".to_string()),
        }
    }

    #[test]
    fn position_starts_empty() {
        let ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(ledger.position(&w).is_none());
    }

    #[test]
    fn buy_opens_long() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, 10);
        assert_eq!(state.short_contracts, 0);
    }

    #[test]
    fn sell_covers_long() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));
        ledger.ingest(&trade(w, Side::Sell, 3, 1_001));
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, 7);
        assert_eq!(state.short_contracts, 0);
    }

    #[test]
    fn sell_flips_to_short() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));
        ledger.ingest(&trade(w, Side::Sell, 13, 1_001));
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, 0);
        assert_eq!(state.short_contracts, 3);
    }

    #[test]
    fn overlay_replaces_wallet_entry() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));
        let key = MarketOutcomeId::new(market(), OutcomeId(0));

        let mut new_positions = HashMap::new();
        new_positions.insert(
            key.clone(),
            pe_copy_signal_engine::PositionState {
                long_contracts: 99,
                short_contracts: 0,
            },
        );
        let snap = PositionSnapshot {
            wallet: w,
            positions: new_positions,
        };
        let mut updates = HashMap::new();
        updates.insert(w, snap);

        ledger.overlay(updates);
        let result = ledger.position(&w).unwrap();
        assert_eq!(result.positions[&key].long_contracts, 99);
    }

    #[test]
    fn overlay_absent_wallet_untouched() {
        let mut ledger = PositionLedger::new();
        let w_a = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w_b = wallet("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        ledger.ingest(&trade(w_a, Side::Buy, 5, 1_000));

        // Overlay only touches w_b; w_a must remain unchanged.
        let updates: HashMap<WalletAddress, PositionSnapshot> = {
            let mut m = HashMap::new();
            m.insert(
                w_b,
                PositionSnapshot {
                    wallet: w_b,
                    positions: HashMap::new(),
                },
            );
            m
        };
        ledger.overlay(updates);

        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = ledger.position(&w_a).unwrap().positions[&key];
        assert_eq!(state.long_contracts, 5);
    }

    #[test]
    fn overlay_empty_snapshot_clears_wallet() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));

        // An empty PositionSnapshot replaces the existing entry, clearing positions.
        let mut updates = HashMap::new();
        updates.insert(
            w,
            PositionSnapshot {
                wallet: w,
                positions: HashMap::new(),
            },
        );
        ledger.overlay(updates);

        let snap = ledger.position(&w).unwrap();
        assert!(snap.positions.is_empty());
    }

    #[test]
    fn overlay_empty_map_is_noop() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));

        ledger.overlay(HashMap::new());

        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = ledger.position(&w).unwrap().positions[&key];
        assert_eq!(state.long_contracts, 10);
    }

    #[test]
    fn rewind_to_is_noop() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000));
        let ts = SourceTimestamp(OffsetDateTime::from_unix_timestamp(900).unwrap());
        ledger.rewind_to(ts); // must not panic or mutate
        assert!(ledger.position(&w).is_some());
    }
}
