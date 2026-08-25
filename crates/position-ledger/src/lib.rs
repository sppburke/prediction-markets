//! Pure deterministic position accumulator.
//!
//! [`PositionLedger`] is a stateful in-memory accumulator; it has no I/O and produces
//! deterministic output given the same ordered input stream.

use std::collections::HashMap;

use pe_copy_signal_engine::{IncomingTrade, PositionSnapshot};
use pe_core_types::{MarketOutcomeId, Side, SourceTimestamp, WalletAddress};

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

    /// Restore the exact pre-trade state for one `(wallet, market-outcome)` — the
    /// inverse of a single `ingest` whose pre-trade `(long, short)` was captured by the
    /// caller (#511 pre-frame rollback: an abandoned-unseen admission must leave the
    /// ledger byte-identical so redelivery classifies identically). `prev = None` means
    /// the trade created the entry — remove it.
    pub fn restore(
        &mut self,
        wallet: WalletAddress,
        key: &MarketOutcomeId,
        prev: Option<(u64, u64)>,
    ) {
        let Some(snap) = self.snapshots.get_mut(&wallet) else {
            return;
        };
        match prev {
            Some((long_contracts, short_contracts)) => {
                let state = snap.positions.entry(key.clone()).or_default();
                state.long_contracts = long_contracts;
                state.short_contracts = short_contracts;
            }
            None => {
                snap.positions.remove(key);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use pe_copy_signal_engine::TradeProvenance;
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
            provenance: TradeProvenance::RestPoll,
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
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
mod restore_tests {
    use super::*;
    use pe_copy_signal_engine::TradeProvenance;
    use pe_core_types::{MarketId, OutcomeId, VenueMarketId};

    #[test]
    fn restore_is_the_exact_inverse_of_one_ingest() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xm".into())), OutcomeId(0));
        let mut ledger = PositionLedger::new();
        let trade = |contracts: u64, side: Side| IncomingTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xm".into())),
            outcome_id: OutcomeId(0),
            side,
            price: pe_core_types::Price(rust_decimal::Decimal::ONE),
            contracts: pe_core_types::ContractQty(contracts),
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
            received_at: time::OffsetDateTime::UNIX_EPOCH,
            source_trade_id: pe_core_types::SourceTradeId("t".into()),
            provenance: TradeProvenance::RestPoll,
        };
        // Entry created by the trade → restore(None) removes it entirely.
        ledger.ingest(&trade(10, Side::Buy));
        ledger.restore(wallet, &key, None);
        assert!(
            !ledger
                .position(&wallet)
                .unwrap()
                .positions
                .contains_key(&key)
        );
        // Existing position: capture, mutate via a partially-covering BUY, restore exactly.
        ledger.ingest(&trade(4, Side::Sell)); // short 4
        let prev = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .map(|st| (st.long_contracts, st.short_contracts));
        assert_eq!(prev, Some((0, 4)));
        ledger.ingest(&trade(10, Side::Buy)); // covers 4, long 6 — NOT trivially invertible
        ledger.restore(wallet, &key, prev);
        let st = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .cloned()
            .unwrap();
        assert_eq!((st.long_contracts, st.short_contracts), (0, 4));
    }
}
