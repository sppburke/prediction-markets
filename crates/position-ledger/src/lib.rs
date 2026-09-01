//! Pure deterministic position accumulator.
//!
//! [`PositionLedger`] is a stateful in-memory accumulator; it has no I/O and produces
//! deterministic output given the same ordered input stream.

use std::collections::HashMap;

use pe_copy_signal_engine::{IncomingTrade, PositionSnapshot, PositionState};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, Price, ShareAmount, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_source_polymarket_public::{ActivityAggregate, ActivityType};

/// One exact, reconciled position effect derived from a complete activity group (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMutation {
    pub source_trade_id: SourceTradeId,
    pub transaction_hash: String,
    pub wallet: WalletAddress,
    pub source_time: SourceTimestamp,
    pub effect: LedgerEffect,
}

/// Supported ledger effects. Non-mutating rows are retained so a complete
/// bucket can prove every group reached a durable disposition (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerEffect {
    Trade {
        market_id: MarketId,
        outcome_id: OutcomeId,
        side: Side,
        amount: ShareAmount,
        price: Price,
    },
    Split {
        market_id: MarketId,
        amount: ShareAmount,
    },
    Merge {
        market_id: MarketId,
        amount: ShareAmount,
    },
    Redeem {
        market_id: MarketId,
        outcome_id: OutcomeId,
        amount: ShareAmount,
    },
    Conversion,
    RawOnly,
    UnknownEffect,
}

impl LedgerMutation {
    /// Map a source-owned reconciled aggregate into the ledger domain.
    pub fn from_activity(aggregate: &ActivityAggregate) -> Result<Self, LedgerError> {
        aggregate
            .group_id
            .verify_components()
            .map_err(|_| LedgerError::InvalidMapping {
                source_trade_id: aggregate.group_id.key().clone(),
            })?;
        let components = aggregate.group_id.components();
        let source_trade_id = aggregate.group_id.key().clone();
        let market = || {
            components
                .condition_id
                .as_ref()
                .map(|condition| MarketId(VenueMarketId(condition.0.clone())))
                .ok_or_else(|| LedgerError::InvalidMapping {
                    source_trade_id: source_trade_id.clone(),
                })
        };
        let effect = match &components.activity_type {
            ActivityType::Conversion => LedgerEffect::Conversion,
            ActivityType::Unknown(_) => LedgerEffect::UnknownEffect,
            ActivityType::Reward
            | ActivityType::Deposit
            | ActivityType::Withdrawal
            | ActivityType::Yield
            | ActivityType::MakerRebate
            | ActivityType::TakerRebate
            | ActivityType::ReferralReward => LedgerEffect::RawOnly,
            _ if aggregate.is_combo => LedgerEffect::RawOnly,
            _ => match &components.activity_type {
                ActivityType::Trade => LedgerEffect::Trade {
                    market_id: market()?,
                    outcome_id: components
                        .outcome
                        .ok_or_else(|| LedgerError::InvalidMapping {
                            source_trade_id: source_trade_id.clone(),
                        })?,
                    side: components.side.ok_or_else(|| LedgerError::InvalidMapping {
                        source_trade_id: source_trade_id.clone(),
                    })?,
                    amount: aggregate.share_sum,
                    price: aggregate.volume_weighted_price().map_err(|_| {
                        LedgerError::InvalidMapping {
                            source_trade_id: source_trade_id.clone(),
                        }
                    })?,
                },
                ActivityType::Split => LedgerEffect::Split {
                    market_id: market()?,
                    amount: aggregate.share_sum,
                },
                ActivityType::Merge => LedgerEffect::Merge {
                    market_id: market()?,
                    amount: aggregate.share_sum,
                },
                ActivityType::Redeem => LedgerEffect::Redeem {
                    market_id: market()?,
                    outcome_id: components
                        .outcome
                        .ok_or_else(|| LedgerError::InvalidMapping {
                            source_trade_id: source_trade_id.clone(),
                        })?,
                    amount: aggregate.share_sum,
                },
                ActivityType::Conversion
                | ActivityType::Unknown(_)
                | ActivityType::Reward
                | ActivityType::Deposit
                | ActivityType::Withdrawal
                | ActivityType::Yield
                | ActivityType::MakerRebate
                | ActivityType::TakerRebate
                | ActivityType::ReferralReward => LedgerEffect::RawOnly,
            },
        };
        Ok(Self {
            source_trade_id,
            transaction_hash: components.transaction_hash.clone(),
            wallet: components.wallet,
            source_time: aggregate.source_time.clone(),
            effect,
        })
    }

    #[must_use]
    pub fn touched_keys(&self) -> Vec<MarketOutcomeId> {
        match &self.effect {
            LedgerEffect::Trade {
                market_id,
                outcome_id,
                ..
            }
            | LedgerEffect::Redeem {
                market_id,
                outcome_id,
                ..
            } => vec![MarketOutcomeId::new(market_id.clone(), *outcome_id)],
            LedgerEffect::Split { market_id, .. } | LedgerEffect::Merge { market_id, .. } => vec![
                MarketOutcomeId::new(market_id.clone(), OutcomeId(0)),
                MarketOutcomeId::new(market_id.clone(), OutcomeId(1)),
            ],
            LedgerEffect::Conversion | LedgerEffect::RawOnly | LedgerEffect::UnknownEffect => {
                Vec::new()
            }
        }
    }
}

/// Defined causes that require the orchestrator to durably fence one wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletFenceCause {
    RevisedAggregate,
    LateEqualSecondGroup,
    InvalidMapping,
    Underflow,
    Overflow,
    Conversion,
    UnknownEffect,
    OrderDependentEqualSecond,
}

impl WalletFenceCause {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RevisedAggregate => "revised_applied_aggregate",
            Self::LateEqualSecondGroup => "late_group_after_bucket_commit",
            Self::InvalidMapping => "invalid_mapping",
            Self::Underflow => "position_underflow",
            Self::Overflow => "position_overflow",
            Self::Conversion => "conversion_unknown_conditions",
            Self::UnknownEffect => "unknown_activity_effect",
            Self::OrderDependentEqualSecond => "order_dependent_equal_second",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("activity group {source_trade_id} has an invalid position mapping")]
    InvalidMapping { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} would underflow leader position")]
    Underflow { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} would overflow leader position")]
    Overflow { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} is a conversion with unknown affected conditions")]
    Conversion { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} has an unknown position effect")]
    UnknownEffect { source_trade_id: SourceTradeId },
}

impl LedgerError {
    #[must_use]
    pub const fn fence_cause(&self) -> WalletFenceCause {
        match self {
            Self::InvalidMapping { .. } => WalletFenceCause::InvalidMapping,
            Self::Underflow { .. } => WalletFenceCause::Underflow,
            Self::Overflow { .. } => WalletFenceCause::Overflow,
            Self::Conversion { .. } => WalletFenceCause::Conversion,
            Self::UnknownEffect { .. } => WalletFenceCause::UnknownEffect,
        }
    }
}

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
#[derive(Clone)]
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
    pub fn ingest(&mut self, trade: &IncomingTrade) -> Result<(), LedgerError> {
        let mutation = LedgerMutation {
            source_trade_id: trade.source_trade_id.clone(),
            transaction_hash: trade
                .transaction_hash
                .clone()
                .unwrap_or_else(|| trade.source_trade_id.0.clone()),
            wallet: trade.wallet,
            source_time: SourceTimestamp(trade.observed_at),
            effect: LedgerEffect::Trade {
                market_id: trade.market_id.clone(),
                outcome_id: trade.outcome_id,
                side: trade.side,
                amount: trade.contracts,
                price: trade.price,
            },
        };
        self.apply(&mutation)
    }

    /// Apply a complete reconciled group with checked exact arithmetic.
    pub fn apply(&mut self, mutation: &LedgerMutation) -> Result<(), LedgerError> {
        let mut candidate = self.clone();
        candidate.apply_in_place(mutation)?;
        *self = candidate;
        Ok(())
    }

    fn apply_in_place(&mut self, mutation: &LedgerMutation) -> Result<(), LedgerError> {
        match &mutation.effect {
            LedgerEffect::Trade {
                market_id,
                outcome_id,
                side,
                amount,
                ..
            } => {
                let state = self.state_mut(mutation.wallet, market_id, *outcome_id);
                apply_trade(state, *side, *amount, &mutation.source_trade_id)
            }
            LedgerEffect::Split { market_id, amount } => {
                for outcome in [OutcomeId(0), OutcomeId(1)] {
                    let state = self.state_mut(mutation.wallet, market_id, outcome);
                    state.long_contracts =
                        state.long_contracts.checked_add(*amount).map_err(|_| {
                            LedgerError::Overflow {
                                source_trade_id: mutation.source_trade_id.clone(),
                            }
                        })?;
                }
                Ok(())
            }
            LedgerEffect::Merge { market_id, amount } => {
                self.checked_remove_pair(mutation, market_id, *amount)
            }
            LedgerEffect::Redeem {
                market_id,
                outcome_id,
                amount,
            } => self.checked_remove(mutation, market_id, *outcome_id, *amount),
            LedgerEffect::Conversion => Err(LedgerError::Conversion {
                source_trade_id: mutation.source_trade_id.clone(),
            }),
            LedgerEffect::RawOnly => Ok(()),
            LedgerEffect::UnknownEffect => Err(LedgerError::UnknownEffect {
                source_trade_id: mutation.source_trade_id.clone(),
            }),
        }
    }

    /// Apply every group atomically. Any invalid mapping/effect/arithmetic leaves
    /// the original ledger byte-equivalent (#544).
    pub fn apply_all_or_none(&mut self, mutations: &[LedgerMutation]) -> Result<(), LedgerError> {
        let mut candidate = self.clone();
        for mutation in mutations {
            candidate.apply_in_place(mutation)?;
        }
        *self = candidate;
        Ok(())
    }

    fn state_mut(
        &mut self,
        wallet: WalletAddress,
        market_id: &MarketId,
        outcome_id: OutcomeId,
    ) -> &mut PositionState {
        let snapshot = self
            .snapshots
            .entry(wallet)
            .or_insert_with(|| PositionSnapshot {
                wallet,
                positions: HashMap::new(),
            });
        snapshot
            .positions
            .entry(MarketOutcomeId::new(market_id.clone(), outcome_id))
            .or_default()
    }

    fn checked_remove_pair(
        &mut self,
        mutation: &LedgerMutation,
        market_id: &MarketId,
        amount: ShareAmount,
    ) -> Result<(), LedgerError> {
        let mut candidate = self.clone();
        candidate.checked_remove(mutation, market_id, OutcomeId(0), amount)?;
        candidate.checked_remove(mutation, market_id, OutcomeId(1), amount)?;
        *self = candidate;
        Ok(())
    }

    fn checked_remove(
        &mut self,
        mutation: &LedgerMutation,
        market_id: &MarketId,
        outcome_id: OutcomeId,
        amount: ShareAmount,
    ) -> Result<(), LedgerError> {
        let state = self.state_mut(mutation.wallet, market_id, outcome_id);
        state.long_contracts =
            state
                .long_contracts
                .checked_sub(amount)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: mutation.source_trade_id.clone(),
                })?;
        Ok(())
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
        prev: Option<(ShareAmount, ShareAmount)>,
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

    /// Read-only state export for replay/isolated canary reconstruction.
    #[must_use]
    pub fn snapshots(&self) -> &HashMap<WalletAddress, PositionSnapshot> {
        &self.snapshots
    }

    /// Required for replay reconciliation; deferred to Phase 1.
    pub fn rewind_to(&mut self, _ts: SourceTimestamp) {}
}

fn apply_trade(
    state: &mut PositionState,
    side: Side,
    amount: ShareAmount,
    source_trade_id: &SourceTradeId,
) -> Result<(), LedgerError> {
    match side {
        Side::Buy => {
            let covered = state.short_contracts.min(amount);
            state.short_contracts =
                state
                    .short_contracts
                    .checked_sub(covered)
                    .map_err(|_| LedgerError::Underflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
            let remainder = amount
                .checked_sub(covered)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: source_trade_id.clone(),
                })?;
            state.long_contracts =
                state
                    .long_contracts
                    .checked_add(remainder)
                    .map_err(|_| LedgerError::Overflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
        }
        Side::Sell => {
            let trimmed = state.long_contracts.min(amount);
            state.long_contracts =
                state
                    .long_contracts
                    .checked_sub(trimmed)
                    .map_err(|_| LedgerError::Underflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
            let remainder = amount
                .checked_sub(trimmed)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: source_trade_id.clone(),
                })?;
            state.short_contracts = state.short_contracts.checked_add(remainder).map_err(|_| {
                LedgerError::Overflow {
                    source_trade_id: source_trade_id.clone(),
                }
            })?;
        }
    }
    Ok(())
}

impl Default for PositionLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use pe_copy_signal_engine::TradeProvenance;
    use pe_core_types::{OutcomeId, Price, SourceTradeId};
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
            contracts: pe_core_types::ShareAmount::from_whole(contracts).unwrap(),
            observed_at: ts,
            received_at: ts,
            source_trade_id: SourceTradeId("t1".to_string()),
            transaction_hash: None,
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
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::from_whole(10).unwrap());
        assert_eq!(state.short_contracts, ShareAmount::ZERO);
    }

    #[test]
    fn sell_covers_long() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        ledger.ingest(&trade(w, Side::Sell, 3, 1_001)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::from_whole(7).unwrap());
        assert_eq!(state.short_contracts, ShareAmount::ZERO);
    }

    #[test]
    fn sell_flips_to_short() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        ledger.ingest(&trade(w, Side::Sell, 13, 1_001)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::ZERO);
        assert_eq!(state.short_contracts, ShareAmount::from_whole(3).unwrap());
    }

    #[test]
    fn rewind_to_is_noop() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        let ts = SourceTimestamp(OffsetDateTime::from_unix_timestamp(900).unwrap());
        ledger.rewind_to(ts); // must not panic or mutate
        assert!(ledger.position(&w).is_some());
    }

    #[test]
    fn split_overflow_on_second_outcome_leaves_first_outcome_unchanged() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let first = MarketOutcomeId::new(market(), OutcomeId(0));
        let second = MarketOutcomeId::new(market(), OutcomeId(1));
        let original_first = ShareAmount::from_atomic(7);
        let mut positions = HashMap::new();
        positions.insert(
            first.clone(),
            PositionState {
                long_contracts: original_first,
                short_contracts: ShareAmount::ZERO,
            },
        );
        positions.insert(
            second,
            PositionState {
                long_contracts: ShareAmount::from_atomic(u64::MAX),
                short_contracts: ShareAmount::ZERO,
            },
        );
        let mut ledger = PositionLedger::from_snapshots(HashMap::from([(
            w,
            PositionSnapshot {
                wallet: w,
                positions,
            },
        )]));
        let mutation = LedgerMutation {
            source_trade_id: SourceTradeId("g2:test".to_owned()),
            transaction_hash: "0xtest".to_owned(),
            wallet: w,
            source_time: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            effect: LedgerEffect::Split {
                market_id: market(),
                amount: ShareAmount::from_atomic(1),
            },
        };

        assert!(matches!(
            ledger.apply(&mutation),
            Err(LedgerError::Overflow { .. })
        ));
        assert_eq!(
            ledger.position(&w).unwrap().positions[&first].long_contracts,
            original_first
        );
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
            contracts: pe_core_types::ShareAmount::from_whole(contracts).unwrap(),
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
            received_at: time::OffsetDateTime::UNIX_EPOCH,
            source_trade_id: pe_core_types::SourceTradeId("t".into()),
            transaction_hash: None,
            provenance: TradeProvenance::RestPoll,
        };
        // Entry created by the trade → restore(None) removes it entirely.
        ledger.ingest(&trade(10, Side::Buy)).unwrap();
        ledger.restore(wallet, &key, None);
        assert!(
            !ledger
                .position(&wallet)
                .unwrap()
                .positions
                .contains_key(&key)
        );
        // Existing position: capture, mutate via a partially-covering BUY, restore exactly.
        ledger.ingest(&trade(4, Side::Sell)).unwrap(); // short 4
        let prev = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .map(|st| (st.long_contracts, st.short_contracts));
        assert_eq!(
            prev,
            Some((ShareAmount::ZERO, ShareAmount::from_whole(4).unwrap()))
        );
        ledger.ingest(&trade(10, Side::Buy)).unwrap(); // covers 4, long 6 — NOT trivially invertible
        ledger.restore(wallet, &key, prev);
        let st = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .cloned()
            .unwrap();
        assert_eq!(
            (st.long_contracts, st.short_contracts),
            (ShareAmount::ZERO, ShareAmount::from_whole(4).unwrap())
        );
    }
}
