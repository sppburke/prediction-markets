//! Pure copy-entry gate: copy only a leader's *first-ever BUY entry* into a market
//! (issues #290, #339).
//!
//! The original leader-price band was removed in #339. The current fill-price band is
//! enforced later in the orchestrator, and live sizing uses that fill basis (see
//! `evaluate_at_price`).
//! [`CopyEntryGate`] now enforces the BUY-only first-entry criterion (the resolution
//! horizon and hold-to-resolution behaviour are enforced elsewhere — see
//! `docs/19-WINNER-FOLLOW-STRATEGY.md`). It is pure and synchronous so it can be
//! unit-tested in isolation; the per-wallet market history it consults is populated at
//! startup by [`crate::wallet_history`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{LeaderAction, MarketId, Side, WalletAddress};

/// Typed reason a [`LeaderSignal`] was rejected by [`CopyEntryGate::admit`].
///
/// `Display` renders a short human-readable reason for structured logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReject {
    /// Current production Winner-Follow copies BUY entries only.
    NotBuy,
    /// Action is not a first-position entry (`Add`/`Trim`/`Exit`/`Flip`/`Unknown`).
    NotAnEntry,
    /// The leader has already entered this market before (a re-entry, not a first entry).
    NotFirstEntry,
    /// The leader's market history could not be loaded and the gate is fail-closed.
    WalletHistoryMissing,
}

impl fmt::Display for GateReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::NotBuy => "copy scope is BUY-only",
            Self::NotAnEntry => "not a first-position entry",
            Self::NotFirstEntry => "leader already entered this market",
            Self::WalletHistoryMissing => "leader market history missing (fail-closed)",
        };
        f.write_str(s)
    }
}

/// Configuration for [`CopyEntryGate`].
///
/// Default (`entry_gate_fail_closed` = false) lives in `docs/_GLOSSARY.md`.
#[derive(Debug, Clone)]
pub struct CopyEntryGateConfig {
    /// When a wallet is absent from the history map: `false` admits the entry
    /// (fail-open, treat as new), `true` rejects it (fail-closed).
    pub fail_closed: bool,
}

/// Pure, synchronous gate enforcing the BUY-only first-entry copy-scope criteria.
///
/// Built once at startup from a per-wallet history map; mutated in-session via
/// [`Self::record_entry`] so same-session re-entries are also blocked.
pub struct CopyEntryGate {
    config: CopyEntryGateConfig,
    /// For each leader wallet, the conservative set of markets with prior trade
    /// activity. A wallet absent from the map has unknown history (governed by
    /// `fail_closed`).
    history: HashMap<WalletAddress, HashSet<MarketId>>,
    /// Entries admitted during this process lifetime. Kept separate so a staging rollback can
    /// remove only its own tentative record without erasing startup/preloaded history.
    same_session: HashSet<(WalletAddress, MarketId)>,
}

impl CopyEntryGate {
    /// Build a gate from its config and the startup-loaded per-wallet history.
    pub fn new(
        config: CopyEntryGateConfig,
        history: HashMap<WalletAddress, HashSet<MarketId>>,
    ) -> Self {
        Self {
            config,
            history,
            same_session: HashSet::new(),
        }
    }

    /// Update the absent-wallet fail-closed posture from a runtime-config poll (#398 WS1). The
    /// accumulated per-wallet history is preserved; only the posture changes.
    pub fn set_fail_closed(&mut self, fail_closed: bool) {
        self.config.fail_closed = fail_closed;
    }

    /// Merge preloaded history for wallets about to join the live set.
    ///
    /// Union semantics preserve both startup history and same-session entries already recorded
    /// by [`Self::record_entry`]. The capacity controller calls this through the orchestrator
    /// before atomically admitting a hot-grown wallet, so it never passes through the
    /// absent-wallet fail-open path merely because membership changed at runtime.
    pub fn merge_history(&mut self, additional: HashMap<WalletAddress, HashSet<MarketId>>) {
        for (wallet, markets) in additional {
            self.history.entry(wallet).or_default().extend(markets);
        }
    }

    /// Returns `None` to admit the signal, or `Some(reason)` to reject it.
    ///
    /// Checks, in order: the side is [`Side::Buy`] → the action is an `Entry` →
    /// first-entry (the leader has not entered this market before). A wallet absent from
    /// the history map is admitted when `fail_closed` is false (treat as new) and rejected
    /// otherwise.
    ///
    /// The wallet is resolved as `signal.leader.0` (`LeaderSignal.leader` is a
    /// `TraderId(WalletAddress)`), equal to the `trade.wallet` used elsewhere in
    /// the copy path.
    pub fn admit(&self, signal: &LeaderSignal) -> Option<GateReject> {
        if signal.leader_side != Side::Buy {
            return Some(GateReject::NotBuy);
        }
        if signal.action != LeaderAction::Entry {
            return Some(GateReject::NotAnEntry);
        }
        let wallet = signal.leader.0;
        if self
            .same_session
            .contains(&(wallet, signal.market_id.clone()))
        {
            return Some(GateReject::NotFirstEntry);
        }
        match self.history.get(&wallet) {
            Some(markets) => {
                if markets.contains(&signal.market_id) {
                    return Some(GateReject::NotFirstEntry);
                }
            }
            None => {
                if self.config.fail_closed {
                    return Some(GateReject::WalletHistoryMissing);
                }
            }
        }
        None
    }

    /// Record an admitted BUY entry so a same-session re-entry into `market` is blocked.
    ///
    /// Called for every admitted `Entry` — including on no-fill and even when a
    /// later gate or the strategy rejects the signal — so duplicate entries within
    /// one run are dropped regardless of downstream outcome.
    pub fn record_entry(&mut self, wallet: WalletAddress, market: &MarketId) {
        self.same_session.insert((wallet, market.clone()));
    }

    /// Roll back the tentative same-session record when durable dispatch staging fails.
    /// Startup and capacity-preloaded history is never mutated by this operation.
    pub fn unrecord_entry(&mut self, wallet: WalletAddress, market: &MarketId) {
        self.same_session.remove(&(wallet, market.clone()));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use pe_copy_signal_engine::LeaderSignal;
    use pe_core_types::{
        ContractQty, LeaderAction, MarketId, OutcomeId, Price, ProbabilityPpm, Quantity,
        ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId, VenueMarketId,
        WalletAddress,
    };
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    fn market(hex: &str) -> MarketId {
        MarketId(VenueMarketId(hex.to_string()))
    }

    /// A `LeaderSignal` for `wallet` entering `market` at `price` with `action`.
    fn signal(action: LeaderAction, market_id: MarketId, price: Price) -> LeaderSignal {
        let ts = OffsetDateTime::UNIX_EPOCH;
        LeaderSignal {
            leader: TraderId(wallet()),
            venue: VenueId::polymarket(),
            market_id,
            outcome_id: OutcomeId(0),
            action,
            leader_side: Side::Buy,
            leader_price: price,
            leader_size: Quantity(ContractQty(100)),
            observed_at: ts,
            received_at: ts,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("t1".to_string()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        }
    }

    /// Fail-open gate config (first-entry only; no band since #339).
    fn band_config() -> CopyEntryGateConfig {
        CopyEntryGateConfig { fail_closed: false }
    }

    #[test]
    fn admits_first_entry() {
        let gate = CopyEntryGate::new(band_config(), HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None);
    }

    #[test]
    fn rejects_non_entry_action() {
        let gate = CopyEntryGate::new(band_config(), HashMap::new());
        for action in [
            LeaderAction::Add,
            LeaderAction::Trim,
            LeaderAction::Exit,
            LeaderAction::Flip,
            LeaderAction::Unknown,
        ] {
            let s = signal(action, market("0xnew"), Price(dec!(0.60)));
            assert_eq!(gate.admit(&s), Some(GateReject::NotAnEntry), "{action:?}");
        }
    }

    #[test]
    fn rejects_sell_before_history_posture() {
        for fail_closed in [false, true] {
            let gate = CopyEntryGate::new(CopyEntryGateConfig { fail_closed }, HashMap::new());
            let mut s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
            s.leader_side = Side::Sell;
            assert_eq!(gate.admit(&s), Some(GateReject::NotBuy));
        }
    }

    #[test]
    fn rejects_known_market_re_entry() {
        let mut history = HashMap::new();
        let mut set = HashSet::new();
        set.insert(market("0xknown"));
        history.insert(wallet(), set);
        let gate = CopyEntryGate::new(band_config(), history);
        let s = signal(LeaderAction::Entry, market("0xknown"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), Some(GateReject::NotFirstEntry));
    }

    #[test]
    fn admits_new_market_for_known_wallet() {
        let mut history = HashMap::new();
        history.insert(wallet(), HashSet::from([market("0xother")]));
        let gate = CopyEntryGate::new(band_config(), history);
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None);
    }

    #[test]
    fn fail_open_admits_absent_wallet() {
        let cfg = CopyEntryGateConfig { fail_closed: false };
        let gate = CopyEntryGate::new(cfg, HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None);
    }

    #[test]
    fn fail_closed_rejects_absent_wallet() {
        let cfg = CopyEntryGateConfig { fail_closed: true };
        let gate = CopyEntryGate::new(cfg, HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), Some(GateReject::WalletHistoryMissing));
    }

    #[test]
    fn record_entry_blocks_same_session_re_entry() {
        let mut gate = CopyEntryGate::new(band_config(), HashMap::new());
        let m = market("0xnew");
        let s = signal(LeaderAction::Entry, m.clone(), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None, "first entry admitted");
        gate.record_entry(wallet(), &m);
        assert_eq!(
            gate.admit(&s),
            Some(GateReject::NotFirstEntry),
            "second entry into same market blocked"
        );
    }

    #[test]
    fn staging_failure_rollback_allows_redelivery() {
        let mut gate = CopyEntryGate::new(band_config(), HashMap::new());
        let m = market("0xredelivery");
        let s = signal(LeaderAction::Entry, m.clone(), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None);
        gate.record_entry(wallet(), &m);
        assert_eq!(gate.admit(&s), Some(GateReject::NotFirstEntry));
        gate.unrecord_entry(wallet(), &m);
        assert_eq!(gate.admit(&s), None, "redelivery must be admitted");
    }

    #[test]
    fn merge_history_unions_without_erasing_same_session_entries() {
        let mut gate = CopyEntryGate::new(band_config(), HashMap::new());
        let in_session = market("0xin-session");
        gate.record_entry(wallet(), &in_session);

        let historical = market("0xhistorical");
        gate.merge_history(HashMap::from([(
            wallet(),
            HashSet::from([historical.clone()]),
        )]));

        for known in [in_session, historical] {
            let s = signal(LeaderAction::Entry, known, Price(dec!(0.60)));
            assert_eq!(gate.admit(&s), Some(GateReject::NotFirstEntry));
        }
    }
}
