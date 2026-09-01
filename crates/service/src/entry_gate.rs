//! Pure copy-entry gate: copy only a leader's *first-ever BUY entry* into a market
//! (issues #290, #339).
//!
//! The original leader-price band was removed in #339. The current fill-price band is
//! enforced later in the orchestrator, and live sizing uses that fill basis (see
//! `evaluate_at_price`).
//! [`CopyEntryGate`] now enforces the BUY-only first-entry criterion (the resolution
//! horizon and hold-to-resolution behaviour are enforced elsewhere — see
//! `docs/19-WINNER-FOLLOW-STRATEGY.md`). It is pure and synchronous so it can be
//! unit-tested in isolation; its per-wallet market history is rebuilt before producers
//! from version-two paper-state records (#544).

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
    /// The leader's reconciled market history is incomplete or unavailable.
    WalletHistoryMissing,
}

impl fmt::Display for GateReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::NotBuy => "copy scope is BUY-only",
            Self::NotAnEntry => "not a first-position entry",
            Self::NotFirstEntry => "leader already entered this market",
            Self::WalletHistoryMissing => "leader reconciled market history unavailable",
        };
        f.write_str(s)
    }
}

/// Reserved construction marker for the durable gate projection (#544).
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyEntryGateConfig;

/// Pure, synchronous gate enforcing the BUY-only first-entry copy-scope criteria.
///
/// Built once at startup from a per-wallet history map; mutated in-session via
/// [`Self::record_entry`] so same-session re-entries are also blocked.
pub struct CopyEntryGate {
    /// For each leader wallet, the conservative set of markets with prior trade
    /// activity. A missing wallet is unavailable and therefore fails closed.
    history: HashMap<WalletAddress, HashSet<MarketId>>,
}

impl CopyEntryGate {
    /// Build a gate from its config and the startup-loaded per-wallet history.
    pub fn new(
        _config: CopyEntryGateConfig,
        history: HashMap<WalletAddress, HashSet<MarketId>>,
    ) -> Self {
        Self { history }
    }

    /// Merge preloaded history for wallets about to join the live set.
    ///
    /// Used only after a durable bucket/import transaction commits.
    pub fn merge_history(&mut self, additional: HashMap<WalletAddress, HashSet<MarketId>>) {
        for (wallet, markets) in additional {
            self.history.entry(wallet).or_default().extend(markets);
        }
    }

    /// Returns `None` to admit the signal, or `Some(reason)` to reject it.
    ///
    /// Checks, in order: the side is [`Side::Buy`] → the action is an `Entry` →
    /// first-entry (the leader has not entered this market before). A wallet absent from
    /// the durable projection always fails closed.
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
        match self.history.get(&wallet) {
            Some(markets) => {
                if markets.contains(&signal.market_id) {
                    return Some(GateReject::NotFirstEntry);
                }
            }
            None => return Some(GateReject::WalletHistoryMissing),
        }
        None
    }

    /// Record an admitted BUY entry so a same-session re-entry into `market` is blocked.
    ///
    /// Called only after the bucket transaction durably consumes history.
    pub fn record_entry(&mut self, wallet: WalletAddress, market: &MarketId) {
        self.history
            .entry(wallet)
            .or_default()
            .insert(market.clone());
    }

    #[must_use]
    pub fn has_wallet(&self, wallet: &WalletAddress) -> bool {
        self.history.contains_key(wallet)
    }

    #[must_use]
    pub fn has_market(&self, wallet: &WalletAddress, market: &MarketId) -> bool {
        self.history
            .get(wallet)
            .is_some_and(|markets| markets.contains(market))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use pe_copy_signal_engine::LeaderSignal;
    use pe_core_types::{
        LeaderAction, MarketId, OutcomeId, Price, ProbabilityPpm, ReconstructionQuality,
        ShareAmount, Side, SourceTradeId, TraderId, VenueId, VenueMarketId, WalletAddress,
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
            leader_size: ShareAmount::from_whole(100).unwrap(),
            observed_at: ts,
            received_at: ts,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("t1".to_string()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        }
    }

    fn band_config() -> CopyEntryGateConfig {
        CopyEntryGateConfig
    }

    #[test]
    fn admits_first_entry() {
        let gate = CopyEntryGate::new(band_config(), HashMap::from([(wallet(), HashSet::new())]));
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
        let gate = CopyEntryGate::new(CopyEntryGateConfig, HashMap::new());
        let mut s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        s.leader_side = Side::Sell;
        assert_eq!(gate.admit(&s), Some(GateReject::NotBuy));
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
    fn absent_wallet_always_fails_closed() {
        let cfg = CopyEntryGateConfig;
        let gate = CopyEntryGate::new(cfg, HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), Some(GateReject::WalletHistoryMissing));
    }

    #[test]
    fn record_entry_blocks_same_session_re_entry() {
        let mut gate =
            CopyEntryGate::new(band_config(), HashMap::from([(wallet(), HashSet::new())]));
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
