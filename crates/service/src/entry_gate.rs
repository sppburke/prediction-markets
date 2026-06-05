//! Pure copy-entry gate aligning the live copy path with the band-cohort
//! selection criteria (issue #290).
//!
//! The "72hr buy-and-hold band" cohort was selected on wallets whose copied
//! trades were *first-ever entries* into a market at a price in `[0.40, 0.80]`.
//! [`CopyEntryGate`] enforces those two criteria (the 72 h horizon and the
//! hold-to-resolution behaviour are enforced elsewhere — see
//! `docs/19-WINNER-FOLLOW-STRATEGY.md`). It is pure and synchronous so it can be
//! unit-tested in isolation; the per-wallet market history it consults is
//! populated at startup by [`crate::wallet_history`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{LeaderAction, MarketId, Price, WalletAddress};

/// Typed reason a [`LeaderSignal`] was rejected by [`CopyEntryGate::admit`].
///
/// `Display` renders a short human-readable reason for structured logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReject {
    /// Action is not a first-position entry (`Add`/`Trim`/`Exit`/`Flip`/`Unknown`).
    NotAnEntry,
    /// Leader entry price is below the band minimum.
    PriceBelowBand,
    /// Leader entry price is above the band maximum.
    PriceAboveBand,
    /// The leader has already entered this market before (a re-entry, not a first entry).
    NotFirstEntry,
    /// The leader's market history could not be loaded and the gate is fail-closed.
    WalletHistoryMissing,
}

impl fmt::Display for GateReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::NotAnEntry => "not a first-position entry",
            Self::PriceBelowBand => "leader price below band minimum",
            Self::PriceAboveBand => "leader price above band maximum",
            Self::NotFirstEntry => "leader already entered this market",
            Self::WalletHistoryMissing => "leader market history missing (fail-closed)",
        };
        f.write_str(s)
    }
}

/// Configuration for [`CopyEntryGate`]. Band bounds are inclusive.
///
/// Defaults (`entry_gate_price_band_lo` = 0.40, `entry_gate_price_band_hi` = 0.80,
/// `entry_gate_fail_closed` = false) live in `docs/_GLOSSARY.md`.
#[derive(Debug, Clone)]
pub struct CopyEntryGateConfig {
    /// Inclusive lower bound on the leader's entry price.
    pub price_band_lo: Price,
    /// Inclusive upper bound on the leader's entry price.
    pub price_band_hi: Price,
    /// When a wallet is absent from the history map: `false` admits the entry
    /// (fail-open, treat as new), `true` rejects it (fail-closed).
    pub fail_closed: bool,
}

/// Pure, synchronous gate enforcing the band-cohort copy-scope criteria.
///
/// Built once at startup from a per-wallet history map; mutated in-session via
/// [`Self::record_entry`] so same-session re-entries are also blocked.
pub struct CopyEntryGate {
    config: CopyEntryGateConfig,
    /// For each leader wallet, the set of markets it has already entered. A wallet
    /// absent from the map has unknown history (governed by `fail_closed`).
    history: HashMap<WalletAddress, HashSet<MarketId>>,
}

impl CopyEntryGate {
    /// Build a gate from its config and the startup-loaded per-wallet history.
    pub fn new(
        config: CopyEntryGateConfig,
        history: HashMap<WalletAddress, HashSet<MarketId>>,
    ) -> Self {
        Self { config, history }
    }

    /// Returns `None` to admit the signal, or `Some(reason)` to reject it.
    ///
    /// Checks, in order: the action is an `Entry` → price ≥ `price_band_lo` →
    /// price ≤ `price_band_hi` → first-entry (the leader has not entered this
    /// market before). A wallet absent from the history map is admitted when
    /// `fail_closed` is false (treat as new) and rejected otherwise.
    ///
    /// The wallet is resolved as `signal.leader.0` (`LeaderSignal.leader` is a
    /// `TraderId(WalletAddress)`), equal to the `trade.wallet` used elsewhere in
    /// the copy path. The band is checked against `signal.leader_price` — the
    /// leader's entry price, which is the selection criterion.
    pub fn admit(&self, signal: &LeaderSignal) -> Option<GateReject> {
        if signal.action != LeaderAction::Entry {
            return Some(GateReject::NotAnEntry);
        }
        if signal.leader_price < self.config.price_band_lo {
            return Some(GateReject::PriceBelowBand);
        }
        if signal.leader_price > self.config.price_band_hi {
            return Some(GateReject::PriceAboveBand);
        }
        let wallet = signal.leader.0;
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

    /// Record an observed entry so a same-session re-entry into `market` is blocked.
    ///
    /// Called for every admitted `Entry` — including on no-fill and even when a
    /// later gate or the strategy rejects the signal — so duplicate entries within
    /// one run are dropped regardless of downstream outcome.
    pub fn record_entry(&mut self, wallet: WalletAddress, market: &MarketId) {
        self.history
            .entry(wallet)
            .or_default()
            .insert(market.clone());
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
        WalletAddress, WinnerFollowSignalKind,
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
            operator_id: None,
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
            signal_kind: WinnerFollowSignalKind::NormalLeaderFollow,
            inherited_prior: None,
            source_trade_id: SourceTradeId("t1".to_string()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        }
    }

    /// Default band [0.40, 0.80], fail-open.
    fn band_config() -> CopyEntryGateConfig {
        CopyEntryGateConfig {
            price_band_lo: Price(dec!(0.40)),
            price_band_hi: Price(dec!(0.80)),
            fail_closed: false,
        }
    }

    #[test]
    fn admits_first_entry_in_band() {
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
    fn rejects_below_band() {
        let gate = CopyEntryGate::new(band_config(), HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.39)));
        assert_eq!(gate.admit(&s), Some(GateReject::PriceBelowBand));
    }

    #[test]
    fn rejects_above_band() {
        let gate = CopyEntryGate::new(band_config(), HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.81)));
        assert_eq!(gate.admit(&s), Some(GateReject::PriceAboveBand));
    }

    #[test]
    fn band_bounds_inclusive() {
        let gate = CopyEntryGate::new(band_config(), HashMap::new());
        let lo = signal(LeaderAction::Entry, market("0xa"), Price(dec!(0.40)));
        let hi = signal(LeaderAction::Entry, market("0xb"), Price(dec!(0.80)));
        assert_eq!(gate.admit(&lo), None);
        assert_eq!(gate.admit(&hi), None);
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
        let cfg = CopyEntryGateConfig {
            fail_closed: false,
            ..band_config()
        };
        let gate = CopyEntryGate::new(cfg, HashMap::new());
        let s = signal(LeaderAction::Entry, market("0xnew"), Price(dec!(0.60)));
        assert_eq!(gate.admit(&s), None);
    }

    #[test]
    fn fail_closed_rejects_absent_wallet() {
        let cfg = CopyEntryGateConfig {
            fail_closed: true,
            ..band_config()
        };
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
}
