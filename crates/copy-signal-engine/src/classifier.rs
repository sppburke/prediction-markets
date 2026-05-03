//! Trade classification logic.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_core_types::{
    InheritedPriorPpm, LeaderAction, MarketOutcomeId, OperatorId, ProbabilityPpm, Quantity,
    ReconstructionQuality, Side, TraderId, VenueId, WalletAddress, WinnerFollowSignalKind,
};
use pe_trader_index::{Watchlist, WatchlistTier};

use crate::{
    config::SignalConfig,
    signal::LeaderSignal,
    snapshot::{ClusterObs, IncomingTrade, PositionSnapshot, WalletProfile},
};

/// Classify an incoming leader trade into a [`LeaderSignal`].
///
/// Returns `None` if the wallet qualifies under no signal kind (not on the active
/// watchlist, not a fresh-wallet first trade, not a cluster-coordination event).
#[allow(clippy::too_many_arguments)]
pub fn classify_trade(
    trade: &IncomingTrade,
    position: Option<&PositionSnapshot>,
    watchlist: &Watchlist,
    wallet_profile: &WalletProfile,
    cluster_obs: Option<&ClusterObs>,
    operator_id: Option<OperatorId>,
    reconstruction_quality: ReconstructionQuality,
    venue: VenueId,
    config: &SignalConfig,
) -> Option<LeaderSignal> {
    let signal_kind = classify_signal_kind(
        trade,
        watchlist,
        wallet_profile,
        cluster_obs,
        operator_id,
        config,
    )?;

    let action = classify_action(trade, position, reconstruction_quality, config);
    let action_confidence_ppm = confidence_from_quality(reconstruction_quality);

    if !is_action_eligible(action, action_confidence_ppm, config) {
        return None;
    }

    // Placeholder: real shrinkage computed by model-calibrated weights per 03-PHASE-MODEL-ENGINE.md.
    let inherited_prior = (signal_kind == WinnerFollowSignalKind::FreshWalletFirstTrade)
        .then_some(InheritedPriorPpm(0));

    Some(LeaderSignal {
        leader: TraderId(trade.wallet),
        operator_id,
        venue,
        market_id: trade.market_id.clone(),
        outcome_id: trade.outcome_id,
        action,
        leader_side: trade.side,
        leader_price: trade.price,
        leader_size: Quantity(trade.contracts),
        observed_at: trade.observed_at,
        received_at: trade.received_at,
        reconstruction_quality,
        signal_kind,
        inherited_prior,
        source_trade_id: trade.source_trade_id.clone(),
        action_confidence_ppm,
    })
}

/// Determine the signal kind in priority order: Cluster > FreshWallet > NormalLeaderFollow.
fn classify_signal_kind(
    trade: &IncomingTrade,
    watchlist: &Watchlist,
    profile: &WalletProfile,
    cluster_obs: Option<&ClusterObs>,
    operator_id: Option<OperatorId>,
    config: &SignalConfig,
) -> Option<WinnerFollowSignalKind> {
    if is_cluster_coordination(trade, cluster_obs, config) {
        return Some(WinnerFollowSignalKind::ClusterCoordination);
    }
    if is_fresh_wallet_first_trade(trade, profile, operator_id, config) {
        return Some(WinnerFollowSignalKind::FreshWalletFirstTrade);
    }
    if is_on_active_watchlist(trade.wallet, watchlist) {
        return Some(WinnerFollowSignalKind::NormalLeaderFollow);
    }
    None
}

/// Classify the action from the current position state.
///
/// Returns [`LeaderAction::Unknown`] only when reconstruction quality is zero and no
/// position data is available — i.e., we have no information about the wallet's state.
fn classify_action(
    trade: &IncomingTrade,
    position: Option<&PositionSnapshot>,
    reconstruction_quality: ReconstructionQuality,
    config: &SignalConfig,
) -> LeaderAction {
    let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);

    let state = position
        .and_then(|p| p.positions.get(&key))
        .copied()
        .unwrap_or_default();

    if reconstruction_quality.get() == 0 && position.is_none() {
        return LeaderAction::Unknown;
    }

    let qty = trade.contracts.0;

    match trade.side {
        Side::Buy => {
            if state.short_contracts == 0 {
                if state.long_contracts == 0 {
                    LeaderAction::Entry
                } else {
                    LeaderAction::Add
                }
            } else if qty > state.short_contracts {
                LeaderAction::Flip
            } else {
                let remaining = state.short_contracts.saturating_sub(qty);
                if is_near_close(remaining, state.short_contracts, config) {
                    LeaderAction::Exit
                } else {
                    LeaderAction::Trim
                }
            }
        }
        Side::Sell => {
            if state.long_contracts == 0 {
                if state.short_contracts == 0 {
                    LeaderAction::Entry
                } else {
                    LeaderAction::Add
                }
            } else if qty > state.long_contracts {
                LeaderAction::Flip
            } else {
                let remaining = state.long_contracts.saturating_sub(qty);
                if is_near_close(remaining, state.long_contracts, config) {
                    LeaderAction::Exit
                } else {
                    LeaderAction::Trim
                }
            }
        }
    }
}

/// `remaining / original ≤ near_close_remaining_pct / 100` using integer arithmetic.
fn is_near_close(remaining: u64, original: u64, config: &SignalConfig) -> bool {
    if original == 0 {
        return true;
    }
    remaining.saturating_mul(100) <= original.saturating_mul(config.near_close_remaining_pct as u64)
}

fn is_cluster_coordination(
    trade: &IncomingTrade,
    cluster_obs: Option<&ClusterObs>,
    config: &SignalConfig,
) -> bool {
    let Some(obs) = cluster_obs else {
        return false;
    };

    if obs.market_id != trade.market_id
        || obs.outcome_id != trade.outcome_id
        || obs.side != trade.side
    {
        return false;
    }

    let trade_unix = trade.observed_at.unix_timestamp();
    let window_start = trade_unix.saturating_sub(config.cluster_coord_window_seconds as i64);

    let mut member_count: u32 = 0;
    let mut aggregate_notional_usd: u64 = 0;

    for entry in &obs.wallet_entries {
        if entry.observed_at_unix >= window_start && entry.observed_at_unix <= trade_unix {
            member_count += 1;
            let notional = price_times_contracts_usd(entry.price.0, entry.contracts.0);
            aggregate_notional_usd = aggregate_notional_usd.saturating_add(notional);
        }
    }

    member_count >= config.cluster_coord_min_members
        && aggregate_notional_usd >= config.cluster_coord_min_aggregate_usd as u64
}

fn is_fresh_wallet_first_trade(
    trade: &IncomingTrade,
    profile: &WalletProfile,
    operator_id: Option<OperatorId>,
    config: &SignalConfig,
) -> bool {
    if operator_id.is_none() {
        return false;
    }
    if profile.closed_trade_count > config.fresh_wallet_max_closed_trades {
        return false;
    }
    if profile.age_seconds > config.fresh_wallet_max_age_seconds {
        return false;
    }
    let notional = price_times_contracts_usd(trade.price.0, trade.contracts.0);
    notional >= config.inherited_prior_min_position_usd as u64
}

fn is_on_active_watchlist(wallet: WalletAddress, watchlist: &Watchlist) -> bool {
    watchlist
        .entries
        .iter()
        .any(|e| e.wallet == wallet && e.tier == WatchlistTier::Active)
}

/// Compute `floor(price * contracts)` in whole USD using integer-safe Decimal arithmetic.
///
/// Returns 0 on overflow or if the conversion fails (price is guaranteed in [0,1]).
fn price_times_contracts_usd(price: Decimal, contracts: u64) -> u64 {
    (price * Decimal::from(contracts)).to_u64().unwrap_or(0)
}

/// Gate on action type and confidence before emitting a signal.
///
/// - `Unknown` is always suppressed (no usable position information).
/// - `Add` requires confidence ≥ `add_high_confidence_threshold_ppm`.
/// - `Trim`/`Exit` require confidence ≥ `exit_high_confidence_threshold_ppm`.
/// - `Entry` and `Flip` pass through unconditionally.
fn is_action_eligible(
    action: LeaderAction,
    confidence_ppm: ProbabilityPpm,
    config: &SignalConfig,
) -> bool {
    match action {
        LeaderAction::Unknown => false,
        LeaderAction::Add => confidence_ppm.0 >= config.add_high_confidence_threshold_ppm,
        LeaderAction::Trim | LeaderAction::Exit => {
            confidence_ppm.0 >= config.exit_high_confidence_threshold_ppm
        }
        LeaderAction::Entry | LeaderAction::Flip => true,
    }
}

/// `quality ∈ [0, 100]` → `ppm ∈ [0, 1_000_000]`.
///
/// Linear proxy for reconstruction fidelity. Model-calibrated weights
/// are deferred to `03-PHASE-MODEL-ENGINE.md`.
fn confidence_from_quality(quality: ReconstructionQuality) -> ProbabilityPpm {
    ProbabilityPpm(quality.get() as u32 * 10_000)
}
