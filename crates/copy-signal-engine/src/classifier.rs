//! Trade classification logic.

use pe_core_types::{
    LeaderAction, MarketOutcomeId, ProbabilityPpm, ReconstructionQuality, ShareAmount, Side,
    TraderId, VenueId, WalletAddress,
};
use pe_trader_index::{Watchlist, WatchlistTier};

use crate::{
    config::SignalConfig,
    signal::LeaderSignal,
    snapshot::{IncomingTrade, PositionSnapshot},
};

/// Classify an incoming leader trade from a watchlisted leader into a [`LeaderSignal`].
///
/// Returns `None` if the wallet is not on the active watchlist, or if the classified
/// action is ineligible (an `Unknown` action, or a low-confidence `Add`/`Trim`/`Exit`).
pub fn classify_trade(
    trade: &IncomingTrade,
    position: Option<&PositionSnapshot>,
    watchlist: &Watchlist,
    reconstruction_quality: ReconstructionQuality,
    venue: VenueId,
    config: &SignalConfig,
) -> Option<LeaderSignal> {
    if !is_on_active_watchlist(trade.wallet, watchlist) {
        return None;
    }

    let action = classify_leader_action(trade, position, reconstruction_quality, config);
    let action_confidence_ppm = confidence_from_quality(reconstruction_quality);

    if !is_action_eligible(action, action_confidence_ppm, config) {
        return None;
    }

    Some(LeaderSignal {
        leader: TraderId(trade.wallet),
        venue,
        market_id: trade.market_id.clone(),
        outcome_id: trade.outcome_id,
        action,
        leader_side: trade.side,
        leader_price: trade.price,
        leader_size: trade.contracts,
        observed_at: trade.observed_at,
        received_at: trade.received_at,
        reconstruction_quality,
        source_trade_id: trade.source_trade_id.clone(),
        action_confidence_ppm,
    })
}

/// Classify the action from the current position state.
///
/// Returns [`LeaderAction::Unknown`] only when reconstruction quality is zero and no
/// position data is available — i.e., we have no information about the wallet's state.
pub fn classify_leader_action(
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

    let qty = trade.contracts;

    match trade.side {
        Side::Buy => {
            if state.short_contracts == ShareAmount::ZERO {
                if state.long_contracts == ShareAmount::ZERO {
                    LeaderAction::Entry
                } else {
                    LeaderAction::Add
                }
            } else if qty > state.short_contracts {
                LeaderAction::Flip
            } else {
                let remaining = ShareAmount::from_atomic(
                    state.short_contracts.atomic().saturating_sub(qty.atomic()),
                );
                if is_near_close(remaining, state.short_contracts, config) {
                    LeaderAction::Exit
                } else {
                    LeaderAction::Trim
                }
            }
        }
        Side::Sell => {
            if state.long_contracts == ShareAmount::ZERO {
                if state.short_contracts == ShareAmount::ZERO {
                    LeaderAction::Entry
                } else {
                    LeaderAction::Add
                }
            } else if qty > state.long_contracts {
                LeaderAction::Flip
            } else {
                let remaining = ShareAmount::from_atomic(
                    state.long_contracts.atomic().saturating_sub(qty.atomic()),
                );
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
/// Cross-multiplies in `u128` so untrusted near-`u64::MAX` atomic quantities compare
/// exactly instead of saturating into an Exit/Trim misclassification (#544 review).
fn is_near_close(remaining: ShareAmount, original: ShareAmount, config: &SignalConfig) -> bool {
    if original == ShareAmount::ZERO {
        return true;
    }
    let pct = u128::from(config.near_close_remaining_pct);
    u128::from(remaining.atomic()) * 100 <= u128::from(original.atomic()) * pct
}

fn is_on_active_watchlist(wallet: WalletAddress, watchlist: &Watchlist) -> bool {
    watchlist
        .entries
        .iter()
        .any(|e| e.wallet == wallet && e.tier == WatchlistTier::Active)
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
