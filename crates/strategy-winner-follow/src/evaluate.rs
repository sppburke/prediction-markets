//! Core strategy evaluation: signal → Kelly → risk gate → `OrderIntent`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, KellyFraction, LeaderAction, Probability, StrategyId, WinnerFollowSignalKind,
};
use pe_kelly_sizer::{
    KELLY_CLUSTER_COORDINATION, KELLY_INHERITED_PRIOR, KELLY_NORMAL, KELLY_PAPER_BACKTEST,
    KellyInput, size_contracts,
};
use pe_risk_engine::{RiskDecision, RiskSnapshot, evaluate_risk};
use pe_venue_core::OrderIntent;

use crate::{
    WinnerFollowConfig, WinnerFollowError,
    mode::{ExecutionMode, clamp_mode, to_risk_trading_mode},
};

const STRATEGY_ID: &str = "winner-follow";
const ORDER_VALIDITY_SECONDS: u32 = 30;

/// Winner-Follow strategy evaluator.
///
/// Holds the approval-flag config; all other inputs are passed per-signal.
pub struct WinnerFollowStrategy {
    config: WinnerFollowConfig,
}

impl WinnerFollowStrategy {
    pub fn new(config: WinnerFollowConfig) -> Self {
        Self { config }
    }

    /// Evaluate a leader signal and produce an `OrderIntent` if all gates pass.
    ///
    /// Steps:
    /// 1. Gate Flip actions on `flip_human_approved`.
    /// 2. Clamp `mode` to the ceiling imposed by `signal.signal_kind`.
    /// 3. Return `Err(ShadowMode)` for Shadow — no order emitted.
    /// 4. Select the Kelly fraction for this mode + signal kind.
    /// 5. Size contracts using fractional Kelly.
    /// 6. Gate on risk snapshot.
    /// 7. Build and return `OrderIntent`.
    ///
    /// `p` = `leader_price + leader_alpha` (capped at 1.0). Model-calibrated `p` deferred to
    /// `03-PHASE-MODEL-ENGINE.md`.
    ///
    /// `c` placeholder: `signal.leader_price` is used as the net price (pre-fee/slippage).
    /// Cost-model adjustment is deferred to `05-PHASE-BACKTESTING.md`.
    pub fn evaluate(
        &self,
        signal: &LeaderSignal,
        mut snapshot: RiskSnapshot,
        bankroll: Decimal,
        mode: ExecutionMode,
    ) -> Result<OrderIntent, WinnerFollowError> {
        // 1. Flip gate.
        if signal.action == LeaderAction::Flip && !self.config.flip_human_approved {
            return Err(WinnerFollowError::FlipNotApproved);
        }

        // 2. Clamp mode to signal-kind ceiling.
        let effective_mode = clamp_mode(mode, signal.signal_kind);

        // 3. Shadow → record only, no order.
        if effective_mode == ExecutionMode::Shadow {
            return Err(WinnerFollowError::ShadowMode);
        }

        // 4. Kelly fraction.
        let kf = kelly_fraction(signal.signal_kind, effective_mode);

        // 5. Size contracts.
        // p = leader_price + alpha, capped at 1.0. Model-calibrated p deferred to Phase 3.
        let p = Probability((signal.leader_price.0 + self.config.leader_alpha).min(Decimal::ONE));
        let c = signal.leader_price;

        let kelly_input = KellyInput {
            p,
            c,
            kelly_fraction: kf,
            bankroll,
        };
        let contracts = size_contracts(&kelly_input)?;

        if contracts.0 == 0 {
            return Err(WinnerFollowError::NoEdge);
        }

        // 6. Risk gate.
        snapshot.trading_mode = to_risk_trading_mode(effective_mode);
        snapshot.proposed_trade_bps =
            proposed_trade_bps(contracts.0, signal.leader_price.0, bankroll);

        match evaluate_risk(&snapshot) {
            RiskDecision::Approved => {}
            RiskDecision::Blocked(reason) => return Err(WinnerFollowError::Blocked(reason)),
        }

        // 7. Build OrderIntent.
        let idempotency_key = build_idempotency_key(signal);

        Ok(OrderIntent {
            strategy_id: StrategyId(STRATEGY_ID.to_string()),
            market_id: signal.market_id.clone(),
            outcome_id: signal.outcome_id,
            side: signal.leader_side,
            contracts,
            limit_price: signal.leader_price,
            validity_seconds: ORDER_VALIDITY_SECONDS,
            idempotency_key,
        })
    }
}

/// Select the Kelly fraction for this signal kind and effective mode.
fn kelly_fraction(
    signal_kind: WinnerFollowSignalKind,
    effective_mode: ExecutionMode,
) -> KellyFraction {
    match signal_kind {
        WinnerFollowSignalKind::FreshWalletFirstTrade => KELLY_INHERITED_PRIOR,
        WinnerFollowSignalKind::ClusterCoordination => KELLY_CLUSTER_COORDINATION,
        WinnerFollowSignalKind::NormalLeaderFollow => match effective_mode {
            ExecutionMode::Paper => KELLY_PAPER_BACKTEST,
            ExecutionMode::LiveTiny | ExecutionMode::Promoted => KELLY_NORMAL,
            // Shadow is filtered before reaching here; fall back to most conservative.
            ExecutionMode::Shadow => KELLY_PAPER_BACKTEST,
        },
    }
}

/// Compute `floor((contracts * price / bankroll) * 10_000)` as basis points.
///
/// Returns `BasisPoints(0)` if the bankroll is zero or conversion fails.
fn proposed_trade_bps(contracts: u64, price: Decimal, bankroll: Decimal) -> BasisPoints {
    if bankroll <= Decimal::ZERO {
        return BasisPoints(0);
    }
    let notional = Decimal::from(contracts) * price;
    let bps_decimal = (notional / bankroll) * Decimal::from(10_000u32);
    BasisPoints(bps_decimal.floor().to_i32().unwrap_or(0))
}

/// Build the idempotency key per `_GLOSSARY.md`.
///
/// Format: `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{bucket}[|{operator_id}]`
/// where `bucket = floor(observed_at_ms / 1_000) = observed_at.unix_timestamp()`.
fn build_idempotency_key(signal: &LeaderSignal) -> String {
    let side_str = match signal.leader_side {
        pe_core_types::Side::Buy => "buy",
        pe_core_types::Side::Sell => "sell",
    };
    let bucket = signal.observed_at.unix_timestamp();
    let base = format!(
        "wf|{}|{}|{}|{}|{}|{}",
        signal.leader,
        signal.source_trade_id.0,
        signal.market_id.0.0,
        signal.outcome_id.0,
        side_str,
        bucket,
    );
    match (signal.signal_kind, signal.operator_id) {
        (WinnerFollowSignalKind::ClusterCoordination, Some(op)) => {
            format!("{base}|{op}")
        }
        _ => base,
    }
}
