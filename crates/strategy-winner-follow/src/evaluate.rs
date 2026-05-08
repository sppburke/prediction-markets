//! Core strategy evaluation: signal → Kelly → risk gate → `OrderIntent`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, KellyFraction, LeaderAction, Price, Probability, Side, StrategyId,
    WinnerFollowSignalKind,
};
use pe_kelly_sizer::{
    KELLY_CLUSTER_COORDINATION, KELLY_INHERITED_PRIOR, KELLY_NORMAL, KELLY_PAPER_BACKTEST,
    KellyInput, size_contracts,
};
use pe_risk_engine::{RiskDecision, RiskSnapshot, clamp_contracts_to_cap, evaluate_risk};
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
    /// 5. Size contracts using fractional Kelly with caller-provided `p` and cost-adjusted `c`.
    ///    5b. Clamp contracts to `per_trade_cap`; return `NoEdge` if clamped to 0 (bankroll < price).
    /// 6. Gate on risk snapshot (per-trade-cap check is defense-in-depth under normal flow).
    /// 7. Build and return `OrderIntent`.
    ///
    /// `p` — empirical win rate supplied by caller (e.g. from `TraderLedger.closed_trades`).
    ///
    /// `c` — computed internally as `leader_price + taker fee + slippage` for BUY orders.
    /// `fee_per_share = price × fee_rate`; `slippage_per_share = price × slippage_rate`.
    /// SELL orders pay neither. See `_GLOSSARY.md` `polymarket_fee_rate`, `slippage_rate`.
    pub fn evaluate(
        &self,
        signal: &LeaderSignal,
        p: Probability,
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
        let kf = kelly_fraction(
            signal.signal_kind,
            effective_mode,
            self.config.kelly_fraction_override,
        );

        // 5. Size contracts.
        // c = leader_price + Polymarket BUY taker fee + expected fill slippage (SELL pays neither).
        // fee_per_share = price × fee_rate (flat taker fee on notional).
        // slippage_per_share = price × slippage_rate (proportional fill impact on BUY).
        let fee_per_share = if signal.leader_side == Side::Buy {
            signal.leader_price.0 * self.config.polymarket_fee_rate
        } else {
            Decimal::ZERO
        };
        let slippage_per_share = if signal.leader_side == Side::Buy {
            signal.leader_price.0 * self.config.slippage_rate
        } else {
            Decimal::ZERO
        };
        let c_raw = signal.leader_price.0 + fee_per_share + slippage_per_share;
        let c = Price::new(c_raw).map_err(|_| WinnerFollowError::NoEdge)?;

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

        // 5b. Clamp to per-trade cap.
        let trading_mode = to_risk_trading_mode(effective_mode);
        let cap_bps = self.config.per_trade_cap.resolve_bps(trading_mode);
        let clamped = clamp_contracts_to_cap(contracts.0, signal.leader_price.0, bankroll, cap_bps);
        // Guard: clamp returns 0 when available_bankroll < price (no fractional contracts).
        if clamped == 0 {
            return Err(WinnerFollowError::NoEdge);
        }

        // 6. Risk gate.
        snapshot.trading_mode = trading_mode;
        snapshot.proposed_trade_bps = proposed_trade_bps(clamped, signal.leader_price.0, bankroll);
        snapshot.per_trade_cap_bps = cap_bps;

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
            contracts: ContractQty(clamped),
            limit_price: signal.leader_price,
            validity_seconds: ORDER_VALIDITY_SECONDS,
            idempotency_key,
        })
    }
}

/// Select the Kelly fraction for this signal kind and effective mode.
///
/// When `override_` is `Some`, it is returned for all signal kinds and modes.
/// In production, `pe-service` validates at startup that any override does not exceed
/// the mode default without `kelly_fraction_above_default_human_approved = true`.
fn kelly_fraction(
    signal_kind: WinnerFollowSignalKind,
    effective_mode: ExecutionMode,
    override_: Option<KellyFraction>,
) -> KellyFraction {
    if let Some(kf) = override_ {
        return kf;
    }
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
