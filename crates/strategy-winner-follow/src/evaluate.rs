//! Core strategy evaluation: signal → Kelly → risk gate → `OrderIntent`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, KellyFraction, LeaderAction, Price, Probability, Side, StrategyId,
};
use pe_kelly_sizer::{KELLY_NORMAL, KELLY_PAPER_BACKTEST, KellyInput, size_contracts};
use pe_risk_engine::{RiskDecision, RiskSnapshot, clamp_contracts_to_cap, evaluate_risk};
use pe_venue_core::OrderIntent;

use crate::{
    WinnerFollowConfig, WinnerFollowError,
    mode::{ExecutionMode, to_risk_trading_mode},
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

    /// Replace the strategy config in place. The orchestrator calls this per event to apply the
    /// latest Supabase-authoritative runtime config (#398 WS1) without reconstructing the strategy.
    pub fn set_config(&mut self, config: WinnerFollowConfig) {
        self.config = config;
    }

    /// Evaluate a leader signal at the leader's own entry price.
    ///
    /// Thin wrapper over [`Self::evaluate_at_price`] that sizes against
    /// `signal.leader_price` — the historical/replay basis. The live copy path calls
    /// [`Self::evaluate_at_price`] with the *current* market price instead (issue #339);
    /// backtest/replay keep this leader-price basis so their output is unchanged.
    pub fn evaluate(
        &self,
        signal: &LeaderSignal,
        p: Probability,
        snapshot: RiskSnapshot,
        bankroll: Decimal,
        mode: ExecutionMode,
    ) -> Result<OrderIntent, WinnerFollowError> {
        self.evaluate_at_price(signal, signal.leader_price, p, snapshot, bankroll, mode)
    }

    /// Evaluate a leader signal at an explicit `current_price` and produce an `OrderIntent`
    /// if all gates pass.
    ///
    /// Steps:
    /// 1. Gate Flip actions on `flip_human_approved`.
    /// 2. Use the requested `mode` directly as the effective mode (no signal-kind clamping).
    /// 3. Return `Err(ShadowMode)` for Shadow — no order emitted.
    /// 4. Size contracts: flat (`config.flat_usd_per_trade` is `Some`) or fractional Kelly.
    ///    Flat path: `max(1, floor(flat / current_price))` — bypasses Kelly fraction and `p`.
    ///    Kelly path: select fraction for the mode, compute cost-adjusted `c`, call `size_contracts`.
    /// 5. Clamp contracts to `per_trade_cap`; return `NoEdge` if clamped to 0.
    /// 6. Gate on risk snapshot.
    /// 7. Build and return `OrderIntent`.
    ///
    /// `current_price` — the price the copy is sized and cost-adjusted against (the leader's
    /// entry price in replay; the live market price on the copy path). The emitted
    /// `limit_price` stays at `signal.leader_price` regardless (don't-chase).
    ///
    /// `p` — empirical win rate supplied by caller. Used only on the Kelly path; ignored
    /// when `config.flat_usd_per_trade` is set.
    ///
    /// `c` — computed internally as `current_price + taker fee + slippage` for BUY orders.
    /// `fee_per_share = current_price × fee_rate`; `slippage_per_share = current_price × slippage_rate`.
    /// SELL orders pay neither. See `_GLOSSARY.md` `polymarket_fee_rate`, `slippage_rate`.
    pub fn evaluate_at_price(
        &self,
        signal: &LeaderSignal,
        current_price: Price,
        p: Probability,
        mut snapshot: RiskSnapshot,
        bankroll: Decimal,
        mode: ExecutionMode,
    ) -> Result<OrderIntent, WinnerFollowError> {
        // 1. Flip gate.
        if signal.action == LeaderAction::Flip && !self.config.flip_human_approved {
            return Err(WinnerFollowError::FlipNotApproved);
        }

        // 2. Effective mode = requested mode (no per-signal-kind clamping).
        let effective_mode = mode;

        // 3. Shadow → record only, no order.
        if effective_mode == ExecutionMode::Shadow {
            return Err(WinnerFollowError::ShadowMode);
        }

        // 4–5. Size contracts: flat path or fractional Kelly.
        let trading_mode = to_risk_trading_mode(effective_mode);
        let raw_contracts: u64 = if let Some(flat) = self.config.flat_usd_per_trade {
            // Flat path: bypass Kelly fraction + size_contracts.
            // Per-trade cap (5b) and risk gate (6) remain active below.
            (flat / current_price.0)
                .floor()
                .to_u64()
                .unwrap_or(1)
                .max(1)
        } else {
            // Kelly path.
            // 4. Kelly fraction.
            let kf = kelly_fraction(effective_mode, self.config.kelly_fraction_override);

            // 5. Size contracts.
            // c = current_price + Polymarket BUY taker fee + expected fill slippage (SELL pays neither).
            // fee_per_share = current_price × fee_rate (flat taker fee on notional).
            // slippage_per_share = current_price × slippage_rate (proportional fill impact on BUY).
            let fee_per_share = if signal.leader_side == Side::Buy {
                current_price.0 * self.config.polymarket_fee_rate
            } else {
                Decimal::ZERO
            };
            let slippage_per_share = if signal.leader_side == Side::Buy {
                current_price.0 * self.config.slippage_rate
            } else {
                Decimal::ZERO
            };
            let c_raw = current_price.0 + fee_per_share + slippage_per_share;
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
            contracts.0
        };

        // 5b. Clamp to per-trade cap.
        let cap_bps = self.config.per_trade_cap.resolve_bps(trading_mode);
        let clamped = clamp_contracts_to_cap(raw_contracts, current_price.0, bankroll, cap_bps);
        // Guard: clamp returns 0 when available_bankroll < price (no fractional contracts).
        if clamped == 0 {
            return Err(WinnerFollowError::NoEdge);
        }

        // 6. Risk gate.
        snapshot.trading_mode = trading_mode;
        snapshot.proposed_trade_bps = proposed_trade_bps(clamped, current_price.0, bankroll);
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

/// Select the Kelly fraction for this effective mode.
///
/// When `override_` is `Some`, it is returned for all modes.
/// In production, `pe-service` validates at startup that any override does not exceed
/// the mode default without `kelly_fraction_above_default_human_approved = true`.
fn kelly_fraction(
    effective_mode: ExecutionMode,
    override_: Option<KellyFraction>,
) -> KellyFraction {
    if let Some(kf) = override_ {
        return kf;
    }
    match effective_mode {
        ExecutionMode::Paper => KELLY_PAPER_BACKTEST,
        ExecutionMode::LiveTiny | ExecutionMode::Promoted => KELLY_NORMAL,
        // Shadow is filtered before reaching here; fall back to most conservative.
        ExecutionMode::Shadow => KELLY_PAPER_BACKTEST,
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
/// Format: `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{bucket}`
/// where `bucket = floor(observed_at_ms / 1_000) = observed_at.unix_timestamp()`.
fn build_idempotency_key(signal: &LeaderSignal) -> String {
    let side_str = match signal.leader_side {
        pe_core_types::Side::Buy => "buy",
        pe_core_types::Side::Sell => "sell",
    };
    let bucket = signal.observed_at.unix_timestamp();
    format!(
        "wf|{}|{}|{}|{}|{}|{}",
        signal.leader,
        signal.source_trade_id.0,
        signal.market_id.0.0,
        signal.outcome_id.0,
        side_str,
        bucket,
    )
}
