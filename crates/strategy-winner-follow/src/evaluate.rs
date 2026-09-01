//! Core strategy evaluation: signal → Kelly → risk gate → `OrderIntent`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, CollateralAmount, ContractQty, KellyFraction, LeaderAction, Price, Probability,
    ShareAmount, Side, StrategyId,
};
use pe_kelly_sizer::{KELLY_NORMAL, KELLY_PAPER_BACKTEST, KellyInput, size_contracts};
use pe_risk_engine::{
    CANARY_MAX_ORDER_DEBIT, CANARY_PER_TRADE_CAP_BPS, RiskDecision, RiskSnapshot,
    clamp_contracts_to_cap, evaluate_risk,
};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};

use crate::{
    SizingMode, WinnerFollowConfig, WinnerFollowError,
    mode::{ExecutionMode, to_risk_trading_mode},
};

const STRATEGY_ID: &str = "winner-follow";
const ORDER_VALIDITY_SECONDS: u32 = 30;
const ORGANIC_CHASE_BPS: u32 = 75;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganicCanaryOrder {
    pub intent: OrderIntent,
    pub kelly_cost: Price,
    pub maximum_collateral: CollateralAmount,
    pub shares: ShareAmount,
}

/// Replay-complete inputs and evidence commitments for one organic canary policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrganicDecisionProof {
    pub signal: LeaderSignal,
    pub probability: Probability,
    pub idempotency_key: String,
    pub evidence_hashes: Vec<String>,
}

pub fn organic_decision_proof_hash(
    proof: &OrganicDecisionProof,
) -> Result<String, serde_json::Error> {
    serde_json::to_vec(proof).map(|bytes| blake3::hash(&bytes).to_hex().to_string())
}

/// Immutable, Supabase-independent sizing policy for the isolated organic canary.
#[derive(Debug, Clone, Copy, Default)]
pub struct OrganicCanaryPolicy;

impl OrganicCanaryPolicy {
    pub fn evaluate(
        self,
        signal: &LeaderSignal,
        p: Probability,
        canary_bankroll: CollateralAmount,
        minimum_tick_size: Price,
    ) -> Result<OrganicCanaryOrder, WinnerFollowError> {
        if signal.leader_side != Side::Buy || signal.action != LeaderAction::Entry {
            return Err(WinnerFollowError::NoEdge);
        }
        if minimum_tick_size.0 <= Decimal::ZERO {
            return Err(WinnerFollowError::NoEdge);
        }
        let chase = Decimal::ONE + Decimal::from(ORGANIC_CHASE_BPS) / Decimal::from(10_000u32);
        let unquantized = signal.leader_price.0 * chase;
        let quantized = (unquantized / minimum_tick_size.0).floor() * minimum_tick_size.0;
        let kelly_cost = Price::new(quantized).map_err(|_| WinnerFollowError::NoEdge)?;
        let sized = size_contracts(&KellyInput {
            p,
            c: kelly_cost,
            kelly_fraction: KELLY_NORMAL,
            bankroll: canary_bankroll.to_decimal(),
        })?
        .0;
        let contracts = clamp_contracts_to_policy_cap(
            sized,
            kelly_cost,
            canary_bankroll.to_decimal(),
            CANARY_PER_TRADE_CAP_BPS.0,
            Some(CANARY_MAX_ORDER_DEBIT),
        );
        if contracts == 0 {
            return Err(WinnerFollowError::NoEdge);
        }
        let maximum_collateral =
            CollateralAmount::from_decimal_exact(Decimal::from(contracts) * kelly_cost.0)
                .map_err(|_| WinnerFollowError::NoEdge)?;
        let shares = ShareAmount::from_atomic(
            contracts
                .checked_mul(1_000_000)
                .ok_or(WinnerFollowError::NoEdge)?,
        );
        Ok(OrganicCanaryOrder {
            intent: build_order_intent(signal, contracts, kelly_cost),
            kelly_cost,
            maximum_collateral,
            shares,
        })
    }
}

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

    /// Read the current strategy config. The orchestrator derives the impact-gate planner
    /// budget from `sizing_mode` (#508 Phase A).
    pub fn config(&self) -> &WinnerFollowConfig {
        &self.config
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
        // The leader-price wrapper (replay/backtest) applies no price-impact book cap and sizes
        // the dollar notional at the same price it cost-adjusts against (`None`).
        self.evaluate_at_price(
            signal,
            signal.leader_price,
            p,
            snapshot,
            bankroll,
            mode,
            None,
            None,
        )
    }

    /// Evaluate a leader signal at an explicit `current_price` and produce an `OrderIntent`
    /// if all gates pass.
    ///
    /// Steps:
    /// 1. Gate Flip actions on `flip_human_approved`.
    /// 2. Use the requested `mode` directly as the effective mode (no signal-kind clamping).
    /// 3. Return `Err(ShadowMode)` for Shadow — no order emitted.
    /// 4. Size contracts by `config.sizing_mode`:
    ///    `Dollar { usd }` → `max(1, floor(usd / dollar_sizing_price ?? current_price))` (bypasses Kelly + `p`);
    ///    `Contract { contracts }` → exactly `contracts` (bypasses Kelly + price math);
    ///    `Kelly` → select the mode fraction, compute cost-adjusted `c`, call `size_contracts`.
    /// 5. Clamp to `per_trade_cap`, then `min` with `book_cap_contracts` (the price-impact book
    ///    cap, #398 WS2). Return `NoEdge` if the result is 0 (per-trade cap exhausted, or a
    ///    `Some(0)` book cap → trade skipped).
    /// 6. Gate on risk snapshot.
    /// 7. Build and return `OrderIntent`.
    ///
    /// `current_price` — the RAW (fee-exclusive) per-share price used only to derive the Kelly
    /// cost `c` (which adds fee + slippage on top). In replay/backtest it is the leader's entry
    /// price; on the live copy path the caller passes the leader's just-executed trade price (a
    /// reliable current-price proxy — the Gamma mid is unreliable, #484). The emitted
    /// `limit_price` stays at `signal.leader_price` regardless (don't-chase).
    ///
    /// `dollar_sizing_price` — the realistic per-share COST of the position (leader + haircut on
    /// the live path). When `Some(p)`, it is the divisor for `SizingMode::Dollar` (`floor(usd /
    /// p)` → `contracts × fill == usd`) AND the basis for the per-trade cap and exposure bps, so
    /// all three bound the real money at risk; `current_price` stays fee-exclusive for the Kelly
    /// `c`, avoiding a double-count. `None` falls back to `current_price` for all of them
    /// (replay/backtest, which size and cost against one price). No effect on the Kelly fraction
    /// or the Contract arm's count.
    ///
    /// `p` — empirical win rate supplied by caller. Used only when `sizing_mode` is `Kelly`.
    ///
    /// `book_cap_contracts` — contracts absorbable within `price_impact_cap_bps` of best ask, from
    /// the live `/book` (#398 WS2). `None` = no book result / gate disabled (fail-open passthrough);
    /// `Some(0)` = a successful read with nothing absorbable (the trade is skipped, distinct from
    /// fail-open). The orchestrator computes it; replay/backtest pass `None`.
    ///
    /// `c` — computed internally as `current_price + taker fee + slippage` for BUY orders.
    /// `fee_per_share = current_price × fee_rate`; `slippage_per_share = current_price × slippage_rate`.
    /// SELL orders pay neither. See `_GLOSSARY.md` `polymarket_fee_rate`, `slippage_rate`.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_at_price(
        &self,
        signal: &LeaderSignal,
        current_price: Price,
        p: Probability,
        mut snapshot: RiskSnapshot,
        bankroll: Decimal,
        mode: ExecutionMode,
        book_cap_contracts: Option<u64>,
        dollar_sizing_price: Option<Price>,
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

        // `notional_price` = the realistic per-share cost of the position, used for the dollar
        // divisor AND the per-trade cap / exposure bps so all three agree on the money at risk.
        // When the caller supplies `dollar_sizing_price` (the live path's fill price = leader +
        // haircut), everything is `contracts × fill`-accurate; else it falls back to
        // `current_price` (replay/backtest, which cost against the same price they size at).
        // `current_price` itself stays the RAW price for the Kelly fee model below (so the
        // fee-additive `c` is not double-counted against a fee-inclusive price). Guard a
        // degenerate zero (`Price::new` admits 0) before it can divide-by-zero.
        let notional_price = dollar_sizing_price.unwrap_or(current_price);
        if notional_price.0.is_zero() {
            return Err(WinnerFollowError::NoEdge);
        }

        // 4–5. Size contracts by sizing mode.
        let trading_mode = to_risk_trading_mode(effective_mode);
        let raw_contracts: u64 = match self.config.sizing_mode {
            SizingMode::Dollar { usd } => {
                // Fixed USD notional: bypass Kelly fraction + size_contracts. `contracts × fill
                // == usd`. The per-trade cap (5b), book cap (5c), and risk gate (6) remain active.
                (usd / notional_price.0)
                    .floor()
                    .to_u64()
                    .unwrap_or(1)
                    .max(1)
            }
            SizingMode::Contract { contracts } => {
                // Exactly N contracts: bypass Kelly + price math. Downstream caps still apply; a
                // configured 0 falls through to the clamped==0 skip below.
                contracts
            }
            SizingMode::Kelly => {
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
            }
        };

        // 5b. Clamp to per-trade cap — against `notional_price` (the real per-share cost), so
        // the cap bounds `contracts × fill`, the actual money at risk (not the fee-exclusive
        // raw price).
        let cap_bps = self.config.per_trade_cap.resolve_bps(trading_mode);
        let capped =
            clamp_contracts_to_policy_cap(raw_contracts, notional_price, bankroll, cap_bps, None);
        // 5c. Price-impact book cap (#398 WS2): `min` with the contracts absorbable within
        // `price_impact_cap_bps` of best ask. `None` = no `/book` result / gate off (fail-open
        // passthrough); `Some(0)` = a successful read with nothing absorbable → clamp to 0 → skip.
        let clamped = match book_cap_contracts {
            Some(n) => capped.min(n),
            None => capped,
        };
        // Guard: 0 from the per-trade cap (available_bankroll < price) OR a Some(0) book cap → skip.
        if clamped == 0 {
            return Err(WinnerFollowError::NoEdge);
        }

        // 6. Risk gate. Exposure bps on `notional_price` (the real per-share cost).
        snapshot.trading_mode = trading_mode;
        snapshot.proposed_trade_bps = proposed_trade_bps(clamped, notional_price.0, bankroll);
        snapshot.per_trade_cap_bps = cap_bps;

        match evaluate_risk(&snapshot) {
            RiskDecision::Approved => {}
            RiskDecision::Blocked(reason) => return Err(WinnerFollowError::Blocked(reason)),
        }

        // 7. Build OrderIntent.
        Ok(build_order_intent(signal, clamped, signal.leader_price))
    }
}

fn clamp_contracts_to_policy_cap(
    contracts: u64,
    price: Price,
    bankroll: Decimal,
    cap_bps: i32,
    absolute_cap: Option<CollateralAmount>,
) -> u64 {
    let proportional = clamp_contracts_to_cap(contracts, price.0, bankroll, cap_bps);
    absolute_cap.map_or(proportional, |cap| {
        let absolute_contracts = (cap.to_decimal() / price.0).floor().to_u64().unwrap_or(0);
        proportional.min(absolute_contracts)
    })
}

fn build_order_intent(signal: &LeaderSignal, contracts: u64, limit_price: Price) -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId(STRATEGY_ID.to_owned()),
        market_id: signal.market_id.clone(),
        outcome_id: signal.outcome_id,
        side: signal.leader_side,
        contracts: ContractQty(contracts),
        limit_price,
        validity_seconds: ORDER_VALIDITY_SECONDS,
        idempotency_key: build_idempotency_key(signal),
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

/// Compute the positive proposed exposure in basis points, rounded outward.
///
/// Exposure is a safety limit, so any positive fractional basis point counts as the next whole
/// basis point. This prevents one atomic unit above a cap from appearing to be within it.
///
/// Returns `BasisPoints(0)` if the bankroll is zero or conversion fails.
fn proposed_trade_bps(contracts: u64, price: Decimal, bankroll: Decimal) -> BasisPoints {
    if bankroll <= Decimal::ZERO {
        return BasisPoints(0);
    }
    let notional = Decimal::from(contracts) * price;
    let bps_decimal = (notional / bankroll) * Decimal::from(10_000u32);
    BasisPoints(bps_decimal.ceil().to_i32().unwrap_or(i32::MAX))
}

/// Build the idempotency key per `_GLOSSARY.md` — public so the #508 dispatch aggregate
/// can derive its `dispatch_id` from the same canonical identity the order intent carries.
///
/// Format: `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{bucket}`
/// where `bucket = floor(observed_at_ms / 1_000) = observed_at.unix_timestamp()`.
pub fn build_idempotency_key(signal: &LeaderSignal) -> String {
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

#[cfg(test)]
mod canary_tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{
        OutcomeId, ProbabilityPpm, ReconstructionQuality, ShareAmount, SourceTradeId, TraderId,
        VenueId, VenueMarketId, WalletAddress,
    };
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    use super::*;

    fn signal() -> LeaderSignal {
        let wallet: WalletAddress =
            serde_json::from_str("\"0x1111111111111111111111111111111111111111\"").unwrap();
        LeaderSignal {
            leader: TraderId(wallet),
            venue: VenueId::polymarket(),
            market_id: pe_core_types::MarketId(VenueMarketId("0xcondition".to_owned())),
            outcome_id: OutcomeId(0),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: Price(dec!(0.50)),
            leader_size: ShareAmount::from_whole(10).unwrap(),
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("trade".to_owned()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        }
    }

    #[test]
    fn organic_policy_quantizes_chase_down_and_caps_at_one_dollar() {
        let order = OrganicCanaryPolicy
            .evaluate(
                &signal(),
                Probability(dec!(0.90)),
                CollateralAmount::from_atomic(200_000_000),
                Price(dec!(0.01)),
            )
            .unwrap();
        assert_eq!(order.kelly_cost, Price(dec!(0.50)));
        assert_eq!(order.intent.contracts, ContractQty(2));
        assert_eq!(order.maximum_collateral.atomic(), 1_000_000);
        assert_eq!(order.shares.atomic(), 2_000_000);
    }

    #[test]
    fn organic_policy_uses_the_lower_dynamic_cap_after_bankroll_declines() {
        let order = OrganicCanaryPolicy
            .evaluate(
                &signal(),
                Probability(dec!(0.90)),
                CollateralAmount::from_atomic(199_000_000),
                Price(dec!(0.01)),
            )
            .unwrap();
        assert_eq!(order.intent.contracts, ContractQty(1));
        assert_eq!(order.maximum_collateral.atomic(), 500_000);
    }

    #[test]
    fn organic_policy_rejects_non_entry_and_non_buy() {
        let mut ineligible = signal();
        ineligible.action = LeaderAction::Add;
        assert!(
            OrganicCanaryPolicy
                .evaluate(
                    &ineligible,
                    Probability(dec!(0.90)),
                    CollateralAmount::from_atomic(200_000_000),
                    Price(dec!(0.01)),
                )
                .is_err()
        );
    }

    #[test]
    fn organic_decision_proof_hash_binds_signal_policy_identity_and_evidence() {
        let signal = signal();
        let evaluated = OrganicCanaryPolicy
            .evaluate(
                &signal,
                Probability(dec!(0.75)),
                CollateralAmount::from_atomic(200_000_000),
                Price(dec!(0.01)),
            )
            .unwrap();
        let mut proof = OrganicDecisionProof {
            signal,
            probability: Probability(dec!(0.75)),
            idempotency_key: evaluated.intent.idempotency_key,
            evidence_hashes: vec!["evidence-envelope".to_owned()],
        };
        let expected = organic_decision_proof_hash(&proof).unwrap();
        proof.evidence_hashes.push("forged".to_owned());
        assert_ne!(organic_decision_proof_hash(&proof).unwrap(), expected);
    }
}
