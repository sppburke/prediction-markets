//! Core strategy evaluation: signal → Kelly → risk gate → `OrderIntent`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    CollateralAmount, ContractQty, KellyFraction, LeaderAction, Price, Probability, ShareAmount,
    Side, StrategyId,
};
use pe_kelly_sizer::{KELLY_NORMAL, KELLY_PAPER_BACKTEST, KellyInput, size_contracts};
use pe_risk_engine::{
    CANARY_MAX_ORDER_DEBIT, CANARY_PER_TRADE_CAP_BPS, RiskDecision, RiskSnapshot,
    clamp_contracts_to_cap, evaluate_risk,
};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};

use crate::{SizingMode, WinnerFollowConfig, WinnerFollowError, mode::ExecutionMode};

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
    /// legacy callers may retain this leader-price basis; economic paper/live/backtest callers
    /// supply their derived all-in price explicitly.
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

    /// Evaluate a leader signal at an explicit all-in Kelly price and produce an `OrderIntent`
    /// if all gates pass.
    ///
    /// Steps:
    /// 1. Gate Flip actions on `flip_human_approved`.
    /// 2. Use the requested `mode` directly as the effective mode (no signal-kind clamping).
    /// 3. Return `Err(ShadowMode)` for Shadow — no order emitted.
    /// 4. Size contracts by `config.sizing_mode`:
    ///    `Dollar { usd }` → `floor(usd / all_in_kelly_price)` (bypasses Kelly + `p`);
    ///    `Contract { contracts }` → exactly `contracts` (bypasses Kelly + price math);
    ///    `Kelly` → select the mode fraction and pass the caller's exact all-in `c` unchanged to
    ///    `size_contracts`.
    /// 5. Return `NoEdge` if the allocation is zero.
    /// 6. Gate on the caller's complete risk snapshot.
    /// 7. Build and return `OrderIntent` with that allocation unchanged.
    ///
    /// `all_in_kelly_price` — exact `c` derived by the caller from the signed principal/minimum
    /// shares, compact taker fee, and configured slippage. The strategy never recomputes or
    /// substitutes any fee input. The emitted `limit_price` stays at `signal.leader_price`
    /// regardless (don't-chase).
    ///
    /// `p` — empirical win rate supplied by caller. Used only when `sizing_mode` is `Kelly`.
    ///
    /// Every book, collateral, and monetary cap is owned by
    /// `pe_venue_polymarket::plan_sized_buy`; this strategy API neither accepts nor applies a
    /// second cap. `c` is never computed internally.
    pub fn evaluate_at_price(
        &self,
        signal: &LeaderSignal,
        all_in_kelly_price: Price,
        p: Probability,
        snapshot: RiskSnapshot,
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

        if all_in_kelly_price.0.is_zero() {
            return Err(WinnerFollowError::NoEdge);
        }

        // 4. Produce the uncapped allocation. The venue planner is the sole cap owner.
        let contracts: u64 = match self.config.sizing_mode {
            SizingMode::Dollar { usd } => {
                // Fixed USD allocation: bypass Kelly fraction + size_contracts.
                (usd / all_in_kelly_price.0)
                    .floor()
                    .to_u64()
                    .ok_or(WinnerFollowError::NoEdge)?
            }
            SizingMode::Contract { contracts } => contracts,
            SizingMode::Kelly => {
                // 4. Kelly fraction.
                let kf = kelly_fraction(effective_mode, self.config.kelly_fraction_override);

                let kelly_input = KellyInput {
                    p,
                    c: all_in_kelly_price,
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
        if contracts == 0 {
            return Err(WinnerFollowError::NoEdge);
        }

        // 6. Risk gate. The caller's snapshot already describes the planned economic proposal.
        match evaluate_risk(&snapshot) {
            RiskDecision::Approved => {}
            RiskDecision::Blocked(reason) => return Err(WinnerFollowError::Blocked(reason)),
        }

        // 7. Build OrderIntent.
        Ok(build_order_intent(signal, contracts, signal.leader_price))
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

/// Build the idempotency key per `_GLOSSARY.md` — public so the #508 dispatch aggregate
/// can derive its `dispatch_id` from the same canonical identity the order intent carries.
///
/// Format: `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{bucket}`
/// where `bucket = floor(observed_at_ms / 1_000) = observed_at.unix_timestamp()`.
/// Canonical `wf|leader|source|market|outcome|side|bucket` key from raw parts —
/// the single format owner shared with offline decision replay (#544), so the
/// replay binding compares EXACT equality rather than substrings.
pub fn build_idempotency_key_parts(
    leader: &str,
    source_trade_id: &str,
    market: &str,
    outcome: u16,
    side: pe_core_types::Side,
    observed_at_bucket: i64,
) -> String {
    let side_str = match side {
        pe_core_types::Side::Buy => "buy",
        pe_core_types::Side::Sell => "sell",
    };
    format!("wf|{leader}|{source_trade_id}|{market}|{outcome}|{side_str}|{observed_at_bucket}")
}

pub fn build_idempotency_key(signal: &LeaderSignal) -> String {
    build_idempotency_key_parts(
        &signal.leader.to_string(),
        &signal.source_trade_id.0,
        &signal.market_id.0.0,
        signal.outcome_id.0,
        signal.leader_side,
        signal.observed_at.unix_timestamp(),
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
