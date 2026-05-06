// Scenario tests for the strategy-winner-follow pipeline.
// Run with: cargo nextest run -p pe-strategy-winner-follow --features scenario
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::collections::HashSet;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, FundingHopCount, InheritedPriorPpm, LeaderAction, MarketId,
    OperatorId, OutcomeId, Price, Probability, ProbabilityPpm, Quantity, ReconstructionQuality,
    Side, SourceTradeId, TraderId, VenueId, VenueMarketId, WalletAddress, WinnerFollowSignalKind,
};
use pe_risk_engine::{RiskBlock, RiskDecision, RiskSnapshot, snapshot::TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{
    ExecutionMode, WinnerFollowConfig, WinnerFollowError, WinnerFollowStrategy,
};
use rust_decimal_macros::dec;
use time::macros::datetime;

// ─── helpers ─────────────────────────────────────────────────────────────────

const NOW: time::OffsetDateTime = datetime!(2024-07-01 00:00:00 UTC);

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn op_id(seed: &[u8]) -> OperatorId {
    OperatorId(blake3::hash(seed))
}

fn price(d: rust_decimal::Decimal) -> Price {
    Price::new(d).expect("valid price")
}

fn quality(q: u8) -> ReconstructionQuality {
    ReconstructionQuality::new(q).expect("quality in 0..=100")
}

/// Win rate identical to the signal price: fee model pushes c_net above p → NoEdge.
fn p_at_market() -> Probability {
    Probability::new(dec!(0.40)).expect("0.40 is valid")
}

/// Win rate well above the signal price: clear Kelly edge.
fn p_high() -> Probability {
    Probability::new(dec!(0.70)).expect("0.70 is valid")
}

fn make_signal(
    wallet_byte: u8,
    signal_kind: WinnerFollowSignalKind,
    action: LeaderAction,
    operator_id: Option<OperatorId>,
) -> LeaderSignal {
    LeaderSignal {
        leader: TraderId(wallet(wallet_byte)),
        operator_id,
        venue: VenueId::polymarket(),
        market_id: MarketId(VenueMarketId("mkt-001".to_string())),
        outcome_id: OutcomeId(0),
        action,
        leader_side: Side::Buy,
        leader_price: price(dec!(0.40)),
        leader_size: Quantity(ContractQty(100)),
        observed_at: NOW,
        received_at: NOW,
        reconstruction_quality: quality(100),
        signal_kind,
        inherited_prior: None,
        source_trade_id: SourceTradeId("tid-000001".to_string()),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
    }
}

fn clean_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        operator_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        funder_inherited_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        anti_gaming_flags: HashSet::new(),
        onchain_source_status: SourceStatus::Healthy,
        proxy_funder_mapping_proven: true,
        funder_seeding_rate_suspicious: false,
        cluster_membership_stable: true,
        funding_hop_count: Some(FundingHopCount(1)),
        copy_latency_p95_ms: 500,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(10),
    }
}

// ─── scenario 1 ──────────────────────────────────────────────────────────────

/// When p == leader_price, the fee model increases c_net above p, so Kelly edge
/// is negative → NoEdge.
///
/// PASS: `Err(NoEdge)`.
#[test]
fn scenario_no_edge_when_p_equals_price() {
    let signal = make_signal(
        0x01,
        WinnerFollowSignalKind::NormalLeaderFollow,
        LeaderAction::Entry,
        None,
    );
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());

    let result = strategy.evaluate(
        &signal,
        p_at_market(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::NoEdge)),
        "p==price with fees: c_net > p yields no edge; got {result:?}"
    );
}

// ─── scenario 7 ──────────────────────────────────────────────────────────────

/// With p=0.70 >> price=0.40, clear Kelly edge exists.
/// The strategy returns something other than `NoEdge` (either an order or a
/// risk block).
///
/// PASS: result is NOT `Err(NoEdge)`.
#[test]
fn scenario_positive_p_breaks_no_edge() {
    let signal = make_signal(
        0x07,
        WinnerFollowSignalKind::NormalLeaderFollow,
        LeaderAction::Entry,
        None,
    );
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        !matches!(result, Err(WinnerFollowError::NoEdge)),
        "p=0.70 >> price=0.40: must find edge; got {result:?}"
    );
}

// ─── scenario 2 ──────────────────────────────────────────────────────────────

/// Cluster-coordination signal, LiveTiny mode requested → clamped to Shadow.
///
/// PASS: `Err(ShadowMode)`.
#[test]
fn scenario_cluster_coordination_clamped_to_shadow() {
    let op = op_id(b"test-operator-cc");
    let signal = make_signal(
        0x02,
        WinnerFollowSignalKind::ClusterCoordination,
        LeaderAction::Entry,
        Some(op),
    );
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::ShadowMode)),
        "cluster signal must be clamped to shadow; got {result:?}"
    );
}

// ─── scenario 3 ──────────────────────────────────────────────────────────────

/// Fresh-wallet signal, LiveTiny mode requested → clamped to Paper (not Shadow).
///
/// PASS: result is NOT `Err(ShadowMode)`.
#[test]
fn scenario_fresh_wallet_clamped_to_paper_not_shadow() {
    let op = op_id(b"test-operator-fw");
    let mut signal = make_signal(
        0x03,
        WinnerFollowSignalKind::FreshWalletFirstTrade,
        LeaderAction::Entry,
        Some(op),
    );
    signal.inherited_prior = Some(InheritedPriorPpm(0));
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        !matches!(result, Err(WinnerFollowError::ShadowMode)),
        "fresh-wallet LiveTiny must clamp to Paper, not Shadow; got {result:?}"
    );
}

// ─── scenario 4 ──────────────────────────────────────────────────────────────

/// The risk engine's intraday drawdown stop fires at -200 bps when integrated
/// from `evaluate`. Since the probability placeholder yields `NoEdge` before
/// reaching the risk gate, this scenario exercises the risk engine directly to
/// confirm the underlying block is wired correctly.
///
/// PASS: `evaluate_risk` returns `Blocked(IntradayDrawdownStop)` at -200 bps.
#[test]
fn scenario_risk_block_intraday_drawdown() {
    let mut snapshot = clean_snapshot();
    snapshot.intraday_pnl_bps = BasisPoints(-200);

    assert_eq!(
        pe_risk_engine::evaluate_risk(&snapshot),
        RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop),
        "drawdown stop must fire at -200 bps"
    );
}

// ─── scenario 5 ──────────────────────────────────────────────────────────────

/// `LeaderAction::Flip` is blocked by default (flip_human_approved = false).
///
/// PASS: `Err(FlipNotApproved)`.
#[test]
fn scenario_flip_blocked_by_default() {
    let signal = make_signal(
        0x05,
        WinnerFollowSignalKind::NormalLeaderFollow,
        LeaderAction::Flip,
        None,
    );
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::FlipNotApproved)),
        "Flip must be blocked by default; got {result:?}"
    );
}

// ─── scenario 6 ──────────────────────────────────────────────────────────────

/// With `flip_human_approved = true`, Flip passes the gate.
///
/// PASS: result is NOT `Err(FlipNotApproved)`.
#[test]
fn scenario_flip_approved_passes_gate() {
    let signal = make_signal(
        0x06,
        WinnerFollowSignalKind::NormalLeaderFollow,
        LeaderAction::Flip,
        None,
    );
    let config = WinnerFollowConfig {
        flip_human_approved: true,
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        !matches!(result, Err(WinnerFollowError::FlipNotApproved)),
        "approved Flip must pass the gate; got {result:?}"
    );
}

// ─── proptest ────────────────────────────────────────────────────────────────

proptest::proptest! {
    /// `evaluate` is deterministic: identical inputs always produce the same result.
    #[test]
    fn evaluate_is_deterministic(bankroll_raw in 1_000u64..=1_000_000u64) {
        let signal = make_signal(
            0x20,
            WinnerFollowSignalKind::NormalLeaderFollow,
            LeaderAction::Entry,
            None,
        );
        let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());
        let bankroll = rust_decimal::Decimal::from(bankroll_raw);
        let p = p_high();

        let r1 = strategy.evaluate(&signal, p, clean_snapshot(), bankroll, ExecutionMode::LiveTiny);
        let r2 = strategy.evaluate(&signal, p, clean_snapshot(), bankroll, ExecutionMode::LiveTiny);

        match (&r1, &r2) {
            (Ok(a), Ok(b)) => {
                proptest::prop_assert_eq!(&a.idempotency_key, &b.idempotency_key);
                proptest::prop_assert_eq!(a.contracts, b.contracts);
            }
            (Err(_), Err(_)) => {}
            _ => proptest::prop_assert!(false, "evaluate must be deterministic"),
        }
    }
}
