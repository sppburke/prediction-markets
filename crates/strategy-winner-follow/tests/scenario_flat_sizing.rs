// Scenario tests for the flat-sizing path (issue #161).
// Run with: cargo nextest run -p pe-strategy-winner-follow --features scenario scenario_flat_sizing
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, LeaderAction, MarketId, OutcomeId, Price, Probability, ProbabilityPpm,
    ReconstructionQuality, ShareAmount, Side, SourceTradeId, TraderId, VenueId, VenueMarketId,
    WalletAddress,
};
use pe_risk_engine::{ConcentrationCaps, RiskBlock, RiskSnapshot, snapshot::TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{
    ExecutionMode, SizingMode, WinnerFollowConfig, WinnerFollowError, WinnerFollowStrategy,
    config::PerTradeCap,
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

fn price(d: rust_decimal::Decimal) -> Price {
    Price::new(d).expect("valid price")
}

fn quality(q: u8) -> ReconstructionQuality {
    ReconstructionQuality::new(q).expect("quality in 0..=100")
}

fn p_high() -> Probability {
    Probability::new(dec!(0.70)).expect("0.70 is valid")
}

fn make_signal_at_price(
    wallet_byte: u8,
    action: LeaderAction,
    leader_price: Price,
) -> LeaderSignal {
    LeaderSignal {
        leader: TraderId(wallet(wallet_byte)),
        venue: VenueId::polymarket(),
        market_id: MarketId(VenueMarketId("mkt-flat-001".to_string())),
        outcome_id: OutcomeId(0),
        action,
        leader_side: Side::Buy,
        leader_price,
        leader_size: ShareAmount::from_whole(100).unwrap(),
        observed_at: NOW,
        received_at: NOW,
        reconstruction_quality: quality(100),
        source_trade_id: SourceTradeId("tid-flat-001".to_string()),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
    }
}

fn make_signal(wallet_byte: u8, action: LeaderAction) -> LeaderSignal {
    make_signal_at_price(wallet_byte, action, price(dec!(0.50)))
}

fn clean_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 500,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(10),
        per_trade_cap_bps: 25,
        concentration_caps: Some(ConcentrationCaps::CANONICAL),
    }
}

fn flat_config(flat_usd: rust_decimal::Decimal) -> WinnerFollowConfig {
    WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: flat_usd },
        per_trade_cap: PerTradeCap::Unlimited, // remove cap so flat count comes through
        ..WinnerFollowConfig::default()
    }
}

// ─── scenario F1 ─────────────────────────────────────────────────────────────

/// flat=$100, price=$0.50 → floor(100/0.50)=200 contracts.
///
/// PASS: `intent.contracts.0 == 200`.
#[test]
fn scenario_flat_sizing_correct_contract_count() {
    let signal = make_signal(0xF1, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("flat sizing should produce order");

    assert_eq!(
        intent.contracts.0, 200,
        "flat=$100 / price=$0.50 must yield exactly 200 contracts, got {}",
        intent.contracts.0
    );
}

// ─── scenario F1b: current-price sizing basis (#339) ──────────────────────────

/// `evaluate_at_price` sizes at the CURRENT price, not the leader's, while the emitted
/// `limit_price` stays at the leader's price (don't-chase). The leader-price wrapper
/// `evaluate` keeps sizing at the leader's price.
///
/// PASS: current-price order = 250 contracts (100/0.40), leader-price order = 125
///       (100/0.80), and the current-price order's `limit_price` == 0.80.
#[test]
fn scenario_evaluate_at_price_sizes_at_current_keeps_leader_limit() {
    let leader_price = price(dec!(0.80));
    let signal = make_signal_at_price(0xF9, LeaderAction::Entry, leader_price);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let at_current = strategy
        .evaluate_at_price(
            &signal,
            price(dec!(0.40)),
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
            None,
            None,
        )
        .expect("current-price sizing should produce order");
    assert_eq!(
        at_current.contracts.0, 250,
        "sized at current price 0.40 → floor(100/0.40)"
    );
    assert_eq!(
        at_current.limit_price, leader_price,
        "limit_price stays at the leader's price (don't-chase)"
    );

    let at_leader = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("leader-price sizing should produce order");
    assert_eq!(
        at_leader.contracts.0, 125,
        "wrapper sizes at the leader price 0.80 → floor(100/0.80)"
    );
    println!(
        "PASS: evaluate_at_price sizes at current (250) vs leader (125); limit stays at leader"
    );
}

// ─── scenario F2 ─────────────────────────────────────────────────────────────

/// flat=$100, price=$0.01 → floor(100/0.01)=10 000 contracts.
///
/// PASS: `intent.contracts.0 == 10_000`.
#[test]
fn scenario_flat_sizing_low_price_large_count() {
    let signal = make_signal_at_price(0xF2, LeaderAction::Entry, price(dec!(0.01)));
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(1_000_000),
            ExecutionMode::LiveTiny,
        )
        .expect("flat sizing should produce order");

    assert_eq!(
        intent.contracts.0, 10_000,
        "flat=$100 / price=$0.01 must yield exactly 10 000 contracts, got {}",
        intent.contracts.0
    );
}

// ─── scenario F3 ─────────────────────────────────────────────────────────────

/// Per-trade cap still clamps the flat-path result.
///
/// flat=$10_000, price=$0.50 → 20_000 contracts raw.
/// Cap=25 bps on $10_000 bankroll → cap_usd=$25.00 → max_contracts=floor(25/0.50)=50.
///
/// PASS: `intent.contracts.0 == 50`.
#[test]
fn scenario_flat_sizing_per_trade_cap_still_clamps() {
    let signal = make_signal(0xF3, LeaderAction::Entry);
    let config = WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: dec!(10_000) },
        per_trade_cap: PerTradeCap::Bps(25),
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("clamped flat sizing should produce order");

    // cap_usd = 10_000 × 25/10_000 = $25.00; max = floor(25.00/0.50) = 50
    assert_eq!(
        intent.contracts.0, 50,
        "25-bps cap on $10k at $0.50 must clamp flat result to 50, got {}",
        intent.contracts.0
    );
}

// ─── scenario F4 ─────────────────────────────────────────────────────────────

/// Risk gate still fires on the flat path.
///
/// Intraday drawdown at -200 bps triggers `IntradayDrawdownStop`.
///
/// PASS: `Err(Blocked(IntradayDrawdownStop))`.
#[test]
fn scenario_flat_sizing_risk_gate_still_fires() {
    let signal = make_signal(0xF4, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let mut snapshot = clean_snapshot();
    snapshot.intraday_pnl_bps = BasisPoints(-200);
    snapshot.per_trade_cap_bps = 10_000;

    let result = strategy.evaluate(
        &signal,
        p_high(),
        snapshot,
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(
            result,
            Err(WinnerFollowError::Blocked(RiskBlock::IntradayDrawdownStop))
        ),
        "risk gate must fire on flat path; got {result:?}"
    );
}

// ─── scenario F5 ─────────────────────────────────────────────────────────────

/// Shadow mode gate fires before flat sizing — no order emitted when Shadow is requested.
///
/// PASS: `Err(ShadowMode)`.
#[test]
fn scenario_flat_sizing_shadow_mode_still_blocks() {
    let signal = make_signal(0xF5, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::Shadow,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::ShadowMode)),
        "Shadow request must yield ShadowMode before flat sizing; got {result:?}"
    );
}

// ─── scenario F6 ─────────────────────────────────────────────────────────────

/// Flip gate fires before flat sizing — unapproved Flip is blocked.
///
/// PASS: `Err(FlipNotApproved)`.
#[test]
fn scenario_flat_sizing_flip_gate_still_fires() {
    let signal = make_signal(0xF6, LeaderAction::Flip);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::FlipNotApproved)),
        "Flip gate must fire before flat sizing; got {result:?}"
    );
}

// ─── scenario F7 ─────────────────────────────────────────────────────────────

/// `sizing_mode = Kelly` → Kelly path is used (existing behaviour unchanged).
///
/// p=0.40 == price=0.40, fee raises c_net above p → NoEdge (same as before #161).
///
/// PASS: `Err(NoEdge)`.
#[test]
fn scenario_kelly_path_used_when_flat_none() {
    let signal = make_signal_at_price(0xF7, LeaderAction::Entry, price(dec!(0.40)));
    let config = WinnerFollowConfig {
        sizing_mode: SizingMode::Kelly,
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);
    let p_at_market = Probability::new(dec!(0.40)).expect("0.40 valid");

    let result = strategy.evaluate(
        &signal,
        p_at_market,
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::NoEdge)),
        "flat=None must use Kelly; p==price+fees yields NoEdge; got {result:?}"
    );
}

// ─── scenario F8 ─────────────────────────────────────────────────────────────

/// When flat < price, floor(flat/price) = 0, clamped to 1 by `.max(1)`.
///
/// flat=$0.30, price=$0.50 → floor(0.30/0.50)=floor(0.60)=0 → max(1,0)=1 contract.
///
/// PASS: `intent.contracts.0 == 1`.
#[test]
fn scenario_flat_below_price_yields_one_contract() {
    let signal = make_signal(0xF8, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(0.30)));

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("flat<price should still produce 1-contract order");

    assert_eq!(
        intent.contracts.0, 1,
        "flat=$0.30 < price=$0.50: floor=0 must be clamped to 1, got {}",
        intent.contracts.0
    );
}

// ─── scenario F9 ─────────────────────────────────────────────────────────────

/// Flat path is deterministic: identical inputs always produce the same result.
///
/// PASS: two calls with identical inputs return equal contract counts.
#[test]
fn scenario_flat_sizing_is_deterministic() {
    let signal = make_signal(0xF9, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(75)));

    let r1 = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("first call");
    let r2 = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("second call");

    assert_eq!(
        r1.contracts, r2.contracts,
        "flat sizing must be deterministic; got {r1:?} vs {r2:?}"
    );
    assert_eq!(
        r1.idempotency_key, r2.idempotency_key,
        "idempotency key must match"
    );
}

// ─── scenario F10 ─────────────────────────────────────────────────────────────

/// Bankroll too small to cover even 1 contract at price → NoEdge (clamp returns 0).
///
/// flat=$100 (would give 200 contracts), bankroll=$0.30 < price=$0.50.
/// `clamp_contracts_to_cap` returns 0 when bankroll < price → NoEdge.
///
/// PASS: `Err(NoEdge)`.
#[test]
fn scenario_flat_sizing_bankroll_below_price_yields_no_edge() {
    let signal = make_signal(0xFA, LeaderAction::Entry);
    let config = WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: dec!(100) },
        per_trade_cap: PerTradeCap::Unlimited,
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);

    let result = strategy.evaluate(
        &signal,
        p_high(),
        clean_snapshot(),
        dec!(0.30), // bankroll $0.30 < price $0.50
        ExecutionMode::LiveTiny,
    );

    assert!(
        matches!(result, Err(WinnerFollowError::NoEdge)),
        "bankroll < price must yield NoEdge even on flat path; got {result:?}"
    );
}

// ─── scenario F8: contract sizing mode (#398 WS2) ─────────────────────────────

/// `sizing_mode = Contract { contracts: 5 }` with the per-trade cap removed → exactly 5
/// contracts (the uncapped case).
///
/// PASS: `intent.contracts.0 == 5`.
#[test]
fn scenario_contract_sizing_uncapped_yields_exact_n() {
    let signal = make_signal(0xF8, LeaderAction::Entry); // price $0.50
    let config = WinnerFollowConfig {
        sizing_mode: SizingMode::Contract { contracts: 5 },
        per_trade_cap: PerTradeCap::Unlimited,
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("contract sizing should produce order");
    assert_eq!(
        intent.contracts.0, 5,
        "contract mode must yield exactly N=5 uncapped, got {}",
        intent.contracts.0
    );
}

/// `sizing_mode = Contract { contracts: 1000 }` with a 25 bps per-trade cap and a $10 000
/// bankroll at price $0.50 → cap_usd = 25, max = floor(25/0.50) = 50, so 1000 is clamped to 50
/// (the capped < N case).
///
/// PASS: `intent.contracts.0 == 50` (< 1000).
#[test]
fn scenario_contract_sizing_is_clamped_by_per_trade_cap() {
    let signal = make_signal(0xF8, LeaderAction::Entry); // price $0.50
    let config = WinnerFollowConfig {
        sizing_mode: SizingMode::Contract { contracts: 1000 },
        per_trade_cap: PerTradeCap::Bps(25),
        ..WinnerFollowConfig::default()
    };
    let strategy = WinnerFollowStrategy::new(config);

    let intent = strategy
        .evaluate(
            &signal,
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
        )
        .expect("contract sizing should produce order");
    assert_eq!(
        intent.contracts.0, 50,
        "contract N=1000 must clamp to the 25 bps per-trade cap (50), got {}",
        intent.contracts.0
    );
    assert!(intent.contracts.0 < 1000, "capped count must be < N");
}

// ─── scenario F9: price-impact book cap (#398 WS2 step 5c) ─────────────────────

/// `book_cap_contracts = None` (gate off / fail-open) leaves the size unchanged: $100/0.50 = 200.
///
/// PASS: `intent.contracts.0 == 200`.
#[test]
fn scenario_book_cap_none_is_fail_open_passthrough() {
    let signal = make_signal(0xF9, LeaderAction::Entry); // price $0.50
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));
    let intent = strategy
        .evaluate_at_price(
            &signal,
            price(dec!(0.50)),
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
            None,
            None,
        )
        .expect("fail-open should produce order");
    assert_eq!(
        intent.contracts.0, 200,
        "None book cap must not change the size"
    );
}

/// `book_cap_contracts = Some(40)` caps the 200-contract size down to 40 (the absorbable depth).
///
/// PASS: `intent.contracts.0 == 40` (< 200).
#[test]
fn scenario_book_cap_some_reduces_size() {
    let signal = make_signal(0xF9, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));
    let intent = strategy
        .evaluate_at_price(
            &signal,
            price(dec!(0.50)),
            p_high(),
            clean_snapshot(),
            dec!(10_000),
            ExecutionMode::LiveTiny,
            Some(40),
            None,
        )
        .expect("capped size should still produce order");
    assert_eq!(
        intent.contracts.0, 40,
        "book cap must min the size to absorbable depth"
    );
}

/// `book_cap_contracts = Some(0)` (a successful /book with nothing absorbable within bps) skips the
/// trade — distinct from the fail-open `None` path.
///
/// PASS: `Err(NoEdge)`.
#[test]
fn scenario_book_cap_zero_skips_trade() {
    let signal = make_signal(0xF9, LeaderAction::Entry);
    let strategy = WinnerFollowStrategy::new(flat_config(dec!(100)));
    let result = strategy.evaluate_at_price(
        &signal,
        price(dec!(0.50)),
        p_high(),
        clean_snapshot(),
        dec!(10_000),
        ExecutionMode::LiveTiny,
        Some(0),
        None,
    );
    assert!(
        matches!(result, Err(WinnerFollowError::NoEdge)),
        "Some(0) book cap must skip the trade (NoEdge); got {result:?}"
    );
}
