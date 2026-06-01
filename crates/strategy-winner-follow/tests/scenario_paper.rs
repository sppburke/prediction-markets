// Scenario tests for PaperExecutor: simulate fills and verify event-log round-trip.
// Run with: cargo nextest run -p pe-strategy-winner-follow --features scenario
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceId, SourceTimestamp, StrategyId,
    VenueMarketId,
};
use pe_event_log::{Reader, Writer};
use pe_strategy_winner_follow::{PaperExecutor, PaperFill};
use pe_venue_core::OrderIntent;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::macros::datetime;

const NOW: time::OffsetDateTime = datetime!(2024-07-01 12:00:00 UTC);

fn sample_intent() -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".to_string()),
        market_id: MarketId(VenueMarketId("mkt-paper-001".to_string())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: ContractQty(50),
        limit_price: Price::new(dec!(0.42)).expect("valid price"),
        validity_seconds: 30,
        idempotency_key: "wf|0x01|tid-001|mkt-paper-001|0|buy|1719835200".to_string(),
    }
}

fn paper_source() -> SourceId {
    SourceId("paper-executor".to_string())
}

// ─── scenario 1 ──────────────────────────────────────────────────────────────

/// Execute one paper fill, read the frame back from the log, and verify the fill
/// round-trips correctly with `simulated_fill_price == intent.limit_price`.
///
/// PASS: deserialised `PaperFill.simulated_fill_price == intent.limit_price`.
#[test]
fn scenario_paper_fill_round_trips_from_log() {
    let dir = TempDir::new().expect("temp dir");
    let log_path = dir.path().join("paper.log");

    let intent = sample_intent();
    let expected_price = intent.limit_price;

    // Write the fill.
    {
        let writer = Writer::open(&log_path).expect("open writer");
        // haircut/slippage = 0 so this round-trip test stays behavior-neutral.
        let mut executor = PaperExecutor::new(writer, paper_source(), 0, 0);
        let (fill, _seq) = executor
            .execute(&intent, SourceTimestamp(NOW))
            .expect("execute");

        assert_eq!(
            fill.simulated_fill_price, expected_price,
            "fill price must equal intent.limit_price"
        );
    }

    // Read it back and verify deserialisation.
    let frames: Vec<_> = Reader::replay(&log_path)
        .expect("open reader")
        .collect::<Result<_, _>>()
        .expect("read frames");

    assert_eq!(frames.len(), 1, "exactly one frame in the log");

    let (_seq, envelope) = &frames[0];
    let recovered: PaperFill =
        serde_json::from_slice(&envelope.payload).expect("deserialise PaperFill");

    assert_eq!(
        recovered.simulated_fill_price, expected_price,
        "recovered fill price must match"
    );
    assert_eq!(
        recovered.intent.idempotency_key, intent.idempotency_key,
        "idempotency key must survive round-trip"
    );
}

// ─── scenario 2 ──────────────────────────────────────────────────────────────

/// Multiple fills written sequentially are all recoverable from the log in order.
///
/// PASS: N fills written → N frames readable → prices match in order.
#[test]
fn scenario_multiple_fills_ordered_in_log() {
    let dir = TempDir::new().expect("temp dir");
    let log_path = dir.path().join("paper.log");

    let intents = vec![
        {
            let mut i = sample_intent();
            i.idempotency_key = "wf|0x01|tid-001|mkt-paper-001|0|buy|1719835200".to_string();
            i.limit_price = Price::new(dec!(0.30)).expect("valid");
            i
        },
        {
            let mut i = sample_intent();
            i.idempotency_key = "wf|0x01|tid-002|mkt-paper-001|0|buy|1719835201".to_string();
            i.limit_price = Price::new(dec!(0.55)).expect("valid");
            i
        },
        {
            let mut i = sample_intent();
            i.idempotency_key = "wf|0x01|tid-003|mkt-paper-001|0|buy|1719835202".to_string();
            i.limit_price = Price::new(dec!(0.70)).expect("valid");
            i
        },
    ];

    {
        let writer = Writer::open(&log_path).expect("open writer");
        let mut executor = PaperExecutor::new(writer, paper_source(), 0, 0);
        for intent in &intents {
            executor
                .execute(intent, SourceTimestamp(NOW))
                .expect("execute");
        }
    }

    let frames: Vec<_> = Reader::replay(&log_path)
        .expect("open reader")
        .collect::<Result<_, _>>()
        .expect("read frames");

    assert_eq!(
        frames.len(),
        intents.len(),
        "frame count must match fill count"
    );

    for ((_seq, envelope), intent) in frames.iter().zip(intents.iter()) {
        let fill: PaperFill =
            serde_json::from_slice(&envelope.payload).expect("deserialise PaperFill");
        assert_eq!(
            fill.simulated_fill_price, intent.limit_price,
            "each fill price must match its intent"
        );
    }
}

// ─── scenario 3 (AC3) ────────────────────────────────────────────────────────

fn intent_with(side: Side, price: rust_decimal::Decimal, key: &str) -> OrderIntent {
    let mut i = sample_intent();
    i.side = side;
    i.limit_price = Price::new(price).expect("valid price");
    i.idempotency_key = key.to_string();
    i
}

fn recorded_fill_price(
    dir: &TempDir,
    log: &str,
    intent: &OrderIntent,
    haircut_bps: u32,
    slippage_bps: u32,
) -> rust_decimal::Decimal {
    let writer = Writer::open(dir.path().join(log)).expect("open writer");
    let mut executor = PaperExecutor::new(writer, paper_source(), haircut_bps, slippage_bps);
    let (fill, _seq) = executor
        .execute(intent, SourceTimestamp(NOW))
        .expect("execute");
    fill.simulated_fill_price.0
}

/// The side-split haircut is applied to the recorded fill price and clamped into (0, 1).
///
/// PASS: BUY pays +haircut (clamped at 0.999), SELL pays −slippage (floored at 0.001),
///       and haircut_bps = 0 reproduces the un-haircut price.
/// FAIL: any recorded fill price deviates from the expected side-split haircut.
#[test]
fn scenario_side_split_haircut_clamped() {
    let dir = TempDir::new().expect("temp dir");

    // BUY pays fee+slippage: 0.40 * (1 + 500/10_000) = 0.42.
    let buy = intent_with(
        Side::Buy,
        dec!(0.40),
        "wf|0x01|b1|mkt-paper-001|0|buy|1719835200",
    );
    assert_eq!(
        recorded_fill_price(&dir, "buy.log", &buy, 500, 100),
        dec!(0.42),
        "BUY haircut"
    );

    // BUY clamps at 0.999: 0.98 * 1.05 = 1.029 -> 0.999.
    let buy_hi = intent_with(
        Side::Buy,
        dec!(0.98),
        "wf|0x01|b2|mkt-paper-001|0|buy|1719835201",
    );
    assert_eq!(
        recorded_fill_price(&dir, "buyhi.log", &buy_hi, 500, 100),
        dec!(0.999),
        "BUY clamp at 0.999"
    );

    // SELL pays slippage only: 0.40 * (1 - 100/10_000) = 0.396.
    let sell = intent_with(
        Side::Sell,
        dec!(0.40),
        "wf|0x01|s1|mkt-paper-001|0|sell|1719835202",
    );
    assert_eq!(
        recorded_fill_price(&dir, "sell.log", &sell, 500, 100),
        dec!(0.396),
        "SELL slippage only"
    );

    // haircut_bps = slippage_bps = 0 reproduces the limit price.
    let neutral = intent_with(
        Side::Buy,
        dec!(0.40),
        "wf|0x01|n1|mkt-paper-001|0|buy|1719835203",
    );
    assert_eq!(
        recorded_fill_price(&dir, "neutral.log", &neutral, 0, 0),
        dec!(0.40),
        "haircut=0 reproduces limit price"
    );

    println!("PASS: side-split haircut applied and clamped into (0, 1)");
}
