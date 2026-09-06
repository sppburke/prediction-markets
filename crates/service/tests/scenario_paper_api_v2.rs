//! Exact paper HTTP-view scenarios for issue #545 RC14.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use axum::extract::Extension;
use axum::http::StatusCode;
use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, VenueMarketId,
};
use pe_event_log::AppendReceipt;
use pe_paper_state::{FinancialFillRecord, PaperStateDb};
use pe_service::market_end_cache::{MarketEndCache, MarketResolution};
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::paper_api::{PaperApiState, fills, pnl, positions, status};
use rust_decimal_macros::dec;
use time::OffsetDateTime;

fn receipt(sequence: u64, byte: u8) -> AppendReceipt {
    AppendReceipt {
        sequence: EventSeq(sequence),
        this_hash: blake3::Hash::from_bytes([byte; 32]),
    }
}

fn market() -> MarketId {
    MarketId(VenueMarketId("condition-golden".to_owned()))
}

fn state(db: Arc<PaperStateDb>, initial_bankroll: rust_decimal::Decimal) -> Arc<PaperApiState> {
    Arc::new(PaperApiState {
        paper_state: db,
        initial_bankroll,
        market_end_cache: MarketEndCache::new("http://unused.invalid".to_owned()),
        mid_price_cache: MidPriceCache::new("http://unused.invalid".to_owned()),
    })
}

/// RC14-PAPER-HTTP-DECIMALS
///
/// Preconditions: an empty $100.250000 book and a second book with one exact fractional fill.
/// PASS: every money/price/quantity field in the four paper API views serializes as a decimal
/// string, while counts and Prepared sequences remain JSON integers.
/// FAIL: JSON emits a floating-point number, loses fractional quantity, or changes the exact cash.
#[tokio::test]
async fn exact_paper_api_contract_uses_decimal_strings() {
    let empty_dir = tempfile::tempdir().unwrap();
    let empty_db = Arc::new(PaperStateDb::open(&empty_dir.path().join("empty.db")).unwrap());
    empty_db.init_bankroll(dec!(100.250000)).unwrap();
    let empty_state = state(empty_db, dec!(100.250000));
    let Ok(empty_pnl) = pnl(Extension(empty_state)).await else {
        panic!("empty paper P&L must be available");
    };
    let body = serde_json::to_value(empty_pnl.0).unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "bankroll": "100.25",
            "initial_bankroll": "100.25",
            "open_market_value": "0",
            "equity": "100.25",
            "absolute_pnl": "0",
            "open_positions": 0,
            "settlements_7d": 0
        })
    );

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("filled.db")).unwrap());
    let start = receipt(10, 10);
    db.reset_financial_era(
        start,
        CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
    )
    .unwrap();
    db.apply_financial_fill(
        start,
        None,
        EventSeq(11),
        &FinancialFillRecord {
            idempotency_key: "wf|0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|g2:golden|condition-golden|0|buy|1800000000".to_owned(),
            market_id: market(),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            quantity: ShareAmount::from_decimal_exact(dec!(1.333333)).unwrap(),
            fill_price: Price::new(dec!(0.5)).unwrap(),
            principal: CollateralAmount::from_decimal_exact(dec!(0.666667)).unwrap(),
            fee: CollateralAmount::from_decimal_exact(dec!(0.000010)).unwrap(),
        },
        dec!(9.333323),
    )
    .unwrap();
    let filled_state = state(Arc::clone(&db), dec!(10));
    filled_state
        .market_end_cache
        .seed_scenario_resolution(
            market(),
            MarketResolution {
                resolution_unix: Some(1_800_000_100),
                source: Some("fixture".to_owned()),
                status: Some("resolved".to_owned()),
            },
        )
        .await;

    assert_eq!(
        serde_json::to_value(
            match positions(Extension(Arc::clone(&filled_state))).await {
                Ok(body) => body.0,
                Err(_) => panic!("positions must be available"),
            }
        )
        .unwrap(),
        serde_json::json!([{
            "market_id": "condition-golden",
            "outcome_id": 0,
            "long_contracts": "1.333333",
            "short_contracts": "0"
        }])
    );
    assert_eq!(
        match status(Extension(Arc::clone(&filled_state))).await {
            Ok(body) => body.0,
            Err(_) => panic!("status must be available"),
        },
        serde_json::json!({
            "bankroll": "9.333323",
            "initial_bankroll": "10",
            "fills_count": 1,
            "last_prepared_seq": 11
        })
    );
    assert_eq!(
        match fills(Extension(filled_state)).await {
            Ok(body) => body.0,
            Err(_) => panic!("fills must be available"),
        },
        serde_json::json!({
            "fills_count": 1,
            "trades": [{
                "idempotency_key": "wf|0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|g2:golden|condition-golden|0|buy|1800000000",
                "leader": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "source_trade_id": "g2:golden",
                "market_id": "condition-golden",
                "outcome_id": 0,
                "side": "buy",
                "quantity": "1.333333",
                "fill_price": "0.5",
                "principal": "0.666667",
                "fee": "0.00001",
                "prepared_seq": 11,
                "entry_unix": 1_800_000_000_i64,
                "resolution_unix": 1_800_000_100_i64,
                "resolution_status": "resolved"
            }]
        })
    );
}

/// RC14-PAPER-HTTP-UNAVAILABLE
///
/// Preconditions: the coherent paper snapshot has one open exact position, and its fresh cache row
/// intentionally has no strict outcome price.
/// PASS: `/paper/pnl` returns HTTP 503 with the typed missing-price reason and no zero substitute.
/// FAIL: the endpoint returns 200, emits zero market value, or hides the typed unavailable cause.
#[tokio::test]
async fn paper_pnl_is_typed_unavailable_when_price_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("missing-price.db")).unwrap());
    let start = receipt(20, 20);
    db.reset_financial_era(
        start,
        CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
    )
    .unwrap();
    db.apply_financial_fill(
        start,
        None,
        EventSeq(21),
        &FinancialFillRecord {
            idempotency_key: "missing-price".to_owned(),
            market_id: market(),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            quantity: ShareAmount::from_whole(1).unwrap(),
            fill_price: Price::new(dec!(0.5)).unwrap(),
            principal: CollateralAmount::from_decimal_exact(dec!(0.5)).unwrap(),
            fee: CollateralAmount::ZERO,
        },
        dec!(9.5),
    )
    .unwrap();
    let api = state(db, dec!(10));
    api.mid_price_cache
        .seed_unavailable_scenario_price(market(), OffsetDateTime::now_utc(), receipt(22, 22))
        .await;

    let (code, body) = pnl(Extension(api)).await.unwrap_err();
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::to_value(body.0).unwrap(),
        serde_json::json!({
            "error": "paper valuation unavailable: a required position price is missing"
        })
    );
}
