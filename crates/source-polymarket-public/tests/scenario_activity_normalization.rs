//! Scenario: exact, source-owned Polymarket activity normalization (#544).
//!
//! PASS: captured rows and documented synthetic variants retain exact atomics,
//! stable group identity, multiset semantic revisions, and typed fail-closed
//! results. FAIL: any row is rounded/dropped, provenance changes semantics,
//! duplicates collapse, or ambiguous source times aggregate.

#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;

use pe_core_types::{
    CollateralAmount, OutcomeId, ReceivedAt, ShareAmount, SourceId, SourceTimestamp, WalletAddress,
};
use pe_source_polymarket_public::{
    ActivityAggregationError, ActivityParseContext, ActivityParseError, ActivityRevisionComparison,
    ActivityTransport, ActivityType, ActivityValidationError, ActivityWindowInvalidation,
    NormalizedActivity, aggregate_activity_rows, parse_activity_response, parse_activity_row,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use time::OffsetDateTime;

const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn wallet(value: &str) -> WalletAddress {
    WalletAddress::from_hex(value).expect("fixture wallet must parse")
}

fn timestamp(value: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value).expect("fixture timestamp must parse")
}

fn context(transport: ActivityTransport, observed: i64, received: i64) -> ActivityParseContext {
    ActivityParseContext {
        source_id: SourceId("polymarket-data-api".to_owned()),
        observed_at: SourceTimestamp(timestamp(observed)),
        received_at: ReceivedAt(timestamp(received)),
        transport,
    }
}

fn base_row(activity_type: &str) -> Value {
    json!({
        "proxyWallet": WALLET,
        "timestamp": 1_788_000_000_i64,
        "conditionId": "0xcondition",
        "type": activity_type,
        "size": "1.000000",
        "usdcSize": "0.500000",
        "transactionHash": "0xtransaction",
        "price": "0.5",
        "asset": "123",
        "side": "BUY",
        "outcomeIndex": 0,
        "outcome": "Yes",
        "title": "presentation"
    })
}

fn response(rows: &[Value]) -> Vec<u8> {
    serde_json::to_vec(rows).expect("test response must serialize")
}

fn parse_rows(rows: &[Value]) -> Vec<NormalizedActivity> {
    parse_activity_response(
        &response(rows),
        wallet(WALLET),
        &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011),
    )
    .expect("test response must parse")
    .rows
}

fn aggregate(rows: &[Value]) -> pe_source_polymarket_public::ActivityAggregate {
    let parsed = parse_rows(rows);
    let mut aggregates = aggregate_activity_rows(&parsed).expect("test aggregate must succeed");
    assert_eq!(aggregates.len(), 1);
    aggregates.remove(0)
}

#[test]
fn captured_position_rows_preserve_fractional_amounts_and_optional_shapes() {
    let trade_redeem = parse_activity_response(
        include_bytes!("fixtures/activity_trade_redeem_w.json"),
        wallet("0x164cb85e1718d0a8f95d588b08fbb52b34233e39"),
        &context(ActivityTransport::Rest, 1_788_200_000, 1_788_200_001),
    )
    .expect("captured TRADE/REDEEM rows must parse");
    assert_eq!(trade_redeem.rows.len(), 2);
    assert_eq!(trade_redeem.rows[0].activity_type, ActivityType::Trade);
    assert_eq!(trade_redeem.rows[0].share_amount.atomic(), 18_550_000);
    assert_eq!(trade_redeem.rows[0].source_usdc_amount.atomic(), 13_693_330);
    assert_eq!(trade_redeem.rows[1].activity_type, ActivityType::Redeem);
    assert_eq!(trade_redeem.rows[1].side, None);

    let no_asset = parse_activity_response(
        include_bytes!("fixtures/activity_redeem_no_asset_w0.json"),
        wallet("0xfd9b763674cb096cacec059fcfe60ae82aae09e8"),
        &context(ActivityTransport::Rest, 1_788_300_000, 1_788_300_001),
    )
    .expect("captured asset-empty REDEEM must parse");
    assert_eq!(no_asset.rows[0].asset, None);
    assert_eq!(no_asset.rows[0].outcome, Some(OutcomeId(0)));

    let merge = parse_activity_response(
        include_bytes!("fixtures/activity_merge_h2.json"),
        wallet("0xcf0aca0d7a395202aec661c3666be9cc098e320a"),
        &context(ActivityTransport::Rest, 1_788_000_000, 1_788_000_001),
    )
    .expect("captured MERGE must parse");
    assert_eq!(merge.rows[0].activity_type, ActivityType::Merge);
    assert_eq!(merge.rows[0].asset, None);
    assert_eq!(merge.rows[0].outcome, None);
    assert_eq!(merge.rows[0].side, None);
    assert_eq!(merge.rows[0].share_amount.atomic(), 187_831_400);
}

#[test]
fn captured_combo_is_retained_but_not_ordinary() {
    let parsed = parse_activity_response(
        include_bytes!("fixtures/activity_combo_w1.json"),
        wallet("0xc0f89d4e30b3ab40ab1f1979ebdcf8a02c39ae2e"),
        &context(
            ActivityTransport::ActivityWebsocket,
            1_788_000_000,
            1_788_000_001,
        ),
    )
    .expect("captured combo trade must parse");
    assert!(parsed.rows[0].is_combo);
    assert!(!parsed.rows[0].is_ordinary_position_change());
    assert_eq!(parsed.rows[0].outcome, Some(OutcomeId(0)));
}

#[test]
fn all_twelve_documented_types_are_typed_and_raw_only_types_do_not_fence() {
    let documented = [
        "TRADE",
        "SPLIT",
        "MERGE",
        "REDEEM",
        "REWARD",
        "CONVERSION",
        "DEPOSIT",
        "WITHDRAWAL",
        "YIELD",
        "MAKER_REBATE",
        "TAKER_REBATE",
        "REFERRAL_REWARD",
    ];
    let rows: Vec<_> = documented
        .iter()
        .enumerate()
        .map(|(index, activity_type)| {
            let mut row = base_row(activity_type);
            row["transactionHash"] = json!(format!("0x{index:02x}"));
            if matches!(
                *activity_type,
                "REWARD"
                    | "DEPOSIT"
                    | "WITHDRAWAL"
                    | "YIELD"
                    | "MAKER_REBATE"
                    | "TAKER_REBATE"
                    | "REFERRAL_REWARD"
            ) {
                row["conditionId"] = json!("");
                row["asset"] = json!("");
                row["side"] = json!("");
                row["outcome"] = json!("");
                row["outcomeIndex"] = json!(999);
            }
            row
        })
        .collect();
    let parsed = parse_rows(&rows);
    assert_eq!(parsed.len(), documented.len());
    assert!(
        parsed
            .iter()
            .all(|row| !matches!(row.activity_type, ActivityType::Unknown(_)))
    );
    let raw_only: HashSet<_> = parsed
        .iter()
        .filter(|row| row.activity_type.is_raw_only())
        .map(|row| row.activity_type.as_str())
        .collect();
    assert_eq!(raw_only.len(), 7);
    assert!(
        parsed
            .iter()
            .filter(|row| row.activity_type.is_raw_only())
            .all(|row| !row.requires_wallet_fence())
    );

    let rebates = parse_activity_response(
        include_bytes!("fixtures/activity_rebates_w0.json"),
        wallet("0xfd9b763674cb096cacec059fcfe60ae82aae09e8"),
        &context(ActivityTransport::Rest, 1_788_300_000, 1_788_300_001),
    )
    .expect("captured rebate rows must parse");
    assert!(
        rebates
            .rows
            .iter()
            .all(|row| row.activity_type.is_raw_only())
    );
}

#[test]
fn split_merge_optional_fields_and_conversion_fence_are_typed() {
    for activity_type in ["SPLIT", "MERGE", "CONVERSION"] {
        let mut row = base_row(activity_type);
        row["asset"] = json!("");
        row["side"] = json!("");
        row["outcome"] = json!("");
        row["outcomeIndex"] = json!(999);
        let parsed = parse_rows(&[row]);
        assert_eq!(parsed[0].asset, None);
        assert_eq!(parsed[0].outcome, None);
        assert_eq!(parsed[0].side, None);
        assert_eq!(
            parsed[0].requires_wallet_fence(),
            activity_type == "CONVERSION"
        );
    }
}

#[test]
fn trade_and_redeem_reject_missing_required_effect_fields() {
    for field in ["side", "price", "size"] {
        let mut trade = base_row("TRADE");
        trade.as_object_mut().unwrap().remove(field);
        assert!(
            parse_activity_response(
                &response(&[trade]),
                wallet(WALLET),
                &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
            )
            .is_err(),
            "missing TRADE {field} must fail validation"
        );
    }

    let mut redeem = base_row("REDEEM");
    redeem["conditionId"] = json!("");
    assert!(matches!(
        parse_activity_response(
            &response(&[redeem]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::InvalidRow {
            source: ActivityValidationError::MissingField {
                field: "conditionId"
            },
            ..
        })
    ));

    // A stamped outcome index with an empty label stays inconsistent; only
    // the venue's unattributed sentinel (999 + no label) parses without an
    // outcome (#544 fix 5).
    let mut redeem = base_row("REDEEM");
    redeem["outcome"] = json!("");
    assert!(matches!(
        parse_activity_response(
            &response(&[redeem]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::InvalidRow {
            source: ActivityValidationError::InvalidConditionOutcomeMapping,
            ..
        })
    ));
}

#[test]
fn unknown_type_is_retained_raw_and_signals_wallet_fence() {
    let row = base_row("FUTURE_POSITION_EFFECT");
    let raw = serde_json::to_vec(&row).unwrap();
    let parsed = parse_activity_row(
        &raw,
        Some(wallet(WALLET)),
        &context(ActivityTransport::Replay, 1_788_000_010, 1_788_000_011),
    )
    .expect("unknown row must remain retained");
    assert_eq!(
        parsed.activity_type,
        ActivityType::Unknown("FUTURE_POSITION_EFFECT".to_owned())
    );
    assert!(parsed.requires_wallet_fence());
    assert_eq!(parsed.raw_row_json, String::from_utf8(raw).unwrap());
    assert_eq!(parsed.raw_row_hash.len(), 64);
}

#[test]
fn wallet_missing_invalid_and_mismatch_invalidate_the_whole_window() {
    let mut missing = base_row("TRADE");
    missing.as_object_mut().unwrap().remove("proxyWallet");
    assert!(matches!(
        parse_activity_response(
            &response(&[base_row("TRADE"), missing]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::WindowInvalidated(
            ActivityWindowInvalidation::MissingWallet { row_index: 1 }
        ))
    ));

    let mut invalid = base_row("TRADE");
    invalid["proxyWallet"] = json!("not-a-wallet");
    assert!(matches!(
        parse_activity_response(
            &response(&[invalid]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::WindowInvalidated(
            ActivityWindowInvalidation::InvalidWallet { .. }
        ))
    ));

    let mut mismatch = base_row("TRADE");
    mismatch["proxyWallet"] = json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    assert!(matches!(
        parse_activity_response(
            &response(&[mismatch]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::WindowInvalidated(
            ActivityWindowInvalidation::WalletMismatch { .. }
        ))
    ));
}

#[test]
fn one_atomic_share_is_exact_and_excess_precision_is_rejected() {
    let mut atomic = base_row("TRADE");
    atomic["size"] = json!("0.000001");
    let parsed = parse_rows(&[atomic]);
    assert_eq!(parsed[0].share_amount, ShareAmount::from_atomic(1));

    let mut excess = base_row("TRADE");
    excess["size"] = json!("0.0000001");
    assert!(matches!(
        parse_activity_response(
            &response(&[excess]),
            wallet(WALLET),
            &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011)
        ),
        Err(ActivityParseError::InvalidRow {
            source: ActivityValidationError::InvalidShareAmount { .. },
            ..
        })
    ));
}

#[test]
fn milliseconds_normalize_to_validated_epoch_seconds() {
    let mut row = base_row("TRADE");
    row["timestamp"] = json!(1_788_000_000_123_i64);
    let parsed = parse_rows(&[row]);
    assert_eq!(parsed[0].source_time.0.unix_timestamp(), 1_788_000_000);
}

#[test]
fn group_identity_excludes_price_quantity_and_time_but_separates_effects() {
    let original = base_row("TRADE");
    let mut observation_change = original.clone();
    observation_change["price"] = json!("0.75");
    observation_change["size"] = json!("2.000000");
    observation_change["timestamp"] = json!(1_788_000_001_i64);
    let parsed = parse_rows(&[original, observation_change]);
    assert_eq!(parsed[0].group_id().unwrap(), parsed[1].group_id().unwrap());

    let mut asset_change = base_row("TRADE");
    asset_change["asset"] = json!("456");
    let mut outcome_change = base_row("TRADE");
    outcome_change["outcomeIndex"] = json!(1);
    outcome_change["outcome"] = json!("No");
    let mut side_change = base_row("TRADE");
    side_change["side"] = json!("SELL");
    let groups = aggregate_activity_rows(&parse_rows(&[
        base_row("TRADE"),
        asset_change,
        outcome_change,
        side_change,
    ]))
    .unwrap();
    assert_eq!(groups.len(), 4);
    assert!(
        groups
            .iter()
            .all(|group| group.group_id.key().0.starts_with("g2:"))
    );
    assert!(
        groups
            .iter()
            .all(|group| group.group_id.key().0.len() == 67)
    );
}

#[test]
fn duplicate_rows_are_multiset_members_and_permutation_is_stable() {
    let row = base_row("TRADE");
    let single = aggregate(std::slice::from_ref(&row));
    let duplicate = aggregate(&[row.clone(), row.clone()]);
    assert_eq!(single.row_count, 1);
    assert_eq!(duplicate.row_count, 2);
    assert_ne!(single.semantic_revision, duplicate.semantic_revision);

    let mut second = row.clone();
    second["price"] = json!("0.75");
    second["size"] = json!("2.000000");
    let forward = aggregate(&[row.clone(), second.clone()]);
    let reverse = aggregate(&[second, row]);
    assert!(forward.semantically_equal(&reverse));
}

#[test]
fn equal_aggregates_with_different_semantic_rows_have_different_revisions() {
    let mut a1 = base_row("TRADE");
    a1["price"] = json!("0.2");
    let mut a2 = base_row("TRADE");
    a2["price"] = json!("0.8");
    let mut b1 = base_row("TRADE");
    b1["price"] = json!("0.4");
    let mut b2 = base_row("TRADE");
    b2["price"] = json!("0.6");

    let first = aggregate(&[a1, a2]);
    let second = aggregate(&[b1, b2]);
    assert_eq!(first.share_sum, second.share_sum);
    assert_eq!(
        first.price_weighted_share_sum,
        second.price_weighted_share_sum
    );
    assert_eq!(
        first.compare_revision(&second),
        ActivityRevisionComparison::Changed
    );
}

#[test]
fn provenance_presentation_and_usdc_changes_are_semantic_noops() {
    let row = base_row("TRADE");
    let rest = parse_activity_row(
        &serde_json::to_vec(&row).unwrap(),
        Some(wallet(WALLET)),
        &context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011),
    )
    .unwrap();

    let mut presentation = row.clone();
    presentation["title"] = json!("different presentation");
    presentation["usdcSize"] = json!("0.999999");
    let replay = parse_activity_row(
        &serde_json::to_vec(&presentation).unwrap(),
        Some(wallet(WALLET)),
        &context(ActivityTransport::Replay, 1_788_000_020, 1_788_000_021),
    )
    .unwrap();
    assert_ne!(rest.raw_row_hash, replay.raw_row_hash);
    assert_ne!(rest.transport, replay.transport);
    assert_ne!(rest.received_at, replay.received_at);

    let left = aggregate_activity_rows(&[rest]).unwrap().remove(0);
    let right = aggregate_activity_rows(&[replay]).unwrap().remove(0);
    assert_eq!(left.source_usdc_sum, CollateralAmount::from_atomic(500_000));
    assert_eq!(
        right.source_usdc_sum,
        CollateralAmount::from_atomic(999_999)
    );
    assert!(left.semantically_equal(&right));
}

#[test]
fn field_order_snake_case_and_string_numerics_normalize_semantically() {
    let camel = br#"{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timestamp":1788000000,"conditionId":"0xcondition","type":"TRADE","size":1,"usdcSize":0.5,"transactionHash":"0xtransaction","price":0.5,"asset":"123","side":"BUY","outcomeIndex":0,"outcome":"Yes"}"#;
    let snake = br#"{"outcome":"Yes","outcome_index":"0","side":"buy","asset":"123","price":"0.5000","transaction_hash":"0xtransaction","usdc_size":"0.500000","size":"1.000000","activity_type":"TRADE","condition_id":"0xcondition","timestamp":"1788000000","proxy_wallet":"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#;
    let parse_context = context(ActivityTransport::Rest, 1_788_000_010, 1_788_000_011);
    let camel = parse_activity_row(camel, Some(wallet(WALLET)), &parse_context).unwrap();
    let snake = parse_activity_row(snake, Some(wallet(WALLET)), &parse_context).unwrap();
    assert_eq!(camel.group_id().unwrap(), snake.group_id().unwrap());
    assert_eq!(
        camel.semantic_row_encoding().unwrap(),
        snake.semantic_row_encoding().unwrap()
    );
    let left = aggregate_activity_rows(&[camel]).unwrap().remove(0);
    let right = aggregate_activity_rows(&[snake]).unwrap().remove(0);
    assert!(left.semantically_equal(&right));
}

#[test]
fn quantity_price_time_combo_and_effect_changes_are_not_semantic_noops() {
    let base = base_row("TRADE");
    let baseline = aggregate(std::slice::from_ref(&base));
    for (field, value) in [
        ("size", json!("2.000000")),
        ("price", json!("0.75")),
        ("timestamp", json!(1_788_000_001_i64)),
        ("isCombo", json!(true)),
    ] {
        let mut changed = base.clone();
        changed[field] = value;
        assert_eq!(
            baseline.compare_revision(&aggregate(&[changed])),
            ActivityRevisionComparison::Changed,
            "{field} must change the semantic revision"
        );
    }

    let mut changed_side = base;
    changed_side["side"] = json!("SELL");
    assert_eq!(
        baseline.compare_revision(&aggregate(&[changed_side])),
        ActivityRevisionComparison::DifferentGroup
    );
}

#[test]
fn mixed_member_timestamps_are_typed_causal_ambiguity() {
    let first = base_row("TRADE");
    let mut second = first.clone();
    second["timestamp"] = json!(1_788_000_001_i64);
    assert!(matches!(
        aggregate_activity_rows(&parse_rows(&[first, second])),
        Err(ActivityAggregationError::CausalAmbiguity {
            expected: 1_788_000_000,
            actual: 1_788_000_001,
            ..
        })
    ));
}

#[test]
fn exact_sums_use_size_times_price_not_source_usdc() {
    let mut first = base_row("TRADE");
    first["size"] = json!("18.550000");
    first["price"] = json!("0.7282911051");
    first["usdcSize"] = json!("13.693330");
    let group = aggregate(&[first]);
    assert_eq!(group.share_sum, ShareAmount::from_atomic(18_550_000));
    assert_eq!(
        group.source_usdc_sum,
        CollateralAmount::from_atomic(13_693_330)
    );
    assert_eq!(
        group.price_weighted_share_sum.0,
        Decimal::from_str_exact("13.509799999605").unwrap()
    );
}
