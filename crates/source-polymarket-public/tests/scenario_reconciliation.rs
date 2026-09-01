#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};

use pe_core_types::{ReceivedAt, ShareAmount, SourceId, SourceTimestamp, WalletAddress};
use pe_source_polymarket_public::{
    ACTIVITY_MAX_OFFSET, ActivityAssetMapping, ActivityParseContext, ActivityReadError,
    ActivityTransport, FixtureFetcher, PolymarketEndpoint, PositionClassification,
    PositionPartition, PositionReadError, RECONCILIATION_PAGE_LIMIT, fetch_complete_activity,
    fetch_complete_positions, parse_activity_response,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use time::OffsetDateTime;

const BASE: &str = "https://api.example.com";
const WALLET: &str = "0xfd9b763674cb096cacec059fcfe60ae82aae09e8";
const CONDITION: &str = "0x3f19d2f61d608fca86bbeab555dc434e41eb81d92d9e4a1c2c42c8cff35075ac";
const ASSET: &str = "82474696549130542383104061200782087590711637544881666586439653268073770672093";

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET).unwrap()
}

fn activity_row(timestamp: i64, transaction: String, asset: String, outcome: u16) -> Value {
    json!({
        "proxyWallet": WALLET,
        "timestamp": timestamp,
        "conditionId": format!("0xcondition{outcome}"),
        "type": "TRADE",
        "size": "1.000000",
        "usdcSize": "0.500000",
        "transactionHash": transaction,
        "price": "0.500000",
        "asset": asset,
        "side": "BUY",
        "outcomeIndex": outcome,
        "outcome": format!("Outcome {outcome}"),
        "isCombo": false
    })
}

fn context() -> ActivityParseContext {
    let now = OffsetDateTime::from_unix_timestamp(200).unwrap();
    ActivityParseContext {
        source_id: SourceId("test".to_owned()),
        observed_at: SourceTimestamp(now),
        received_at: ReceivedAt(now),
        transport: ActivityTransport::Rest,
    }
}

fn mapping(rows: Vec<Value>) -> ActivityAssetMapping {
    let raw = serde_json::to_vec(&rows).unwrap();
    let parsed = parse_activity_response(&raw, wallet(), &context()).unwrap();
    ActivityAssetMapping::from_rows(&parsed.rows).unwrap()
}

fn position_url(partition: PositionPartition, offset: u32) -> String {
    PolymarketEndpoint::CurrentPositionsReconciliationPage {
        user: WALLET.to_owned(),
        partition,
        offset,
    }
    .url(BASE)
}

fn captured_assets(bytes: &[u8]) -> BTreeSet<String> {
    serde_json::from_slice::<Vec<Value>>(bytes)
        .unwrap()
        .into_iter()
        .map(|row| row["asset"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn captured_explicit_partitions_are_disjoint_and_match_the_omitted_sample() {
    let not_redeemable = captured_assets(include_bytes!(
        "fixtures/positions_redeemable_false_w0_trimmed.json"
    ));
    let not_redeemable_archived = captured_assets(include_bytes!(
        "fixtures/positions_false_archived_w0_trimmed.json"
    ));
    let redeemable = captured_assets(include_bytes!(
        "fixtures/positions_partition_true_w0_trimmed.json"
    ));
    let omitted = captured_assets(include_bytes!("fixtures/positions_omitted_w0_trimmed.json"));
    let manifest: Vec<Value> = serde_json::from_slice(include_bytes!(
        "fixtures/positions_w0_manifest_trimmed.json"
    ))
    .unwrap();

    assert!(not_redeemable.is_disjoint(&redeemable));
    assert_eq!(not_redeemable, not_redeemable_archived);
    assert_eq!(
        not_redeemable
            .union(&redeemable)
            .cloned()
            .collect::<BTreeSet<_>>(),
        omitted
    );
    assert_eq!(manifest.len(), 4);
    assert!(manifest.iter().all(|entry| {
        entry["fetched_at_unix"].as_i64().is_some()
            && entry["sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
            && entry["url"].as_str().is_some()
    }));
}

fn activity_url(start: Option<i64>, end: i64, offset: u32) -> String {
    PolymarketEndpoint::UserPositionActivityPage {
        user: WALLET.to_owned(),
        end,
        start,
        offset,
    }
    .url(BASE)
}

async fn one_position_read(
    activity_rows: Vec<Value>,
    position_rows: Vec<Value>,
) -> pe_source_polymarket_public::CompletePositionsRead {
    let activity = mapping(activity_rows);
    let fetcher = FixtureFetcher::new(HashMap::from([
        (
            position_url(PositionPartition::NotRedeemable, 0),
            serde_json::to_vec(&position_rows).unwrap(),
        ),
        (
            position_url(PositionPartition::Redeemable, 0),
            b"[]".to_vec(),
        ),
    ]));
    fetch_complete_positions(&fetcher, BASE, wallet(), &activity)
        .await
        .unwrap()
}

#[tokio::test]
async fn captured_partition_row_uses_exact_size_and_activity_combo_identity() {
    let captured = include_bytes!("fixtures/positions_partition_true_w0_trimmed.json").to_vec();
    let activity = mapping(vec![json!({
        "proxyWallet": WALLET,
        "timestamp": 100,
        "conditionId": CONDITION,
        "type": "TRADE",
        "size": "0.120000",
        "usdcSize": "0.060000",
        "transactionHash": "0xactivity",
        "price": "0.500000",
        "asset": ASSET,
        "side": "BUY",
        "outcomeIndex": 1,
        "outcome": "Down",
        "isCombo": false
    })]);
    let fetcher = FixtureFetcher::new(HashMap::from([
        (
            position_url(PositionPartition::NotRedeemable, 0),
            b"[]".to_vec(),
        ),
        (position_url(PositionPartition::Redeemable, 0), captured),
    ]));

    let read = fetch_complete_positions(&fetcher, BASE, wallet(), &activity)
        .await
        .unwrap();
    assert_eq!(read.positions.len(), 1);
    assert_eq!(
        read.positions[0].size,
        ShareAmount::from_decimal_exact(Decimal::new(12, 2)).unwrap()
    );
    assert_eq!(
        read.positions[0].classification,
        PositionClassification::Ordinary,
        "negativeRisk=true in the positions fixture must not define combo identity"
    );
    assert_eq!(read.pages.len(), 2);
}

#[tokio::test]
async fn partition_layout_and_presentation_do_not_change_semantic_proof() {
    let activity = mapping(vec![activity_row(
        100,
        "0xactivity".to_owned(),
        "asset-1".to_owned(),
        0,
    )]);
    let compact = serde_json::to_vec(&vec![json!({
        "proxyWallet": WALLET,
        "asset": "asset-1",
        "conditionId": "0xcondition0",
        "size": "1.250000",
        "outcomeIndex": 0,
        "negativeRisk": true,
        "cashPnl": 10
    })])
    .unwrap();
    let presented = br#"[
      {"title":"changed", "cashPnl":-999, "outcomeIndex":0,
       "size":"1.25", "conditionId":"0xcondition0", "asset":"asset-1",
       "proxyWallet":"0xfd9b763674cb096cacec059fcfe60ae82aae09e8",
       "negativeRisk":false}
    ]"#
    .to_vec();
    let first = FixtureFetcher::new(HashMap::from([
        (position_url(PositionPartition::NotRedeemable, 0), compact),
        (
            position_url(PositionPartition::Redeemable, 0),
            b"[]".to_vec(),
        ),
    ]));
    let second = FixtureFetcher::new(HashMap::from([
        (
            position_url(PositionPartition::NotRedeemable, 0),
            b"[]".to_vec(),
        ),
        (position_url(PositionPartition::Redeemable, 0), presented),
    ]));
    let left = fetch_complete_positions(&first, BASE, wallet(), &activity)
        .await
        .unwrap();
    let right = fetch_complete_positions(&second, BASE, wallet(), &activity)
        .await
        .unwrap();
    assert!(left.semantically_equal(&right));
    assert_eq!(left.semantic_hash(), right.semantic_hash());
    assert_ne!(left.pages, right.pages);
}

#[tokio::test]
async fn every_position_semantic_field_changes_the_proof() {
    let base_activity = activity_row(100, "0xbase".to_owned(), "asset-1".to_owned(), 0);
    let base_position = json!({
        "proxyWallet": WALLET,
        "asset": "asset-1",
        "conditionId": "0xcondition0",
        "size": "1.000000",
        "outcomeIndex": 0,
    });
    let baseline = one_position_read(vec![base_activity], vec![base_position]).await;

    let mut cases = Vec::new();
    cases.push((
        activity_row(100, "0xasset".to_owned(), "asset-2".to_owned(), 0),
        json!({
            "proxyWallet": WALLET,
            "asset": "asset-2",
            "conditionId": "0xcondition0",
            "size": "1.000000",
            "outcomeIndex": 0,
        }),
    ));
    let mut condition_activity =
        activity_row(100, "0xcondition".to_owned(), "asset-1".to_owned(), 0);
    condition_activity["conditionId"] = json!("0xchanged-condition");
    cases.push((
        condition_activity,
        json!({
            "proxyWallet": WALLET,
            "asset": "asset-1",
            "conditionId": "0xchanged-condition",
            "size": "1.000000",
            "outcomeIndex": 0,
        }),
    ));
    let mut outcome_activity = activity_row(100, "0xoutcome".to_owned(), "asset-1".to_owned(), 1);
    outcome_activity["conditionId"] = json!("0xcondition0");
    cases.push((
        outcome_activity,
        json!({
            "proxyWallet": WALLET,
            "asset": "asset-1",
            "conditionId": "0xcondition0",
            "size": "1.000000",
            "outcomeIndex": 1,
        }),
    ));
    let mut combo_activity = activity_row(100, "0xcombo".to_owned(), "asset-1".to_owned(), 0);
    combo_activity["isCombo"] = json!(true);
    cases.push((
        combo_activity,
        json!({
            "proxyWallet": WALLET,
            "asset": "asset-1",
            "conditionId": "0xcondition0",
            "size": "1.000000",
            "outcomeIndex": 0,
            "negativeRisk": false,
        }),
    ));
    cases.push((
        activity_row(100, "0xsize".to_owned(), "asset-1".to_owned(), 0),
        json!({
            "proxyWallet": WALLET,
            "asset": "asset-1",
            "conditionId": "0xcondition0",
            "size": "2.000000",
            "outcomeIndex": 0,
        }),
    ));

    for (activity, position) in cases {
        let changed = one_position_read(vec![activity], vec![position]).await;
        assert!(!baseline.semantically_equal(&changed));
        assert_ne!(baseline.semantic_hash(), changed.semantic_hash());
    }
}

#[tokio::test]
async fn duplicate_asset_across_explicit_partitions_rejects() {
    let activity = mapping(vec![activity_row(
        100,
        "0xactivity".to_owned(),
        "asset-1".to_owned(),
        0,
    )]);
    let body = serde_json::to_vec(&vec![json!({
        "proxyWallet": WALLET,
        "asset": "asset-1",
        "conditionId": "0xcondition0",
        "size": "1",
        "outcomeIndex": 0
    })])
    .unwrap();
    let fetcher = FixtureFetcher::new(HashMap::from([
        (
            position_url(PositionPartition::NotRedeemable, 0),
            body.clone(),
        ),
        (position_url(PositionPartition::Redeemable, 0), body),
    ]));
    let error = fetch_complete_positions(&fetcher, BASE, wallet(), &activity)
        .await
        .expect_err("overlapping partitions must fail");
    assert!(matches!(error, PositionReadError::DuplicateAsset { .. }));
}

#[tokio::test]
async fn missing_or_conflicting_activity_mapping_rejects() {
    let activity = mapping(Vec::new());
    let body = serde_json::to_vec(&vec![json!({
        "proxyWallet": WALLET,
        "asset": "asset-1",
        "conditionId": "0xcondition0",
        "size": "1",
        "outcomeIndex": 0
    })])
    .unwrap();
    let fetcher = FixtureFetcher::new(HashMap::from([(
        position_url(PositionPartition::NotRedeemable, 0),
        body,
    )]));
    assert!(matches!(
        fetch_complete_positions(&fetcher, BASE, wallet(), &activity).await,
        Err(PositionReadError::MissingActivityMapping { .. })
    ));

    let conflicting = vec![
        activity_row(100, "0xone".to_owned(), "asset-1".to_owned(), 0),
        activity_row(101, "0xtwo".to_owned(), "asset-1".to_owned(), 1),
    ];
    let raw = serde_json::to_vec(&conflicting).unwrap();
    let parsed = parse_activity_response(&raw, wallet(), &context()).unwrap();
    assert!(matches!(
        ActivityAssetMapping::from_rows(&parsed.rows),
        Err(PositionReadError::ConflictingActivityMapping { .. })
    ));
}

#[test]
fn incomplete_split_identity_does_not_override_a_complete_activity_mapping() {
    let raw = serde_json::to_vec(&vec![json!({
        "proxyWallet": WALLET,
        "timestamp": 100,
        "conditionId": CONDITION,
        "type": "SPLIT",
        "size": "1.000000",
        "usdcSize": "1.000000",
        "transactionHash": "0xsplit",
        "price": "0.500000",
        "asset": ASSET,
        "isCombo": false
    })])
    .unwrap();
    let parsed = parse_activity_response(&raw, wallet(), &context()).unwrap();
    let incomplete = ActivityAssetMapping::from_rows(&parsed.rows).unwrap();
    assert!(
        incomplete
            .identity(&pe_core_types::PolymarketTokenId(ASSET.to_owned()))
            .is_none()
    );
}

#[tokio::test]
async fn exact_position_precision_and_wallet_identity_fail_closed() {
    let activity = mapping(vec![activity_row(
        100,
        "0xactivity".to_owned(),
        "asset-1".to_owned(),
        0,
    )]);
    for (wallet_value, size, expected_wallet_error) in [
        (WALLET, "0.0000001", false),
        (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "1.000000",
            true,
        ),
    ] {
        let body = serde_json::to_vec(&vec![json!({
            "proxyWallet": wallet_value,
            "asset": "asset-1",
            "conditionId": "0xcondition0",
            "size": size,
            "outcomeIndex": 0
        })])
        .unwrap();
        let fetcher = FixtureFetcher::new(HashMap::from([(
            position_url(PositionPartition::NotRedeemable, 0),
            body,
        )]));
        let error = fetch_complete_positions(&fetcher, BASE, wallet(), &activity)
            .await
            .expect_err("invalid semantic field must fail");
        assert_eq!(
            matches!(error, PositionReadError::WalletMismatch { .. }),
            expected_wallet_error
        );
    }
}

#[tokio::test]
async fn saturated_positions_terminal_offset_is_typed_incomplete() {
    let total = 10_500_u32;
    let activity_rows = (0..total)
        .map(|index| {
            let mut row = activity_row(100, format!("0x{index:064x}"), format!("asset-{index}"), 0);
            row["conditionId"] = json!(format!("0xcondition{index}"));
            row
        })
        .collect::<Vec<_>>();
    let activity = mapping(activity_rows);
    let mut responses = HashMap::new();
    for offset in (0..=10_000_u32).step_by(500) {
        let rows = (offset..offset + RECONCILIATION_PAGE_LIMIT)
            .map(|index| {
                json!({
                    "proxyWallet": WALLET,
                    "asset": format!("asset-{index}"),
                    "conditionId": format!("0xcondition{index}"),
                    "size": "1.000000",
                    "outcomeIndex": 0,
                })
            })
            .collect::<Vec<_>>();
        responses.insert(
            position_url(PositionPartition::NotRedeemable, offset),
            serde_json::to_vec(&rows).unwrap(),
        );
    }
    let fetcher = FixtureFetcher::new(responses);
    assert!(matches!(
        fetch_complete_positions(&fetcher, BASE, wallet(), &activity).await,
        Err(PositionReadError::SaturatedTerminalPage {
            partition: PositionPartition::NotRedeemable,
            offset: 10_000,
        })
    ));
}

#[tokio::test]
async fn oversized_activity_and_position_pages_are_typed_incomplete() {
    let activity_rows = (0..=RECONCILIATION_PAGE_LIMIT)
        .map(|index| {
            activity_row(
                100,
                format!("0xoversized{index}"),
                format!("asset-{index}"),
                0,
            )
        })
        .collect::<Vec<_>>();
    let activity_fetcher = FixtureFetcher::new(HashMap::from([(
        activity_url(Some(0), 100, 0),
        serde_json::to_vec(&activity_rows).unwrap(),
    )]));
    assert!(matches!(
        fetch_complete_activity(&activity_fetcher, BASE, wallet(), Some(0), 100).await,
        Err(ActivityReadError::PageTooLarge {
            row_count: 501,
            limit: RECONCILIATION_PAGE_LIMIT,
        })
    ));

    let position_rows = (0..=RECONCILIATION_PAGE_LIMIT)
        .map(|index| {
            json!({
                "proxyWallet": WALLET,
                "asset": format!("asset-{index}"),
                "conditionId": format!("0xcondition{index}"),
                "size": "1.000000",
                "outcomeIndex": 0,
            })
        })
        .collect::<Vec<_>>();
    let positions_fetcher = FixtureFetcher::new(HashMap::from([(
        position_url(PositionPartition::NotRedeemable, 0),
        serde_json::to_vec(&position_rows).unwrap(),
    )]));
    assert!(matches!(
        fetch_complete_positions(
            &positions_fetcher,
            BASE,
            wallet(),
            &ActivityAssetMapping::from_rows(&[]).unwrap(),
        )
        .await,
        Err(PositionReadError::PageTooLarge {
            row_count: 501,
            limit: RECONCILIATION_PAGE_LIMIT,
        })
    ));
}

#[tokio::test]
async fn activity_order_is_semantic_and_ignores_presentation_changes() {
    let mut one = activity_row(100, "0xone".to_owned(), "asset-one".to_owned(), 0);
    let two = activity_row(100, "0xtwo".to_owned(), "asset-two".to_owned(), 0);
    let first = FixtureFetcher::new(HashMap::from([(
        activity_url(None, 100, 0),
        serde_json::to_vec(&vec![one.clone(), two.clone()]).unwrap(),
    )]));
    one["title"] = json!("presentation changed");
    let second = FixtureFetcher::new(HashMap::from([(
        activity_url(None, 100, 0),
        serde_json::to_vec(&vec![two, one]).unwrap(),
    )]));
    let left = fetch_complete_activity(&first, BASE, wallet(), None, 100)
        .await
        .unwrap();
    let right = fetch_complete_activity(&second, BASE, wallet(), None, 100)
        .await
        .unwrap();
    let ids = |read: &pe_source_polymarket_public::CompleteActivityRead| {
        read.rows
            .iter()
            .map(|row| row.group_id().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&left), ids(&right));
    assert_ne!(
        left.rows
            .iter()
            .map(|row| &row.raw_row_hash)
            .collect::<Vec<_>>(),
        right
            .rows
            .iter()
            .map(|row| &row.raw_row_hash)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn saturated_activity_window_splits_and_returns_stable_ascending_rows() {
    let saturated = (0..RECONCILIATION_PAGE_LIMIT)
        .map(|index| {
            activity_row(
                50,
                format!("0xsaturated{index}"),
                format!("asset-saturated-{index}"),
                0,
            )
        })
        .collect::<Vec<_>>();
    let mut responses = HashMap::new();
    for offset in (0..=ACTIVITY_MAX_OFFSET).step_by(500) {
        responses.insert(
            activity_url(Some(0), 100, offset),
            serde_json::to_vec(&saturated).unwrap(),
        );
    }
    responses.insert(
        activity_url(Some(0), 49, 0),
        serde_json::to_vec(&vec![activity_row(
            10,
            "0xold".to_owned(),
            "asset-old".to_owned(),
            0,
        )])
        .unwrap(),
    );
    responses.insert(
        activity_url(Some(49), 50, 0),
        serde_json::to_vec(&vec![activity_row(
            50,
            "0xboundary".to_owned(),
            "asset-boundary".to_owned(),
            0,
        )])
        .unwrap(),
    );
    responses.insert(
        activity_url(Some(50), 100, 0),
        serde_json::to_vec(&vec![activity_row(
            90,
            "0xnew".to_owned(),
            "asset-new".to_owned(),
            0,
        )])
        .unwrap(),
    );
    let read = fetch_complete_activity(
        &FixtureFetcher::new(responses),
        BASE,
        wallet(),
        Some(0),
        100,
    )
    .await
    .unwrap();
    assert_eq!(
        read.rows
            .iter()
            .map(|row| row.source_time.0.unix_timestamp())
            .collect::<Vec<_>>(),
        vec![10, 50, 90]
    );
    assert!(
        read.pages
            .iter()
            .any(|page| page.offset == ACTIVITY_MAX_OFFSET),
        "the saturated parent walk remains linked as provenance"
    );
    assert_eq!(read.pages.iter().filter(|page| page.offset == 0).count(), 4);
}

#[tokio::test]
async fn still_full_one_second_activity_window_is_typed_incomplete() {
    let page = (0..RECONCILIATION_PAGE_LIMIT)
        .map(|index| {
            activity_row(
                100,
                format!("0xterminal{index}"),
                format!("asset-terminal-{index}"),
                0,
            )
        })
        .collect::<Vec<_>>();
    let mut responses = HashMap::new();
    for offset in (0..=ACTIVITY_MAX_OFFSET).step_by(500) {
        responses.insert(
            activity_url(Some(99), 100, offset),
            serde_json::to_vec(&page).unwrap(),
        );
    }
    let error = fetch_complete_activity(
        &FixtureFetcher::new(responses),
        BASE,
        wallet(),
        Some(99),
        100,
    )
    .await
    .expect_err("a full terminal second cannot prove completeness");
    assert!(matches!(
        error,
        ActivityReadError::SaturatedTerminalSecond {
            end: 100,
            offset: ACTIVITY_MAX_OFFSET,
        }
    ));
}
