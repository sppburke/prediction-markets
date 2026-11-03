#![allow(clippy::unwrap_used)]

#[path = "support/data_api_v2.rs"]
mod data_api_v2;

use axum::http::StatusCode;
use data_api_v2::DataApiV2;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn activity_query(direction: &str) -> HashMap<String, String> {
    query(&[
        ("user", "wallet"),
        ("type", "TRADE,SPLIT,MERGE,REDEEM,CONVERSION"),
        ("start", "10"),
        ("end", "30"),
        ("sort_direction", direction),
        ("limit", "1000"),
    ])
}

fn positions_query() -> HashMap<String, String> {
    query(&[
        ("user", "wallet"),
        ("status", "OPEN"),
        ("filter_type", "TOKENS"),
        ("filter_amount", "0"),
        ("include_archived", "true"),
        ("sort_by", "TOKENS"),
        ("sort_direction", "ASC"),
        ("limit", "1000"),
    ])
}

fn walk(
    api: &mut DataApiV2,
    path: &str,
    stored: &[Value],
    mut request: HashMap<String, String>,
    cap: usize,
    expected_offsets: &[usize],
) -> Vec<Value> {
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let mut terminal_pages = 0;
    for offset in expected_offsets {
        let page = api.reply(path, stored, &request, Some(cap)).unwrap();
        assert_eq!(page.as_object().unwrap().len(), 2);
        let pagination = &page["pagination"];
        assert_eq!(pagination.as_object().unwrap().len(), 4);
        assert_eq!(pagination["offset"], *offset);
        assert_eq!(pagination["offset"], rows.len());
        assert_eq!(pagination["limit"], cap);
        rows.extend(page["data"].as_array().unwrap().iter().cloned());
        if let Some(cursor) = pagination["next_cursor"].as_str() {
            assert_eq!(pagination["has_more"], true);
            assert!(seen.insert(cursor.to_owned()));
            request.remove("limit");
            request.insert("cursor".to_owned(), cursor.to_owned());
        } else {
            assert!(pagination["next_cursor"].is_null());
            assert_eq!(pagination["has_more"], false);
            assert_eq!(Some(offset), expected_offsets.last());
            terminal_pages += 1;
        }
    }
    assert_eq!(terminal_pages, 1);
    rows
}

#[test]
fn activity_walk_filters_orders_and_preserves_values() {
    let mut stored: Vec<_> = [
        (20, "TRADE", "middle-first"),
        (30, "REDEEM", "end"),
        (10, "SPLIT", "start"),
        (20, "MERGE", "middle-second"),
        (20, "CONVERSION", "middle-third"),
        (9, "TRADE", "too-early"),
        (31, "TRADE", "too-late"),
        (20, "REWARD", "wrong-type"),
    ]
    .into_iter()
    .map(|(at, kind, transaction)| {
        json!({
            "proxyWallet": "wallet", "timestamp": at, "conditionId": "condition",
            "type": kind, "size": "5.000001", "usdcSize": "2.5000005",
            "transactionHash": transaction, "price": "0.5000001", "asset": "123",
            "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false,
        })
    })
    .collect();
    let mut other = stored[0].clone();
    other["proxyWallet"] = "other-wallet".into();
    stored.push(other);
    let original = stored.clone();
    for direction in ["ASC", "DESC"] {
        let rows = walk(
            &mut DataApiV2::default(),
            "/v2/activity",
            &stored,
            activity_query(direction),
            2,
            &[0, 2, 4],
        );
        let mut expected = vec![
            "start",
            "middle-first",
            "middle-second",
            "middle-third",
            "end",
        ];
        if direction == "DESC" {
            expected.reverse();
        }
        assert_eq!(
            rows.iter()
                .map(|row| row["transaction_hash"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            rows.iter()
                .map(|row| &row["transaction_hash"])
                .collect::<HashSet<_>>()
                .len(),
            5
        );
        for row in rows {
            let source = stored
                .iter()
                .find(|source| source["transactionHash"] == row["transaction_hash"])
                .unwrap();
            for (old, new) in [
                ("proxyWallet", "proxy_wallet"),
                ("timestamp", "timestamp"),
                ("conditionId", "condition_id"),
                ("type", "type"),
                ("size", "size"),
                ("usdcSize", "usdc_size"),
                ("transactionHash", "transaction_hash"),
                ("price", "price"),
                ("asset", "token_id"),
                ("side", "side"),
                ("outcomeIndex", "outcome_index"),
                ("outcome", "outcome"),
                ("isCombo", "is_combo"),
            ] {
                assert_eq!(row[new], source[old]);
                if old != new {
                    assert!(row.get(old).is_none());
                }
            }
            assert_eq!(
                row.as_object().unwrap().len(),
                source.as_object().unwrap().len()
            );
        }
    }
    assert_eq!(stored, original);
}

#[test]
fn positions_walk_includes_both_redeemable_states_and_current_holdings() {
    let stored = vec![
        json!({"proxyWallet":"wallet", "asset":"big", "conditionId":"condition", "outcomeIndex":0, "size":"10", "redeemable":false, "negativeRisk":true}),
        json!({"proxyWallet":"wallet", "asset":"dust", "conditionId":"condition", "outcomeIndex":1, "size":"0.000001", "redeemable":true, "negativeRisk":false}),
        json!({"proxyWallet":"elsewhere", "asset":"other", "size":"1", "redeemable":false}),
    ];
    let rows = walk(
        &mut DataApiV2::default(),
        "/v2/positions",
        &stored,
        positions_query(),
        1,
        &[0, 1],
    );
    assert_eq!(
        rows[0],
        json!({"proxy_wallet":"wallet", "token_id":"dust", "condition_id":"condition", "outcome_index":1, "current_size":"0.000001", "total_size":"1.000001", "redeemable":true, "negative_risk":false, "status":"REDEEMABLE"})
    );
    assert_eq!(
        rows[1],
        json!({"proxy_wallet":"wallet", "token_id":"big", "condition_id":"condition", "outcome_index":0, "current_size":"10", "total_size":"11", "redeemable":false, "negative_risk":true, "status":"OPEN"})
    );
    for row in rows {
        assert_ne!(row["current_size"], row["total_size"]);
    }
}

#[test]
fn cursor_pages_reject_offset_limit_changed_filters_and_unknown_cursors() {
    for (path, first, stored) in [
        (
            "/v2/activity",
            activity_query("DESC"),
            vec![json!({"proxyWallet":"wallet", "timestamp":20, "type":"TRADE"}); 3],
        ),
        (
            "/v2/positions",
            positions_query(),
            vec![json!({"proxyWallet":"wallet", "size":"1", "redeemable":false}); 3],
        ),
    ] {
        let mut api = DataApiV2::default();
        let page = api.reply(path, &stored, &first, Some(1)).unwrap();
        let cursor = page["pagination"]["next_cursor"].as_str().unwrap();
        let mut next = first.clone();
        next.remove("limit");
        next.insert("cursor".into(), cursor.into());
        assert_eq!(
            api.reply(path, &stored, &next, None).unwrap()["pagination"]["limit"],
            1
        );
        for (key, value) in [("offset", "0"), ("limit", "1"), ("cursor", "unknown")] {
            let mut invalid = next.clone();
            invalid.insert(key.into(), value.into());
            assert_eq!(
                api.reply(path, &stored, &invalid, None),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        for key in first.keys().filter(|key| key.as_str() != "limit") {
            let mut invalid = next.clone();
            invalid.insert(key.clone(), "changed".into());
            assert_eq!(
                api.reply(path, &stored, &invalid, None),
                Err(StatusCode::BAD_REQUEST)
            );
            invalid.remove(key);
            assert_eq!(
                api.reply(path, &stored, &invalid, None),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        for (key, value) in [("offset", "0"), ("limit", "1001")] {
            let mut invalid = first.clone();
            invalid.insert(key.into(), value.into());
            assert_eq!(
                api.reply(path, &stored, &invalid, None),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        let mut unknown = first;
        unknown.remove("limit");
        unknown.insert("cursor".into(), cursor.into());
        assert_eq!(
            DataApiV2::default().reply(path, &stored, &unknown, None),
            Err(StatusCode::BAD_REQUEST)
        );
    }
}

#[test]
fn empty_and_exact_multiple_walks_end_without_an_extra_page() {
    walk(
        &mut DataApiV2::default(),
        "/v2/activity",
        &[],
        activity_query("DESC"),
        2,
        &[0],
    );
    let stored = vec![json!({"proxyWallet":"wallet", "timestamp":20, "type":"TRADE"}); 4];
    walk(
        &mut DataApiV2::default(),
        "/v2/activity",
        &stored,
        activity_query("DESC"),
        2,
        &[0, 2],
    );
    let page = DataApiV2::default()
        .reply("/v2/activity", &stored, &activity_query("DESC"), None)
        .unwrap();
    assert_eq!(page["pagination"]["limit"], 1000);
    assert_eq!(page["data"].as_array().unwrap().len(), 4);
    assert_eq!(page["pagination"]["has_more"], false);
}
