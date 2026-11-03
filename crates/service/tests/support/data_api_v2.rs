//! Cursor-paged Data API v2 replies over the fakes' stored v1 rows.
#![allow(dead_code, clippy::unwrap_used)]

use axum::http::StatusCode;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::HashMap;

struct Cursor {
    path: String,
    filters: HashMap<String, String>,
    offset: usize,
    limit: usize,
}

#[derive(Default)]
pub struct DataApiV2 {
    cursors: HashMap<String, Cursor>,
}

fn amount(value: &Value) -> Decimal {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
        .parse()
        .unwrap()
}

fn convert(row: &Value, positions: bool) -> Value {
    let mut converted = serde_json::Map::new();
    for (key, value) in row.as_object().unwrap() {
        let key = match key.as_str() {
            "proxyWallet" => "proxy_wallet",
            "conditionId" => "condition_id",
            "usdcSize" => "usdc_size",
            "transactionHash" => "transaction_hash",
            "asset" => "token_id",
            "outcomeIndex" => "outcome_index",
            "isCombo" => "is_combo",
            "negativeRisk" => "negative_risk",
            "size" if positions => "current_size",
            key => key,
        };
        converted.insert(key.to_owned(), value.clone());
    }
    if positions {
        converted.insert(
            "status".to_owned(),
            if row["redeemable"] == true {
                "REDEEMABLE"
            } else {
                "OPEN"
            }
            .into(),
        );
        // Lifetime buys deliberately differ from the current holding.
        converted.insert(
            "total_size".to_owned(),
            (amount(&row["size"]) + Decimal::ONE).to_string().into(),
        );
    }
    converted.into()
}

impl DataApiV2 {
    pub fn reply(
        &mut self,
        path: &str,
        rows: &[Value],
        query: &HashMap<String, String>,
        page_size_cap: Option<usize>,
    ) -> Result<Value, StatusCode> {
        let bad = StatusCode::BAD_REQUEST;
        let positions = match path {
            "/v2/activity" => false,
            "/v2/positions" => true,
            _ => return Err(bad),
        };
        if query.contains_key("offset") {
            return Err(bad);
        }
        let user = query
            .get("user")
            .filter(|user| !user.is_empty())
            .ok_or(bad)?;
        let direction = query.get("sort_direction").map_or("DESC", String::as_str);
        if !matches!(direction, "ASC" | "DESC") {
            return Err(bad);
        }
        if positions
            && [
                ("status", "OPEN"),
                ("filter_type", "TOKENS"),
                ("filter_amount", "0"),
                ("include_archived", "true"),
                ("sort_by", "TOKENS"),
            ]
            .iter()
            .any(|(key, value)| query.get(*key).map(String::as_str) != Some(*value))
        {
            return Err(bad);
        }
        let filters: HashMap<_, _> = query
            .iter()
            .filter(|(key, _)| !matches!(key.as_str(), "limit" | "cursor"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let (offset, limit) = if let Some(token) = query.get("cursor") {
            let cursor = self.cursors.get(token).ok_or(bad)?;
            if query.contains_key("limit") || cursor.path != path || cursor.filters != filters {
                return Err(bad);
            }
            (cursor.offset, cursor.limit)
        } else {
            let limit = query
                .get("limit")
                .map_or(Ok(100), |limit| limit.parse::<usize>())
                .map_err(|_| bad)?;
            if !(1..=1000).contains(&limit) || page_size_cap == Some(0) {
                return Err(bad);
            }
            (0, page_size_cap.map_or(limit, |cap| cap.min(limit)))
        };
        let bound = |key: &str, default| {
            query
                .get(key)
                .map_or(Ok(default), |value| value.parse::<i64>())
                .map_err(|_| bad)
        };
        let start = bound("start", i64::MIN)?;
        let end = bound("end", i64::MAX)?;
        let mut matching: Vec<_> = rows
            .iter()
            .filter(|row| row["proxyWallet"] == *user)
            .filter(|row| {
                positions
                    || (row["timestamp"]
                        .as_i64()
                        .is_some_and(|at| at >= start && at <= end)
                        && query
                            .get("type")
                            .is_none_or(|types| types.split(',').any(|kind| row["type"] == kind)))
            })
            .collect();
        // Stable sorting preserves stored position for equal timestamps/holdings.
        if positions {
            matching.sort_by_key(|row| amount(&row["size"]));
        } else {
            matching.sort_by_key(|row| row["timestamp"].as_i64().unwrap());
        }
        if direction == "DESC" {
            matching.reverse();
        }
        let data: Vec<_> = matching
            .iter()
            .skip(offset)
            .take(limit)
            .map(|row| convert(row, positions))
            .collect();
        let next_offset = offset + data.len();
        let has_more = next_offset < matching.len();
        let next_cursor = has_more.then(|| {
            let token = format!("fake-v2-{}", self.cursors.len());
            self.cursors.insert(
                token.clone(),
                Cursor {
                    path: path.to_owned(),
                    filters,
                    offset: next_offset,
                    limit,
                },
            );
            token
        });
        Ok(json!({"data": data, "pagination": {
            "limit": limit, "offset": offset, "has_more": has_more, "next_cursor": next_cursor,
        }}))
    }
}

/// Adapt an already selected single-page fixture, preserving deliberately invalid rows.
/// Malformed JSON and non-array fixtures remain malformed source evidence.
pub fn fixture_page(url: &str, raw: Vec<u8>) -> Vec<u8> {
    let Ok(url) = reqwest::Url::parse(url) else {
        return raw;
    };
    if !matches!(url.path(), "/v2/activity" | "/v2/positions") {
        return raw;
    }
    let Ok(Value::Array(mut rows)) = serde_json::from_slice(&raw) else {
        return raw;
    };
    let positions = url.path() == "/v2/positions";
    if positions {
        for row in &mut rows {
            row.as_object_mut()
                .unwrap()
                .entry("redeemable")
                .or_insert(false.into());
        }
    }
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    // Selected negative fixtures must reach the production parser unchanged by fake filtering.
    // Ordinary fixtures use the same filtering, conversion and pagination as the HTTP fake.
    let well_shaped = rows.iter().all(|row| {
        row["proxyWallet"]
            .as_str()
            .is_some_and(|wallet| query.get("user").is_some_and(|user| wallet == user))
            && if positions {
                row.get("size").is_some_and(|size| {
                    size.as_str()
                        .map_or_else(|| size.to_string(), str::to_owned)
                        .parse::<Decimal>()
                        .is_ok()
                })
            } else {
                row["timestamp"].as_i64().is_some()
            }
    });
    if well_shaped && !query.contains_key("cursor") {
        let page = DataApiV2::default()
            .reply(url.path(), &rows, &query, None)
            .unwrap();
        if page["data"].as_array().unwrap().len() == rows.len() {
            return serde_json::to_vec(&page).unwrap();
        }
    }
    let data: Vec<_> = rows.iter().map(|row| convert(row, positions)).collect();
    serde_json::to_vec(&json!({"data":data,"pagination":{
        "limit":1000,"offset":0,"has_more":false,"next_cursor":null
    }}))
    .unwrap()
}

/// Read the old fixture queues for a v2 request. Positions combine both former partitions.
pub fn queued_fixture(
    url: &str,
    responses: &mut HashMap<String, std::collections::VecDeque<Vec<u8>>>,
) -> Option<Vec<u8>> {
    use pe_core_types::WalletAddress;
    use pe_source_polymarket_public::{PolymarketEndpoint, PositionPartition};
    if let Some(raw) = responses
        .get_mut(url)
        .and_then(std::collections::VecDeque::pop_front)
    {
        return Some(fixture_page(url, raw));
    }
    let parsed = reqwest::Url::parse(url).unwrap();
    let query: HashMap<_, _> = parsed.query_pairs().into_owned().collect();
    if !matches!(parsed.path(), "/v2/activity" | "/v2/positions") {
        return responses.get_mut(url)?.pop_front();
    }
    let base = url.split("/v2/").next().unwrap();
    let user = WalletAddress::from_hex(&query["user"]).unwrap().to_string();
    let raw = if parsed.path() == "/v2/activity" {
        let endpoint = |start| {
            PolymarketEndpoint::UserPositionActivityPage {
                user: user.clone(),
                end: query["end"].parse().unwrap(),
                start,
                offset: 0,
            }
            .url(base)
        };
        let start = query["start"].parse().unwrap();
        responses
            .get_mut(&endpoint(Some(start)))
            .and_then(std::collections::VecDeque::pop_front)
            .or_else(|| responses.get_mut(&endpoint(None))?.pop_front())?
    } else {
        let mut combined = Vec::new();
        for partition in [
            PositionPartition::NotRedeemable,
            PositionPartition::Redeemable,
        ] {
            let key = PolymarketEndpoint::CurrentPositionsReconciliationPage {
                user: user.clone(),
                partition,
                offset: 0,
            }
            .url(base);
            let raw = responses.get_mut(&key)?.pop_front()?;
            let Ok(Value::Array(rows)) = serde_json::from_slice(&raw) else {
                return Some(raw);
            };
            combined.extend(rows.into_iter().map(|mut row| {
                row["redeemable"] = partition.redeemable().into();
                row
            }));
        }
        serde_json::to_vec(&combined).unwrap()
    };
    Some(fixture_page(url, raw))
}
