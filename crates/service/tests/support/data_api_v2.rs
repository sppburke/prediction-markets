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
