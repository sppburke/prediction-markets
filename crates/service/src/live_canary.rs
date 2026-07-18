//! Thin composition for the isolated canary actor. Financial state and POST authority remain in
//! `execution-core`; this module only supplies raw venue/source I/O and pure response decoding.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use pe_core_types::{
    CollateralAmount, OutcomeId, PolymarketConditionId, RawEvidence, RawHttpAttempt,
    RawHttpResponse, RawTransportFailure, ShareAmount, VenueOrderId,
};
use pe_execution_core::{
    AttemptPhase, CanaryPosition, CanaryReconciler, CanaryReconciliation, CanaryResolution,
    CanarySubmitter, PendingAttempt, RawReconciliation, raw_evidence_hash,
};
use pe_source_polymarket_public::{HttpRequestContext, PolymarketEndpoint, ReqwestFetcher};
use pe_venue_core::{ExactExecutionReport, VenueFill};
use pe_venue_polymarket::{CanaryV2Client, PostOnceResult, PreparedSubmission, V2BuyRequest};
use rust_decimal::Decimal;
use serde_json::Value;

pub struct LiveCanaryIo {
    venue: CanaryV2Client,
    positions: ReqwestFetcher,
    positions_wallet: String,
    standard_spender: String,
    boot_observations: Mutex<Vec<RawHttpResponse>>,
}

impl LiveCanaryIo {
    pub fn new(venue: CanaryV2Client) -> Result<Self, String> {
        let wallet = venue.deposit_wallet();
        let boot_observations = venue.take_raw_observations("boot");
        Ok(Self {
            venue,
            positions: ReqwestFetcher::new(reqwest::Client::new()).with_max_retries(0),
            positions_wallet: wallet,
            standard_spender: CanaryV2Client::standard_spender().map_err(|e| e.to_string())?,
            boot_observations: Mutex::new(boot_observations),
        })
    }
}

impl CanarySubmitter for LiveCanaryIo {
    fn prepare_buy(
        &self,
        request: V2BuyRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PreparedSubmission, String>> + Send + '_>> {
        Box::pin(async move {
            self.venue
                .prepare_buy(request)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn post_order_once(
        &self,
        submission: PreparedSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>
    {
        Box::pin(async move { self.venue.post_order_once(submission).await })
    }

    fn parse_post_response(&self, response: &RawHttpResponse) -> Result<PostOnceResult, String> {
        CanaryV2Client::parse_post_response(response).map_err(|error| error.to_string())
    }

    fn cancel_order_once(
        &self,
        order_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>
    {
        Box::pin(async move { self.venue.cancel_order_once(&order_id).await })
    }
}

impl CanaryReconciler for LiveCanaryIo {
    fn reconcile_raw(
        &self,
        tracked_conditions: Vec<PolymarketConditionId>,
        pending_order_hash: Option<String>,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = Result<RawReconciliation, String>> + Send + '_>> {
        Box::pin(async move {
            let mut evidence = std::mem::take(
                &mut *self
                    .boot_observations
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            )
            .into_iter()
            .map(RawEvidence::HttpResponse)
            .collect::<Vec<_>>();
            let (clob_evidence, clob_protocol_failure) = self
                .venue
                .clob_reconciliation_raw(pending_order_hash.as_deref(), deadline)
                .await;
            let clob_failed = clob_evidence
                .iter()
                .any(|observation| matches!(observation, RawEvidence::HttpTransportFailure(_)));
            evidence.extend(clob_evidence);
            if clob_failed || clob_protocol_failure.is_some() {
                return Ok(RawReconciliation {
                    evidence,
                    protocol_failure: clob_protocol_failure,
                });
            }
            const PAGE_SIZE: u32 = 500;
            const PAGE_LIMIT: u32 = 20;
            let mut positions_complete = false;
            for page in 0..PAGE_LIMIT {
                let url = PolymarketEndpoint::CurrentPositions {
                    user: self.positions_wallet.clone(),
                    limit: Some(PAGE_SIZE),
                    offset: Some(page.saturating_mul(PAGE_SIZE)),
                    redeemable: None,
                    size_threshold: Some(0),
                }
                .url("https://data-api.polymarket.com");
                let (attempts, protocol_error) = fetch_observed_until(
                    &self.positions,
                    &url,
                    HttpRequestContext {
                        source_id: "polymarket-data-api",
                        endpoint_kind: "data-positions",
                    },
                    deadline,
                )
                .await;
                let mut page_response = None;
                for attempt in attempts {
                    match attempt {
                        RawHttpAttempt::Response(response) => {
                            page_response = Some(response.clone());
                            evidence.push(RawEvidence::HttpResponse(response));
                        }
                        RawHttpAttempt::TransportFailure(failure) => {
                            return Ok(RawReconciliation {
                                evidence: evidence
                                    .into_iter()
                                    .chain([RawEvidence::HttpTransportFailure(failure)])
                                    .collect(),
                                protocol_failure: None,
                            });
                        }
                    }
                }
                if let Some(error) = protocol_error {
                    return Ok(RawReconciliation {
                        evidence,
                        protocol_failure: Some(error),
                    });
                }
                let Some(page_response) = page_response else {
                    return Ok(RawReconciliation {
                        evidence,
                        protocol_failure: Some(
                            "positions request produced no observation".to_owned(),
                        ),
                    });
                };
                if !(200..300).contains(&page_response.status) {
                    return Ok(RawReconciliation {
                        evidence,
                        protocol_failure: Some(format!(
                            "positions page returned HTTP {}",
                            page_response.status
                        )),
                    });
                }
                let count = match serde_json::from_slice::<Vec<Value>>(&page_response.body) {
                    Ok(rows) => rows.len(),
                    Err(error) => {
                        return Ok(RawReconciliation {
                            evidence,
                            protocol_failure: Some(format!("positions page is malformed: {error}")),
                        });
                    }
                };
                if count < usize::try_from(PAGE_SIZE).unwrap_or(usize::MAX) {
                    positions_complete = true;
                    break;
                }
            }
            if !positions_complete {
                return Ok(RawReconciliation {
                    evidence,
                    protocol_failure: Some("positions pagination cap reached".to_owned()),
                });
            }
            for condition in tracked_conditions {
                let url = format!("https://clob.polymarket.com/markets/{}", condition.0);
                let (attempts, protocol_error) = fetch_observed_until(
                    &self.positions,
                    &url,
                    HttpRequestContext {
                        source_id: "polymarket-clob-public",
                        endpoint_kind: "clob-market-resolution",
                    },
                    deadline,
                )
                .await;
                for attempt in attempts {
                    match attempt {
                        RawHttpAttempt::Response(response) => {
                            evidence.push(RawEvidence::HttpResponse(response));
                        }
                        RawHttpAttempt::TransportFailure(failure) => {
                            return Ok(RawReconciliation {
                                evidence: evidence
                                    .into_iter()
                                    .chain([RawEvidence::HttpTransportFailure(failure)])
                                    .collect(),
                                protocol_failure: None,
                            });
                        }
                    }
                }
                if let Some(error) = protocol_error {
                    return Ok(RawReconciliation {
                        evidence,
                        protocol_failure: Some(error),
                    });
                }
            }
            Ok(RawReconciliation {
                evidence,
                protocol_failure: None,
            })
        })
    }

    fn parse_reconciliation(
        &self,
        raw: &RawReconciliation,
        pending: Option<&PendingAttempt>,
        known_trade_ids: &[String],
    ) -> Result<CanaryReconciliation, String> {
        parse_reconciliation(raw, pending, known_trade_ids, &self.standard_spender)
    }
}

async fn fetch_observed_until(
    fetcher: &ReqwestFetcher,
    url: &str,
    context: HttpRequestContext,
    deadline: tokio::time::Instant,
) -> (Vec<RawHttpAttempt>, Option<String>) {
    let mut attempts = Vec::new();
    let result = fetcher
        .fetch_page_observed_until(url, context, deadline.into_std(), |attempt| {
            attempts.push(attempt);
            Ok(())
        })
        .await;
    match result {
        Err(error) => (attempts, Some(error.to_string())),
        Ok(_) => (attempts, None),
    }
}

fn parse_reconciliation(
    raw: &RawReconciliation,
    pending: Option<&PendingAttempt>,
    known_trade_ids: &[String],
    standard_spender: &str,
) -> Result<CanaryReconciliation, String> {
    let response = |path: &str| {
        raw.evidence
            .iter()
            .filter_map(|observation| match observation {
                RawEvidence::HttpResponse(response) => Some(response),
                RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
            })
            .find(|response| response.path == path)
            .ok_or_else(|| format!("missing reconciliation response {path}"))
            .and_then(parse_json_response)
    };
    let geoblock = response("/api/geoblock")?;
    let closed_only = response("/auth/ban-status/closed-only")?;
    let balance = response("/balance-allowance")?;
    let raw_pages = |path: &str| {
        let matches = raw
            .evidence
            .iter()
            .filter_map(|observation| match observation {
                RawEvidence::HttpResponse(response) if response.path == path => Some(response),
                RawEvidence::HttpResponse(_)
                | RawEvidence::HttpTransportFailure(_)
                | RawEvidence::Artifact(_) => None,
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(format!("missing reconciliation response {path}"));
        }
        Ok(matches)
    };
    let order_responses = raw_pages("/data/orders")?;
    let trade_responses = raw_pages("/data/trades")?;
    let position_responses = raw_pages("/positions")?;
    validate_clob_page_sequence(&order_responses)?;
    validate_clob_page_sequence(&trade_responses)?;
    validate_position_page_sequence(&position_responses)?;
    let parse_pages = |responses: &[&RawHttpResponse]| {
        responses
            .iter()
            .map(|response| parse_json_response(response))
            .collect::<Result<Vec<_>, _>>()
    };
    let orders = parse_pages(&order_responses)?;
    let trades = parse_pages(&trade_responses)?;
    let positions = parse_pages(&position_responses)?;
    let resolutions = raw
        .evidence
        .iter()
        .filter_map(|observation| match observation {
            RawEvidence::HttpResponse(response)
                if response.endpoint_kind == "clob-market-resolution" =>
            {
                Some(response)
            }
            RawEvidence::HttpResponse(_)
            | RawEvidence::HttpTransportFailure(_)
            | RawEvidence::Artifact(_) => None,
        })
        .map(parse_resolution)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let geoblocked = geoblock
        .get("blocked")
        .and_then(Value::as_bool)
        .ok_or_else(|| "geoblock response omitted blocked".to_owned())?;
    let closed_only = closed_only
        .get("closed_only")
        .and_then(Value::as_bool)
        .ok_or_else(|| "closed-only response omitted closed_only".to_owned())?;
    let free_collateral = atomic_amount(
        balance
            .get("balance")
            .ok_or_else(|| "balance response omitted balance".to_owned())?,
    )?;
    let allowances = balance
        .get("allowances")
        .and_then(Value::as_object)
        .ok_or_else(|| "balance response omitted allowances".to_owned())?;
    let mut standard_allowance = CollateralAmount::ZERO;
    let mut standard_spender_only = true;
    for (spender, value) in allowances {
        let amount = atomic_amount(value)?;
        if spender.eq_ignore_ascii_case(standard_spender) {
            standard_allowance = amount;
        } else if amount != CollateralAmount::ZERO {
            standard_spender_only = false;
        }
    }

    let order_rows = orders
        .iter()
        .map(page_data)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let mut open_order_ids = order_rows
        .into_iter()
        .map(|order| {
            order
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "open order omitted a valid id".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let trade_rows = trades
        .iter()
        .map(page_data)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let all_trade_ids = trade_rows
        .iter()
        .map(|trade| {
            trade
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| "trade omitted id".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let position_rows = positions
        .iter()
        .map(|page| {
            page.as_array()
                .ok_or_else(|| "positions response is not an array".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let parsed_positions = position_rows
        .iter()
        .map(|position| {
            let condition_id = position
                .get("conditionId")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "position omitted conditionId".to_owned())?;
            let outcome = position
                .get("outcomeIndex")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or_else(|| "position omitted a valid outcomeIndex".to_owned())?;
            let shares = ShareAmount::from_decimal_exact(decimal(position.get("size"), "size")?)
                .map_err(|error| error.to_string())?;
            Ok(CanaryPosition {
                condition_id: PolymarketConditionId(condition_id.to_owned()),
                outcome_id: OutcomeId(outcome),
                shares,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut position_identities = std::collections::HashSet::new();
    if parsed_positions.iter().any(|position| {
        !position_identities.insert((position.condition_id.0.clone(), position.outcome_id.0))
    }) {
        return Err("positions repeated a condition/outcome identity".to_owned());
    }
    let position_count = parsed_positions.len();

    let exact_order_state = pending
        .filter(|attempt| attempt.phase != AttemptPhase::Review)
        .map(|attempt| {
            let expected_path = format!("/data/order/{}", attempt.order_hash);
            let response = raw
                .evidence
                .iter()
                .find_map(|observation| match observation {
                    RawEvidence::HttpResponse(response)
                        if response.endpoint_kind == "exact-order"
                            && response.path == expected_path =>
                    {
                        Some(response)
                    }
                    RawEvidence::HttpResponse(_)
                    | RawEvidence::HttpTransportFailure(_)
                    | RawEvidence::Artifact(_) => None,
                })
                .ok_or_else(|| "missing exact pending-order response".to_owned())?;
            if response.status == 404 {
                return Ok(false);
            }
            let value = parse_json_response(response)?;
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| id.eq_ignore_ascii_case(&attempt.order_hash))
                .ok_or_else(|| "exact order lookup returned a different order".to_owned())?;
            Ok::<_, String>(!id.is_empty())
        })
        .transpose()?;

    let matching = trade_rows
        .iter()
        .copied()
        .filter(|trade| {
            pending.is_some_and(|attempt| {
                trade_matches_order(trade, &attempt.order_hash)
                    || attempt
                        .venue_order_id
                        .as_ref()
                        .is_some_and(|order| trade_matches_order(trade, &order.0))
            })
        })
        .collect::<Vec<_>>();
    if exact_order_state == Some(true) && matching.is_empty() {
        let order_hash = pending
            .map(|attempt| attempt.order_hash.clone())
            .ok_or_else(|| "exact order state has no pending attempt".to_owned())?;
        if !open_order_ids.contains(&order_hash) {
            open_order_ids.push(order_hash);
        }
    }
    let unexpected_activity = all_trade_ids.iter().any(|trade_id| {
        !known_trade_ids.contains(trade_id)
            && !matching
                .iter()
                .any(|trade| trade.get("id").and_then(Value::as_str) == Some(trade_id))
    });
    let execution_report = pending
        .filter(|attempt| attempt.phase != AttemptPhase::Review)
        .filter(|_| exact_order_state == Some(false) || !matching.is_empty())
        .map(|attempt| execution_report(attempt, &matching))
        .transpose()?;
    let evidence_hashes = raw
        .evidence
        .iter()
        .map(raw_evidence_hash)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .into_iter()
        .flatten()
        .collect();

    let observed_at = raw
        .evidence
        .iter()
        .filter_map(|observation| match observation {
            RawEvidence::HttpResponse(response) => Some(response),
            RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
        })
        .map(|response| response.observed_at)
        .min()
        .ok_or_else(|| "reconciliation returned no response evidence".to_owned())?;
    Ok(CanaryReconciliation {
        observed_at,
        geoblocked,
        closed_only,
        free_collateral,
        allowance: standard_allowance,
        standard_spender_only,
        open_order_ids,
        all_trade_ids,
        position_count,
        positions: parsed_positions,
        resolutions,
        unexpected_activity,
        execution_report,
        evidence_hashes,
    })
}

fn parse_resolution(response: &RawHttpResponse) -> Result<Option<CanaryResolution>, String> {
    let value = parse_json_response(response)?;
    let condition_id = value
        .get("condition_id")
        .and_then(Value::as_str)
        .filter(|condition| !condition.trim().is_empty())
        .ok_or_else(|| "resolution market omitted condition_id".to_owned())?;
    let tokens = value
        .get("tokens")
        .and_then(Value::as_array)
        .filter(|tokens| tokens.len() == 2)
        .ok_or_else(|| "resolution market is not binary".to_owned())?;
    let winners = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| token.get("winner").and_then(Value::as_bool) == Some(true))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match winners.as_slice() {
        [] => Ok(None),
        [winner] => Ok(Some(CanaryResolution {
            condition_id: PolymarketConditionId(condition_id.to_owned()),
            winner: OutcomeId(
                u16::try_from(*winner)
                    .map_err(|_| "resolution winner index overflowed".to_owned())?,
            ),
        })),
        _ => Err("resolution market reported multiple winners".to_owned()),
    }
}

fn parse_json_response(response: &RawHttpResponse) -> Result<Value, String> {
    if !(200..300).contains(&response.status) {
        return Err(format!(
            "{} returned status {}",
            response.path, response.status
        ));
    }
    serde_json::from_slice(&response.body)
        .map_err(|error| format!("{} response decode: {error}", response.path))
}

fn page_data(value: &Value) -> Result<&[Value], String> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| "paginated response omitted data".to_owned())
}

fn validate_clob_page_sequence(responses: &[&RawHttpResponse]) -> Result<(), String> {
    let mut expected_cursor: Option<String> = None;
    for (index, response) in responses.iter().enumerate() {
        let supplied_cursor = response
            .ordered_query
            .iter()
            .find(|(key, _)| key == "next_cursor")
            .map(|(_, value)| value.as_str());
        if supplied_cursor != expected_cursor.as_deref() {
            return Err("reconciliation cursor sequence is discontinuous".to_owned());
        }
        let page = parse_json_response(response)?;
        let next_cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .filter(|cursor| !cursor.is_empty())
            .ok_or_else(|| "reconciliation page omitted next_cursor".to_owned())?;
        let is_last = index + 1 == responses.len();
        if next_cursor == "LTE=" {
            if !is_last {
                return Err("reconciliation continued after the terminal cursor".to_owned());
            }
            expected_cursor = None;
        } else {
            if is_last {
                return Err("reconciliation stopped before the terminal cursor".to_owned());
            }
            expected_cursor = Some(next_cursor.to_owned());
        }
    }
    Ok(())
}

fn validate_position_page_sequence(responses: &[&RawHttpResponse]) -> Result<(), String> {
    const PAGE_SIZE: usize = 500;
    for (index, response) in responses.iter().enumerate() {
        let expected_offset = index.saturating_mul(PAGE_SIZE).to_string();
        let offset = response
            .ordered_query
            .iter()
            .find(|(key, _)| key == "offset")
            .map(|(_, value)| value.as_str());
        let limit = response
            .ordered_query
            .iter()
            .find(|(key, _)| key == "limit")
            .map(|(_, value)| value.as_str());
        if offset != Some(expected_offset.as_str()) || limit != Some("500") {
            return Err("positions page sequence is discontinuous".to_owned());
        }
        let count = parse_json_response(response)?
            .as_array()
            .ok_or_else(|| "positions response is not an array".to_owned())?
            .len();
        let is_last = index + 1 == responses.len();
        if (!is_last && count != PAGE_SIZE) || (is_last && count >= PAGE_SIZE) {
            return Err("positions pagination omitted its short terminal page".to_owned());
        }
    }
    Ok(())
}

fn atomic_amount(value: &Value) -> Result<CollateralAmount, String> {
    let atomic = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
        .parse::<u64>()
        .map_err(|_| "amount is not an exact nonnegative atomic integer".to_owned())?;
    Ok(CollateralAmount::from_atomic(atomic))
}

fn decimal(value: Option<&Value>, field: &str) -> Result<Decimal, String> {
    value
        .ok_or_else(|| format!("trade omitted {field}"))
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
                .parse::<Decimal>()
                .map_err(|_| format!("trade {field} is not exact decimal"))
        })
}

fn trade_matches_order(trade: &Value, order_id: &str) -> bool {
    trade
        .get("taker_order_id")
        .or_else(|| trade.get("takerOrderId"))
        .and_then(Value::as_str)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(order_id))
        || trade
            .get("maker_orders")
            .or_else(|| trade.get("makerOrders"))
            .and_then(Value::as_array)
            .is_some_and(|orders| {
                orders.iter().any(|order| {
                    order
                        .get("order_id")
                        .or_else(|| order.get("orderId"))
                        .and_then(Value::as_str)
                        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(order_id))
                })
            })
}

fn execution_report(
    pending: &PendingAttempt,
    matching: &[&Value],
) -> Result<ExactExecutionReport, String> {
    let mut fills = Vec::new();
    for trade in matching {
        let fee_rate = decimal(
            trade
                .get("fee_rate_bps")
                .or_else(|| trade.get("feeRateBps")),
            "fee_rate_bps",
        )?;
        if fee_rate != Decimal::ZERO {
            return Err("canary reconciliation found a nonzero fee".to_owned());
        }
        let shares_decimal = decimal(trade.get("size"), "size")?;
        let price = decimal(trade.get("price"), "price")?;
        let shares =
            ShareAmount::from_decimal_exact(shares_decimal).map_err(|error| error.to_string())?;
        let collateral = CollateralAmount::from_decimal_exact(shares_decimal * price)
            .map_err(|error| error.to_string())?;
        fills.push(VenueFill {
            trade_id: trade
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| "trade omitted id".to_owned())?
                .to_owned(),
            collateral_debit: collateral,
            shares,
            fee: CollateralAmount::ZERO,
        });
    }
    let filled_collateral = fills
        .iter()
        .try_fold(CollateralAmount::ZERO, |sum, fill| {
            sum.checked_add(fill.collateral_debit)
        })
        .map_err(|error| error.to_string())?;
    let filled_shares = fills
        .iter()
        .try_fold(ShareAmount::ZERO, |sum, fill| sum.checked_add(fill.shares))
        .map_err(|error| error.to_string())?;
    Ok(ExactExecutionReport {
        venue_order_id: pending.venue_order_id.clone().map(|id| VenueOrderId(id.0)),
        requested_collateral: pending.worst_case_debit,
        requested_shares: pending.requested_shares,
        filled_collateral,
        filled_shares,
        fees: CollateralAmount::ZERO,
        fills,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde_json::json;

    use super::*;
    use time::OffsetDateTime;

    fn response(path: &str, query: Vec<(&str, &str)>, body: Value) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "test".to_owned(),
            endpoint_kind: path.to_owned(),
            method: "GET".to_owned(),
            path: path.to_owned(),
            ordered_query: query
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        }
    }

    #[test]
    fn clob_pages_require_a_contiguous_terminal_cursor() {
        let first = response(
            "/data/orders",
            Vec::new(),
            json!({"data": [], "next_cursor": "page-2"}),
        );
        let second = response(
            "/data/orders",
            vec![("next_cursor", "page-2")],
            json!({"data": [], "next_cursor": "LTE="}),
        );
        assert!(validate_clob_page_sequence(&[&first, &second]).is_ok());
        assert!(validate_clob_page_sequence(&[&first]).is_err());
        let skipped = response(
            "/data/orders",
            vec![("next_cursor", "different")],
            json!({"data": [], "next_cursor": "LTE="}),
        );
        assert!(validate_clob_page_sequence(&[&first, &skipped]).is_err());
    }

    #[test]
    fn positions_pages_require_contiguous_offsets_and_a_short_terminal_page() {
        let full_rows = vec![json!({}); 500];
        let first = response(
            "/positions",
            vec![("limit", "500"), ("offset", "0")],
            Value::Array(full_rows),
        );
        let terminal = response(
            "/positions",
            vec![("limit", "500"), ("offset", "500")],
            json!([]),
        );
        assert!(validate_position_page_sequence(&[&first, &terminal]).is_ok());
        assert!(validate_position_page_sequence(&[&first]).is_err());
        let skipped = response(
            "/positions",
            vec![("limit", "500"), ("offset", "1000")],
            json!([]),
        );
        assert!(validate_position_page_sequence(&[&first, &skipped]).is_err());
    }

    fn ambiguous_pending() -> PendingAttempt {
        PendingAttempt {
            identity: "attempt-1".to_owned(),
            origin: pe_execution_core::AttemptOrigin::Organic,
            worst_case_debit: CollateralAmount::from_atomic(500_000),
            requested_shares: ShareAmount::from_atomic(5_000_000),
            order_hash: "0xorder-hash".to_owned(),
            prepared_body_hash: "body-hash".to_owned(),
            venue_order_id: None,
            post_success: None,
            phase: pe_execution_core::AttemptPhase::PostInFlight,
        }
    }

    fn complete_raw(
        trades: Value,
        positions: Value,
        exact_order_exists: bool,
    ) -> RawReconciliation {
        let mut exact_order = response(
            "/data/order/0xorder-hash",
            Vec::new(),
            json!({"id": "0xorder-hash"}),
        );
        exact_order.endpoint_kind = "exact-order".to_owned();
        exact_order.status = if exact_order_exists { 200 } else { 404 };
        RawReconciliation {
            evidence: vec![
                RawEvidence::HttpResponse(response(
                    "/api/geoblock",
                    Vec::new(),
                    json!({"blocked": false}),
                )),
                RawEvidence::HttpResponse(response(
                    "/auth/ban-status/closed-only",
                    Vec::new(),
                    json!({"closed_only": false}),
                )),
                RawEvidence::HttpResponse(response(
                    "/balance-allowance",
                    Vec::new(),
                    json!({"balance": "400000000", "allowances": {"spender": "8000000"}}),
                )),
                RawEvidence::HttpResponse(exact_order),
                RawEvidence::HttpResponse(response(
                    "/data/orders",
                    Vec::new(),
                    json!({"data": [], "next_cursor": "LTE="}),
                )),
                RawEvidence::HttpResponse(response(
                    "/data/trades",
                    Vec::new(),
                    json!({"data": trades, "next_cursor": "LTE="}),
                )),
                RawEvidence::HttpResponse(response(
                    "/positions",
                    vec![("limit", "500"), ("offset", "0")],
                    positions,
                )),
            ],
            protocol_failure: None,
        }
    }

    #[test]
    fn exhaustive_reconciliation_proves_ambiguous_post_had_no_fill() {
        let snapshot = parse_reconciliation(
            &complete_raw(json!([]), json!([]), false),
            Some(&ambiguous_pending()),
            &[],
            "spender",
        )
        .unwrap();
        let report = snapshot.execution_report.unwrap();
        assert_eq!(report.filled_collateral, CollateralAmount::ZERO);
        assert_eq!(report.filled_shares, ShareAmount::ZERO);
        assert!(report.fills.is_empty());
    }

    #[test]
    fn exact_order_hash_proves_ambiguous_post_fully_filled() {
        let trades = json!([{
            "id": "trade-1",
            "taker_order_id": "0xorder-hash",
            "fee_rate_bps": "0",
            "size": "5",
            "price": "0.1"
        }]);
        let positions = json!([{
            "conditionId": "condition",
            "outcomeIndex": 0,
            "size": "5"
        }]);
        let snapshot = parse_reconciliation(
            &complete_raw(trades, positions, true),
            Some(&ambiguous_pending()),
            &[],
            "spender",
        )
        .unwrap();
        let report = snapshot.execution_report.unwrap();
        assert_eq!(report.filled_collateral.atomic(), 500_000);
        assert_eq!(report.filled_shares.atomic(), 5_000_000);
        assert_eq!(report.fills.len(), 1);
        assert!(!snapshot.unexpected_activity);
    }

    #[test]
    fn exact_order_presence_keeps_an_ambiguous_attempt_open_without_a_fill() {
        let snapshot = parse_reconciliation(
            &complete_raw(json!([]), json!([]), true),
            Some(&ambiguous_pending()),
            &[],
            "spender",
        )
        .unwrap();
        assert!(snapshot.execution_report.is_none());
        assert_eq!(snapshot.open_order_ids, ["0xorder-hash"]);
    }
}
