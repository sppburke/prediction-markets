//! One receipt-recording, zero-retry historical-price adapter shared by live and paper marks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pe_core_types::{RawHttpAttempt, SourceId};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ClassifiedPricesHistory, ClobPricesHistoryClient, FixtureFetcher, GAMMA_MARKETS_SOURCE_ID,
    GammaMarketsClient, GammaMarketsError, HttpRequestContext, MarketFilter, ReqwestFetcher,
};

use crate::activity_ingest::SourceLogHandle;
use crate::risk_inputs::{
    BoundaryMarkError, CLOSED_MARK_LOOKBACK_SECS, HistoricalMarkPrice,
    MAX_HISTORICAL_MARK_AGE_SECS, closed_historical_mark_price, historical_mark_price,
};

/// The cadence owns retry timing; each request performs exactly one transport attempt.
pub struct HistoricalMarkAdapter {
    base_url: String,
    fetcher: ReqwestFetcher,
    source_log: SourceLogHandle,
    gamma: Option<GammaMarketsClient<ReqwestFetcher>>,
}

impl HistoricalMarkAdapter {
    #[must_use]
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        source_log: SourceLogHandle,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            fetcher: ReqwestFetcher::new(client)
                .with_min_interval_ms(200)
                .with_max_retries(0),
            source_log,
            gamma: None,
        }
    }

    #[must_use]
    pub fn with_gamma_base_url(mut self, client: reqwest::Client, base_url: String) -> Self {
        self.gamma = Some(GammaMarketsClient::new(
            base_url.trim_end_matches('/').to_owned(),
            ReqwestFetcher::new(client)
                .with_min_interval_ms(200)
                .with_max_retries(0),
        ));
        self
    }

    pub async fn fetch(
        &self,
        token_id: &str,
        cutoff_unix: i64,
    ) -> Result<HistoricalMarkPrice, BoundaryMarkError> {
        let (classified, receipt) = self
            .fetch_window(token_id, cutoff_unix, MAX_HISTORICAL_MARK_AGE_SECS)
            .await?;
        historical_mark_price(&classified, cutoff_unix, receipt).map_err(BoundaryMarkError::Invalid)
    }

    /// Only the paper daily-mark caller may use recorded closure to extend an empty window.
    pub async fn fetch_paper(
        &self,
        condition_id: &str,
        token_id: &str,
        cutoff_unix: i64,
    ) -> Result<(HistoricalMarkPrice, Option<pe_event_log::AppendReceipt>), BoundaryMarkError> {
        let (classified, receipt) = self
            .fetch_window(token_id, cutoff_unix, MAX_HISTORICAL_MARK_AGE_SECS)
            .await?;
        if !matches!(classified, ClassifiedPricesHistory::Empty) {
            return historical_mark_price(&classified, cutoff_unix, receipt)
                .map(|price| (price, None))
                .map_err(BoundaryMarkError::Invalid);
        }
        let gamma = self.gamma.as_ref().ok_or_else(|| {
            BoundaryMarkError::Classification("paper closure reader is unavailable".to_owned())
        })?;
        let fetched = gamma
            .fetch_markets_with_pages(&[condition_id.to_owned()], MarketFilter::ClosedOnly)
            .await;
        let pages = match &fetched {
            Ok(result) => &result.pages,
            Err(error) => &error.pages,
        };
        let mut closure_receipts = HashMap::new();
        for (evidence, payload) in pages {
            let receipt = self
                .source_log
                .append(EnvelopeIn {
                    source_id: SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
                    schema_version: evidence.schema_version,
                    parser_version: evidence.parser_version,
                    observed_at: pe_core_types::SourceTimestamp(evidence.received_at.0),
                    received_at: evidence.received_at.clone(),
                    content_type: ContentType::Json,
                    payload: payload.clone(),
                })
                .await
                .map_err(|_| BoundaryMarkError::SourceLogClosed)?;
            closure_receipts.insert(evidence.raw_page_hash.clone(), receipt);
        }
        let fetched = fetched.map_err(|error| match error.source {
            GammaMarketsError::Fetch(message) => BoundaryMarkError::Retryable(message),
            error => BoundaryMarkError::Classification(error.to_string()),
        })?;
        let closure =
            fetched
                .markets
                .markets
                .get(condition_id)
                .ok_or(BoundaryMarkError::Invalid(
                    crate::risk_inputs::RiskInputsUnavailable::MarkInvalid,
                ))?;
        if !closure.closed
            || closure
                .closed_time_unix
                .is_none_or(|closed| closed > cutoff_unix)
        {
            return Err(BoundaryMarkError::Invalid(
                crate::risk_inputs::RiskInputsUnavailable::MarkInvalid,
            ));
        }
        let closure_receipt = fetched
            .condition_page_hashes
            .get(condition_id)
            .and_then(|hash| closure_receipts.get(hash))
            .copied()
            .ok_or_else(|| {
                BoundaryMarkError::Classification("closure body was not recorded".to_owned())
            })?;
        let (classified, receipt) = self
            .fetch_window(token_id, cutoff_unix, CLOSED_MARK_LOOKBACK_SECS)
            .await?;
        closed_historical_mark_price(&classified, condition_id, closure, cutoff_unix, receipt)
            .map(|price| (price, Some(closure_receipt)))
            .map_err(BoundaryMarkError::Invalid)
    }

    async fn fetch_window(
        &self,
        token_id: &str,
        cutoff_unix: i64,
        lookback_secs: i64,
    ) -> Result<(ClassifiedPricesHistory, pe_event_log::AppendReceipt), BoundaryMarkError> {
        let start_unix = cutoff_unix
            .checked_sub(lookback_secs)
            .ok_or_else(|| BoundaryMarkError::Classification("mark cutoff underflow".to_owned()))?;
        let url = format!(
            "{}/prices-history?market={token_id}&startTs={start_unix}&endTs={cutoff_unix}&fidelity=1",
            self.base_url
        );
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&attempts);
        let fetched = self
            .fetcher
            .fetch_page_observed(
                &url,
                HttpRequestContext {
                    source_id: "pe-service.clob-prices-history",
                    endpoint_kind: "clob-prices-history",
                },
                move |attempt| {
                    observed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(attempt);
                    Ok(())
                },
            )
            .await;
        let attempts = std::mem::take(
            &mut *attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut responses = Vec::new();
        for attempt in attempts {
            let RawHttpAttempt::Response(response) = attempt else {
                continue;
            };
            let receipt = self
                .source_log
                .append(EnvelopeIn {
                    source_id: SourceId(response.source_id.clone()),
                    schema_version: u32::from(response.schema_version),
                    parser_version: u32::from(response.parser_version),
                    observed_at: pe_core_types::SourceTimestamp(response.observed_at),
                    received_at: pe_core_types::ReceivedAt(response.received_at),
                    content_type: ContentType::Json,
                    payload: response.body.clone(),
                })
                .await
                .map_err(|_| BoundaryMarkError::SourceLogClosed)?;
            responses.push((response, receipt));
        }
        let (body, classified) = match fetched {
            Ok(body) => {
                let pages = HashMap::from([(url.clone(), body.clone())]);
                let outcome =
                    ClobPricesHistoryClient::new(self.base_url.clone(), FixtureFetcher::new(pages))
                        .with_fidelity_minutes(1)
                        .fetch_prices_history_classified(token_id, start_unix, cutoff_unix)
                        .await
                        .map_err(|error| BoundaryMarkError::Classification(error.to_string()))?
                        .outcome;
                (body, outcome)
            }
            Err(SourceError::Fatal { message }) => {
                let response = responses.last().ok_or_else(|| {
                    BoundaryMarkError::Classification("fatal response was not recorded".to_owned())
                })?;
                (
                    response.0.body.clone(),
                    ClassifiedPricesHistory::Rejected { message },
                )
            }
            Err(error) => return Err(BoundaryMarkError::Retryable(error.to_string())),
        };
        let receipt = responses
            .iter()
            .rev()
            .find(|(response, _)| response.body == body)
            .map(|(_, receipt)| *receipt)
            .ok_or_else(|| {
                BoundaryMarkError::Classification("classified body was not recorded".to_owned())
            })?;
        Ok((classified, receipt))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::activity_ingest::ActivityIngest;
    use crate::health::new_shared_health_with_ws;
    use crate::risk_inputs::RiskInputsUnavailable;
    use crate::source_event_sink::SourceEventSink;
    use axum::{Router, extract::OriginalUri, routing::get};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn paper_closed_mark_records_london_closure_while_live_keeps_empty_window_refusal() {
        let cutoff = 1_790_985_600;
        let condition = "0x50365ef0731cfa26e35d89b66a91bca5ef1ee8090b02d1ced099acc738868e87";
        let token = "27556300168112004153063282106116891181228462668915873615116883805553394180154";
        let manifest: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/london_closed_mark/MANIFEST.json"
        ))
        .unwrap();
        let paths = [
            "prices_history_120s.json",
            "gamma_closed.json",
            "prices_history_13d.json",
        ]
        .map(|name| reqwest::Url::parse(manifest[name]["url"].as_str().unwrap()).unwrap())
        .map(|url| format!("{}?{}", url.path(), url.query().unwrap()));
        assert!(paths[2].contains(&format!("startTs={}", cutoff - CLOSED_MARK_LOOKBACK_SECS)));
        let short = include_bytes!("../tests/fixtures/london_closed_mark/prices_history_120s.json")
            .to_vec();
        let long =
            include_bytes!("../tests/fixtures/london_closed_mark/prices_history_13d.json").to_vec();
        let gamma =
            include_bytes!("../tests/fixtures/london_closed_mark/gamma_closed.json").to_vec();
        for mutation in ["closed", "open", "later", "unknown", "wrong_condition"] {
            let mut closure: serde_json::Value = serde_json::from_slice(&gamma).unwrap();
            match mutation {
                "open" => closure[0]["closed"] = serde_json::json!(false),
                "later" => closure[0]["closedTime"] = serde_json::json!("2026-10-03 00:00:01+00"),
                "unknown" => closure[0]["closedTime"] = serde_json::Value::Null,
                "wrong_condition" => closure[0]["conditionId"] = serde_json::json!("other"),
                _ => {}
            }
            let closure = if mutation == "closed" {
                gamma.clone()
            } else {
                serde_json::to_vec(&closure).unwrap()
            };
            let pages = Arc::new(HashMap::from([
                (paths[0].clone(), short.clone()),
                (paths[1].clone(), closure),
                (paths[2].clone(), long.clone()),
            ]));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&requests);
            let app = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let pages = Arc::clone(&pages);
                let observed = Arc::clone(&observed);
                async move {
                    let path = uri.to_string();
                    observed.lock().unwrap().push(path.clone());
                    (
                        [("content-type", "application/json")],
                        pages.get(&path).unwrap().clone(),
                    )
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let (source_log, source_rx) = SourceLogHandle::channel(4);
            let (trigger_tx, _trigger_rx) = mpsc::channel(1);
            let ingest = tokio::spawn(
                ActivityIngest::poll_only(
                    SourceEventSink::open(&path).unwrap(),
                    source_rx,
                    trigger_tx,
                    new_shared_health_with_ws(false, false, 90),
                )
                .run(),
            );
            let adapter = HistoricalMarkAdapter::new(reqwest::Client::new(), &base, source_log)
                .with_gamma_base_url(reqwest::Client::new(), base);
            let paper = adapter.fetch_paper(condition, token, cutoff).await;
            if mutation == "closed" {
                let (price, closure_receipt) = paper.unwrap();
                assert_eq!(price.price.0, rust_decimal::Decimal::new(5, 4));
                assert_eq!(price.sample_unix, 1_790_985_077);
                let envelopes = pe_event_log::Reader::replay(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().1)
                    .collect::<Vec<_>>();
                assert_eq!(envelopes.len(), 3);
                assert_eq!(envelopes[1].payload, gamma);
                assert_eq!(envelopes[1].seq, closure_receipt.unwrap().sequence);
                assert_eq!(envelopes[1].this_hash, closure_receipt.unwrap().this_hash);
                assert_eq!(envelopes[2].payload, long);
                assert_eq!(envelopes[2].seq, price.receipt.sequence);
                assert_eq!(envelopes[2].this_hash, price.receipt.this_hash);
                assert_eq!(*requests.lock().unwrap(), paths);
            } else {
                assert!(
                    matches!(
                        paper,
                        Err(BoundaryMarkError::Invalid(
                            RiskInputsUnavailable::MarkInvalid
                        ))
                    ),
                    "{mutation}: {paper:?}"
                );
                assert_eq!(*requests.lock().unwrap(), paths[..2]);
            }
            assert!(matches!(
                adapter.fetch(token, cutoff).await,
                Err(BoundaryMarkError::Invalid(
                    RiskInputsUnavailable::PriceMissing
                ))
            ));
            assert_eq!(requests.lock().unwrap().last(), Some(&paths[0]));
            ingest.abort();
            server.abort();
            let _ = ingest.await;
            let _ = server.await;
        }
    }
}
