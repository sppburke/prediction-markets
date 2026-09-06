//! One receipt-recording, zero-retry historical-price adapter shared by live and paper marks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pe_core_types::{RawHttpAttempt, SourceId};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ClassifiedPricesHistory, ClobPricesHistoryClient, FixtureFetcher, HttpRequestContext,
    ReqwestFetcher,
};

use crate::activity_ingest::SourceLogHandle;
use crate::risk_inputs::{
    BoundaryMarkError, HistoricalMarkPrice, MAX_HISTORICAL_MARK_AGE_SECS, historical_mark_price,
};

/// The cadence owns retry timing; one call performs exactly one transport attempt.
pub struct HistoricalMarkAdapter {
    base_url: String,
    fetcher: ReqwestFetcher,
    source_log: SourceLogHandle,
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
        }
    }

    pub async fn fetch(
        &self,
        token_id: &str,
        cutoff_unix: i64,
    ) -> Result<HistoricalMarkPrice, BoundaryMarkError> {
        let start_unix = cutoff_unix
            .checked_sub(MAX_HISTORICAL_MARK_AGE_SECS)
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
        historical_mark_price(&classified, cutoff_unix, receipt).map_err(BoundaryMarkError::Invalid)
    }
}
