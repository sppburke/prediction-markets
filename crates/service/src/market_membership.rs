use anyhow::{Context as _, Result};
use pe_core_types::{RawHttpAttempt, ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_paper_state::{MarketMembership, PaperStateDb};
use pe_source_polymarket_public::{
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, HttpRequestContext,
    ReqwestFetcher, parse_clob_market,
};

use crate::activity_ingest::SourceLogHandle;

const HTTP_ATTEMPT_SOURCE_ID: &str = "polymarket.clob.market.http";
const HTTP_ATTEMPT_SCHEMA_VERSION: u32 = 1;
const HTTP_ATTEMPT_PARSER_VERSION: u32 = 1;

/// Acquire and record one market's membership. Every error permits a later retry.
pub async fn fetch_market_membership(
    paper_state: &PaperStateDb,
    clob_base_url: &str,
    fetcher: &ReqwestFetcher,
    source_log: &SourceLogHandle,
    condition_id: &str,
) -> Result<MarketMembership> {
    let url = format!(
        "{}/markets/{condition_id}",
        clob_base_url.trim_end_matches('/')
    );
    let mut attempts = Vec::new();
    let fetched = fetcher
        .fetch_page_observed(
            &url,
            HttpRequestContext {
                source_id: "polymarket.clob.market",
                endpoint_kind: "clob-market",
            },
            |attempt| {
                attempts.push(attempt);
                Ok(())
            },
        )
        .await;
    let mut last_response = None;
    for attempt in attempts {
        // Keep transport failures too; they carry no fabricated market body.
        let (observed_at, received_at) = match &attempt {
            RawHttpAttempt::Response(response) => (response.observed_at, response.received_at),
            RawHttpAttempt::TransportFailure(failure) => (failure.observed_at, failure.received_at),
        };
        source_log
            .append(EnvelopeIn {
                source_id: SourceId(HTTP_ATTEMPT_SOURCE_ID.to_owned()),
                schema_version: HTTP_ATTEMPT_SCHEMA_VERSION,
                parser_version: HTTP_ATTEMPT_PARSER_VERSION,
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&attempt)?,
            })
            .await
            .context("append CLOB market attempt")?;
        if let RawHttpAttempt::Response(response) = attempt {
            source_log
                .append(EnvelopeIn {
                    source_id: SourceId("polymarket.clob.market".to_owned()),
                    schema_version: CLOB_RESOLUTION_SCHEMA_VERSION,
                    parser_version: CLOB_RESOLUTION_PARSER_VERSION,
                    observed_at: SourceTimestamp(response.observed_at),
                    received_at: ReceivedAt(response.received_at),
                    content_type: ContentType::Json,
                    payload: response.body.clone(),
                })
                .await
                .context("append CLOB market response")?;
            last_response = Some(response);
        }
    }
    let value = if let Some(response) = &last_response
        && matches!(response.status, 400 | 404)
        && serde_json::from_slice::<serde_json::Value>(&response.body)
            .ok()
            .is_some_and(|body| {
                (response.status == 404 && body == serde_json::json!({"error": "market not found"}))
                    || (response.status == 400
                        && body == serde_json::json!({"error": "Invalid market"}))
            }) {
        "absent".to_owned()
    } else {
        let body = fetched.context("retry CLOB market membership read")?;
        anyhow::ensure!(
            last_response
                .as_ref()
                .is_some_and(|response| response.status == 200),
            "retry non-200 CLOB market membership"
        );
        let market = parse_clob_market(&body).context("retry malformed CLOB market membership")?;
        anyhow::ensure!(
            market.condition_id.as_deref() == Some(condition_id),
            "retry CLOB body naming another market"
        );
        if market.neg_risk == Some(true) {
            market
                .neg_risk_market_id
                .context("retry CLOB market without neg-risk group")?
        } else {
            String::new()
        }
    };
    paper_state
        .record_market_group(condition_id, &value)
        .context("retry CLOB membership recording")?;
    paper_state
        .market_membership(condition_id)?
        .context("recorded CLOB membership missing")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, http::StatusCode, routing::get};
    use pe_event_log::Writer;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn clob_answers_record_only_verified_membership_and_log_every_attempt() {
        for (status, body, expected) in [
            (
                200,
                json!({"condition_id":"market", "neg_risk":true,"neg_risk_market_id":"group"}),
                Some(MarketMembership::Grouped("group".to_owned())),
            ),
            (
                200,
                json!({"condition_id":"market", "neg_risk":false}),
                Some(MarketMembership::Ungrouped),
            ),
            (
                404,
                json!({"error":"market not found"}),
                Some(MarketMembership::Absent),
            ),
            (
                400,
                json!({"error":"Invalid market"}),
                Some(MarketMembership::Absent),
            ),
            (400, json!({"error":"context canceled"}), None),
            (408, json!({"error":"timeout"}), None),
            (425, json!({"error":"too early"}), None),
            (500, json!({"error":"internal error"}), None),
            (503, json!({"error":"unavailable"}), None),
            (404, json!({"error":"other"}), None),
            (
                200,
                json!({"condition_id":"another", "neg_risk":false}),
                None,
            ),
            (200, json!({"condition_id":"market", "neg_risk":true}), None),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
            let source_path = temp.path().join("source.log");
            let mut writer = Writer::open(&source_path).unwrap();
            let (source, mut source_rx) = SourceLogHandle::channel(2);
            let recorder = tokio::spawn(async move {
                while let Some((envelope, ack)) = source_rx.recv_for_test().await {
                    ack.send(writer.append_synced(envelope).unwrap()).unwrap();
                }
            });
            let response = body.clone();
            let app = Router::new().route(
                "/markets/market",
                get(move || {
                    let response = response.clone();
                    async move { (StatusCode::from_u16(status).unwrap(), Json(response)) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let fetcher = ReqwestFetcher::new(reqwest::Client::new())
                .with_min_interval_ms(0)
                .with_initial_backoff_ms(0)
                .with_max_retries(1);
            let result = fetch_market_membership(&state, &base, &fetcher, &source, "market").await;
            assert_eq!(
                result.is_ok(),
                expected.is_some(),
                "status {status}: {result:?}"
            );
            assert_eq!(state.market_membership("market").unwrap(), expected);
            if let Ok(membership) = result {
                assert_eq!(Some(membership), expected);
            }
            drop(source);
            recorder.await.unwrap();
            let frames = pe_event_log::Reader::replay(&source_path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let attempts = frames
                .iter()
                .filter(|(_, frame)| frame.source_id.0 == "polymarket.clob.market.http")
                .map(|(_, frame)| serde_json::from_slice::<RawHttpAttempt>(&frame.payload).unwrap())
                .collect::<Vec<_>>();
            let expected_attempts = if status == 408 || status >= 500 { 2 } else { 1 };
            assert_eq!(attempts.len(), expected_attempts);
            let raw = frames
                .iter()
                .filter(|(_, frame)| frame.source_id.0 == "polymarket.clob.market")
                .collect::<Vec<_>>();
            assert_eq!(raw.len(), expected_attempts);
            for (index, attempt) in attempts.iter().enumerate() {
                let RawHttpAttempt::Response(response) = attempt else {
                    unreachable!()
                };
                assert_eq!(response.status, status);
                assert_eq!(response.attempt_ordinal, u32::try_from(index + 1).unwrap());
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                    body
                );
                assert_eq!(raw[index].1.payload, response.body);
                assert_eq!(raw[index].1.schema_version, CLOB_RESOLUTION_SCHEMA_VERSION);
                assert_eq!(raw[index].1.parser_version, CLOB_RESOLUTION_PARSER_VERSION);
            }
            assert!(!state.is_group("market").unwrap());
            server.abort();
        }
    }

    #[tokio::test]
    async fn clob_timeout_and_network_failure_leave_membership_unknown() {
        let hits = Arc::new(Mutex::new(0));
        let server_hits = hits.clone();
        let app = Router::new().route(
            "/markets/market",
            get(move || {
                *server_hits.lock().unwrap() += 1;
                async { std::future::pending::<&'static str>().await }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let temp = tempfile::tempdir().unwrap();
        let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
        let (source, mut source_rx) = SourceLogHandle::channel(4);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let frames = captured.clone();
        let recorder = tokio::spawn(async move {
            let mut writer = Writer::open(temp.path().join("source.log")).unwrap();
            while let Some((envelope, ack)) = source_rx.recv_for_test().await {
                frames
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice::<RawHttpAttempt>(&envelope.payload).unwrap());
                ack.send(writer.append_synced(envelope).unwrap()).unwrap();
            }
        });
        let fetcher = ReqwestFetcher::new(reqwest::Client::new())
            .with_timeout(1)
            .with_min_interval_ms(0)
            .with_initial_backoff_ms(0)
            .with_max_retries(1);
        assert!(
            fetch_market_membership(&state, &base, &fetcher, &source, "market")
                .await
                .is_err()
        );
        assert_eq!(*hits.lock().unwrap(), 2);
        assert_eq!(state.market_membership("market").unwrap(), None);
        assert!(captured.lock().unwrap().iter().all(|attempt| matches!(attempt,
            RawHttpAttempt::TransportFailure(failure) if failure.error_class == pe_core_types::TransportErrorClass::Timeout)));
        server.abort();
        assert!(
            fetch_market_membership(&state, &base, &fetcher, &source, "market")
                .await
                .is_err()
        );
        assert_eq!(state.market_membership("market").unwrap(), None);
        assert_eq!(captured.lock().unwrap().len(), 4);
        drop(source);
        recorder.await.unwrap();
    }

    #[tokio::test]
    async fn clob_membership_retries_an_open_batch_and_survives_later_rollback() {
        let app = Router::new().route(
            "/markets/market",
            get(|| async { Json(json!({"condition_id":"market","neg_risk":false})) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("paper.db");
        let state = PaperStateDb::open(&state_path).unwrap();
        let (source, mut source_rx) = SourceLogHandle::channel(2);
        let mut writer = Writer::open(temp.path().join("source.log")).unwrap();
        let recorder = tokio::spawn(async move {
            while let Some((envelope, ack)) = source_rx.recv_for_test().await {
                ack.send(writer.append_synced(envelope).unwrap()).unwrap();
            }
        });
        let fetcher = ReqwestFetcher::new(reqwest::Client::new())
            .with_min_interval_ms(0)
            .with_max_retries(0);
        state.begin_batch().unwrap();
        let failure = fetch_market_membership(&state, &base, &fetcher, &source, "market")
            .await
            .unwrap_err();
        assert!(matches!(
            failure.downcast_ref::<pe_paper_state::PaperStateError>(),
            Some(pe_paper_state::PaperStateError::MembershipBatchOpen)
        ));
        assert_eq!(state.market_membership("market").unwrap(), None);
        state.rollback_batch().unwrap();
        fetch_market_membership(&state, &base, &fetcher, &source, "market")
            .await
            .unwrap();
        state.begin_batch().unwrap();
        state.rollback_batch().unwrap();
        drop(state);
        assert_eq!(
            PaperStateDb::open(&state_path)
                .unwrap()
                .market_membership("market")
                .unwrap(),
            Some(MarketMembership::Ungrouped)
        );
        drop(source);
        recorder.await.unwrap();
        server.abort();
    }
}
