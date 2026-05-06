//! CLOBClient trait and implementations (Reqwest for production, Fixture for tests).
//!
//! Mirrors the PageFetcher / FixtureFetcher pattern from
//! `crates/source-polymarket-public/src/fetcher.rs`.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::PolymarketError;

// Defaults — canonical values live in `docs/_GLOSSARY.md` "Polymarket CLOB" section.
/// 5 req/s sustained = 200ms min interval between requests.
const MIN_INTERVAL_MS: u64 = 200;
const REQUEST_TIMEOUT_SECS: u64 = 10;
const MAX_RETRIES: u32 = 3;

// ── Request/response DTOs ─────────────────────────────────────────────────────

/// Signed order body for POST /order.
#[derive(Debug, Clone, Serialize)]
pub struct PostOrderBody {
    pub order: SignedOrderFields,
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
}

/// Fields of a signed CTFExchange order.
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrderFields {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: u8,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// Response from POST /order.
#[derive(Debug, Clone, Deserialize)]
pub struct PostOrderResponse {
    #[serde(default)]
    pub success: bool,
    #[serde(default, rename = "errorMsg")]
    pub error_msg: String,
    #[serde(default, rename = "orderID")]
    pub order_id: String,
}

/// Terminal status values from GET /order/{id}.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Live,
    Matched,
    Filled,
    Cancelled,
    Expired,
    Delayed,
    Unmatched,
    Retrying,
    #[serde(other)]
    Unknown,
}

impl OrderStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Expired | Self::Matched
        )
    }
}

/// Response from GET /order/{id}.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderStatusResponse {
    #[serde(default)]
    pub id: String,
    pub status: OrderStatus,
    #[serde(default, rename = "quantityFilled")]
    pub quantity_filled: String,
    #[serde(default, rename = "quantityRemaining")]
    pub quantity_remaining: String,
    #[serde(default, rename = "avgPrice")]
    pub avg_price: String,
}

// ── CLOBClient trait ──────────────────────────────────────────────────────────

/// Abstracts Polymarket CLOB HTTP calls so production and test code share the same logic.
///
/// `&self` (not `&mut self`) to allow sharing via `Arc`; rate limiting uses interior mutability.
#[allow(async_fn_in_trait)]
pub trait CLOBClient {
    /// Submit a signed order. Returns the venue-assigned order ID on success.
    async fn post_order(
        &self,
        base_url: &str,
        body: &PostOrderBody,
        headers: &crate::signing::L2Headers,
    ) -> Result<PostOrderResponse, PolymarketError>;

    /// Poll order status by ID.
    async fn get_order_status(
        &self,
        base_url: &str,
        order_id: &str,
        headers: &crate::signing::L2Headers,
    ) -> Result<OrderStatusResponse, PolymarketError>;
}

// ── Production client ─────────────────────────────────────────────────────────

/// A [`CLOBClient`] backed by a [`reqwest::Client`].
///
/// - Rate limit: ≤ 5 req/s (200ms min interval) per `docs/_GLOSSARY.md`.
/// - 429 → [`PolymarketError::RateLimited`] (returned to caller, not retried).
/// - 5xx → bounded retry with exponential backoff.
/// - 4xx (non-429) → [`PolymarketError::Http`] (fatal).
pub struct ReqwestCLOBClient {
    client: reqwest::Client,
    timeout: Duration,
    max_retries: u32,
    initial_backoff_ms: u64,
    last_request_at: Mutex<Option<Instant>>,
}

impl ReqwestCLOBClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            max_retries: MAX_RETRIES,
            initial_backoff_ms: 200,
            last_request_at: Mutex::new(None),
        }
    }

    async fn rate_limit_gate(&self) {
        let min_interval = Duration::from_millis(MIN_INTERVAL_MS);
        let sleep_for = {
            let mut guard = self
                .last_request_at
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            let next_slot = match *guard {
                None => now,
                Some(last) => last.max(now) + min_interval,
            };
            *guard = Some(next_slot);
            next_slot.checked_duration_since(now)
        };
        if let Some(d) = sleep_for {
            tokio::time::sleep(d).await;
        }
    }

    async fn raw_post(
        &self,
        url: &str,
        body: &PostOrderBody,
        hdrs: &crate::signing::L2Headers,
    ) -> Result<(u16, Vec<u8>), PolymarketError> {
        self.rate_limit_gate().await;
        let mut attempt = 0u32;
        loop {
            let resp = self
                .client
                .post(url)
                .timeout(self.timeout)
                .header("POLY_ADDRESS", &hdrs.poly_address)
                .header("POLY_API_KEY", &hdrs.poly_api_key)
                .header("POLY_SIGNATURE", &hdrs.poly_signature)
                .header("POLY_TIMESTAMP", &hdrs.poly_timestamp)
                .header("POLY_PASSPHRASE", &hdrs.poly_passphrase)
                .json(body)
                .send()
                .await;

            match resp {
                Err(e) => {
                    if attempt >= self.max_retries {
                        return Err(PolymarketError::Network(e));
                    }
                    attempt += 1;
                    tokio::time::sleep(backoff(self.initial_backoff_ms, attempt)).await;
                }
                Ok(r) => {
                    let status = r.status().as_u16();
                    if status == 429 {
                        let retry_after = r
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or(30);
                        return Err(PolymarketError::RateLimited {
                            retry_after_secs: retry_after,
                        });
                    }
                    if status >= 500 {
                        if attempt >= self.max_retries {
                            return Err(PolymarketError::Http {
                                status,
                                body: String::new(),
                            });
                        }
                        attempt += 1;
                        tokio::time::sleep(backoff(self.initial_backoff_ms, attempt)).await;
                        continue;
                    }
                    let bytes = r.bytes().await.map_err(PolymarketError::Network)?.to_vec();
                    return Ok((status, bytes));
                }
            }
        }
    }

    async fn raw_get(
        &self,
        url: &str,
        hdrs: &crate::signing::L2Headers,
    ) -> Result<(u16, Vec<u8>), PolymarketError> {
        self.rate_limit_gate().await;
        let resp = self
            .client
            .get(url)
            .timeout(self.timeout)
            .header("POLY_ADDRESS", &hdrs.poly_address)
            .header("POLY_API_KEY", &hdrs.poly_api_key)
            .header("POLY_SIGNATURE", &hdrs.poly_signature)
            .header("POLY_TIMESTAMP", &hdrs.poly_timestamp)
            .header("POLY_PASSPHRASE", &hdrs.poly_passphrase)
            .send()
            .await
            .map_err(PolymarketError::Network)?;

        let status = resp.status().as_u16();
        if status == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(30);
            return Err(PolymarketError::RateLimited {
                retry_after_secs: retry_after,
            });
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(PolymarketError::Network)?
            .to_vec();
        Ok((status, bytes))
    }
}

impl CLOBClient for ReqwestCLOBClient {
    async fn post_order(
        &self,
        base_url: &str,
        body: &PostOrderBody,
        headers: &crate::signing::L2Headers,
    ) -> Result<PostOrderResponse, PolymarketError> {
        let url = format!("{base_url}/order");
        let (status, bytes) = self.raw_post(&url, body, headers).await?;
        if status >= 400 {
            return Err(PolymarketError::Http {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        let parsed: PostOrderResponse = serde_json::from_slice(&bytes)?;
        Ok(parsed)
    }

    async fn get_order_status(
        &self,
        base_url: &str,
        order_id: &str,
        headers: &crate::signing::L2Headers,
    ) -> Result<OrderStatusResponse, PolymarketError> {
        let url = format!("{base_url}/order/{order_id}");
        let (status, bytes) = self.raw_get(&url, headers).await?;
        if status >= 400 {
            return Err(PolymarketError::Http {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        let parsed: OrderStatusResponse = serde_json::from_slice(&bytes)?;
        Ok(parsed)
    }
}

fn backoff(initial_ms: u64, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1);
    let ms = initial_ms.saturating_mul(1u64 << shift.min(10));
    Duration::from_millis(ms.min(30_000))
}

// ── Fixture client ────────────────────────────────────────────────────────────

/// A [`CLOBClient`] that replays pre-loaded fixture responses in order.
///
/// `post_order` consumes from `post_responses`, `get_order_status` from `status_responses`.
/// Returns [`PolymarketError::Http { status: 500, .. }`] when a queue is exhausted.
pub struct FixtureCLOBClient {
    post_responses: Mutex<VecDeque<Result<PostOrderResponse, PolymarketError>>>,
    status_responses: Mutex<VecDeque<Result<OrderStatusResponse, PolymarketError>>>,
}

impl FixtureCLOBClient {
    pub fn new(
        post_responses: Vec<Result<PostOrderResponse, PolymarketError>>,
        status_responses: Vec<Result<OrderStatusResponse, PolymarketError>>,
    ) -> Self {
        Self {
            post_responses: Mutex::new(post_responses.into()),
            status_responses: Mutex::new(status_responses.into()),
        }
    }
}

impl CLOBClient for FixtureCLOBClient {
    async fn post_order(
        &self,
        _base_url: &str,
        _body: &PostOrderBody,
        _headers: &crate::signing::L2Headers,
    ) -> Result<PostOrderResponse, PolymarketError> {
        self.post_responses
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop_front()
            .unwrap_or(Err(PolymarketError::Http {
                status: 500,
                body: "fixture post queue exhausted".into(),
            }))
    }

    async fn get_order_status(
        &self,
        _base_url: &str,
        _order_id: &str,
        _headers: &crate::signing::L2Headers,
    ) -> Result<OrderStatusResponse, PolymarketError> {
        self.status_responses
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop_front()
            .unwrap_or(Err(PolymarketError::Http {
                status: 500,
                body: "fixture status queue exhausted".into(),
            }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn ok_post(order_id: &str) -> Result<PostOrderResponse, PolymarketError> {
        Ok(PostOrderResponse {
            success: true,
            error_msg: String::new(),
            order_id: order_id.into(),
        })
    }

    fn ok_status(
        s: OrderStatus,
        filled: &str,
        remaining: &str,
    ) -> Result<OrderStatusResponse, PolymarketError> {
        Ok(OrderStatusResponse {
            id: "test-id".into(),
            status: s,
            quantity_filled: filled.into(),
            quantity_remaining: remaining.into(),
            avg_price: "0.65".into(),
        })
    }

    #[tokio::test]
    async fn fixture_client_replays_post() {
        let client = FixtureCLOBClient::new(vec![ok_post("order-1")], vec![]);
        let dummy_headers = crate::signing::L2Headers {
            poly_address: String::new(),
            poly_api_key: String::new(),
            poly_signature: String::new(),
            poly_timestamp: String::new(),
            poly_passphrase: String::new(),
        };
        let body = PostOrderBody {
            order: SignedOrderFields {
                salt: "1".into(),
                maker: "0x0".into(),
                signer: "0x0".into(),
                taker: "0x0".into(),
                token_id: "1".into(),
                maker_amount: "500000".into(),
                taker_amount: "1000000".into(),
                expiration: "0".into(),
                nonce: "0".into(),
                fee_rate_bps: "0".into(),
                side: 0,
                signature_type: 0,
                signature: "0x00".into(),
            },
            owner: "0x0".into(),
            order_type: "GTC".into(),
        };
        let resp = client.post_order("", &body, &dummy_headers).await.unwrap();
        assert_eq!(resp.order_id, "order-1");
    }

    #[tokio::test]
    async fn fixture_client_replays_status_sequence() {
        let client = FixtureCLOBClient::new(
            vec![],
            vec![
                ok_status(OrderStatus::Live, "0", "1000000"),
                ok_status(OrderStatus::Filled, "1000000", "0"),
            ],
        );
        let dummy = crate::signing::L2Headers {
            poly_address: String::new(),
            poly_api_key: String::new(),
            poly_signature: String::new(),
            poly_timestamp: String::new(),
            poly_passphrase: String::new(),
        };
        let s1 = client.get_order_status("", "x", &dummy).await.unwrap();
        assert_eq!(s1.status, OrderStatus::Live);
        let s2 = client.get_order_status("", "x", &dummy).await.unwrap();
        assert_eq!(s2.status, OrderStatus::Filled);
    }

    #[test]
    fn order_status_terminal_flags() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(OrderStatus::Cancelled.is_terminal());
        assert!(OrderStatus::Expired.is_terminal());
        assert!(OrderStatus::Matched.is_terminal());
        assert!(!OrderStatus::Live.is_terminal());
        assert!(!OrderStatus::Unmatched.is_terminal());
    }
}
