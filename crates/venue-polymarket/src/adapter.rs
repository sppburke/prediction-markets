//! `PolymarketVenueAdapter` — submits `OrderIntent` to the Polymarket CLOB REST API.
//!
//! Submit flow:
//!   1. Build `ClobOrder` from `OrderIntent` (amount arithmetic, salt from idempotency key).
//!   2. Sign with L1 EIP-712 (funder EOA private key).
//!   3. Compute L2 HMAC-SHA256 auth headers.
//!   4. POST /order.
//!   5. Poll GET /order/{id} every `POLL_INTERVAL_MS` until terminal or `validity_seconds` elapsed.
//!   6. Map to `OrderOutcome`.

use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256, keccak256};
use pe_core_types::{ContractQty, Price, Side};
use pe_venue_core::{OrderIntent, OrderOutcome, VenueError};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use rust_decimal_macros::dec;
use time::OffsetDateTime;
use tracing::debug;

use crate::clob_client::{CLOBClient, OrderStatus, PostOrderBody, SignedOrderFields};
use crate::signing::{
    ClobOrder, L2Credentials, MAINNET_CHAIN_ID, MAINNET_EXCHANGE_ADDRESS, SIG_TYPE_EOA,
    compute_l2_headers, decimal_to_u256, sign_order_eip712,
};

// Defaults — canonical value in `docs/_GLOSSARY.md` "Polymarket CLOB" section.
const POLL_INTERVAL_MS: u64 = 100;
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// USDC scale factor: 1 USDC = 1_000_000 micro-units.
const USDC_SCALE: Decimal = dec!(1_000_000);

// ── Credentials ───────────────────────────────────────────────────────────────

/// Full credentials for Polymarket CLOB order submission.
///
/// Secrets (`private_key_hex`, `api_secret_b64`) must not appear in any log output.
pub struct PolymarketCredentials {
    /// Funder EOA address (hex, checksummed). Used in L2 headers and as `maker`.
    pub funder_address: String,
    /// EOA private key (hex, with or without "0x" prefix). L1 signing only.
    pub private_key_hex: String,
    pub l2: L2Credentials,
    /// Chain ID for EIP-712 domain (default: 137 Polygon mainnet).
    pub chain_id: u64,
    /// CTFExchange contract address for EIP-712 domain.
    pub exchange_address: Address,
}

impl PolymarketCredentials {
    /// Mainnet credentials.
    pub fn mainnet(
        funder_address: String,
        private_key_hex: String,
        api_key: String,
        api_secret_b64: String,
        api_passphrase: String,
    ) -> Self {
        Self {
            l2: L2Credentials {
                funder_address: funder_address.clone(),
                api_key,
                api_secret_b64,
                api_passphrase,
            },
            funder_address,
            private_key_hex,
            chain_id: MAINNET_CHAIN_ID,
            exchange_address: MAINNET_EXCHANGE_ADDRESS,
        }
    }
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Submits `OrderIntent` to the Polymarket CLOB.
///
/// Generic over `C: CLOBClient` to allow fixture injection in tests.
/// In production, `C = ReqwestCLOBClient`.
pub struct PolymarketVenueAdapter<C: CLOBClient> {
    client: C,
    creds: PolymarketCredentials,
    base_url: String,
}

impl<C: CLOBClient> PolymarketVenueAdapter<C> {
    pub fn new(client: C, creds: PolymarketCredentials) -> Self {
        Self {
            client,
            creds,
            base_url: CLOB_BASE_URL.to_owned(),
        }
    }

    /// Override CLOB base URL — used in tests.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Submit an `OrderIntent` to the Polymarket CLOB.
    ///
    /// Polls for fill confirmation until the order reaches a terminal state or
    /// `intent.validity_seconds` elapses.
    pub async fn submit(&self, intent: &OrderIntent) -> Result<OrderOutcome, VenueError> {
        // 1. Build the on-chain order struct.
        let order = self.build_order(intent)?;

        // 2. L1: sign with EIP-712.
        let sig = sign_order_eip712(
            &order,
            self.creds.chain_id,
            self.creds.exchange_address,
            &self.creds.private_key_hex,
        )
        .await
        .map_err(VenueError::from)?;

        // 3. Compute L2 auth.
        let body_str = self.serialise_body(&order, &sig, intent)?;
        let l2_hdrs = compute_l2_headers(&self.creds.l2, "POST", "/order", &body_str, now_secs())
            .map_err(VenueError::from)?;

        // 4. POST /order.
        let body = self.build_post_body(&order, &sig, intent);
        let post_resp = self
            .client
            .post_order(&self.base_url, &body, &l2_hdrs)
            .await
            .map_err(VenueError::from)?;

        if !post_resp.success || post_resp.order_id.is_empty() {
            return Ok(OrderOutcome::Rejected {
                reason: post_resp.error_msg,
            });
        }

        let order_id = &post_resp.order_id;
        debug!(order_id, idempotency_key = %intent.idempotency_key, "polymarket order submitted");

        // 5. Poll until terminal.
        self.poll_until_terminal(order_id, intent).await
    }

    fn build_order(&self, intent: &OrderIntent) -> Result<ClobOrder, VenueError> {
        let salt = {
            let hash = keccak256(intent.idempotency_key.as_bytes());
            U256::from_be_bytes(hash.0)
        };

        let maker: Address = self
            .creds
            .funder_address
            .parse()
            .map_err(|_| VenueError::Other {
                message: format!("invalid funder address: {}", self.creds.funder_address),
            })?;

        let token_id: U256 =
            intent
                .market_id
                .to_string()
                .parse()
                .map_err(|_| VenueError::Other {
                    message: format!("market_id is not a U256 token ID: {}", intent.market_id),
                })?;

        let qty = Decimal::from(intent.contracts.0);
        let price: Decimal = intent.limit_price.0;

        let (maker_amount, taker_amount, side_u8) = compute_amounts(qty, price, intent.side)?;

        let now = now_secs();
        let expiration_secs = now
            .checked_add(i64::from(intent.validity_seconds))
            .unwrap_or(i64::MAX);
        let expiration = u64::try_from(expiration_secs.max(0))
            .map_err(|_| VenueError::Other {
                message: "expiration timestamp overflow".into(),
            })
            .map(U256::from)?;

        Ok(ClobOrder {
            salt,
            maker,
            signer: maker,
            taker: Address::ZERO,
            tokenId: token_id,
            makerAmount: maker_amount,
            takerAmount: taker_amount,
            expiration,
            nonce: U256::ZERO,
            feeRateBps: U256::ZERO,
            side: side_u8,
            signatureType: SIG_TYPE_EOA,
        })
    }

    fn build_post_body(
        &self,
        order: &ClobOrder,
        sig: &str,
        _intent: &OrderIntent,
    ) -> PostOrderBody {
        PostOrderBody {
            order: SignedOrderFields {
                salt: order.salt.to_string(),
                maker: format!("{:#x}", order.maker),
                signer: format!("{:#x}", order.signer),
                taker: format!("{:#x}", order.taker),
                token_id: order.tokenId.to_string(),
                maker_amount: order.makerAmount.to_string(),
                taker_amount: order.takerAmount.to_string(),
                expiration: order.expiration.to_string(),
                nonce: order.nonce.to_string(),
                fee_rate_bps: order.feeRateBps.to_string(),
                side: order.side,
                signature_type: order.signatureType,
                signature: sig.to_owned(),
            },
            owner: self.creds.funder_address.clone(),
            order_type: "GTC".into(),
        }
    }

    fn serialise_body(
        &self,
        order: &ClobOrder,
        sig: &str,
        intent: &OrderIntent,
    ) -> Result<String, VenueError> {
        let body = self.build_post_body(order, sig, intent);
        serde_json::to_string(&body).map_err(|e| VenueError::Other {
            message: format!("body serialisation: {e}"),
        })
    }

    async fn poll_until_terminal(
        &self,
        order_id: &str,
        intent: &OrderIntent,
    ) -> Result<OrderOutcome, VenueError> {
        let deadline = Instant::now() + Duration::from_secs(u64::from(intent.validity_seconds));
        let poll_interval = Duration::from_millis(POLL_INTERVAL_MS);

        loop {
            if Instant::now() > deadline {
                return Ok(OrderOutcome::Expired);
            }

            let l2_hdrs = compute_l2_headers(
                &self.creds.l2,
                "GET",
                &format!("/order/{order_id}"),
                "",
                now_secs(),
            )
            .map_err(VenueError::from)?;

            let status_resp = self
                .client
                .get_order_status(&self.base_url, order_id, &l2_hdrs)
                .await
                .map_err(VenueError::from)?;

            debug!(order_id, status = ?status_resp.status, "polymarket poll");

            if status_resp.status.is_terminal() {
                return map_terminal_status(status_resp, intent);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn compute_amounts(
    qty: Decimal,
    price: Decimal,
    side: Side,
) -> Result<(U256, U256, u8), VenueError> {
    match side {
        Side::Buy => {
            let maker =
                decimal_to_u256((qty * price * USDC_SCALE).floor()).map_err(VenueError::from)?;
            let taker = decimal_to_u256((qty * USDC_SCALE).floor()).map_err(VenueError::from)?;
            Ok((maker, taker, 0u8))
        }
        Side::Sell => {
            let maker = decimal_to_u256((qty * USDC_SCALE).floor()).map_err(VenueError::from)?;
            let taker =
                decimal_to_u256((qty * price * USDC_SCALE).floor()).map_err(VenueError::from)?;
            Ok((maker, taker, 1u8))
        }
    }
}

fn map_terminal_status(
    resp: crate::clob_client::OrderStatusResponse,
    intent: &OrderIntent,
) -> Result<OrderOutcome, VenueError> {
    match resp.status {
        OrderStatus::Filled | OrderStatus::Matched => {
            let fill_price = resp
                .avg_price
                .parse::<Decimal>()
                .ok()
                .and_then(|d| Price::new(d).ok())
                .unwrap_or(intent.limit_price);
            let filled_qty = resp
                .quantity_filled
                .parse::<Decimal>()
                .map(|d| (d / USDC_SCALE).floor())
                .ok();
            let remaining_qty = resp
                .quantity_remaining
                .parse::<Decimal>()
                .map(|d| (d / USDC_SCALE).floor())
                .ok();

            match (filled_qty, remaining_qty) {
                (Some(filled), Some(remaining)) if remaining > Decimal::ZERO => {
                    Ok(OrderOutcome::PartialFill {
                        fill_price,
                        filled_contracts: ContractQty(filled.to_u64().unwrap_or(0)),
                        remaining_contracts: ContractQty(remaining.to_u64().unwrap_or(0)),
                    })
                }
                _ => Ok(OrderOutcome::Filled {
                    fill_price,
                    contracts: intent.contracts,
                }),
            }
        }
        OrderStatus::Cancelled => Ok(OrderOutcome::Cancelled),
        OrderStatus::Expired => Ok(OrderOutcome::Expired),
        other => Ok(OrderOutcome::Rejected {
            reason: format!("unexpected terminal status: {other:?}"),
        }),
    }
}

fn now_secs() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use pe_core_types::{ContractQty, MarketId, OutcomeId, StrategyId, VenueMarketId};
    use pe_venue_core::OrderIntent;
    use rust_decimal_macros::dec;

    use crate::clob_client::{FixtureCLOBClient, OrderStatusResponse, PostOrderResponse};

    fn test_creds() -> PolymarketCredentials {
        PolymarketCredentials::mainnet(
            "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf".into(),
            "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
            "test-key".into(),
            base64::engine::general_purpose::STANDARD.encode(b"test-secret"),
            "test-pass".into(),
        )
    }

    fn test_intent() -> OrderIntent {
        OrderIntent {
            strategy_id: StrategyId("winner-follow".into()),
            // tokenId as decimal string (Polymarket format)
            market_id: MarketId(VenueMarketId(
                "71321045679252212594626385532706912750332728571942532289631379312455583992563"
                    .into(),
            )),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            contracts: ContractQty(10),
            limit_price: Price::new(dec!(0.65)).unwrap(),
            validity_seconds: 60,
            idempotency_key: "winner-follow|trade-1|market-abc|0|buy|1700000000".into(),
        }
    }

    #[test]
    fn compute_amounts_buy() {
        // 10 contracts at 0.65: makerAmount = floor(10 * 0.65 * 1_000_000) = 6_500_000
        let (maker, taker, side) = compute_amounts(dec!(10), dec!(0.65), Side::Buy).unwrap();
        assert_eq!(maker, U256::from(6_500_000u64));
        assert_eq!(taker, U256::from(10_000_000u64));
        assert_eq!(side, 0);
    }

    #[test]
    fn compute_amounts_sell() {
        // 10 contracts selling at 0.65: makerAmount = 10M, takerAmount = floor(10 * 0.65 * 1M) = 6.5M
        let (maker, taker, side) = compute_amounts(dec!(10), dec!(0.65), Side::Sell).unwrap();
        assert_eq!(maker, U256::from(10_000_000u64));
        assert_eq!(taker, U256::from(6_500_000u64));
        assert_eq!(side, 1);
    }

    #[tokio::test]
    async fn submit_filled_returns_filled_outcome() {
        let client = FixtureCLOBClient::new(
            vec![Ok(PostOrderResponse {
                success: true,
                error_msg: String::new(),
                order_id: "order-abc".into(),
            })],
            vec![Ok(OrderStatusResponse {
                id: "order-abc".into(),
                status: OrderStatus::Filled,
                quantity_filled: "10000000".into(),
                quantity_remaining: "0".into(),
                avg_price: "0.65".into(),
            })],
        );
        let adapter = PolymarketVenueAdapter::new(client, test_creds());
        let intent = test_intent();
        let outcome = adapter.submit(&intent).await.unwrap();
        assert!(
            matches!(outcome, OrderOutcome::Filled { .. }),
            "expected Filled, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn submit_rejected_on_failed_post() {
        let client = FixtureCLOBClient::new(
            vec![Ok(PostOrderResponse {
                success: false,
                error_msg: "insufficient balance".into(),
                order_id: String::new(),
            })],
            vec![],
        );
        let adapter = PolymarketVenueAdapter::new(client, test_creds());
        let outcome = adapter.submit(&test_intent()).await.unwrap();
        assert!(
            matches!(outcome, OrderOutcome::Rejected { .. }),
            "expected Rejected"
        );
    }

    #[tokio::test]
    async fn submit_cancelled_returns_cancelled_outcome() {
        let client = FixtureCLOBClient::new(
            vec![Ok(PostOrderResponse {
                success: true,
                error_msg: String::new(),
                order_id: "order-xyz".into(),
            })],
            vec![Ok(OrderStatusResponse {
                id: "order-xyz".into(),
                status: OrderStatus::Cancelled,
                quantity_filled: "0".into(),
                quantity_remaining: "10000000".into(),
                avg_price: "0".into(),
            })],
        );
        let adapter = PolymarketVenueAdapter::new(client, test_creds());
        let outcome = adapter.submit(&test_intent()).await.unwrap();
        assert_eq!(outcome, OrderOutcome::Cancelled);
    }
}
