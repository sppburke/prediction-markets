//! Per-wallet Polymarket trade poller.
//!
//! Polls `UserTrades` for every watchlisted wallet on a fixed interval and pushes
//! parsed [`IncomingTrade`]s into a bounded mpsc channel for the orchestrator.
//! This module contains all the I/O for Polymarket trade ingestion; the orchestrator
//! itself is pure dispatch logic.

use std::time::Duration;

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use tokio::sync::mpsc;
use tracing::warn;

use crate::trade_parser;

/// Configuration for the trade poller.
#[derive(Debug, Clone)]
pub struct TradePollerConfig {
    /// Base URL for the Polymarket Data API (no trailing slash).
    pub base_url: String,
    /// Seconds between polling rounds (one round = all wallets).
    /// Set to 0 in tests to poll once without sleeping.
    pub poll_interval_secs: u64,
}

/// A live trade poller that drives a [`PageFetcher`] per wallet.
pub struct TradePoller<F: PageFetcher> {
    config: TradePollerConfig,
    wallets: Vec<WalletAddress>,
    fetcher: F,
    tx: mpsc::Sender<IncomingTrade>,
}

impl<F: PageFetcher + Send + 'static> TradePoller<F> {
    pub fn new(
        config: TradePollerConfig,
        wallets: Vec<WalletAddress>,
        fetcher: F,
        tx: mpsc::Sender<IncomingTrade>,
    ) -> Self {
        Self {
            config,
            wallets,
            fetcher,
            tx,
        }
    }

    /// Run the polling loop until the channel is closed (i.e. orchestrator dropped).
    ///
    /// Each round fetches trades for every wallet in sequence, then sleeps for
    /// `poll_interval_secs`. Returns when the downstream channel is closed.
    pub async fn run(self) {
        loop {
            for &wallet in &self.wallets {
                let url = PolymarketEndpoint::UserTrades {
                    user: format!("{wallet}"),
                }
                .url(&self.config.base_url);

                match self.fetcher.fetch_page(&url).await {
                    Err(e) => warn!(wallet = %wallet, error = %e, "trade fetch error"),
                    Ok(bytes) => match trade_parser::parse_trades(&bytes, wallet) {
                        Err(e) => warn!(wallet = %wallet, error = %e, "trade parse error"),
                        Ok(trades) => {
                            for trade in trades {
                                if self.tx.send(trade).await.is_err() {
                                    // Channel closed; orchestrator is shutting down.
                                    return;
                                }
                            }
                        }
                    },
                }
            }

            if self.config.poll_interval_secs > 0 {
                tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)).await;
            } else {
                // poll_interval_secs == 0 → single-shot (used in tests).
                return;
            }
        }
    }
}
