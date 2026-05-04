//! Live Polygon connector: backfill via HTTP `eth_getLogs` + real-time via WebSocket.
//!
//! On construction ([`LivePolygonConnector::connect`]) two background tokio tasks are spawned:
//!
//! 1. **Backfill worker** — paginates `eth_getLogs` from the last checkpointed block (or
//!    `current_block - backfill_blocks`) to the current head, 10 000 blocks per page.
//! 2. **WS worker** — subscribes to `eth_subscribe(logs, filter)` and forwards live events.
//!    Reconnects with exponential backoff (1 s → 2 s → … → 60 s) on disconnect.
//!
//! Both workers push decoded [`PolygonEvent`]s into a bounded [`tokio::sync::mpsc`] channel.
//! [`SourceConnector::next_event`] drains that channel.
//!
//! **Deferred (Phase 0B out-of-scope)**:
//! - `DepositAddressFunding` — deposit-wallet factory event ABI not publicly available.
//! - `BridgeOnrampReceipt` — requires a maintained, configurable bridge address list.

use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy::{
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_source_core::{SourceConnector, SourceError, SourceEvent, SourceHealth, SourceStatus};

use crate::{
    contracts::{MONITORED_ADDRESSES, MONITORED_TOPICS},
    decoder,
    event::PolygonEvent,
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum blocks per `eth_getLogs` page (Alchemy free-tier limit).
const PAGE_SIZE: u64 = 10_000;

/// Log backfill progress every N pages (≈ 100 × 10K = 1M blocks).
const PROGRESS_LOG_INTERVAL: u64 = 100;

/// Save the block checkpoint every N pages during backfill.
const CHECKPOINT_SAVE_INTERVAL: u64 = 1_000;

/// Maximum WS reconnect delay in seconds.
const MAX_BACKOFF_SECS: u64 = 60;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for [`LivePolygonConnector`].
#[derive(Debug, Clone)]
pub struct PolygonConnectorConfig {
    /// Alchemy (or compatible) HTTPS endpoint for `eth_getLogs` backfill.
    pub http_url: String,
    /// Alchemy (or compatible) WSS endpoint for `eth_subscribe` live logs.
    pub ws_url: String,
    /// Blocks to backfill from current head on first run (default ≈ 16 months).
    /// See `docs/_GLOSSARY.md`: `polygon_backfill_blocks`.
    pub backfill_blocks: u64,
    /// Path to the JSON block-checkpoint file. Resumes backfill from `last_block + 1`.
    pub checkpoint_path: PathBuf,
    /// Bounded channel capacity. See `docs/_GLOSSARY.md`: `polygon_channel_capacity`.
    pub channel_capacity: usize,
}

impl Default for PolygonConnectorConfig {
    fn default() -> Self {
        Self {
            http_url: String::new(),
            ws_url: String::new(),
            backfill_blocks: 21_000_000,
            checkpoint_path: PathBuf::from("./polygon_checkpoint.json"),
            channel_capacity: 256,
        }
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum LivePolygonError {
    #[error("parse HTTP URL: {0}")]
    HttpUrl(String),
    #[error("get block number: {0}")]
    BlockNumber(String),
    #[error("connect WS: {0}")]
    WsConnect(String),
}

// ── Connector ─────────────────────────────────────────────────────────────────

/// A live Polygon connector that ingests decoded on-chain events from Alchemy.
pub struct LivePolygonConnector {
    source_id: SourceId,
    event_rx: mpsc::Receiver<PolygonEvent>,
    last_event_at: Option<SourceTimestamp>,
}

impl LivePolygonConnector {
    /// Connect to Polygon and start backfill + WS tasks.
    ///
    /// Returns once the providers are connected and both background tasks are
    /// running. The caller drives event consumption via [`SourceConnector::next_event`].
    pub async fn connect(
        source_id: SourceId,
        config: PolygonConnectorConfig,
    ) -> Result<Self, LivePolygonError> {
        let (event_tx, event_rx) = mpsc::channel(config.channel_capacity);

        // HTTP provider for backfill and current-block query.
        let http_url: url::Url = config
            .http_url
            .parse()
            .map_err(|e: url::ParseError| LivePolygonError::HttpUrl(e.to_string()))?;
        let http_provider = ProviderBuilder::new().connect_http(http_url);

        // Current chain head.
        let current_block = http_provider
            .get_block_number()
            .await
            .map_err(|e| LivePolygonError::BlockNumber(e.to_string()))?;

        // Resume from checkpoint or default to (head - backfill_blocks).
        let start_block = load_checkpoint(&config.checkpoint_path)
            .unwrap_or_else(|| current_block.saturating_sub(config.backfill_blocks));

        info!(
            current_block,
            start_block,
            pages = (current_block.saturating_sub(start_block)) / PAGE_SIZE + 1,
            "LivePolygonConnector: starting backfill"
        );

        // Spawn backfill task (HTTP).
        let backfill_tx = event_tx.clone();
        let checkpoint_path = config.checkpoint_path.clone();
        tokio::spawn(async move {
            run_backfill(
                backfill_tx,
                http_provider,
                start_block,
                current_block,
                checkpoint_path,
            )
            .await;
        });

        // Spawn WS subscription task.
        let ws_url = config.ws_url.clone();
        tokio::spawn(async move {
            run_ws_subscription(event_tx, ws_url).await;
        });

        Ok(Self {
            source_id,
            event_rx,
            last_event_at: None,
        })
    }
}

impl SourceConnector for LivePolygonConnector {
    fn source_id(&self) -> SourceId {
        self.source_id.clone()
    }

    fn health(&self) -> SourceHealth {
        let status = if self.event_rx.is_closed() {
            SourceStatus::Dead
        } else {
            SourceStatus::Healthy
        };
        SourceHealth {
            source_id: self.source_id.clone(),
            last_event_at: self.last_event_at.clone(),
            status,
        }
    }

    async fn next_event(&mut self) -> Result<SourceEvent, SourceError> {
        match self.event_rx.recv().await {
            Some(event) => {
                let ts = event.timestamp().clone();
                let payload = serde_json::to_vec(&event).map_err(|e| SourceError::Fatal {
                    message: e.to_string(),
                })?;
                self.last_event_at = Some(ts.clone());
                Ok(SourceEvent {
                    source_id: self.source_id.clone(),
                    schema_version: 1,
                    parser_version: 1,
                    observed_at: ts,
                    received_at: ReceivedAt::now_utc(),
                    payload,
                })
            }
            None => Err(SourceError::Fatal {
                message: "Polygon event channel closed — both background workers exited".into(),
            }),
        }
    }
}

// ── Backfill worker ───────────────────────────────────────────────────────────

async fn run_backfill<P: Provider + Clone>(
    tx: mpsc::Sender<PolygonEvent>,
    provider: P,
    start_block: u64,
    end_block: u64,
    checkpoint_path: PathBuf,
) {
    let total_blocks = end_block.saturating_sub(start_block);
    if total_blocks == 0 {
        info!("Backfill: already at current head, skipping");
        return;
    }

    let total_pages = total_blocks / PAGE_SIZE + 1;
    let mut page: u64 = 0;
    let mut block = start_block;

    while block <= end_block {
        if tx.is_closed() {
            debug!("Backfill: channel closed, stopping");
            return;
        }

        let to_block = (block + PAGE_SIZE - 1).min(end_block);
        let filter = make_range_filter(block, to_block);

        match provider.get_logs(&filter).await {
            Ok(logs) => {
                for log in &logs {
                    if let Some(event) = decoder::decode_log(log)
                        && tx.send(event).await.is_err()
                    {
                        debug!("Backfill: receiver dropped, stopping");
                        return;
                    }
                }
            }
            Err(e) => {
                warn!(block, to_block, error = %e, "Backfill: get_logs failed; retrying page");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue; // retry same page
            }
        }

        page += 1;

        // Progress logging every PROGRESS_LOG_INTERVAL pages.
        if page.is_multiple_of(PROGRESS_LOG_INTERVAL) {
            let processed = block.saturating_sub(start_block);
            let pct_x10 = (processed * 1000).checked_div(total_blocks).unwrap_or(1000);
            info!(
                block,
                end_block,
                page,
                total_pages,
                "Backfill progress: {}.{}% complete",
                pct_x10 / 10,
                pct_x10 % 10,
            );
        }

        // Persist checkpoint periodically.
        if page.is_multiple_of(CHECKPOINT_SAVE_INTERVAL)
            && let Err(e) = save_checkpoint(&checkpoint_path, to_block)
        {
            warn!(error = %e, "Backfill: checkpoint save failed");
        }

        block = to_block + 1;
    }

    if let Err(e) = save_checkpoint(&checkpoint_path, end_block) {
        warn!(error = %e, "Backfill: final checkpoint save failed");
    }
    info!(end_block, pages = page, "Backfill complete");
}

// ── WS subscription worker ────────────────────────────────────────────────────

async fn run_ws_subscription(tx: mpsc::Sender<PolygonEvent>, ws_url: String) {
    let mut backoff_secs: u64 = 1;

    loop {
        if tx.is_closed() {
            debug!("WS worker: channel closed, stopping");
            return;
        }

        match connect_and_stream(&ws_url, &tx).await {
            StreamOutcome::ChannelClosed => return,
            StreamOutcome::Reconnect(reason) => {
                warn!(reason, backoff_secs, "WS disconnected; reconnecting");
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
        }
    }
}

#[derive(Debug)]
enum StreamOutcome {
    ChannelClosed,
    Reconnect(String),
}

async fn connect_and_stream(ws_url: &str, tx: &mpsc::Sender<PolygonEvent>) -> StreamOutcome {
    let connect = WsConnect::new(ws_url);
    let provider = match ProviderBuilder::new().connect_ws(connect).await {
        Ok(p) => p,
        Err(e) => return StreamOutcome::Reconnect(e.to_string()),
    };

    let filter = make_subscription_filter();
    let mut sub = match provider.subscribe_logs(&filter).await {
        Ok(s) => s,
        Err(e) => return StreamOutcome::Reconnect(e.to_string()),
    };

    info!("WS subscription active");

    loop {
        match sub.recv().await {
            Ok(log) => {
                if let Some(event) = decoder::decode_log(&log)
                    && tx.send(event).await.is_err()
                {
                    return StreamOutcome::ChannelClosed;
                }
            }
            Err(e) => return StreamOutcome::Reconnect(e.to_string()),
        }
    }
}

// ── Filter construction ───────────────────────────────────────────────────────

fn make_range_filter(from_block: u64, to_block: u64) -> Filter {
    Filter::new()
        .address(MONITORED_ADDRESSES.to_vec())
        .event_signature(MONITORED_TOPICS.to_vec())
        .from_block(BlockNumberOrTag::Number(from_block))
        .to_block(BlockNumberOrTag::Number(to_block))
}

fn make_subscription_filter() -> Filter {
    Filter::new()
        .address(MONITORED_ADDRESSES.to_vec())
        .event_signature(MONITORED_TOPICS.to_vec())
}

// ── Checkpoint ────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    last_block: u64,
}

fn load_checkpoint(path: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(path).ok()?;
    let cp: Checkpoint = serde_json::from_str(&content).ok()?;
    Some(cp.last_block + 1) // resume from the block after the last seen
}

fn save_checkpoint(path: &Path, block: u64) -> Result<(), std::io::Error> {
    let cp = Checkpoint { last_block: block };
    let content = serde_json::to_string(&cp).map_err(|e| std::io::Error::other(e.to_string()))?;
    std::fs::write(path, content)
}
