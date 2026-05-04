//! Live Polygon connector: tightly-filtered backfill via HTTP `eth_getLogs`
//! plus real-time via WebSocket.
//!
//! Startup runs [`funder_discovery::discover_to_depth`] to (a) page through
//! the historical block range with `topic[2] ∈ seed_wallets` to populate the
//! funding graph, and (b) compute the closure of transitive funders. The WS
//! subscription then uses that closure as its `topic[2]` filter — so live
//! events are limited to USDC transfers that touch a wallet of interest.
//!
//! Two parallel WS subscriptions are merged via `tokio::select!`:
//!
//! 1. **USDC** — `topic[0] = Transfer`, `topic[2] ∈ closure`. The CU saver.
//! 2. **Other** — `address ∈ {WCOL, GnosisSafeFactory}`, no topic filter
//!    beyond `topic[0]`. Volume is low enough on these contracts that an
//!    address-level filter suffices.
//!
//! WCOL Mint/Burn events and ProxyCreation events that occurred BEFORE
//! service start are not backfilled; they are observed only from live WS.
//! In Phase 0B these only feed `wallet_first_seen` for anti-gaming flags
//! that require multi-week histories anyway.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy::{
    primitives::B256,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast::error::RecvError as BroadcastRecvError, mpsc};
use tracing::{debug, info, warn};

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_source_core::{SourceConnector, SourceError, SourceEvent, SourceHealth, SourceStatus};

use crate::{
    contracts::{GNOSIS_SAFE_FACTORY, MONITORED_TOPICS, TOPIC_ERC20_TRANSFER, USDC, WCOL},
    decoder,
    event::PolygonEvent,
    funder_discovery::{BlockRange, EthGetLogsLookup, discover_to_depth, wallet_to_topic},
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum WS reconnect delay in seconds.
const MAX_BACKOFF_SECS: u64 = 60;

/// A cached funder closure is reused (skipping discovery) only if the
/// checkpoint's `last_block` is within this many blocks of current chain head.
/// 50_000 ≈ 1 day on Polygon at ~2s/block. Beyond that we re-discover to catch
/// new funders that appeared in the interim.
const CACHED_CLOSURE_MAX_AGE_BLOCKS: u64 = 50_000;

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
    /// Max blocks per `eth_getLogs` page during discovery/backfill.
    /// Alchemy free tier caps this at 10; paid tiers allow ~2_000+.
    /// See `docs/_GLOSSARY.md`: `polygon_backfill_page_size`.
    pub backfill_page_size: u64,
    /// Path to the JSON block-checkpoint file. Resumes backfill from `last_block + 1`.
    pub checkpoint_path: PathBuf,
    /// Bounded channel capacity. See `docs/_GLOSSARY.md`: `polygon_channel_capacity`.
    pub channel_capacity: usize,
    /// Seed wallets used as the initial `topic[2]` filter for discovery and
    /// the WS subscription. Typically the leaderboard top-N.
    /// Empty falls back to a global USDC firehose with a warning.
    pub seed_wallets: Vec<WalletAddress>,
    /// Recursion depth for funder discovery. `0` disables discovery; the WS
    /// subscription then uses `seed_wallets` as the topic[2] filter directly.
    /// See `docs/_GLOSSARY.md`: `funding_max_hops`.
    pub funding_max_hops: u8,
}

impl Default for PolygonConnectorConfig {
    fn default() -> Self {
        Self {
            http_url: String::new(),
            ws_url: String::new(),
            backfill_blocks: 21_000_000,
            backfill_page_size: 10,
            checkpoint_path: PathBuf::from("./polygon_checkpoint.json"),
            channel_capacity: 256,
            seed_wallets: Vec::new(),
            funding_max_hops: 3,
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
    #[error("funder discovery: {0}")]
    Discovery(String),
}

// ── Connector ─────────────────────────────────────────────────────────────────

/// A live Polygon connector that ingests decoded on-chain events from Alchemy.
pub struct LivePolygonConnector {
    source_id: SourceId,
    event_rx: mpsc::Receiver<PolygonEvent>,
    last_event_at: Option<SourceTimestamp>,
}

impl LivePolygonConnector {
    /// Connect to Polygon and start discovery + WS tasks.
    ///
    /// Discovery runs synchronously (paging `eth_getLogs` to populate the
    /// funding graph and compute the funder closure); the WS subscription is
    /// then spawned with the closure as its `topic[2]` filter.
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

        let current_block = http_provider
            .get_block_number()
            .await
            .map_err(|e| LivePolygonError::BlockNumber(e.to_string()))?;

        // Resume from checkpoint (or default to head - backfill_blocks).
        let prev_checkpoint = load_checkpoint(&config.checkpoint_path);
        let start_block = prev_checkpoint
            .as_ref()
            .map(|cp| cp.last_block + 1)
            .unwrap_or_else(|| current_block.saturating_sub(config.backfill_blocks));

        info!(
            current_block,
            start_block,
            seed_wallet_count = config.seed_wallets.len(),
            funding_max_hops = config.funding_max_hops,
            "LivePolygonConnector: starting"
        );

        // If a recent checkpoint preserves a usable closure, skip discovery
        // entirely — this is the fast restart path. We still re-discover when
        // the chain has advanced beyond CACHED_CLOSURE_MAX_AGE_BLOCKS so new
        // funders that appeared in the interim are picked up.
        let cached_closure = prev_checkpoint.as_ref().and_then(|cp| {
            if cp.closure.is_empty() {
                return None;
            }
            let chain_advance = current_block.saturating_sub(cp.last_block);
            if chain_advance <= CACHED_CLOSURE_MAX_AGE_BLOCKS {
                Some(cp.closure.clone())
            } else {
                info!(
                    chain_advance,
                    threshold = CACHED_CLOSURE_MAX_AGE_BLOCKS,
                    "cached closure too stale; re-running discovery"
                );
                None
            }
        });

        // Discovery + backfill: page eth_getLogs with topic[2] = seed_wallets,
        // recursing to funding_max_hops. Decoded events are forwarded via tx
        // as a side-effect, so the funding graph picks up historical USDC
        // transfers without a separate broad backfill. The per-hop callback
        // persists the closure so an interrupted run preserves completed-hop
        // progress.
        let ws_filter_wallets = if let Some(closure) = cached_closure {
            info!(
                closure_size = closure.len(),
                "using cached funder closure (skipping discovery)"
            );
            closure
        } else if start_block >= current_block
            || config.seed_wallets.is_empty()
            || config.funding_max_hops == 0
        {
            info!(
                seed_wallets_in_filter = config.seed_wallets.len(),
                "skipping funder discovery (range empty, no seeds, or max_hops=0)"
            );
            config.seed_wallets.clone()
        } else {
            let lookup = EthGetLogsLookup {
                provider: http_provider.clone(),
                page_size: config.backfill_page_size,
                event_tx: event_tx.clone(),
            };
            let seeds: HashSet<WalletAddress> = config.seed_wallets.iter().copied().collect();
            let range = BlockRange {
                from: start_block,
                to: current_block,
            };
            let checkpoint_path = config.checkpoint_path.clone();
            let closure = discover_to_depth(
                &lookup,
                seeds,
                config.funding_max_hops,
                range,
                |hop, closure_so_far| {
                    let cl: Vec<WalletAddress> = closure_so_far.iter().copied().collect();
                    if let Err(e) = save_checkpoint(&checkpoint_path, current_block, &cl) {
                        warn!(error = %e, hop, "checkpoint save after hop failed");
                    } else {
                        debug!(hop, closure_size = cl.len(), "checkpoint saved after hop");
                    }
                },
            )
            .await
            .map_err(|e| LivePolygonError::Discovery(e.to_string()))?;
            closure.into_iter().collect()
        };

        info!(
            ws_filter_address_count = ws_filter_wallets.len(),
            "WS topic[2] filter populated"
        );

        // Spawn WS subscription task with the discovered wallet closure.
        let ws_url = config.ws_url.clone();
        tokio::spawn(async move {
            run_ws_subscription(event_tx, ws_url, ws_filter_wallets).await;
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

// ── WS subscription worker ────────────────────────────────────────────────────

async fn run_ws_subscription(
    tx: mpsc::Sender<PolygonEvent>,
    ws_url: String,
    seed_wallets: Vec<WalletAddress>,
) {
    let mut backoff_secs: u64 = 1;

    loop {
        if tx.is_closed() {
            debug!("WS worker: channel closed, stopping");
            return;
        }

        match connect_and_stream(&ws_url, &tx, &seed_wallets).await {
            StreamOutcome::ChannelClosed => return,
            StreamOutcome::ConnectFailed(reason) => {
                warn!(reason, backoff_secs, "WS connect failed; retrying");
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
            StreamOutcome::StreamEnded(reason) => {
                // Stream was active; reset backoff so a brief reconnect isn't penalised.
                warn!(reason, "WS stream ended; reconnecting");
                backoff_secs = 1;
            }
        }
    }
}

#[derive(Debug)]
enum StreamOutcome {
    ChannelClosed,
    /// WS connection or subscription failed before streaming began; maintain backoff.
    ConnectFailed(String),
    /// WS stream was active and then disconnected; backoff should reset on next connect.
    StreamEnded(String),
}

async fn connect_and_stream(
    ws_url: &str,
    tx: &mpsc::Sender<PolygonEvent>,
    seed_wallets: &[WalletAddress],
) -> StreamOutcome {
    let connect = WsConnect::new(ws_url);
    let provider = match ProviderBuilder::new().connect_ws(connect).await {
        Ok(p) => p,
        Err(e) => return StreamOutcome::ConnectFailed(e.to_string()),
    };

    let usdc_filter = make_usdc_subscription_filter(seed_wallets);
    let other_filter = make_other_subscription_filter();

    let mut sub_usdc = match provider.subscribe_logs(&usdc_filter).await {
        Ok(s) => s,
        Err(e) => return StreamOutcome::ConnectFailed(format!("usdc sub: {e}")),
    };
    let mut sub_other = match provider.subscribe_logs(&other_filter).await {
        Ok(s) => s,
        Err(e) => return StreamOutcome::ConnectFailed(format!("other sub: {e}")),
    };

    info!(
        seed_wallet_count = seed_wallets.len(),
        "WS subscriptions active (usdc filtered, wcol+factory unfiltered)"
    );

    loop {
        tokio::select! {
            r = sub_usdc.recv() => match r {
                Ok(log) => {
                    if let Some(event) = decoder::decode_log(&log)
                        && tx.send(event).await.is_err()
                    {
                        return StreamOutcome::ChannelClosed;
                    }
                }
                // Lag is recoverable: the channel is still alive; we lost `n` events
                // from the broadcast buffer. Warn and continue rather than reconnect.
                Err(BroadcastRecvError::Lagged(n)) => {
                    warn!(dropped = n, sub = "usdc", "subscription lagged; events dropped");
                }
                Err(e) => return StreamOutcome::StreamEnded(format!("usdc: {e}")),
            },
            r = sub_other.recv() => match r {
                Ok(log) => {
                    if let Some(event) = decoder::decode_log(&log)
                        && tx.send(event).await.is_err()
                    {
                        return StreamOutcome::ChannelClosed;
                    }
                }
                Err(BroadcastRecvError::Lagged(n)) => {
                    warn!(dropped = n, sub = "other", "subscription lagged; events dropped");
                }
                Err(e) => return StreamOutcome::StreamEnded(format!("other: {e}")),
            },
        }
    }
}

// ── Filter construction ───────────────────────────────────────────────────────

/// USDC Transfer subscription. When `seed_wallets` is non-empty, restricts to
/// transfers TO any of those wallets (`topic[2] = recipient`). Empty falls
/// back to the full USDC firehose with a warning — the deployment should
/// always populate `seed_wallets`.
fn make_usdc_subscription_filter(seed_wallets: &[WalletAddress]) -> Filter {
    let base = Filter::new()
        .address(USDC)
        .event_signature(TOPIC_ERC20_TRANSFER);
    if seed_wallets.is_empty() {
        warn!(
            "WS USDC subscription has no topic[2] filter (empty seed_wallets); falling back to full firehose"
        );
        base
    } else {
        let topic2: Vec<B256> = seed_wallets.iter().map(wallet_to_topic).collect();
        base.topic2(topic2)
    }
}

/// WCOL Mint/Burn (Transfer) and Gnosis Safe ProxyCreation events. Volume on
/// these two contracts is low enough that a topic[2] filter is unnecessary
/// and the OR-of-topics filter captures both event types in one subscription.
fn make_other_subscription_filter() -> Filter {
    Filter::new()
        .address(vec![WCOL, GNOSIS_SAFE_FACTORY])
        .event_signature(MONITORED_TOPICS.to_vec())
}

// ── Checkpoint ────────────────────────────────────────────────────────────────

/// Persisted discovery state. `closure` is the union of seed wallets plus all
/// funders discovered during the most recent successful (or partial) run.
/// Empty `closure` means "no discovery has completed yet".
#[derive(Serialize, Deserialize, Default)]
struct Checkpoint {
    last_block: u64,
    #[serde(default)]
    closure: Vec<WalletAddress>,
}

fn load_checkpoint(path: &Path) -> Option<Checkpoint> {
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_checkpoint(
    path: &Path,
    last_block: u64,
    closure: &[WalletAddress],
) -> Result<(), std::io::Error> {
    let cp = Checkpoint {
        last_block,
        closure: closure.to_vec(),
    };
    let content = serde_json::to_string(&cp).map_err(|e| std::io::Error::other(e.to_string()))?;
    // Write to a sibling tmp file then rename for POSIX-atomic swap: a crash
    // between write and rename leaves the previous checkpoint intact.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)
}
