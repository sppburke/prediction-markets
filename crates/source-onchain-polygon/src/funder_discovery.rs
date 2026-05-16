//! Multi-pass funder discovery via topic-filtered `eth_getLogs`.
//!
//! Given a set of seed wallets (typically the leaderboard top-N), this module
//! recursively discovers their transitive funders by issuing tight
//! `eth_getLogs` queries with `topic[2] ∈ frontier` over a fixed block range.
//! The returned closure is the input to the live WS subscription's `topic[2]`
//! filter — dramatically cutting Polygon mainnet event volume (and Alchemy
//! compute units) compared to a global USDC Transfer subscription.
//!
//! The BFS is decoupled from the network via the [`FunderLookup`] trait so
//! it can be unit-tested without an RPC.

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, Filter};
use pe_core_types::WalletAddress;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::contracts::{TOPIC_ERC20_TRANSFER, USDC};
use crate::decoder;
use crate::event::PolygonEvent;

/// Cap on `eth_getLogs` retry backoff. Mirrors `live::MAX_BACKOFF_SECS`.
const MAX_GET_LOGS_BACKOFF_SECS: u64 = 60;

/// Errors produced by funder discovery.
#[derive(Debug, thiserror::Error)]
pub enum FunderDiscoveryError {
    #[error("eth_getLogs: {0}")]
    GetLogs(String),
    #[error("etherscan: {0}")]
    Etherscan(String),
    #[error("receiver dropped during discovery")]
    ReceiverDropped,
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

/// Inclusive block range `[from, to]`.
#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    pub from: u64,
    pub to: u64,
}

/// "Given a set of recipient wallets and a block range, return the set of
/// `from` addresses that funded them via USDC Transfer."
///
/// The trait keeps the BFS in [`discover_to_depth`] independent of any RPC
/// implementation so it can be exercised against in-memory fixtures.
pub trait FunderLookup {
    fn funders_of(
        &self,
        wallets: &HashSet<WalletAddress>,
        range: BlockRange,
    ) -> impl std::future::Future<Output = Result<HashSet<WalletAddress>, FunderDiscoveryError>> + Send;
}

/// Compute the closure of funders reachable from `seeds` within `max_hops`.
///
/// Returned set always contains `seeds` plus every funder discovered at any
/// depth ≤ `max_hops`. The frontier shrinks each iteration as cycles and
/// already-seen funders are skipped, so the BFS terminates even on circular
/// funding graphs.
///
/// `on_hop_complete` is invoked after each successful hop with the hop number
/// (1-indexed) and the closure-so-far. Callers use it to persist partial
/// progress (e.g. a checkpoint) so an interrupted discovery doesn't lose
/// every completed hop's work.
pub async fn discover_to_depth<L, F>(
    lookup: &L,
    seeds: HashSet<WalletAddress>,
    max_hops: u8,
    range: BlockRange,
    mut on_hop_complete: F,
) -> Result<HashSet<WalletAddress>, FunderDiscoveryError>
where
    L: FunderLookup,
    F: FnMut(u8, &HashSet<WalletAddress>),
{
    let seed_count = seeds.len();
    info!(
        seed_count,
        max_hops,
        from_block = range.from,
        to_block = range.to,
        "funder discovery: starting"
    );
    let mut closure = seeds.clone();
    let mut frontier = seeds;
    for hop in 1..=max_hops {
        if frontier.is_empty() {
            debug!(hop, "funder discovery: frontier empty, stopping");
            break;
        }
        let funders = lookup.funders_of(&frontier, range).await?;
        let new_funders: HashSet<WalletAddress> = funders.difference(&closure).copied().collect();
        info!(
            hop,
            new_funders = new_funders.len(),
            cumulative = closure.len() + new_funders.len(),
            "funder discovery: hop complete"
        );
        closure.extend(new_funders.iter().copied());
        frontier = new_funders;
        on_hop_complete(hop, &closure);
    }
    info!(total = closure.len(), "funder discovery: complete");
    Ok(closure)
}

/// [`FunderLookup`] impl backed by `eth_getLogs` against a real provider.
///
/// Pages the block range at `page_size` blocks per request — set this to the
/// RPC tier's per-call limit (Alchemy free: 10; paid tiers: 2_000+).
///
/// As a side-effect, every decoded `UsdcTransfer` log encountered is forwarded
/// via `event_tx` so the discovery pass also serves as the backfill. This
/// avoids paying for the same `eth_getLogs` query twice (once to discover,
/// once to backfill).
pub struct EthGetLogsLookup<P> {
    pub provider: P,
    pub page_size: u64,
    pub event_tx: mpsc::Sender<PolygonEvent>,
}

impl<P: Provider + Clone + Send + Sync> FunderLookup for EthGetLogsLookup<P> {
    async fn funders_of(
        &self,
        wallets: &HashSet<WalletAddress>,
        range: BlockRange,
    ) -> Result<HashSet<WalletAddress>, FunderDiscoveryError> {
        if wallets.is_empty() {
            return Ok(HashSet::new());
        }
        if self.page_size == 0 {
            return Err(FunderDiscoveryError::InvalidConfig(
                "page_size must be > 0".to_string(),
            ));
        }
        let topic2: Vec<B256> = wallets.iter().map(wallet_to_topic).collect();
        let mut funders: HashSet<WalletAddress> = HashSet::new();
        let mut block = range.from;
        let mut events_decoded: u64 = 0;
        while block <= range.to {
            let to_block = (block + self.page_size - 1).min(range.to);
            let filter = Filter::new()
                .address(USDC)
                .event_signature(TOPIC_ERC20_TRANSFER)
                .topic2(topic2.clone())
                .from_block(BlockNumberOrTag::Number(block))
                .to_block(BlockNumberOrTag::Number(to_block));
            // Retry transient `eth_getLogs` failures with exponential backoff.
            // Matches the resilience pattern from PR #40's `run_backfill`.
            let logs = {
                let mut backoff_secs: u64 = 1;
                loop {
                    if self.event_tx.is_closed() {
                        return Err(FunderDiscoveryError::ReceiverDropped);
                    }
                    match self.provider.get_logs(&filter).await {
                        Ok(logs) => break logs,
                        Err(e) => {
                            warn!(
                                block,
                                to_block,
                                backoff_secs,
                                error = %e,
                                "eth_getLogs failed; retrying"
                            );
                            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                            backoff_secs = (backoff_secs * 2).min(MAX_GET_LOGS_BACKOFF_SECS);
                        }
                    }
                }
            };
            for log in logs {
                if let Some(from) = topic_to_wallet(log.topics().get(1)) {
                    funders.insert(from);
                }
                if let Some(event) = decoder::decode_log(&log) {
                    events_decoded += 1;
                    if self.event_tx.send(event).await.is_err() {
                        return Err(FunderDiscoveryError::ReceiverDropped);
                    }
                }
            }
            block = to_block + 1;
        }
        debug!(
            events_decoded,
            funders = funders.len(),
            "funder discovery pass: events forwarded to accumulator"
        );
        Ok(funders)
    }
}

/// Left-pad a 20-byte address into a 32-byte topic value.
pub fn wallet_to_topic(w: &WalletAddress) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(&w.0);
    B256::from(bytes)
}

/// Read the lower 20 bytes of a 32-byte topic as a wallet address.
///
/// Issue #176 promoted to `pub` for reuse in `pe_bootstrap::polygon_ctf_delta`,
/// which extracts maker (`topic[2]`) and taker (`topic[3]`) addresses from
/// `OrderFilled` log topics across the four exchange contracts.
pub fn topic_to_wallet(topic: Option<&B256>) -> Option<WalletAddress> {
    let topic = topic?;
    let bytes = topic.as_slice();
    if bytes.len() != 32 {
        return None;
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&bytes[12..]);
    Some(WalletAddress(addr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    /// In-memory `FunderLookup` for BFS unit tests.
    ///
    /// Keyed by the input wallet — given any frontier, we look up each wallet
    /// and union the results. This models the per-pass `eth_getLogs` behavior
    /// without a real RPC.
    struct InMemoryLookup {
        funders: HashMap<WalletAddress, HashSet<WalletAddress>>,
        call_count: Mutex<u32>,
    }

    impl InMemoryLookup {
        fn new(funders: HashMap<WalletAddress, HashSet<WalletAddress>>) -> Self {
            Self {
                funders,
                call_count: Mutex::new(0),
            }
        }
        fn calls(&self) -> u32 {
            *self.call_count.lock().unwrap()
        }
    }

    impl FunderLookup for InMemoryLookup {
        async fn funders_of(
            &self,
            wallets: &HashSet<WalletAddress>,
            _range: BlockRange,
        ) -> Result<HashSet<WalletAddress>, FunderDiscoveryError> {
            *self.call_count.lock().unwrap() += 1;
            let mut out = HashSet::new();
            for w in wallets {
                if let Some(funders) = self.funders.get(w) {
                    out.extend(funders.iter().copied());
                }
            }
            Ok(out)
        }
    }

    fn w(b: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = b;
        WalletAddress(bytes)
    }

    fn rng() -> BlockRange {
        BlockRange { from: 0, to: 100 }
    }

    #[tokio::test]
    async fn empty_seeds_returns_empty() {
        let lookup = InMemoryLookup::new(HashMap::new());
        let out = discover_to_depth(&lookup, HashSet::new(), 3, rng(), |_, _| {})
            .await
            .unwrap();
        assert!(out.is_empty());
        // No hops should fire — frontier is empty from the start.
        assert_eq!(lookup.calls(), 0);
    }

    #[tokio::test]
    async fn depth_zero_returns_seeds_unchanged() {
        let lookup = InMemoryLookup::new(HashMap::new());
        let seeds: HashSet<_> = [w(0x01), w(0x02)].into_iter().collect();
        let out = discover_to_depth(&lookup, seeds.clone(), 0, rng(), |_, _| {})
            .await
            .unwrap();
        assert_eq!(out, seeds);
        assert_eq!(lookup.calls(), 0);
    }

    #[tokio::test]
    async fn depth_one_discovers_direct_funders() {
        let leader = w(0xa0);
        let funder = w(0xb0);
        let mut funders = HashMap::new();
        funders.insert(leader, [funder].into_iter().collect());
        let lookup = InMemoryLookup::new(funders);
        let out = discover_to_depth(&lookup, [leader].into_iter().collect(), 1, rng(), |_, _| {})
            .await
            .unwrap();
        assert_eq!(out, [leader, funder].into_iter().collect());
        assert_eq!(lookup.calls(), 1);
    }

    #[tokio::test]
    async fn depth_three_traverses_chain() {
        // L ← F1 ← F2 ← F3
        let l = w(0xa0);
        let f1 = w(0xb0);
        let f2 = w(0xc0);
        let f3 = w(0xd0);
        let mut funders = HashMap::new();
        funders.insert(l, [f1].into_iter().collect());
        funders.insert(f1, [f2].into_iter().collect());
        funders.insert(f2, [f3].into_iter().collect());
        let lookup = InMemoryLookup::new(funders);
        let out = discover_to_depth(&lookup, [l].into_iter().collect(), 3, rng(), |_, _| {})
            .await
            .unwrap();
        assert_eq!(out, [l, f1, f2, f3].into_iter().collect());
        assert_eq!(lookup.calls(), 3);
    }

    #[tokio::test]
    async fn depth_two_stops_short_of_chain() {
        let l = w(0xa0);
        let f1 = w(0xb0);
        let f2 = w(0xc0);
        let f3 = w(0xd0);
        let mut funders = HashMap::new();
        funders.insert(l, [f1].into_iter().collect());
        funders.insert(f1, [f2].into_iter().collect());
        funders.insert(f2, [f3].into_iter().collect());
        let lookup = InMemoryLookup::new(funders);
        let out = discover_to_depth(&lookup, [l].into_iter().collect(), 2, rng(), |_, _| {})
            .await
            .unwrap();
        // Depth-2 closure: leader, f1, f2 — but NOT f3.
        assert_eq!(out, [l, f1, f2].into_iter().collect());
    }

    #[tokio::test]
    async fn cycle_in_funder_graph_terminates() {
        // A funded by B; B funded by A. Should not loop forever.
        let a = w(0xa0);
        let b = w(0xb0);
        let mut funders = HashMap::new();
        funders.insert(a, [b].into_iter().collect());
        funders.insert(b, [a].into_iter().collect());
        let lookup = InMemoryLookup::new(funders);
        let out = discover_to_depth(&lookup, [a].into_iter().collect(), 5, rng(), |_, _| {})
            .await
            .unwrap();
        // Closure contains both wallets and the BFS terminates.
        assert_eq!(out, [a, b].into_iter().collect());
        // After hop 1 discovers B, hop 2's frontier {B} returns {A} which is
        // already in the closure, so frontier becomes empty and we stop.
        assert!(lookup.calls() <= 2);
    }

    #[tokio::test]
    async fn diamond_funder_visited_once() {
        // L1 and L2 both funded by F. F should appear once in the closure
        // and the second call's frontier should be empty.
        let l1 = w(0xa1);
        let l2 = w(0xa2);
        let f = w(0xb0);
        let mut funders = HashMap::new();
        funders.insert(l1, [f].into_iter().collect());
        funders.insert(l2, [f].into_iter().collect());
        let lookup = InMemoryLookup::new(funders);
        let out = discover_to_depth(&lookup, [l1, l2].into_iter().collect(), 3, rng(), |_, _| {})
            .await
            .unwrap();
        assert_eq!(out, [l1, l2, f].into_iter().collect());
    }

    #[test]
    fn wallet_topic_round_trip() {
        let w_in = w(0xab);
        let topic = wallet_to_topic(&w_in);
        let w_out = topic_to_wallet(Some(&topic)).unwrap();
        assert_eq!(w_in, w_out);
    }

    #[test]
    fn wallet_topic_left_pads_zero() {
        let w_in = w(0x42);
        let topic = wallet_to_topic(&w_in);
        // First 12 bytes must be zero; address occupies bytes 12..32.
        assert_eq!(&topic.as_slice()[..12], &[0u8; 12]);
        assert_eq!(&topic.as_slice()[12..], &w_in.0);
    }
}
