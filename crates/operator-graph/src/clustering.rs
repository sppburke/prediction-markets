//! Operator clustering algorithm.
//!
//! [`build_operator_identities`] groups wallets that share a common funder root
//! (within [`ClusteringConfig::funding_max_hops`] hops) into a single
//! [`OperatorIdentity`]. The algorithm is pure and deterministic: same inputs
//! always produce the same output.

use std::collections::{HashMap, HashSet, VecDeque};

use pe_core_types::{ClusterSize, FunderRootId, FundingHopCount, OperatorId, WalletAddress};
use rust_decimal::Decimal;
use tracing::warn;

use crate::{
    error::OperatorGraphError,
    funding::FundingSnapshot,
    identity::{AntiGamingFlag, OperatorIdentity},
};

/// Configuration for the clustering algorithm.
///
/// All defaults from `docs/_GLOSSARY.md`.
#[derive(Debug, Clone)]
pub struct ClusteringConfig {
    /// Maximum hop count from wallet to funder root (default: 3).
    pub funding_max_hops: u8,
    /// Minimum confidence to emit an OperatorIdentity (default: 850_000 ppm).
    pub funder_root_min_confidence_ppm: u32,
    /// Minimum cluster size (default: 1).
    pub cluster_min_size: u16,
    /// Maximum cluster size before marking for review (default: 25).
    pub cluster_max_size: u16,
    /// Anti-gaming: seeding velocity warn threshold (default: 5 wallets/week).
    pub seeding_velocity_warn_per_week: u32,
    /// Anti-gaming: fresh wallet max closed trades (default: 2).
    pub fresh_wallet_max_closed_trades: u32,
    /// Anti-gaming: fresh wallet max age in seconds (default: 1_209_600 = 14 days).
    pub fresh_wallet_max_age_seconds: u32,
    /// Anti-gaming: cluster membership max churn pct (default: 30).
    pub cluster_membership_max_churn_pct: u32,
    /// Anti-gaming: cluster membership stability window days (default: 30).
    pub cluster_membership_stability_window_d: u32,
    /// Anti-gaming: LaunderedFunder min fanout count in 72h (default: 5).
    pub laundered_funder_min_fanout: u32,
    /// Anti-gaming: LaunderedFunder max funder age in days (default: 7).
    pub laundered_funder_max_age_days: u32,
    /// Anti-gaming: WashCluster intra-cluster trade match threshold (default: 60%).
    pub wash_cluster_match_threshold_pct: u32,
    /// Anti-gaming: MarketNarrowness family concentration warn pct (default: 60%).
    pub market_narrowness_warn_pct: u32,
    /// Anti-gaming: inherited prior max position USD for BaitWallet (default: 5000).
    pub bait_wallet_max_position_usd: Decimal,
}

impl Default for ClusteringConfig {
    fn default() -> Self {
        Self {
            funding_max_hops: 3,
            funder_root_min_confidence_ppm: 850_000,
            cluster_min_size: 1,
            cluster_max_size: 25,
            seeding_velocity_warn_per_week: 5,
            fresh_wallet_max_closed_trades: 2,
            fresh_wallet_max_age_seconds: 1_209_600,
            cluster_membership_max_churn_pct: 30,
            cluster_membership_stability_window_d: 30,
            laundered_funder_min_fanout: 5,
            laundered_funder_max_age_days: 7,
            wash_cluster_match_threshold_pct: 60,
            market_narrowness_warn_pct: 60,
            bait_wallet_max_position_usd: Decimal::from(5000u32),
        }
    }
}

/// Derive [`OperatorId`] from the bytes of the funder root wallet address.
///
/// `OperatorId = blake3(funder_root_address_bytes)` — deterministic and stable.
fn operator_id_from_root(root: WalletAddress) -> OperatorId {
    OperatorId(blake3::hash(&root.0))
}

/// Collect all wallets mentioned in the snapshot (funders and funded).
fn all_wallets(snapshot: &FundingSnapshot) -> Vec<WalletAddress> {
    let mut seen: HashSet<WalletAddress> = HashSet::new();
    for edge in &snapshot.edges {
        seen.insert(edge.funder);
        seen.insert(edge.funded);
    }
    for w in snapshot.wallet_ages.keys() {
        seen.insert(*w);
    }
    for w in snapshot.closed_trade_counts.keys() {
        seen.insert(*w);
    }
    // Sort by raw bytes for determinism.
    let mut wallets: Vec<WalletAddress> = seen.into_iter().collect();
    wallets.sort_by_key(|w| w.0);
    wallets
}

/// Build reverse adjacency map: funded → list of funders.
fn build_reverse_adj(snapshot: &FundingSnapshot) -> HashMap<WalletAddress, Vec<WalletAddress>> {
    let mut adj: HashMap<WalletAddress, Vec<WalletAddress>> = HashMap::new();
    for edge in &snapshot.edges {
        adj.entry(edge.funded).or_default().push(edge.funder);
    }
    adj
}

/// BFS upward from `start` to find the funder root and hop count.
///
/// A "funder root" is any wallet with no inbound edges that is reachable within
/// `max_hops`. If no true root is found within budget the furthest ancestor
/// found becomes the boundary root.
///
/// Returns `(root, hops_from_start_to_root)`.
///
/// On a circular funding graph (every reachable ancestor was already visited
/// before any true root or boundary was reached), `start` becomes its own root
/// with hops=0 — the wallet is treated as self-funded. A `tracing::warn!` is
/// emitted so the cycle is observable.
fn find_root(
    start: WalletAddress,
    reverse_adj: &HashMap<WalletAddress, Vec<WalletAddress>>,
    max_hops: u8,
) -> Result<(WalletAddress, u8), OperatorGraphError> {
    let mut visited: HashMap<WalletAddress, u8> = HashMap::new();
    let mut queue: VecDeque<(WalletAddress, u8)> = VecDeque::new();
    queue.push_back((start, 0));
    visited.insert(start, 0);

    let mut root_candidates: Vec<(WalletAddress, u8)> = Vec::new();

    while let Some((wallet, hops)) = queue.pop_front() {
        match reverse_adj.get(&wallet) {
            None => {
                // True root — no funders.
                root_candidates.push((wallet, hops));
            }
            Some(funders) => {
                let next_hops = match hops.checked_add(1) {
                    Some(n) => n,
                    None => {
                        // Arithmetic overflow guard — treat current wallet as boundary.
                        root_candidates.push((wallet, hops));
                        continue;
                    }
                };
                if next_hops > max_hops {
                    // Budget exhausted — this wallet becomes the boundary root.
                    root_candidates.push((wallet, hops));
                    continue;
                }
                for &funder in funders {
                    if let std::collections::hash_map::Entry::Vacant(e) = visited.entry(funder) {
                        e.insert(next_hops);
                        queue.push_back((funder, next_hops));
                    }
                    // Already visited → skip (handles diamonds and cycles gracefully).
                }
            }
        }
    }

    if root_candidates.is_empty() {
        // Cycle: every reachable ancestor was already visited. Treat `start`
        // as self-funded so the wallet still appears as a singleton cluster
        // downstream rather than poisoning the entire rebuild.
        warn!(wallet = ?start, "cycle in funding graph; treating as self-funded");
        return Ok((start, 0));
    }

    // Pick root with minimum hops; break ties by smallest wallet bytes (determinism).
    root_candidates.sort_by(|(wa, ha), (wb, hb)| ha.cmp(hb).then(wa.0.cmp(&wb.0)));
    let (root, hops) = root_candidates[0];
    Ok((root, hops))
}

/// Count wallets funded by `funder` in the last 7 days relative to `snapshot.snapshot_at`.
fn seeding_velocity_last_7d(funder: WalletAddress, snapshot: &FundingSnapshot) -> u32 {
    let window_secs: i64 = 7 * 24 * 3600;
    let snapshot_unix = snapshot.snapshot_at.0.unix_timestamp();
    let mut count: u32 = 0;
    for edge in &snapshot.edges {
        if edge.funder == funder {
            let age_secs = snapshot_unix.saturating_sub(edge.timestamp.0.unix_timestamp());
            if age_secs <= window_secs {
                count = count.saturating_add(1);
            }
        }
    }
    count
}

/// Check BaitWalletSuspect: seeding velocity above threshold AND a fresh wallet present.
fn flag_bait_wallet_suspect(
    root: WalletAddress,
    members: &[WalletAddress],
    snapshot: &FundingSnapshot,
    config: &ClusteringConfig,
) -> bool {
    let velocity = seeding_velocity_last_7d(root, snapshot);
    if velocity < config.seeding_velocity_warn_per_week {
        return false;
    }
    members.iter().any(|member| {
        let age = snapshot
            .wallet_ages
            .get(member)
            .copied()
            .unwrap_or(u32::MAX);
        let trades = snapshot
            .closed_trade_counts
            .get(member)
            .copied()
            .unwrap_or(0);
        age <= config.fresh_wallet_max_age_seconds
            && trades <= config.fresh_wallet_max_closed_trades
    })
}

/// Check LaunderedFunder: root is young, in known_external, and fans out to ≥ N wallets in 72h.
fn flag_laundered_funder(
    root: WalletAddress,
    snapshot: &FundingSnapshot,
    config: &ClusteringConfig,
) -> bool {
    if !snapshot.known_external.contains_key(&root) {
        return false;
    }
    let max_age_secs = (config.laundered_funder_max_age_days as u64)
        .saturating_mul(86_400)
        .min(u32::MAX as u64) as u32;
    let root_age = snapshot.wallet_ages.get(&root).copied().unwrap_or(u32::MAX);
    if root_age > max_age_secs {
        return false;
    }
    let window_secs: i64 = 72 * 3600;
    let snapshot_unix = snapshot.snapshot_at.0.unix_timestamp();
    let fanout = snapshot
        .edges
        .iter()
        .filter(|e| {
            e.funder == root
                && snapshot_unix.saturating_sub(e.timestamp.0.unix_timestamp()) <= window_secs
        })
        .count() as u32;
    fanout >= config.laundered_funder_min_fanout
}

/// Check DilutionAttack: cluster is oversized (full impl deferred — needs historical data).
///
/// In Phase 0A we set this flag if `cluster_size > cluster_max_size`.
fn flag_dilution_attack(cluster_size: u16, config: &ClusteringConfig) -> bool {
    cluster_size > config.cluster_max_size
}

/// Compute confidence_ppm for a cluster.
///
/// Starts at 1_000_000. Reduced by:
/// - 50_000 per hop beyond 1 (based on max hop count in cluster)
/// - 100_000 if any member is missing age data
/// - forced to 0 if cluster_size > cluster_max_size
fn compute_confidence(
    members: &[WalletAddress],
    hop_counts: &HashMap<WalletAddress, FundingHopCount>,
    snapshot: &FundingSnapshot,
    config: &ClusteringConfig,
) -> u32 {
    let size = members.len() as u16;
    if size > config.cluster_max_size {
        return 0;
    }

    let max_hops = hop_counts.values().map(|h| h.0).max().unwrap_or(0);

    let mut confidence: u32 = 1_000_000;

    if max_hops > 1 {
        let deduct = 50_000u32.saturating_mul((max_hops - 1) as u32);
        confidence = confidence.saturating_sub(deduct);
    }

    let missing_age = members
        .iter()
        .any(|w| !snapshot.wallet_ages.contains_key(w));
    if missing_age {
        confidence = confidence.saturating_sub(100_000);
    }

    confidence
}

/// Build [`OperatorIdentity`] instances from a [`FundingSnapshot`].
///
/// # Algorithm
/// 1. Collect all wallets from edges and side-tables.
/// 2. Build reverse adjacency (funded → funders).
/// 3. BFS upward per wallet to find funder root within `max_hops`.
/// 4. Group wallets by funder root → clusters.
/// 5. Compute `OperatorId`, confidence, flags per cluster.
/// 6. Skip clusters below `funder_root_min_confidence_ppm`.
pub fn build_operator_identities(
    snapshot: &FundingSnapshot,
    config: &ClusteringConfig,
) -> Result<Vec<OperatorIdentity>, OperatorGraphError> {
    let reverse_adj = build_reverse_adj(snapshot);
    let wallets = all_wallets(snapshot);

    // wallet → (funder_root, hops)
    let mut wallet_root: HashMap<WalletAddress, (WalletAddress, u8)> = HashMap::new();

    for &wallet in &wallets {
        let (root, hops) = find_root(wallet, &reverse_adj, config.funding_max_hops)?;
        wallet_root.insert(wallet, (root, hops));
    }

    // Group by root; sort for deterministic output order.
    let mut clusters_map: HashMap<WalletAddress, Vec<WalletAddress>> = HashMap::new();
    for (&wallet, &(root, _hops)) in &wallet_root {
        clusters_map.entry(root).or_default().push(wallet);
    }
    let mut clusters: Vec<(WalletAddress, Vec<WalletAddress>)> = clusters_map.into_iter().collect();
    clusters.sort_by_key(|(root, _)| root.0);

    let mut identities: Vec<OperatorIdentity> = Vec::new();

    for (root, mut members) in clusters {
        members.sort_by_key(|w| w.0);

        let cluster_size = members.len() as u16;
        if cluster_size < config.cluster_min_size {
            continue;
        }

        let mut hop_counts: HashMap<WalletAddress, FundingHopCount> = HashMap::new();
        for &member in &members {
            let hops = wallet_root.get(&member).map_or(0, |&(_, h)| h);
            hop_counts.insert(member, FundingHopCount(hops));
        }

        let confidence_ppm = compute_confidence(&members, &hop_counts, snapshot, config);

        if confidence_ppm < config.funder_root_min_confidence_ppm {
            continue;
        }

        let rq_raw = (confidence_ppm / 10_000).min(100) as u8;
        // new() fails only if n > 100; rq_raw is clamped to 100 above.
        let reconstruction_quality = match pe_core_types::ReconstructionQuality::new(rq_raw) {
            Ok(rq) => rq,
            Err(_) => match pe_core_types::ReconstructionQuality::new(100) {
                Ok(rq) => rq,
                Err(_) => continue, // Unreachable but no panic.
            },
        };

        let mut anti_gaming_flags: HashSet<AntiGamingFlag> = HashSet::new();

        if flag_bait_wallet_suspect(root, &members, snapshot, config) {
            anti_gaming_flags.insert(AntiGamingFlag::BaitWalletSuspect);
        }
        if flag_laundered_funder(root, snapshot, config) {
            anti_gaming_flags.insert(AntiGamingFlag::LaunderedFunder);
        }
        if flag_dilution_attack(cluster_size, config) {
            anti_gaming_flags.insert(AntiGamingFlag::DilutionAttack);
        }
        // WashCluster and MarketNarrowness: deferred — require trade-level data.
        // TODO(trader-index): implement when trade data is available in trader-index /
        // copy-signal-engine.

        identities.push(OperatorIdentity {
            operator_id: operator_id_from_root(root),
            funder_root: FunderRootId(root),
            member_wallets: members,
            hop_counts,
            confidence_ppm,
            reconstruction_quality,
            cluster_size: ClusterSize(cluster_size),
            anti_gaming_flags,
        });
    }

    Ok(identities)
}
