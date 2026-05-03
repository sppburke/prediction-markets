//! Unit tests for the operator clustering algorithm.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use pe_core_types::{SourceTimestamp, WalletAddress};
use rust_decimal::Decimal;

use crate::{
    clustering::{build_operator_identities, ClusteringConfig},
    funding::{AddressCategory, FundingEdge, FundingSnapshot},
    identity::AntiGamingFlag,
};

/// Helper: construct a WalletAddress from a single distinguishing byte.
fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

/// Helper: a fixed SourceTimestamp (2024-01-01T00:00:00Z).
fn ts_epoch() -> SourceTimestamp {
    use time::macros::datetime;
    SourceTimestamp(datetime!(2024-01-01 00:00:00 UTC))
}

/// Helper: a SourceTimestamp `days` days before epoch.
fn ts_days_ago(days: i64) -> SourceTimestamp {
    use time::Duration;
    SourceTimestamp(ts_epoch().0 - Duration::days(days))
}

/// Helper: empty snapshot with just a snapshot_at.
fn empty_snapshot() -> FundingSnapshot {
    FundingSnapshot {
        edges: vec![],
        wallet_ages: HashMap::new(),
        known_external: HashMap::new(),
        closed_trade_counts: HashMap::new(),
        realized_pnl_usd: HashMap::new(),
        snapshot_at: ts_epoch(),
    }
}

fn make_edge(funder: WalletAddress, funded: WalletAddress) -> FundingEdge {
    FundingEdge {
        funder,
        funded,
        amount_usd: Decimal::from(100u32),
        timestamp: ts_days_ago(1), // 1 day ago — within 7d window
    }
}

// ─── Test 1: Single wallet, no edges ────────────────────────────────────────

#[test]
fn single_wallet_no_edges_is_own_root() {
    let w = wallet(0x01);
    let mut snapshot = empty_snapshot();
    snapshot.wallet_ages.insert(w, 30 * 86_400); // 30 days old

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    assert_eq!(result.len(), 1);
    let identity = &result[0];
    assert_eq!(identity.member_wallets, vec![w]);
    assert_eq!(identity.funder_root.0, w);
    // No hops from root to itself.
    assert_eq!(identity.hop_counts[&w].0, 0);
    // Confidence: 1_000_000 (no hops beyond 1, no missing age).
    assert_eq!(identity.confidence_ppm, 1_000_000);
    assert!(identity.anti_gaming_flags.is_empty());
}

// ─── Test 2: Two wallets, funder → wallet_a ─────────────────────────────────

#[test]
fn two_wallets_one_funder() {
    let funder = wallet(0x10);
    let wa = wallet(0x11);

    let mut snapshot = empty_snapshot();
    snapshot.edges.push(make_edge(funder, wa));
    snapshot.wallet_ages.insert(funder, 365 * 86_400);
    snapshot.wallet_ages.insert(wa, 180 * 86_400);

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    // Both wallets map to funder as root → one cluster.
    assert_eq!(result.len(), 1);
    let id = &result[0];
    assert_eq!(id.funder_root.0, funder);
    assert!(id.member_wallets.contains(&funder));
    assert!(id.member_wallets.contains(&wa));
    assert_eq!(id.member_wallets.len(), 2);

    // OperatorId is deterministic.
    let expected_op_id = pe_core_types::OperatorId(blake3::hash(&funder.0));
    assert_eq!(id.operator_id, expected_op_id);

    // wa is 1 hop from root; funder is 0 hops.
    assert_eq!(id.hop_counts[&funder].0, 0);
    assert_eq!(id.hop_counts[&wa].0, 1);
}

// ─── Test 3: Three wallets, shared funder ───────────────────────────────────

#[test]
fn three_wallets_shared_funder_one_cluster() {
    let funder = wallet(0x20);
    let wa = wallet(0x21);
    let wb = wallet(0x22);

    let mut snapshot = empty_snapshot();
    snapshot.edges.push(make_edge(funder, wa));
    snapshot.edges.push(make_edge(funder, wb));
    snapshot.wallet_ages.insert(funder, 365 * 86_400);
    snapshot.wallet_ages.insert(wa, 180 * 86_400);
    snapshot.wallet_ages.insert(wb, 180 * 86_400);

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    assert_eq!(result.len(), 1);
    let id = &result[0];
    assert_eq!(id.funder_root.0, funder);
    assert_eq!(id.cluster_size.0, 3);
    assert!(id.member_wallets.contains(&funder));
    assert!(id.member_wallets.contains(&wa));
    assert!(id.member_wallets.contains(&wb));
}

// ─── Test 4: Hop count limit ─────────────────────────────────────────────────

#[test]
fn hop_count_limit_respected() {
    // Chain: A → B → C → D  (3 hops from A to D)
    let a = wallet(0x30);
    let b = wallet(0x31);
    let c = wallet(0x32);
    let d = wallet(0x33);

    let mut snapshot = empty_snapshot();
    snapshot.edges.push(make_edge(a, b));
    snapshot.edges.push(make_edge(b, c));
    snapshot.edges.push(make_edge(c, d));
    snapshot.wallet_ages.insert(a, 365 * 86_400);
    snapshot.wallet_ages.insert(b, 300 * 86_400);
    snapshot.wallet_ages.insert(c, 200 * 86_400);
    snapshot.wallet_ages.insert(d, 100 * 86_400);

    // Lower min confidence so the chain is included despite multiple hops.
    let config = ClusteringConfig {
        funding_max_hops: 3,
        funder_root_min_confidence_ppm: 0,
        ..ClusteringConfig::default()
    };

    let result = build_operator_identities(&snapshot, &config).unwrap();

    // With max_hops=3: d traces back to a in 3 hops — all in one cluster.
    assert_eq!(result.len(), 1);
    let id = &result[0];
    assert_eq!(id.funder_root.0, a);
    assert_eq!(id.cluster_size.0, 4);

    // Hop counts.
    assert_eq!(id.hop_counts[&a].0, 0);
    assert_eq!(id.hop_counts[&b].0, 1);
    assert_eq!(id.hop_counts[&c].0, 2);
    assert_eq!(id.hop_counts[&d].0, 3);
}

#[test]
fn hop_count_limit_splits_chain() {
    // Chain: A → B → C → D → E (4 hops from A to E)
    let a = wallet(0x40);
    let b = wallet(0x41);
    let c = wallet(0x42);
    let d = wallet(0x43);
    let e = wallet(0x44);

    let mut snapshot = empty_snapshot();
    snapshot.edges.push(make_edge(a, b));
    snapshot.edges.push(make_edge(b, c));
    snapshot.edges.push(make_edge(c, d));
    snapshot.edges.push(make_edge(d, e));
    for w in [a, b, c, d, e] {
        snapshot.wallet_ages.insert(w, 365 * 86_400);
    }

    let config = ClusteringConfig {
        funding_max_hops: 3,
        funder_root_min_confidence_ppm: 0,
        ..ClusteringConfig::default()
    };

    let result = build_operator_identities(&snapshot, &config).unwrap();

    // With max_hops=3: e can only trace back 3 hops to b, which is NOT a true
    // root (it has funder a). So b becomes e's boundary root.
    // Meanwhile a, b, c, d trace back to a as true root.
    // So we expect 2 clusters: {a,b,c,d} root=a, and {e,b} root=b (boundary).
    let roots: Vec<_> = result.iter().map(|id| id.funder_root.0).collect();
    assert!(roots.contains(&a), "a should be a root");
    // e's boundary root should be b (3 hops from e: e→d→c→b).
    assert!(roots.contains(&b), "b should be boundary root for e");
}

// ─── Test 5: Proptest — determinism ─────────────────────────────────────────

/// Helper to build a reproducible test snapshot from a u64 seed.
pub fn make_test_snapshot(seed: u64) -> FundingSnapshot {
    // Derive a funder and a funded wallet from seed.
    let funder_byte = (seed % 200) as u8;
    let funded_byte = ((seed / 200) % 200 + 200) as u8;
    let funder = wallet(funder_byte);
    let funded = wallet(funded_byte);

    let mut snapshot = empty_snapshot();
    if funder != funded {
        snapshot.edges.push(make_edge(funder, funded));
    }
    snapshot.wallet_ages.insert(funder, 365 * 86_400);
    snapshot.wallet_ages.insert(funded, 180 * 86_400);
    snapshot
}

proptest::proptest! {
    #[test]
    fn operator_id_is_deterministic(seed in 0u64..1000u64) {
        let snapshot = make_test_snapshot(seed);
        let config = ClusteringConfig::default();
        let result1 = build_operator_identities(&snapshot, &config).unwrap();
        let result2 = build_operator_identities(&snapshot, &config).unwrap();
        let ids1: std::collections::BTreeSet<_> = result1
            .iter()
            .map(|o| o.operator_id.0.to_hex())
            .collect();
        let ids2: std::collections::BTreeSet<_> = result2
            .iter()
            .map(|o| o.operator_id.0.to_hex())
            .collect();
        proptest::prop_assert_eq!(ids1, ids2);
    }
}

// ─── Test 6: LaunderedFunder flag ───────────────────────────────────────────

#[test]
fn laundered_funder_flag_fires() {
    let root = wallet(0x60);
    let mut snapshot = empty_snapshot();
    // Root is a CEX address.
    snapshot
        .known_external
        .insert(root, AddressCategory::CexDeposit);
    // Root age < 7 days.
    snapshot.wallet_ages.insert(root, 3 * 86_400); // 3 days

    // 5 wallets funded by root in last 72h.
    for i in 0u8..5 {
        let funded = wallet(0x61 + i);
        snapshot.edges.push(FundingEdge {
            funder: root,
            funded,
            amount_usd: Decimal::from(1000u32),
            timestamp: ts_days_ago(1), // 1 day ago — within 72h
        });
        snapshot.wallet_ages.insert(funded, 86_400);
    }

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    assert_eq!(result.len(), 1);
    let id = &result[0];
    assert!(
        id.anti_gaming_flags
            .contains(&AntiGamingFlag::LaunderedFunder),
        "LaunderedFunder should be set; flags = {:?}",
        id.anti_gaming_flags
    );
}

// ─── Test 7: BaitWalletSuspect flag ─────────────────────────────────────────

#[test]
fn bait_wallet_suspect_flag_fires() {
    let root = wallet(0x70);
    let mut snapshot = empty_snapshot();
    snapshot.wallet_ages.insert(root, 365 * 86_400);

    // Fund 6 wallets in last 7 days (above threshold of 5).
    for i in 0u8..6 {
        let funded = wallet(0x71 + i);
        snapshot.edges.push(FundingEdge {
            funder: root,
            funded,
            amount_usd: Decimal::from(100u32),
            timestamp: ts_days_ago(2),
        });
        // All wallets are fresh (1 day old, 0 trades).
        snapshot.wallet_ages.insert(funded, 86_400); // 1 day
        snapshot.closed_trade_counts.insert(funded, 0);
    }

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    assert_eq!(result.len(), 1);
    let id = &result[0];
    assert!(
        id.anti_gaming_flags
            .contains(&AntiGamingFlag::BaitWalletSuspect),
        "BaitWalletSuspect should be set; flags = {:?}",
        id.anti_gaming_flags
    );
}
