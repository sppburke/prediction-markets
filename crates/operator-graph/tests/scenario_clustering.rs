// Scenario: clustering produces stable OperatorIds and anti-gaming flags fire correctly.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_operator_graph::{
    clustering::{ClusteringConfig, build_operator_identities},
    funding::{AddressCategory, FundingEdge, FundingSnapshot},
    identity::AntiGamingFlag,
};
use rust_decimal::Decimal;

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn ts_epoch() -> SourceTimestamp {
    use time::macros::datetime;
    SourceTimestamp(datetime!(2024-01-01 00:00:00 UTC))
}

fn ts_days_ago(days: i64) -> SourceTimestamp {
    use time::Duration;
    SourceTimestamp(ts_epoch().0 - Duration::days(days))
}

fn make_edge(funder: WalletAddress, funded: WalletAddress) -> FundingEdge {
    FundingEdge {
        funder,
        funded,
        amount_usd: Decimal::from(500u32),
        timestamp: ts_days_ago(1),
    }
}

/// Scenario: 3 wallets funded by the same root → one OperatorIdentity with cluster_size=3.
/// Assertion: all 3 are in one OperatorIdentity.
/// Assertion: operator_id equals blake3(funder_root_address_bytes).
#[test]
fn shared_funder_produces_one_cluster() {
    let root = wallet(0xA0);
    let w1 = wallet(0xA1);
    let w2 = wallet(0xA2);

    let mut snapshot = FundingSnapshot {
        edges: vec![make_edge(root, w1), make_edge(root, w2)],
        wallet_ages: HashMap::new(),
        known_external: HashMap::new(),
        closed_trade_counts: HashMap::new(),
        realized_pnl_usd: HashMap::new(),
        snapshot_at: ts_epoch(),
    };
    snapshot.wallet_ages.insert(root, 365 * 86_400);
    snapshot.wallet_ages.insert(w1, 180 * 86_400);
    snapshot.wallet_ages.insert(w2, 180 * 86_400);

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    // All 3 wallets share the same root → one cluster.
    assert_eq!(result.len(), 1, "expected exactly one OperatorIdentity");
    let id = &result[0];
    assert_eq!(id.cluster_size.0, 3, "cluster_size should be 3");
    assert_eq!(id.funder_root.0, root);
    assert!(id.member_wallets.contains(&root));
    assert!(id.member_wallets.contains(&w1));
    assert!(id.member_wallets.contains(&w2));

    // operator_id is blake3 of the funder root address bytes.
    let expected = pe_core_types::OperatorId(blake3::hash(&root.0));
    assert_eq!(id.operator_id, expected);

    // OperatorId is stable across two identical runs.
    let result2 = build_operator_identities(&snapshot, &config).unwrap();
    assert_eq!(result2[0].operator_id, expected);
}

/// Scenario: young CEX-funded root fans out to 5+ wallets in 72h → LaunderedFunder fires.
#[test]
fn laundered_funder_flag_fires() {
    let root = wallet(0xB0);

    let mut snapshot = FundingSnapshot {
        edges: vec![],
        wallet_ages: HashMap::new(),
        known_external: HashMap::new(),
        closed_trade_counts: HashMap::new(),
        realized_pnl_usd: HashMap::new(),
        snapshot_at: ts_epoch(),
    };

    // Root is a CEX deposit address.
    snapshot
        .known_external
        .insert(root, AddressCategory::CexDeposit);
    // Root age: 4 days (< 7-day threshold).
    snapshot.wallet_ages.insert(root, 4 * 86_400);

    // 5 wallets funded by root within 72h.
    for i in 0u8..5 {
        let funded = wallet(0xB1 + i);
        snapshot.edges.push(FundingEdge {
            funder: root,
            funded,
            amount_usd: Decimal::from(1000u32),
            timestamp: ts_days_ago(1), // 1 day ago = within 72h
        });
        snapshot.wallet_ages.insert(funded, 86_400); // 1 day old
    }

    let config = ClusteringConfig::default();
    let result = build_operator_identities(&snapshot, &config).unwrap();

    assert_eq!(result.len(), 1, "should produce one cluster");
    let id = &result[0];
    assert!(
        id.anti_gaming_flags
            .contains(&AntiGamingFlag::LaunderedFunder),
        "LaunderedFunder flag must be set; actual flags: {:?}",
        id.anti_gaming_flags
    );
}
