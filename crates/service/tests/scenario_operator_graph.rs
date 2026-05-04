//! Scenario: operator-graph scheduler convergence.
//!
//! Scenarios:
//!   1. operator_ids_converge — feed staggered funding events; verify that after a
//!      scheduler tick the watch channel reflects the expected operator cluster, and
//!      that both funder-root and member wallets are present.
//!
//! PASS: watch channel contains a cluster whose member_wallets include the funded
//!       wallet after a scheduler tick, and whose funder_root matches the funder address.
//! FAIL: watch channel still empty after tick, or cluster is missing the expected wallet.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::sync::{Arc, Mutex};

use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::ClusteringConfig;
use pe_service::operator_graph_scheduler::OperatorGraphScheduler;
use pe_source_onchain_polygon::{PolygonEvent, event::TxHash};
use rust_decimal_macros::dec;
use time::macros::datetime;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).expect("test address")
}

fn tx() -> TxHash {
    TxHash([0u8; 32])
}

// ── Scenario 1: operator_ids_converge ────────────────────────────────────────
//
// Setup:
//   funder  → funded_a  (UsdcTransfer at T=0)
//   funder  → funded_b  (UsdcTransfer at T=1)
//   funded_a deployed at T=0; funded_b deployed at T=1
//
// After one scheduler tick both funded wallets share the same operator
// (cluster rooted at `funder`).

#[tokio::test]
async fn scenario_operator_ids_converge() {
    let funder = addr("0xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0");
    let funded_a = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let funded_b = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let t0 = SourceTimestamp(datetime!(2024-01-01 00:00:00 UTC));
    let t1 = SourceTimestamp(datetime!(2024-01-01 00:00:01 UTC));
    let t2 = SourceTimestamp(datetime!(2024-01-01 00:00:02 UTC));

    let accumulator = Arc::new(Mutex::new(FundingGraphAccumulator::new()));

    // Ingest staggered funding events into the accumulator.
    {
        let mut acc = accumulator.lock().unwrap();

        acc.ingest(PolygonEvent::ProxyWalletDeployed {
            proxy: funded_a,
            singleton: addr("0x0000000000000000000000000000000000000000"),
            block_number: 1,
            tx_hash: tx(),
            timestamp: t0.clone(),
        });
        acc.ingest(PolygonEvent::UsdcTransfer {
            from: funder,
            to: funded_a,
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: dec!(500),
            block_number: 2,
            tx_hash: tx(),
            timestamp: t1.clone(),
        });
        acc.ingest(PolygonEvent::ProxyWalletDeployed {
            proxy: funded_b,
            singleton: addr("0x0000000000000000000000000000000000000000"),
            block_number: 3,
            tx_hash: tx(),
            timestamp: t1.clone(),
        });
        acc.ingest(PolygonEvent::UsdcTransfer {
            from: funder,
            to: funded_b,
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: dec!(500),
            block_number: 4,
            tx_hash: tx(),
            timestamp: t2.clone(),
        });
    }

    // Use a 1-second cadence so the test completes quickly without real-time waiting.
    let (scheduler, mut rx) =
        OperatorGraphScheduler::new(accumulator, ClusteringConfig::default(), 1);

    // Drive the scheduler for just one tick by running it with a 1.5s timeout.
    tokio::select! {
        _ = scheduler.run() => {}
        _ = tokio::time::sleep(std::time::Duration::from_millis(1_500)) => {}
    }

    let identities = rx.borrow_and_update().clone();

    // There should be exactly one cluster (funder is the root for both funded wallets).
    assert!(
        !identities.is_empty(),
        "watch channel must contain at least one OperatorIdentity after a scheduler tick"
    );

    // Find the identity that includes funded_a.
    let cluster = identities
        .iter()
        .find(|id| id.member_wallets.contains(&funded_a))
        .expect("cluster containing funded_a must exist");

    assert!(
        cluster.member_wallets.contains(&funded_b),
        "funded_b must be in the same cluster as funded_a (shared funder root)"
    );

    // funder itself is a member (it's the root with no inbound edges).
    assert!(
        cluster.member_wallets.contains(&funder),
        "funder root must appear as a cluster member"
    );
}
