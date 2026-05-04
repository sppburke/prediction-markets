//! Scenario: [`FundingGraphAccumulator`] end-to-end event replay.
//!
//! Gated behind the `scenario` cargo feature — runs automatically when
//! `cargo nextest run --all-features` is used; excluded from quick local
//! iteration that omits `--all-features`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::WalletAddress;
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::AddressCategory;
use pe_source_onchain_polygon::{ExternalAddressKind, PolygonEvent, event::TxHash};
use rust_decimal_macros::dec;
use time::format_description::well_known::Rfc3339;

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).expect("test address")
}

fn ts(s: &str) -> pe_core_types::SourceTimestamp {
    pe_core_types::SourceTimestamp(
        time::OffsetDateTime::parse(s, &Rfc3339).expect("test timestamp"),
    )
}

fn tx() -> TxHash {
    TxHash([0u8; 32])
}

/// Scenario: a multi-event replay sequence produces the correct snapshot.
///
/// PASS: snapshot contains all expected edges, wallet ages, known_external
///       classification, and empty pnl/trade-count maps.
/// FAIL: any expected field is missing or incorrect.
#[test]
fn scenario_multi_event_replay() {
    let mut acc = FundingGraphAccumulator::new();

    // 1. Bridge receipt — marks a known CEX intermediary.
    acc.ingest(PolygonEvent::BridgeOnrampReceipt {
        to: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        bridge: addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        amount_usd: dec!(500.00),
        source_kind: ExternalAddressKind::Bridge,
        block_number: 100,
        tx_hash: tx(),
        timestamp: ts("2024-01-01T00:00:00Z"),
    });

    // 2. USDC transfer from bridge to a proxy wallet.
    acc.ingest(PolygonEvent::UsdcTransfer {
        from: addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        to: addr("0xcccccccccccccccccccccccccccccccccccccccc"),
        to_collateral_contract: false,
        from_collateral_contract: false,
        amount_usd: dec!(500.00),
        block_number: 110,
        tx_hash: tx(),
        timestamp: ts("2024-01-01T01:00:00Z"),
    });

    // 3. Proxy wallet deployment.
    acc.ingest(PolygonEvent::ProxyWalletDeployed {
        proxy: addr("0xcccccccccccccccccccccccccccccccccccccccc"),
        singleton: addr("0x0000000000000000000000000000000000000000"),
        block_number: 120,
        tx_hash: tx(),
        timestamp: ts("2024-01-01T02:00:00Z"),
    });

    // 4. pUSD mint — advances watermark only; no edge in Phase 0B.
    acc.ingest(PolygonEvent::PUsdMint {
        to: addr("0xcccccccccccccccccccccccccccccccccccccccc"),
        amount_usd: dec!(500.00),
        block_number: 130,
        tx_hash: tx(),
        // Snapshot_at will be 2024-01-02 → wallet age = 86_400 - 7_200 = 79_200 s.
        timestamp: ts("2024-01-02T00:00:00Z"),
    });

    let snap = acc.snapshot();

    // Exactly one USDC funding edge.
    assert_eq!(snap.edges.len(), 1);
    assert_eq!(
        snap.edges[0].funder,
        addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        "funder should be the bridge address"
    );
    assert_eq!(
        snap.edges[0].funded,
        addr("0xcccccccccccccccccccccccccccccccccccccccc"),
        "funded should be the proxy wallet"
    );
    assert_eq!(snap.edges[0].amount_usd, dec!(500.00));

    // Bridge classified correctly.
    assert_eq!(
        snap.known_external
            .get(&addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
        Some(&AddressCategory::Bridge),
        "bridge address should be classified as Bridge"
    );

    // Wallet age for the proxy wallet: deployed at 02:00, snapshot at 2024-01-02T00:00 = 22 h.
    let age = snap
        .wallet_ages
        .get(&addr("0xcccccccccccccccccccccccccccccccccccccccc"))
        .copied()
        .expect("proxy wallet should have age entry");
    assert_eq!(age, 79_200, "age should be 22 hours = 79,200 seconds");

    // Phase 0B: PnL and trade-count maps are empty.
    assert!(snap.closed_trade_counts.is_empty());
    assert!(snap.realized_pnl_usd.is_empty());

    // Highest block tracked.
    assert_eq!(acc.highest_block(), 130);
}

/// Scenario: deposit-address funding adds both an edge and a CexDeposit classification.
///
/// PASS: one edge from funder to deposit_address; deposit_address classified as CexDeposit.
/// FAIL: edge missing, wrong direction, or wrong classification.
#[test]
fn scenario_deposit_address_funding() {
    let mut acc = FundingGraphAccumulator::new();

    acc.ingest(PolygonEvent::DepositAddressFunding {
        from: addr("0xdddddddddddddddddddddddddddddddddddddddd"),
        deposit_address: addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        amount_usd: dec!(1000.00),
        block_number: 200,
        tx_hash: tx(),
        timestamp: ts("2024-03-01T12:00:00Z"),
    });

    let snap = acc.snapshot();
    assert_eq!(snap.edges.len(), 1);
    assert_eq!(
        snap.edges[0].funder,
        addr("0xdddddddddddddddddddddddddddddddddddddddd")
    );
    assert_eq!(
        snap.edges[0].funded,
        addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    );
    assert_eq!(
        snap.known_external
            .get(&addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")),
        Some(&AddressCategory::CexDeposit),
        "deposit address should be classified as CexDeposit"
    );
}

/// Scenario: pUSD burn event advances the watermark without creating edges.
///
/// PASS: no edges, highest_block updated, snapshot_at advances.
/// FAIL: any edge created, or block not tracked.
#[test]
fn scenario_pnl_fields_empty_phase_0b() {
    let mut acc = FundingGraphAccumulator::new();

    acc.ingest(PolygonEvent::PUsdBurn {
        from: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        amount_usd: dec!(250.00),
        block_number: 500,
        tx_hash: tx(),
        timestamp: ts("2024-06-01T00:00:00Z"),
    });

    let snap = acc.snapshot();
    assert!(snap.edges.is_empty(), "PUsdBurn should not create edges");
    assert!(snap.realized_pnl_usd.is_empty());
    assert!(snap.closed_trade_counts.is_empty());
    assert_eq!(acc.highest_block(), 500);
}

/// Scenario: repeated ProxyWalletDeployed for the same address retains first-seen timestamp.
///
/// PASS: wallet_age is computed from first event's timestamp, not the later one.
/// FAIL: wallet_age is computed from the later timestamp.
#[test]
fn scenario_proxy_first_seen_immutable() {
    let mut acc = FundingGraphAccumulator::new();

    acc.ingest(PolygonEvent::ProxyWalletDeployed {
        proxy: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        singleton: addr("0x0000000000000000000000000000000000000000"),
        block_number: 10,
        tx_hash: tx(),
        timestamp: ts("2024-01-01T00:00:00Z"),
    });
    // Second deployment event for same proxy — should be ignored.
    acc.ingest(PolygonEvent::ProxyWalletDeployed {
        proxy: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        singleton: addr("0x0000000000000000000000000000000000000000"),
        block_number: 20,
        tx_hash: tx(),
        timestamp: ts("2024-01-01T12:00:00Z"),
    });
    // Advance watermark to 2 days after first deployment.
    acc.ingest(PolygonEvent::PUsdMint {
        to: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        amount_usd: dec!(1.00),
        block_number: 30,
        tx_hash: tx(),
        timestamp: ts("2024-01-03T00:00:00Z"),
    });

    let snap = acc.snapshot();
    let age = snap
        .wallet_ages
        .get(&addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .copied()
        .expect("wallet should have age");
    // Age should be 2 days = 172_800 s (from first seen, not second).
    assert_eq!(age, 172_800);
}
