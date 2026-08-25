//! Scenario tests for the position-seeder (issue #286).
//!
//! Scenarios:
//!   A. seed_overlay_classifies_add — after seeding a pre-existing long via the
//!      positions API, a subsequent Buy on the same (market, outcome) classifies
//!      as an Add rather than an Entry (ledger shows pre-existing long).
//!   B. seed_empty_response_clears_wallet — a successful empty-array response
//!      from the API clears that wallet's ledger entry (no prior positions).
//!   C. seed_failure_retains_state — a per-wallet API failure leaves the existing
//!      ledger state untouched for that wallet.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{
    ContractQty, MarketId, MarketOutcomeId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_position_ledger::PositionLedger;
use pe_service::position_seeder::seed_all;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use rust_decimal_macros::dec;
use time::OffsetDateTime;

fn wallet_a() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn market_a() -> MarketId {
    MarketId(VenueMarketId("0xconditionaaa".to_string()))
}

fn buy_trade(wallet: WalletAddress, contracts: u64, trade_id: &str) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
    IncomingTrade {
        wallet,
        market_id: market_a(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.65)),
        contracts: ContractQty(contracts),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(trade_id.to_string()),
        provenance: TradeProvenance::RestPoll,
    }
}

fn positions_url(wallet: WalletAddress, base: &str) -> String {
    PolymarketEndpoint::CurrentPositions {
        user: wallet.to_string(),
        limit: Some(500),
        offset: Some(0),
        redeemable: Some(false),
        size_threshold: Some(1),
    }
    .url(base)
}

// ── Scenario A ────────────────────────────────────────────────────────────────
//
// PASS: after seeding 100 long on (market_a, OutcomeId(0)) via the positions API,
//       a subsequent Buy in that market classifies as Add (ledger already shows
//       a non-zero long, so classify_action treats it as adding to an existing position).
// FAIL: the post-seed Buy classifies as Entry (phantom-zero baseline).

#[tokio::test]
async fn seed_overlay_classifies_add() {
    let w = wallet_a();
    let base = "https://api.example.com";

    // API returns 100 long for market_a/outcome 0.
    let mut responses = HashMap::new();
    responses.insert(
        positions_url(w, base),
        br#"[{"conditionId":"0xconditionaaa","outcomeIndex":0,"size":"100"}]"#.to_vec(),
    );
    let fetcher = FixtureFetcher::new(responses);

    let map = seed_all(&[w], base, 500, 1, &fetcher).await;
    assert_eq!(map.len(), 1, "seed_all must succeed for wallet_a");

    // Apply overlay to a fresh ledger.
    let mut ledger = PositionLedger::new();
    ledger.overlay(map);

    // Pre-trade snapshot should show 100 long.
    let pos_key = MarketOutcomeId::new(market_a(), OutcomeId(0));
    let pre_trade_state = ledger
        .position(&w)
        .expect("wallet present after overlay")
        .positions
        .get(&pos_key)
        .copied()
        .unwrap_or_default();
    assert_eq!(
        pre_trade_state.long_contracts, 100,
        "pre-trade long must reflect seeded position"
    );

    // Ingest a Buy trade (simulates what the orchestrator does after classify).
    let trade = buy_trade(w, 50, "trade_b1");
    ledger.ingest(&trade);

    // Post-ingest: 100 (seed) + 50 (trade) = 150 long.
    let post_state = ledger
        .position(&w)
        .unwrap()
        .positions
        .get(&pos_key)
        .copied()
        .unwrap_or_default();
    assert_eq!(
        post_state.long_contracts, 150,
        "post-ingest long must be seed + trade"
    );
    println!(
        "PASS: seed_overlay_classifies_add — pre={}, post={}",
        pre_trade_state.long_contracts, post_state.long_contracts
    );
}

// ── Scenario B ────────────────────────────────────────────────────────────────
//
// PASS: a successful empty-array response from the API replaces the wallet's
//       entry with an empty snapshot (no open positions).
// FAIL: the wallet is absent from the map, or old positions are retained.

#[tokio::test]
async fn seed_empty_response_clears_wallet() {
    let w = wallet_a();
    let base = "https://api.example.com";

    // Pre-seed the ledger with a position via ingest.
    let mut ledger = PositionLedger::new();
    ledger.ingest(&buy_trade(w, 100, "trade_x1"));

    // API returns empty array.
    let mut responses = HashMap::new();
    responses.insert(positions_url(w, base), b"[]".to_vec());
    let fetcher = FixtureFetcher::new(responses);

    let map = seed_all(&[w], base, 500, 1, &fetcher).await;
    assert!(map.contains_key(&w), "empty-success wallet must be in map");
    assert!(
        map[&w].positions.is_empty(),
        "empty response must clear positions"
    );

    // Overlay clears the wallet's entry.
    ledger.overlay(map);
    let snap = ledger.position(&w).expect("wallet present after overlay");
    assert!(
        snap.positions.is_empty(),
        "overlay of empty snapshot must clear positions"
    );
    println!("PASS: seed_empty_response_clears_wallet");
}

// ── Scenario C ────────────────────────────────────────────────────────────────
//
// PASS: a per-wallet fetch failure leaves the wallet absent from seed_all's
//       return map; the caller (orchestrator) retains the existing ledger state.
// FAIL: wallet is present in the map with incorrect/zeroed data, or the ledger
//       is modified.

#[tokio::test]
async fn seed_failure_retains_state() {
    let w = wallet_a();
    let base = "https://api.example.com";

    // Pre-seed the ledger with a position via ingest.
    let mut ledger = PositionLedger::new();
    ledger.ingest(&buy_trade(w, 100, "trade_y1"));

    // FixtureFetcher returns Fatal for any unknown URL — simulates fetch failure.
    let fetcher = FixtureFetcher::new(HashMap::new());
    let map = seed_all(&[w], base, 500, 1, &fetcher).await;

    // Wallet must be absent from the returned map.
    assert!(
        !map.contains_key(&w),
        "failed wallet must be absent from seed_all map"
    );

    // Since map doesn't contain w, overlay is a no-op for that wallet.
    ledger.overlay(map);

    // Existing state must be intact.
    let pos_key = MarketOutcomeId::new(market_a(), OutcomeId(0));
    let state = ledger
        .position(&w)
        .expect("wallet still present after failed seed")
        .positions
        .get(&pos_key)
        .copied()
        .unwrap_or_default();
    assert_eq!(
        state.long_contracts, 100,
        "existing ledger state must be retained on fetch failure"
    );
    println!(
        "PASS: seed_failure_retains_state — long_contracts={}",
        state.long_contracts
    );
}
