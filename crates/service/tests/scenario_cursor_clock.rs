//! Scenario: real-last-trade clock — poll-cursor forward-sweep reconstruction (#357).
//!
//! Proves the #357 design's correctness claim. A poll cursor seeded to a wallet's *real last
//! trade* (a lower bound) — even one 80h in the past, left stale by a downtime gap — drives the
//! poller's forward `/activity` sweep to reconstruct every trade since, advancing the cursor (the
//! inactivity clock) to the real most-recent trade. The cursor is only ever a lower bound; the
//! poller reconstructs truth. Re-delivered already-seen trades are deduped downstream by the
//! orchestrator's persistent `is_seen` — not exercised here, which isolates the poller.
//!
//! Deterministic: fixed timestamps, a [`FixtureFetcher`] (no network) keyed by the exact URL the
//! poller builds from the seeded cursor, single-shot poll (`poll_interval_secs = 0`), and a
//! tempfile-backed [`PaperStateDb`].
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
use std::sync::Arc;

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_paper_state::PaperStateDb;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::trade_poller::{TradePoller, TradePollerConfig};
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const T0: i64 = 1_900_000_000; // fixed "now" reference (seconds; < 9_999_999_999 → never read as ms)
const H80: i64 = 80 * 3600;
const H2: i64 = 2 * 3600;
const BASE_URL: &str = "https://data.example.test";

fn wallet() -> WalletAddress {
    let mut b = [0u8; 20];
    b[0] = 0x11;
    WalletAddress(b)
}

fn watchlist_of(w: WalletAddress) -> Watchlist {
    let entries = vec![WatchlistEntry {
        wallet: w,
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(100),
        lcb_5pct_bps: BasisPoints(100),
        win_rate_bps: BasisPoints(7_000),
        closed_trades_in_window: 0,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
    }];
    Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn temp_db() -> (TempDir, Arc<PaperStateDb>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    (dir, db)
}

/// A two-trade `/activity` payload: A at `a_ts`, then B at `b_ts`. Field shape mirrors
/// `trade_parser::parse_trades` (camelCase keys; `timestamp` in seconds).
fn activity_payload(a_ts: i64, b_ts: i64) -> Vec<u8> {
    format!(
        r#"[{{"transactionHash":"0xAAA","conditionId":"0xcondA","side":"BUY","size":40,"price":0.60,"timestamp":{a_ts}}},{{"transactionHash":"0xBBB","conditionId":"0xcondB","side":"BUY","size":10,"price":0.45,"timestamp":{b_ts}}}]"#
    )
    .into_bytes()
}

#[tokio::test]
async fn downtime_forward_sweep_reconstructs_real_last_trade() {
    let (_dir, db) = temp_db();
    let w = wallet();

    // Wallet traded A at T0−80h, then B at T0−2h; the service was DOWN when B happened, so the
    // persisted shutdown cursor is the stale T0−80h (B never observed). #357 keeps that real
    // lower-bound seed rather than resetting it to `now`.
    let a_ts = T0 - H80;
    let b_ts = T0 - H2;
    db.set_cursor(&w, a_ts).unwrap();

    // The poller fetches `/activity` from `start = cursor − 1` (the endpoint's `start` is
    // exclusive; trade_poller's `cursor_start` steps back one second). Key the fixture by that
    // exact URL — built via the same public endpoint helper — so the test also asserts the fetch
    // window opens at the seeded lower bound, not at `now`.
    let url = PolymarketEndpoint::UserTradeActivity {
        user: format!("{w}"),
        end: None,
        start: Some(a_ts - 1),
    }
    .url(BASE_URL);
    let mut responses = HashMap::new();
    responses.insert(url, activity_payload(a_ts, b_ts));
    let fetcher = FixtureFetcher::new(responses);

    let (tx, mut rx) = mpsc::channel(16);
    let poller = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_string(),
            poll_interval_secs: 0, // single-shot: one round, then return
        },
        LiveWatchlist::new(watchlist_of(w)),
        fetcher,
        tx,
        db.clone(),
        new_shared_health(false),
    );

    poller.run().await;

    // The forward sweep advanced the cursor to the real most-recent trade B (T0−2h) across the
    // downtime gap — NOT left at the stale T0−80h seed, and NOT reset to `now`.
    assert_eq!(
        db.cursor(&w).unwrap(),
        Some(b_ts),
        "cursor advanced to real last trade B via the forward sweep from the lower-bound seed"
    );

    // Both trades were re-delivered; the stale A is deduped downstream by the orchestrator's
    // persistent `is_seen` (out of scope here).
    let mut got: Vec<String> = Vec::new();
    while let Ok(t) = rx.try_recv() {
        got.push(t.source_trade_id.0);
    }
    got.sort();
    assert_eq!(
        got,
        vec!["0xAAA".to_string(), "0xBBB".to_string()],
        "PASS: the forward sweep re-delivered both A and B (dedup is downstream)"
    );
    println!("PASS: downtime-forward-sweep-reconstructs-real-last-trade");
}
