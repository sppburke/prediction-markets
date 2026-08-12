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
fn second_round_responses(
    w: pe_core_types::WalletAddress,
    a_ts: i64,
    b_ts: i64,
) -> HashMap<String, Vec<u8>> {
    // Round 2 fetches from the HELD cursor (a_ts − 1) — same window, same payload.
    let url = PolymarketEndpoint::UserTradeActivity {
        user: format!("{w}"),
        end: None,
        start: Some(a_ts - 1),
    }
    .url(BASE_URL);
    let mut responses = HashMap::new();
    responses.insert(url, activity_payload(a_ts, b_ts));
    responses
}

fn leader_row(w: pe_core_types::WalletAddress) -> pe_paper_state::LeaderPositionRow {
    pe_paper_state::LeaderPositionRow {
        wallet: w,
        market_id: pe_core_types::MarketId(pe_core_types::VenueMarketId("0xmkt".into())),
        outcome_id: pe_core_types::OutcomeId(0),
        long_contracts: 0,
        short_contracts: 0,
    }
}

fn activity_payload(a_ts: i64, b_ts: i64) -> Vec<u8> {
    format!(
        r#"[{{"transactionHash":"0xAAA","conditionId":"0xcondA","outcomeIndex":0,"side":"BUY","size":40,"price":0.60,"timestamp":{a_ts}}},{{"transactionHash":"0xBBB","conditionId":"0xcondB","outcomeIndex":1,"side":"BUY","size":10,"price":0.45,"timestamp":{b_ts}}}]"#
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

    // #511 held cursor: the sweep DELIVERED A and B but nothing has marked them seen (this
    // scenario isolates the poller), so the delivery cursor HOLDS at the seed — it is a
    // correctness-preserving lower bound now, not a delivery high-water mark. The #357
    // inactivity-clock claim moved to the ACTIVITY clock, which does advance to B.
    assert_eq!(
        db.cursor(&w).unwrap(),
        Some(a_ts),
        "delivery cursor holds below the unseen trades (#511)"
    );
    assert_eq!(
        db.activity(&w).unwrap(),
        Some(b_ts),
        "activity clock advanced to real last trade B across the downtime gap (#357/#511)"
    );

    // Once the trades are durably seen (the orchestrator's job), the next sweep advances
    // the delivery cursor to B — the #357 forward-sweep reconstruction completes.
    for trade_id in ["0xAAA", "0xBBB"] {
        db.commit_seen_no_fill_with_flip(
            &pe_core_types::SourceTradeId(trade_id.to_string()),
            &leader_row(w),
            None,
        )
        .unwrap();
    }
    let (tx2, rx2) = mpsc::channel(16);
    let poller2 = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_string(),
            poll_interval_secs: 0,
        },
        LiveWatchlist::new(watchlist_of(w)),
        FixtureFetcher::new(second_round_responses(w, a_ts, b_ts)),
        tx2,
        db.clone(),
        new_shared_health(false),
    );
    poller2.run().await;
    drop(rx2);
    assert_eq!(
        db.cursor(&w).unwrap(),
        Some(b_ts),
        "cursor advances to B once every trade in the window is seen"
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

/// #511: an unseen trade hidden BEHIND a full first page (500 seen rows) must still hold
/// the cursor — the poller rescans the window via the strict descending page endpoint
/// under the data-derived end bound and finds it on page 2.
///
/// PASS: cursor holds at `unseen_ts − 1`; the unseen trade is delivered.
/// FAIL: a full first page of seen rows advances the cursor over the hidden trade.
#[tokio::test]
async fn full_page_rescan_finds_unseen_trade_behind_500_seen_rows() {
    let db =
        Arc::new(PaperStateDb::open(&tempfile::tempdir().unwrap().path().join("p.db")).unwrap());
    let w = WalletAddress::from_hex("0xcccccccccccccccccccccccccccccccccccccccc").unwrap();
    let base_ts: i64 = 1_900_000_000;
    let unseen_ts = base_ts - 10; // older than every seen row → page 2 in DESC order
    db.seed_cursor_if_absent(&w, unseen_ts).unwrap();

    // 500 seen rows (newest-first ts base..base-499… all marked seen in the DB).
    let row = |id: &str, ts: i64| {
        format!(
            r#"{{"transactionHash":"{id}","conditionId":"0xc","outcomeIndex":0,"side":"BUY","size":5,"price":0.5,"timestamp":{ts}}}"#
        )
    };
    let mut seen_rows = Vec::new();
    for i in 0..500i64 {
        let id = format!("0xseen{i}");
        db.commit_seen_no_fill_with_flip(
            &pe_core_types::SourceTradeId(id.clone()),
            &leader_row(w),
            None,
        )
        .unwrap();
        seen_rows.push(row(&id, base_ts - i));
    }
    let full_page = format!("[{}]", seen_rows.join(","));
    let page2 = format!("[{}]", row("0xhidden", unseen_ts));

    let start = unseen_ts - 1; // cursor_start(cursor)
    let mut responses = HashMap::new();
    responses.insert(
        PolymarketEndpoint::UserTradeActivity {
            user: format!("{w}"),
            end: None,
            start: Some(start),
        }
        .url(BASE_URL),
        full_page.clone().into_bytes(),
    );
    // Paged rescan: fixed end = max ts of the initial page; DESC offsets 0 and 500.
    responses.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: format!("{w}"),
            end: base_ts,
            start: Some(start),
            offset: 0,
        }
        .url(BASE_URL),
        full_page.into_bytes(),
    );
    responses.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: format!("{w}"),
            end: base_ts,
            start: Some(start),
            offset: 500,
        }
        .url(BASE_URL),
        page2.into_bytes(),
    );

    let (tx, mut rx) = mpsc::channel(600);
    let poller = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_string(),
            poll_interval_secs: 0,
        },
        LiveWatchlist::new(watchlist_of(w)),
        FixtureFetcher::new(responses),
        tx,
        db.clone(),
        new_shared_health(false),
    );
    poller.run().await;

    // The hidden unseen trade was delivered, and the cursor held below it (MAX-upsert
    // keeps the seeded unseen_ts against the candidate unseen_ts − 1).
    let delivered = rx.recv().await.expect("hidden trade delivered");
    assert_eq!(delivered.source_trade_id.0, "0xhidden");
    assert_eq!(
        db.cursor(&w).unwrap(),
        Some(unseen_ts),
        "cursor held at the unseen trade despite 500 seen rows in front (#511)"
    );
    assert_eq!(
        db.activity(&w).unwrap(),
        Some(base_ts),
        "activity advanced to newest"
    );
    println!("PASS: full-page rescan found the hidden unseen trade and held the cursor");
}
