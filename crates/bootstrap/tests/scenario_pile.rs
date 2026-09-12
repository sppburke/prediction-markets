//! Scenario tests for the wallet pile (issue #166).
//!
//! Operator-level checks of the migrate → backfill → activation flow against a
//! synthetic SQLite + fixture-backed Polymarket fetcher. No network calls;
//! deterministic.
//!
//! Each scenario has a single PASS/FAIL criterion stated before the test body.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::migrate::run_migrate;
use pe_bootstrap::pile::{self, PILE_ACTIVATION_MIN_TRADES, SRC_LEADERBOARD, SRC_WALLET_SET_JSON};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use tempfile::TempDir;

const BASE_URL: &str = "https://data-api.polymarket.com";

fn wallet_hex(byte: u8) -> String {
    format!("0x{:040x}", byte)
}

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress::from_hex(&wallet_hex(byte)).unwrap()
}

fn trade_url_cold(w: WalletAddress) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: w.to_string(),
        end: 2_000_000_000,
        start: Some(1),
        offset: 0,
    }
    .url(BASE_URL)
}

fn page_json(trades: &[(&str, &str, i64)]) -> Vec<u8> {
    // (trade_id, market_id, timestamp_unix). Side = buy, contracts = 1, outcome 0.
    let mut s = String::from("[");
    for (i, (tid, mid, ts)) in trades.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"{{"transactionHash":"{tid}","conditionId":"{mid}","outcomeIndex":0,"side":"BUY","price":"0.5","size":"1","timestamp":{ts},"asset":"0","title":"t","slug":"s","icon":"","eventSlug":"e","outcome":"YES","name":"n","pseudonym":"p","bio":"","profileImage":"","profileImageOptimized":""}}"#
        ));
    }
    s.push(']');
    s.into_bytes()
}

// ── Scenario 1: leaderboard membership bypasses the trade-count gate ─────────
//
// PASS: a wallet upserted with SRC_LEADERBOARD and 0 DB trades is activated by
//       apply_activation_rules — leaderboard membership is itself an activation
//       source (no trade-count gate).
// FAIL: wallet stays inactive.

#[tokio::test]
async fn scenario_leaderboard_membership_activates_with_zero_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    cache
        .upsert_wallet(&wallet_hex(0xaa), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    let activated = pile::apply_activation_rules(&mut cache).unwrap();
    assert_eq!(activated, 1);
    assert_eq!(cache.active_wallet_count().unwrap(), 1);
}

// ── Scenario 2: is_infra blocks activation across all paths ──────────────────
//
// PASS: a wallet with is_infra=1 stays inactive even with SRC_LEADERBOARD set —
//       is_infra takes precedence over every activation branch.
// FAIL: wallet is_active=1, which would copy from an infrastructure address.

#[tokio::test]
async fn scenario_is_infra_takes_precedence_over_leaderboard() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let target = wallet_hex(0xbb);

    cache
        .upsert_wallet(&target, SRC_LEADERBOARD, true, None, None, None)
        .unwrap();
    let activated = pile::apply_activation_rules(&mut cache).unwrap();
    assert_eq!(activated, 0, "is_infra must block leaderboard activation");
    assert!(cache.conn_for_test_is_infra(&target));
}

// ── Scenario 3: dune_closed_markets proxy activates a non-leaderboard wallet ─
//
// PASS: a wallet (SRC_WALLET_SET_JSON — not a leaderboard bypass) with
//       dune_closed_markets >= PILE_ACTIVATION_MIN_TRADES is activated by
//       apply_activation_rules with no DB trades.
// FAIL: wallet stays inactive (chicken-and-egg from an earlier review).

#[tokio::test]
async fn scenario_closed_markets_proxy_activates_wallet() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    cache
        .upsert_wallet(
            &wallet_hex(0xcc),
            SRC_WALLET_SET_JSON,
            false,
            Some(1_700_000_000),
            Some(150), // dune_closed_markets >= PILE_ACTIVATION_MIN_TRADES
            None,
        )
        .unwrap();
    let activated = pile::apply_activation_rules(&mut cache).unwrap();
    assert_eq!(activated, 1);
}

// ── Scenario 4: backfill drains the queue and refreshes trade_count ─────────
//
// PASS: After PolymarketBulkFetcher returns N>=PILE_ACTIVATION_MIN_TRADES new
//       trades for a wallet known only from the set (source_bits =
//       SRC_WALLET_SET_JSON, no closed-markets proxy), running
//       refresh_trade_counts + apply_activation_rules promotes it to is_active=1.
// FAIL: trade_count stays 0 after backfill, or activation never fires.

#[tokio::test]
async fn scenario_backfill_refreshes_trade_count_and_activates() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let w = wallet(0xdd);

    // Pre-stage: known wallet WITHOUT any activation source set yet.
    cache
        .upsert_wallet(&w.to_string(), SRC_WALLET_SET_JSON, false, None, None, None)
        .unwrap();
    assert_eq!(
        pile::apply_activation_rules(&mut cache).unwrap(),
        0,
        "no activation source set yet"
    );

    // Build a fixture response with PILE_ACTIVATION_MIN_TRADES trades.
    let mut trades_owned: Vec<(String, String, i64)> = Vec::new();
    let n_trades = usize::try_from(PILE_ACTIVATION_MIN_TRADES).unwrap();
    for i in 0..n_trades {
        let tid = format!("0x{:064x}", i);
        let mid = format!("0xm{:063x}", i);
        let ts = 1_700_000_000 - i64::try_from(i).unwrap();
        trades_owned.push((tid, mid, ts));
    }
    let trades_refs: Vec<(&str, &str, i64)> = trades_owned
        .iter()
        .map(|(t, m, ts)| (t.as_str(), m.as_str(), *ts))
        .collect();
    let page = page_json(&trades_refs);
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(w), page);

    let fetcher = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_120);
    fetcher.fetch_all(&[w], &mut cache).await.unwrap();

    let count_in_db = cache.trade_count();
    assert!(
        count_in_db >= n_trades,
        "expected ≥ {n_trades} trades, got {count_in_db}"
    );

    cache.refresh_trade_counts().unwrap();
    let activated = pile::apply_activation_rules(&mut cache).unwrap();
    assert_eq!(
        activated, 1,
        "backfill must promote the wallet via trade_count"
    );
}

// ── Scenario 5: select_backfill_due ordering by quality ─────────────────────
//
// PASS: Two wallets both NULL last_polymarket_fetch_at; ordered first by
//       dune_win_rate_bps DESC NULLS LAST, then dune_closed_markets DESC NULLS
//       LAST. Higher quality surfaces first.
// FAIL: ordering follows insertion order or any non-quality criterion.

#[tokio::test]
async fn scenario_backfill_due_ordering_prioritises_quality_wallets() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    // Three wallets, all activated via leaderboard, all NULL fetch timestamps.
    cache
        .upsert_wallet(
            &wallet_hex(0x10),
            SRC_LEADERBOARD,
            false,
            None,
            Some(50),
            Some(8500),
        )
        .unwrap();
    cache
        .upsert_wallet(
            &wallet_hex(0x20),
            SRC_LEADERBOARD,
            false,
            None,
            Some(200),
            Some(9900),
        )
        .unwrap();
    cache
        .upsert_wallet(&wallet_hex(0x30), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    let due = pile::select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
    assert_eq!(due.len(), 3);
    assert_eq!(due[0], wallet_hex(0x20), "highest win_rate first");
    assert_eq!(due[1], wallet_hex(0x10));
    assert_eq!(due[2], wallet_hex(0x30), "NULL quality signals last");
}

// ── Scenario 6: source bits accumulate (bit-OR) across upserts ──────────────
//
// PASS: a wallet present in wallet_set.json (migrate) then re-upserted as a
//       leaderboard member ends with source_bits = wallet_set_json | leaderboard
//       (bit-OR). No row duplication.
// FAIL: the second source's bit is dropped, or the row is duplicated.

#[tokio::test]
async fn scenario_source_bits_accumulate_across_upserts() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let target = wallet_hex(0x55);

    // Run 1: wallet_set.json via migrate.
    let wallet_set_path = dir.path().join("wallet_set.json");
    std::fs::write(&wallet_set_path, format!(r#"["{}"]"#, target)).unwrap();
    let _r1 = run_migrate(&mut cache, &wallet_set_path).unwrap();
    assert_eq!(
        cache.conn_for_test_source_bits(&target),
        pile::SRC_WALLET_SET_JSON
    );

    // Run 2: same wallet re-upserted as a leaderboard member.
    cache
        .upsert_wallet(&target, SRC_LEADERBOARD, false, None, None, None)
        .unwrap();

    let bits = cache.conn_for_test_source_bits(&target);
    assert_eq!(
        bits,
        pile::SRC_WALLET_SET_JSON | pile::SRC_LEADERBOARD,
        "expected both bits set, got 0b{:07b}",
        bits
    );
    assert_eq!(cache.wallet_pile_size().unwrap(), 1, "no duplicate rows");
}

// ── Scenario 7: migrate ordering — timestamps seed BEFORE activation ────────
//
// PASS: A wallet with existing trades has its last_polymarket_fetch_at seeded
//       to MAX(trades.timestamp_unix) and is activated. select_backfill_due
//       does NOT return it within the staleness window after migrate.
// FAIL: select_backfill_due returns the wallet with NULL timestamp, which
//       would trigger a redundant Polymarket re-fetch on the next backfill.

#[tokio::test]
async fn scenario_migrate_seeds_timestamps_before_activating() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let w = wallet(0x77);
    let n_trades = usize::try_from(PILE_ACTIVATION_MIN_TRADES).unwrap();

    // Seed trades directly via the public fetch fixture path.
    let mut trades_owned: Vec<(String, String, i64)> = Vec::new();
    for i in 0..n_trades {
        let tid = format!("0x{:064x}", 0xa0 + i);
        let mid = format!("0xm{:063x}", 0xb0 + i);
        let ts = 1_690_000_000_i64 + i64::try_from(i).unwrap();
        trades_owned.push((tid, mid, ts));
    }
    let trades_refs: Vec<(&str, &str, i64)> = trades_owned
        .iter()
        .map(|(t, m, ts)| (t.as_str(), m.as_str(), *ts))
        .collect();
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(w), page_json(&trades_refs));
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_120)
        .fetch_all(&[w], &mut cache)
        .await
        .unwrap();

    // Run migrate (no wallet_set entries; trades-derived wallet only).
    let wallet_set_path = dir.path().join("wallet_set.json");
    std::fs::write(&wallet_set_path, "[]").unwrap();
    let report = run_migrate(&mut cache, &wallet_set_path).unwrap();

    assert_eq!(report.trades_rows, 1, "trade-derived wallet inserted");
    assert_eq!(report.last_polymarket_fetch_seeded, 1);
    assert_eq!(report.activated, 1);

    // select_backfill_due should NOT return this wallet — fresh fetch stamp.
    let newest_ts = 1_690_000_000_i64 + i64::try_from(n_trades - 1).unwrap();
    let now = newest_ts + 100;
    let due = pile::select_backfill_due(&cache, now, 0).unwrap();
    assert!(due.is_empty(), "migrate-seeded wallet must not be due");

    // After 1 day + 1s of simulated time-pass, it IS due again.
    let later = now + 86_401;
    let due_later = pile::select_backfill_due(&cache, later, 0).unwrap();
    assert_eq!(due_later, vec![w.to_string()]);
}
