//! Operator-only retroactive infra classification and selector contracts.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::pile::{self, SRC_LEADERBOARD, SRC_WALLET_SET_JSON};
use pe_core_types::WalletAddress;
use tempfile::TempDir;
fn wallet(byte: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{byte:040x}")).unwrap()
}
#[tokio::test]
async fn retroactive_flags_dense_wallet() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&path).unwrap();

    let dense_w = wallet(0xF1);
    let sparse_w = wallet(0xF2);
    cache
        .upsert_wallets_bulk(&[
            (
                dense_w.to_string(),
                SRC_WALLET_SET_JSON,
                false,
                None,
                None,
                None,
                0,
            ),
            (
                sparse_w.to_string(),
                SRC_WALLET_SET_JSON,
                false,
                None,
                None,
                None,
                0,
            ),
        ])
        .unwrap();

    // Insert 500 dense trades for dense_w via cache.insert_new (with synthetic
    // RawTrade rows constructed via the trade-insert helper).
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
        VenueMarketId,
    };
    use pe_trader_index::snapshot::RawTrade;
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    fn make_trades(
        w: WalletAddress,
        n: usize,
        ts_for: impl Fn(usize) -> i64,
        prefix: &str,
    ) -> Vec<RawTrade> {
        (0..n)
            .map(|i| RawTrade {
                wallet: w,
                market_id: MarketId(VenueMarketId(format!("{prefix}_m_{i}"))),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                price: Price::new(dec!(0.5)).unwrap(),
                contracts: ContractQty(1),
                timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts_for(i)).unwrap()),
                source_trade_id: SourceTradeId(format!("0x{prefix}{i:064x}")),
            })
            .collect()
    }

    // 500 dense trades, all within 60s.
    let dense_trades = make_trades(dense_w, 500, |i| 1_700_000_000 + (i as i64 % 60), "d");
    cache
        .insert_new(&dense_w.to_string(), dense_trades)
        .unwrap();
    // 500 sparse trades, ~96s apart.
    let sparse_trades = make_trades(sparse_w, 500, |i| 1_700_000_000 + (i as i64) * 96, "s");
    cache
        .insert_new(&sparse_w.to_string(), sparse_trades)
        .unwrap();

    let report = cache.classify_infra_retroactive(3600, false).unwrap();
    assert_eq!(report.scanned, 2, "both wallets have 500 trades");
    assert_eq!(report.flagged, 1, "only dense wallet meets the threshold");
    assert!(!report.dry_run);
    assert!(cache.conn_for_test_is_infra(&dense_w.to_string()));
    assert!(!cache.conn_for_test_is_infra(&sparse_w.to_string()));
}

// ── Scenario 7: retroactive sweep dry-run does NOT update DB ─────────────────
//
// PASS: report.flagged > 0 (preview shows what would change), but
//       is_infra column stays 0 because dry_run=true.
// FAIL: dry_run silently writes to DB.

#[tokio::test]
async fn retroactive_dry_run_does_not_update() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&path).unwrap();

    let dense_w = wallet(0xF3);
    cache
        .upsert_wallets_bulk(&[(
            dense_w.to_string(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();

    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
        VenueMarketId,
    };
    use pe_trader_index::snapshot::RawTrade;
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    let trades: Vec<RawTrade> = (0..500)
        .map(|i| RawTrade {
            wallet: dense_w,
            market_id: MarketId(VenueMarketId(format!("m_{i}"))),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            contracts: ContractQty(1),
            timestamp: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(1_700_000_000 + (i as i64 % 60)).unwrap(),
            ),
            source_trade_id: SourceTradeId(format!("0xdry{i:061x}")),
        })
        .collect();
    cache.insert_new(&dense_w.to_string(), trades).unwrap();

    let report = cache.classify_infra_retroactive(3600, true).unwrap();
    assert_eq!(
        report.flagged, 1,
        "preview must report what would be flagged"
    );
    assert!(report.dry_run);
    assert!(
        !cache.conn_for_test_is_infra(&dense_w.to_string()),
        "dry_run must NOT update is_infra in the DB"
    );
}

// ── Scenario 8: active_tradeable_wallets view excludes infra wallets ─────────
//
// PASS: select_backfill_due / wallets_with_source_bit both exclude wallets
//       with is_infra = 1 even when those wallets satisfy the other criteria.
// FAIL: an infra wallet appears in either selector output.

#[tokio::test]
async fn view_excludes_infra_wallets_from_all_selectors() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&path).unwrap();

    let normal = wallet(0xA1);
    let flagged = wallet(0xA2);

    // Both wallets are active with the same source bit; flagged is also marked infra.
    cache
        .upsert_wallets_bulk(&[
            (
                normal.to_string(),
                SRC_WALLET_SET_JSON,
                false,
                None,
                None,
                None,
                0,
            ),
            (
                flagged.to_string(),
                SRC_WALLET_SET_JSON,
                true,
                None,
                None,
                None,
                0,
            ),
        ])
        .unwrap();
    // Force both into is_active=1 by direct UPDATE (apply_activation_rules
    // would refuse the infra one, defeating the test).
    cache.conn_for_test_set_active(&normal.to_string(), 1);
    cache.conn_for_test_set_active(&flagged.to_string(), 1);

    let now = 1_700_000_000_i64;
    let staleness = 86_400_i64;

    let backfill_due = cache.select_backfill_due(now, staleness, 0).unwrap();
    assert!(backfill_due.contains(&normal.to_string()));
    assert!(
        !backfill_due.contains(&flagged.to_string()),
        "select_backfill_due must exclude is_infra=1"
    );

    let by_source = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert!(by_source.contains(&normal.to_string()));
    assert!(
        !by_source.contains(&flagged.to_string()),
        "wallets_with_source_bit must exclude is_infra=1"
    );
}

fn insert(cache: &WalletCache, id: &str, ts: i64) {
    cache.raw_conn_for_test().execute("INSERT INTO trades (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) VALUES (?1, ?2, 'market', 0, 'buy', '0.5', 1, ?3)", rusqlite::params![id, wallet(1).to_string(), ts]).unwrap();
}
#[test]
fn retroactive_infra_ignores_partial_middle_slice_in_preview_and_apply() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let target = wallet(1).to_string();
    cache
        .upsert_wallet(&target, SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();
    for t in 10000..10500 {
        insert(&cache, &format!("middle{t}"), t);
    }
    cache.raw_conn_for_test().execute_batch("UPDATE wallets SET backfill_partial=1, backward_floor_unix=10000, forward_frontier_unix=10499").unwrap();
    for preview in [true, false] {
        let result = cache.classify_infra_retroactive(3600, preview).unwrap();
        assert_eq!((result.scanned, result.flagged), (0, 0));
        assert_eq!(
            pile::select_backfill_due(&cache, 20000, 0).unwrap(),
            [wallet(1).to_string()]
        );
    }
    for i in 0..500 {
        insert(&cache, &format!("old{i}"), 1000 + i * 10);
    }
    cache
        .raw_conn_for_test()
        .execute_batch("UPDATE wallets SET backfill_partial=0")
        .unwrap();
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, false)
            .unwrap()
            .flagged,
        0
    );
    // Existing completed dense behavior remains unchanged.
    cache
        .raw_conn_for_test()
        .execute_batch("DELETE FROM trades WHERE source_trade_id LIKE 'old%'")
        .unwrap();
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, true)
            .unwrap()
            .flagged,
        1
    );
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, false)
            .unwrap()
            .flagged,
        1
    );
    assert!(cache.active_tradeable_wallet_hexes().unwrap().is_empty());
}
