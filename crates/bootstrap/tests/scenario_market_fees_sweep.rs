//! Scenario: Gamma /events sweep populates market_fees (issue #23, PR 1).
//!
//! PASS: after sweeping a fixture with takerBaseFee/makerBaseFee fields,
//!       `market_fees` contains the expected bps values for each market.
//! FAIL: market_fees is empty, or bps values don't match the fixture.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::events::GammaEventsFetcher;
use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use tempfile::TempDir;

/// Serves one response then HTTP 422, mirroring the existing events-sweep pattern.
struct OneThen422 {
    body: Vec<u8>,
    calls: Arc<AtomicUsize>,
}

impl OneThen422 {
    fn new(body: Vec<u8>) -> Self {
        Self {
            body,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl PageFetcher for OneThen422 {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            Ok(self.body.clone())
        } else {
            Err(SourceError::Fatal {
                message: "HTTP 422".to_owned(),
            })
        }
    }
}

const BASE: &str = "https://gamma-api.polymarket.com";

#[tokio::test]
async fn sweep_populates_market_fees() {
    // Fixture: one event with two markets.
    // Market 0xaa: takerBaseFee = 0.02 (fraction form → 200 bps), makerBaseFee = 0
    // Market 0xbb: takerFee = 150 (alias, already bps), makerBaseFee = 50
    let fixture = br#"[{
        "id": 1,
        "slug": "test-event",
        "markets": [
            {
                "conditionId": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "takerBaseFee": 0.02,
                "makerBaseFee": 0.0
            },
            {
                "conditionId": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "takerFee": 150,
                "makerBaseFee": 50
            }
        ]
    }]"#;

    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

    let fetcher = GammaEventsFetcher::new(BASE.to_owned(), OneThen422::new(fixture.to_vec()));
    let report = fetcher.sweep(&mut cache).await.unwrap();

    // Report must count 2 fee rows upserted.
    assert_eq!(
        report.fees_upserted, 2,
        "FAIL: fees_upserted={}",
        report.fees_upserted
    );

    let fees = cache.load_market_fees().unwrap();
    assert_eq!(
        fees.len(),
        2,
        "FAIL: market_fees has {} rows, expected 2",
        fees.len()
    );

    let aa = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let fee_aa = fees.get(aa).expect("FAIL: 0xaa not in market_fees");
    assert_eq!(fee_aa.taker_base_fee_bps, 200, "FAIL: 0xaa taker bps");
    assert_eq!(fee_aa.maker_base_fee_bps, 0, "FAIL: 0xaa maker bps");

    let bb = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let fee_bb = fees.get(bb).expect("FAIL: 0xbb not in market_fees");
    assert_eq!(fee_bb.taker_base_fee_bps, 150, "FAIL: 0xbb taker bps");
    assert_eq!(fee_bb.maker_base_fee_bps, 50, "FAIL: 0xbb maker bps");

    println!("PASS: sweep_populates_market_fees — 2 markets with correct bps values");
}
