#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Complete-walk, legacy-sealing, and cross-reader payout proof (#544).

use std::collections::HashMap;
use std::process::Command;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::clob::ClobFetcher;
use pe_source_polymarket_public::{ClobPayoutResolution, FixtureFetcher};
use tempfile::TempDir;

const BASE_URL: &str = "https://clob.example";
const HALF: &str =
    include_str!("../../source-polymarket-public/tests/fixtures/clob_market_5050.json");
const WINNER: &str =
    include_str!("../../source-polymarket-public/tests/fixtures/clob_market_winner.json");
const HALF_ID: &str = "0x0fbd0991b8cd88bebb6b48441c549d32a1eedb1e5ce47a5730829cec02c0e635";

fn page_url(cursor: Option<&str>) -> String {
    match cursor {
        Some(cursor) => {
            format!("{BASE_URL}/markets?closed=true&limit=1000&next_cursor={cursor}")
        }
        None => format!("{BASE_URL}/markets?closed=true&limit=1000"),
    }
}

fn terminal_page() -> Vec<u8> {
    format!(r#"{{"data":[{HALF},{WINNER}],"next_cursor":"LTE="}}"#).into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_walk_installs_manifest_and_every_reader_returns_identical_vector() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let parquet_dir = dir.path().join("parquet");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let mut responses = HashMap::new();
    responses.insert(page_url(None), terminal_page());
    let fetcher = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let report = fetcher.fetch_closed_markets(&mut cache).await.unwrap();
    let manifest = report
        .coverage_manifest
        .expect("a page-one terminal walk must install a manifest");
    assert_eq!(manifest.counts.pages, 1);
    assert_eq!(manifest.counts.markets, 2);
    assert_eq!(manifest.counts.resolved_payouts, 2);
    assert_eq!(
        cache.latest_clob_payout_coverage_manifest_v2().unwrap(),
        Some(manifest)
    );

    // SQLite -> canonical Rust row.
    let sqlite_row = cache
        .clob_payout_evidence_v2(HALF_ID)
        .unwrap()
        .expect("half payout row must be installed");
    let rust_vector = match &sqlite_row.payout {
        ClobPayoutResolution::Resolved(vector) => vector.canonical_json(),
        ClobPayoutResolution::Unresolved(reason) => {
            panic!("recorded half fixture unexpectedly unresolved: {reason:?}")
        }
    };
    assert_eq!(rust_vector, "[\"0.5\",\"0.5\"]");
    assert_eq!(sqlite_row.is_50_50_outcome, Some(true));
    assert_eq!(sqlite_row.origin, "clob_closed_walk_v2");

    // The helper performs SQLite -> Parquet -> DuckDB -> Python typed reader.
    // Compare its canonical bytes with the Rust struct read above.
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/read_clob_payout_parity.py");
    let output = Command::new("python3")
        .arg(script)
        .arg("--db")
        .arg(&db_path)
        .arg("--out-dir")
        .arg(&parquet_dir)
        .arg("--market-id")
        .arg(HALF_ID)
        .output()
        .expect("python cross-reader helper must start");
    assert!(
        output.status.success(),
        "cross-reader helper failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let python_vector = stdout
        .lines()
        .find_map(|line| line.strip_prefix("CANONICAL_VECTOR="))
        .expect("python helper must emit its canonical vector");
    assert_eq!(python_vector.as_bytes(), rust_vector.as_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_resolution_and_cursor_cannot_seed_v2_payouts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
    cache
        .insert_resolution_with_source("legacy-only", Some(0), 100, 101, "clob")
        .unwrap();

    let mut responses = HashMap::new();
    responses.insert(page_url(Some("PAGE2")), terminal_page());
    let fetcher = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
    let report = fetcher.fetch_closed_markets(&mut cache).await.unwrap();
    assert!(
        report.coverage_manifest.is_none(),
        "a legacy mid-walk cursor must not forge complete v2 coverage"
    );
    assert!(cache.clob_payout_evidence_v2(HALF_ID).unwrap().is_none());
    assert!(
        cache
            .latest_clob_payout_coverage_manifest_v2()
            .unwrap()
            .is_none()
    );

    // Even direct SQL cannot label a legacy-derived row as v2: the schema
    // requires the one canonical closed-walk origin.
    drop(cache);
    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let result = connection.execute(
        "INSERT INTO clob_payout_evidence_v2 \
         (market_id, is_50_50_outcome, payout_status, payout_vector_json, closed, \
          tokens_json, raw_page_sha256, coverage_generation, page_ordinal, schema_version, \
          parser_version, fetched_at_unix, origin) \
         VALUES ('legacy-seed', 0, 'resolved', '[\"1\",\"0\"]', 1, '[]', ?1, 1, 0, \
                 2, 2, 101, 'legacy_resolution_row')",
        ["a".repeat(64)],
    );
    assert!(result.is_err(), "schema must reject a legacy-origin v2 row");
}
