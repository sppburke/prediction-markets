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

/// PASS: a walk whose pages repeat a market commits one evidence row per distinct
/// market, and coverage records that committed count rather than the manifest's
/// per-page sum (#672). FAIL: the repeat is counted twice, dropped, or unrecorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_market_repeated_across_pages_commits_once_and_is_counted_once() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let mut responses = HashMap::new();
    // The first page ends on a cursor; the second repeats the half-payout market,
    // exactly as a live walk over a growing market set does.
    responses.insert(
        page_url(None),
        format!(r#"{{"data":[{HALF},{WINNER}],"next_cursor":"MTAwMA=="}}"#).into_bytes(),
    );
    responses.insert(
        page_url(Some("MTAwMA==")),
        format!(r#"{{"data":[{HALF}],"next_cursor":"LTE="}}"#).into_bytes(),
    );
    let fetcher = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let report = fetcher.fetch_closed_markets(&mut cache).await.unwrap();
    let manifest = report
        .coverage_manifest
        .expect("a terminal walk must install a manifest");
    assert_eq!(manifest.counts.pages, 2);
    // The manifest sums what each page returned, repeats included.
    assert_eq!(manifest.counts.markets, 3);

    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let generation = i64::try_from(manifest.generation).unwrap();
    let evidence: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM clob_payout_evidence_v2 WHERE coverage_generation = ?1",
            [generation],
            |row| row.get(0),
        )
        .unwrap();
    let committed: Option<i64> = connection
        .query_row(
            "SELECT evidence_count FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
            [generation],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(evidence, 2, "the repeated market must be stored once");
    assert_eq!(
        committed,
        Some(2),
        "coverage must record the rows the commit wrote"
    );
    // The half-payout market keeps the later page's evidence, not a duplicate row.
    assert!(
        cache.clob_payout_evidence_v2(HALF_ID).unwrap().is_some(),
        "the repeated market must still be readable"
    );
}

/// PASS: an entry the venue returns without a market id is counted by the page
/// proof, staged by nobody, and excluded from the committed count, so coverage
/// still verifies (#672). FAIL: the blank entry inflates the committed count or
/// fails the walk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_without_a_market_id_is_counted_but_not_committed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let mut blank: serde_json::Value = serde_json::from_str(WINNER).unwrap();
    blank["condition_id"] = serde_json::Value::String(String::new());
    let mut responses = HashMap::new();
    responses.insert(
        page_url(None),
        format!(r#"{{"data":[{HALF},{WINNER},{blank}],"next_cursor":"LTE="}}"#).into_bytes(),
    );
    let fetcher = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let manifest = fetcher
        .fetch_closed_markets(&mut cache)
        .await
        .unwrap()
        .coverage_manifest
        .expect("a terminal walk must install a manifest");
    assert_eq!(
        manifest.counts.markets, 3,
        "the page counts what it carried"
    );

    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let generation = i64::try_from(manifest.generation).unwrap();
    let (evidence, committed): (i64, Option<i64>) = (
        connection
            .query_row(
                "SELECT COUNT(*) FROM clob_payout_evidence_v2 WHERE coverage_generation = ?1",
                [generation],
                |row| row.get(0),
            )
            .unwrap(),
        connection
            .query_row(
                "SELECT evidence_count FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
                [generation],
                |row| row.get(0),
            )
            .unwrap(),
    );
    assert_eq!(evidence, 2, "only markets with an id are stored");
    assert_eq!(committed, Some(2), "the committed count excludes it too");
}

/// PASS: a page whose evidence slice is shorter than the markets it counts is
/// refused, so a truncated page cannot quietly shrink coverage (#672).
/// FAIL: the short page stages and the shortfall surfaces only much later.
#[test]
fn a_page_carrying_fewer_markets_than_it_counts_is_refused() {
    use pe_source_polymarket_public::ClobCoveragePage;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let state = cache
        .begin_or_resume_clob_payout_walk_v2(1_800_000_000)
        .unwrap();
    let page = ClobCoveragePage {
        ordinal: 0,
        request_cursor: None,
        returned_next_cursor: Some("LTE=".to_owned()),
        raw_sha256: "b".repeat(64),
        market_count: 2,
        closed_market_count: 0,
        resolved_payout_count: 0,
        unresolved_payout_count: 0,
        explicit_fifty_fifty_count: 0,
    };
    let error = cache
        .commit_clob_payout_page_v2(state.generation, &page, &[], 1_800_000_010)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("carries 0 markets but counts 2"),
        "reported {error}"
    );
}

/// PASS: a staged market lost before the completion copy fails the walk, which
/// the manifest's per-page sum could no longer prove on its own (#672).
/// FAIL: the loss is committed and certified.
#[test]
fn a_staged_market_lost_before_completion_fails_the_walk() {
    use pe_source_polymarket_public::{ClobCoverageManifest, ClobCoveragePage};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let state = cache
        .begin_or_resume_clob_payout_walk_v2(1_800_000_000)
        .unwrap();
    let evidence: Vec<_> = [HALF, WINNER]
        .iter()
        .map(|body| {
            pe_source_polymarket_public::parse_clob_market(body.as_bytes())
                .unwrap()
                .resolution_evidence()
        })
        .collect();
    let page = ClobCoveragePage {
        ordinal: 0,
        request_cursor: None,
        returned_next_cursor: Some("LTE=".to_owned()),
        raw_sha256: "c".repeat(64),
        market_count: 2,
        closed_market_count: 2,
        resolved_payout_count: 2,
        unresolved_payout_count: 0,
        explicit_fifty_fifty_count: 1,
    };
    cache
        .commit_clob_payout_page_v2(state.generation, &page, &evidence, 1_800_000_010)
        .unwrap();
    // Something removes a staged market between the walk and its completion.
    rusqlite::Connection::open(&db_path)
        .unwrap()
        .execute(
            "DELETE FROM clob_payout_evidence_staging_v2 WHERE market_id = ?1",
            [HALF_ID],
        )
        .unwrap();
    let manifest = ClobCoverageManifest::complete(state.generation, vec![page]).unwrap();
    let error = cache
        .complete_clob_payout_walk_v2(&manifest, 1_800_000_020)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("copied 1 markets but staged 2"),
        "reported {error}"
    );
}

/// Stages, without completing, a two-page walk whose second page repeats the
/// half-payout market, and returns the walk's coverage manifest.
fn stage_two_pages_repeating_a_market(
    cache: &mut WalletCache,
) -> pe_source_polymarket_public::ClobCoverageManifest {
    use pe_source_polymarket_public::{ClobCoverageManifest, ClobCoveragePage};

    let state = cache
        .begin_or_resume_clob_payout_walk_v2(1_800_000_000)
        .unwrap();
    let evidence = |bodies: &[&str]| -> Vec<_> {
        bodies
            .iter()
            .map(|body| {
                pe_source_polymarket_public::parse_clob_market(body.as_bytes())
                    .unwrap()
                    .resolution_evidence()
            })
            .collect()
    };
    let first = ClobCoveragePage {
        ordinal: 0,
        request_cursor: None,
        returned_next_cursor: Some("MTAwMA==".to_owned()),
        raw_sha256: "c".repeat(64),
        market_count: 2,
        closed_market_count: 2,
        resolved_payout_count: 2,
        unresolved_payout_count: 0,
        explicit_fifty_fifty_count: 1,
    };
    let second = ClobCoveragePage {
        ordinal: 1,
        request_cursor: Some("MTAwMA==".to_owned()),
        returned_next_cursor: Some("LTE=".to_owned()),
        raw_sha256: "d".repeat(64),
        market_count: 1,
        closed_market_count: 1,
        resolved_payout_count: 1,
        unresolved_payout_count: 0,
        explicit_fifty_fifty_count: 1,
    };
    cache
        .commit_clob_payout_page_v2(
            state.generation,
            &first,
            &evidence(&[HALF, WINNER]),
            1_800_000_010,
        )
        .unwrap();
    cache
        .commit_clob_payout_page_v2(state.generation, &second, &evidence(&[HALF]), 1_800_000_020)
        .unwrap();
    ClobCoverageManifest::complete(state.generation, vec![first, second]).unwrap()
}

/// Every column of a payout evidence table, its generation column last, by market.
fn evidence_rows(
    connection: &rusqlite::Connection,
    table: &str,
    generation_column: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT market_id, end_date_unix, neg_risk_market_id, neg_risk, is_50_50_outcome, \
                    payout_status, payout_vector_json, closed, tokens_json, raw_page_sha256, \
                    page_ordinal, schema_version, parser_version, fetched_at_unix, origin, \
                    {generation_column} \
             FROM {table} ORDER BY market_id"
        ))
        .unwrap();
    statement
        .query_map([], |row| (0..16).map(|index| row.get(index)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// PASS: completion installs exactly the staged rows, column by column, and leaves
/// no staging, page or walk-state row behind. FAIL: a row or column differs, or
/// the walk survives its install.
#[test]
fn completion_installs_the_staged_rows_and_clears_the_walk() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let manifest = stage_two_pages_repeating_a_market(&mut cache);
    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let staged = evidence_rows(&connection, "clob_payout_evidence_staging_v2", "generation");
    assert_eq!(staged.len(), 2, "the repeated market is staged once");

    cache
        .complete_clob_payout_walk_v2(&manifest, 1_800_000_030)
        .unwrap();

    assert_eq!(
        evidence_rows(
            &connection,
            "clob_payout_evidence_v2",
            "coverage_generation"
        ),
        staged
    );
    let count = |sql: &str| -> i64 { connection.query_row(sql, [], |row| row.get(0)).unwrap() };
    assert_eq!(
        count("SELECT COUNT(*) FROM clob_payout_evidence_staging_v2"),
        0
    );
    assert_eq!(count("SELECT COUNT(*) FROM clob_payout_walk_pages_v2"), 0);
    assert_eq!(count("SELECT COUNT(*) FROM clob_payout_walk_state_v2"), 0);
    let committed: Option<i64> = connection
        .query_row(
            "SELECT evidence_count FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
            [i64::try_from(manifest.generation).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(committed, Some(2));
}

/// PASS: a failure after the staging clear rolls the whole install back: staging,
/// the walk's pages and its state stay as staged, and neither evidence nor
/// coverage is installed. FAIL: the clear survives the failed completion, or part
/// of the install is visible.
#[test]
fn a_failure_after_the_staging_clear_rolls_the_install_back() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&db_path).unwrap();
    let manifest = stage_two_pages_repeating_a_market(&mut cache);
    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let staged = evidence_rows(&connection, "clob_payout_evidence_staging_v2", "generation");
    // Deleting the walk state is the completion's last statement, after the clear.
    connection
        .execute_batch(
            "CREATE TRIGGER refuse_walk_state_delete BEFORE DELETE ON clob_payout_walk_state_v2 \
             BEGIN SELECT RAISE(ABORT, 'injected walk state failure'); END;",
        )
        .unwrap();

    let error = cache
        .complete_clob_payout_walk_v2(&manifest, 1_800_000_030)
        .unwrap_err()
        .to_string();
    assert!(error.contains("injected walk state failure"), "{error}");

    assert_eq!(
        evidence_rows(&connection, "clob_payout_evidence_staging_v2", "generation"),
        staged
    );
    let count = |sql: &str| -> i64 { connection.query_row(sql, [], |row| row.get(0)).unwrap() };
    assert_eq!(count("SELECT COUNT(*) FROM clob_payout_evidence_v2"), 0);
    assert_eq!(
        count("SELECT COUNT(*) FROM clob_payout_coverage_manifests_v2"),
        0
    );
    assert_eq!(count("SELECT COUNT(*) FROM clob_payout_walk_pages_v2"), 2);
    assert_eq!(
        cache
            .clob_payout_walk_state_v2()
            .unwrap()
            .map(|state| state.generation),
        Some(manifest.generation)
    );
}
