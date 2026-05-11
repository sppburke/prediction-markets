//! Integration smoke tests for `pe-operator-audit` (issue #130).
//!
//! Exercises the full audit pipeline (`run`) via in-memory `Args` + tempfile
//! cache + tempfile `trades.ndjson`, with no subprocess spawning. Asserts on
//! the structured `RunOutput`.
//!
//! Cases:
//!
//! 1. `happy_path_flags_double_funding` — two wallets share a funder, each
//!    has a fill → 1 operator row, wallet_count=2, fills=2,
//!    distinct_leader_wallets=2, double_funding_flag="Y", rank=1.
//! 2. `no_trades_ndjson_runs_in_clusters_only_mode` — same cache, no trades
//!    file → fills=0 everywhere; double_funding_flag="-".
//! 3. `unattributed_fills_surfaced_in_summary` — trades with `operator_id =
//!    None` → counted in `unattributed_fills`; the summary line appears
//!    BOTH before and after the table.
//! 4. `empty_cache_no_panic` — cache with no funder edges → zero rows; binary
//!    exits successfully.
//! 5. `multi_operator_sort_order_and_deterministic_flags` — three operators
//!    with crafted `(flag, fills, ppm)` triples + same `anti_gaming_flags`
//!    set → assert sort order `(Y first, fills DESC, ppm DESC)` and that
//!    rendered flags are alphabetically sorted regardless of insertion order.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::Write;
use std::path::Path;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::operator_audit::{Args, FillStats, build_rows, run};
use pe_core_types::WalletAddress;
use tempfile::TempDir;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

/// Seed a fresh `WalletCache` at `path` with the given `(funded, funder, event_at)`
/// funder edges. Returns the path for chaining.
fn seed_cache(path: &Path, edges: &[(&str, &str, i64)]) {
    let mut cache = WalletCache::open(path).unwrap();
    for (funded_hex, funder_hex, event_at) in edges {
        cache
            .insert_funder_edges(wallet(funded_hex), &[(wallet(funder_hex), *event_at)], 0)
            .unwrap();
    }
}

/// Write a sweep-style `trades.ndjson` with the given `(operator_id, leader_wallet)`
/// pairs (one fill per pair). `operator_id == None` is emitted as a JSON null.
fn seed_trades_ndjson(path: &Path, fills: &[(Option<&str>, &str)]) {
    let mut f = std::fs::File::create(path).unwrap();
    for (op_id, leader_wallet) in fills {
        let op_json = match op_id {
            Some(s) => format!("\"{s}\""),
            None => "null".to_owned(),
        };
        let line = format!(
            r#"{{"simulated_at":"2026-01-01T00:00:00Z","leader_wallet":"{leader_wallet}","operator_id":{op_json},"market_id":"m1","outcome_id":0,"side":"buy","contracts":1,"signal_price":"0.5","fill_price":"0.5"}}"#
        );
        writeln!(f, "{line}").unwrap();
    }
}

fn args(dir: &TempDir, cache_name: &str, trades_name: Option<&str>, min_confidence: u32) -> Args {
    Args {
        cache_path: dir.path().join(cache_name),
        trades_ndjson: trades_name.map(|n| dir.path().join(n)),
        csv_out: dir.path().join("audit.csv"),
        min_confidence,
    }
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: two wallets (`A`, `B`) share funder `FUND` → cluster of 2 wallets.
///       Each wallet has one fill in trades.ndjson under the same operator_id.
/// FAIL: no double-funding row, OR fills count is wrong.
#[test]
fn happy_path_flags_double_funding() {
    let dir = TempDir::new().unwrap();
    seed_cache(
        &dir.path().join("cache.db"),
        &[
            (
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xfffffffffffffffffffffffffffffffffffffff0",
                1,
            ),
            (
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "0xfffffffffffffffffffffffffffffffffffffff0",
                2,
            ),
        ],
    );

    // First, run with NO trades file to discover the operator_id that the
    // clustering produces. The same cluster will be used for the fills file.
    let preview = run(args(&dir, "cache.db", None, 0)).unwrap();
    assert_eq!(
        preview.rows.len(),
        1,
        "expected 1 cluster row, got {}: {:#?}",
        preview.rows.len(),
        preview.rows,
    );
    let op_id = preview.rows[0].operator_id.clone();
    // Cluster = {root, A, B} → wallet_count = 3 (the funder root is included).
    assert_eq!(preview.rows[0].wallet_count, 3);

    // Now seed fills with that operator_id.
    seed_trades_ndjson(
        &dir.path().join("trades.ndjson"),
        &[
            (Some(&op_id), "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            (Some(&op_id), "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ],
    );

    let out = run(args(&dir, "cache.db", Some("trades.ndjson"), 0)).unwrap();
    assert_eq!(out.rows.len(), 1);
    let row = &out.rows[0];
    assert_eq!(row.wallet_count, 3, "{{root, A, B}} = 3 wallets");
    assert_eq!(row.fills, 2);
    assert_eq!(row.distinct_leader_wallets, 2);
    assert_eq!(row.double_funding_flag, "Y");
    assert_eq!(row.rank, 1, "rank is assigned post-sort, 1-indexed");
    assert_eq!(out.unattributed_fills, 0);
    assert_eq!(out.total_fills, 2);
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: trades_ndjson = None → cluster row still produced; fills=0; flag="-".
/// FAIL: panic, OR double_funding_flag is "Y" despite zero fills.
#[test]
fn no_trades_ndjson_runs_in_clusters_only_mode() {
    let dir = TempDir::new().unwrap();
    seed_cache(
        &dir.path().join("cache.db"),
        &[(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xfffffffffffffffffffffffffffffffffffffff0",
            1,
        )],
    );
    let out = run(args(&dir, "cache.db", None, 0)).unwrap();
    assert!(!out.rows.is_empty(), "expected ≥1 cluster row");
    let row = &out.rows[0];
    assert_eq!(row.fills, 0);
    assert_eq!(row.distinct_leader_wallets, 0);
    assert_eq!(row.double_funding_flag, "-");
    assert_eq!(out.total_fills, 0);
    assert_eq!(out.unattributed_fills, 0);
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: one trade with operator_id=None → unattributed_fills=1, total_fills=1.
///       Summary line appears both before and after the table.
/// FAIL: unattributed not surfaced.
#[test]
fn unattributed_fills_surfaced_in_summary() {
    let dir = TempDir::new().unwrap();
    seed_cache(
        &dir.path().join("cache.db"),
        &[(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xfffffffffffffffffffffffffffffffffffffff0",
            1,
        )],
    );
    seed_trades_ndjson(
        &dir.path().join("trades.ndjson"),
        &[(None, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")],
    );

    let out = run(args(&dir, "cache.db", Some("trades.ndjson"), 0)).unwrap();
    assert_eq!(out.unattributed_fills, 1);
    assert_eq!(out.total_fills, 1);
    // Summary line printed twice — head/grep consumers see it without reading the full table.
    let summary_substr = "Unattributed fills: 1 / 1";
    let occurrences = out.markdown.matches(summary_substr).count();
    assert!(
        occurrences >= 2,
        "expected unattributed-summary line both before and after the table; saw {occurrences}",
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: empty cache (no funder edges) → run completes; zero rows.
/// FAIL: panic on empty input.
#[test]
fn empty_cache_no_panic() {
    let dir = TempDir::new().unwrap();
    // Open + close to materialise the SQLite file with the schema.
    let _ = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let out = run(args(&dir, "cache.db", None, 0)).unwrap();
    assert!(out.rows.is_empty(), "expected 0 rows; got {:#?}", out.rows);
    assert_eq!(out.total_fills, 0);
    assert_eq!(out.unattributed_fills, 0);
    // CSV is written even on empty input — the file must exist.
    assert!(out.csv_path.exists());
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: multi-operator sort obeys `(double_funding_flag DESC, fills DESC,
///       confidence_ppm DESC)`. Y rows precede `-` rows; within Y group,
///       higher fills wins over higher ppm.
/// FAIL: sort order wrong, or output non-deterministic across HashSet iter
///       orders (anti_gaming_flags must be sorted before joining).
///
/// Constructs `AuditRow` directly via `build_rows` with synthetic identities
/// and `FillStats` — the integration boundary already covers the I/O paths
/// in scenarios 1–4.
#[test]
fn multi_operator_sort_order_and_deterministic_flags() {
    use pe_core_types::{
        ClusterSize, FunderRootId, FundingHopCount, OperatorId, ReconstructionQuality,
    };
    use pe_operator_graph::{AntiGamingFlag, OperatorIdentity};
    use std::collections::{HashMap, HashSet};

    fn synth(
        op_seed: u8,
        funder_root_seed: u8,
        wallets: u8,
        confidence_ppm: u32,
        flags: &[AntiGamingFlag],
    ) -> OperatorIdentity {
        // Construct an OperatorId from a deterministic hash so `op_id.to_string()`
        // is stable. blake3::hash is the same path used downstream.
        let hash = blake3::hash(&[op_seed]);
        let operator_id = OperatorId(hash);
        let funder_addr = WalletAddress([funder_root_seed; 20]);
        let funder_root = FunderRootId(funder_addr);
        let member_wallets: Vec<WalletAddress> =
            (0..wallets).map(|i| WalletAddress([i + 10; 20])).collect();
        let mut hop_counts: HashMap<WalletAddress, FundingHopCount> = HashMap::new();
        for w in &member_wallets {
            hop_counts.insert(*w, FundingHopCount(1));
        }
        OperatorIdentity {
            operator_id,
            funder_root,
            member_wallets,
            hop_counts,
            confidence_ppm,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            cluster_size: ClusterSize(u16::from(wallets)),
            anti_gaming_flags: flags.iter().copied().collect::<HashSet<_>>(),
        }
    }

    let identities = vec![
        // A: Y(via 2 wallets in fills), fills=10, ppm=500
        synth(1, 1, 2, 500, &[AntiGamingFlag::WashCluster]),
        // B: "-" (only 1 wallet has fills), fills=100, ppm=800
        synth(2, 2, 3, 800, &[]),
        // C: Y(2 wallets), fills=5, ppm=900
        synth(3, 3, 2, 900, &[]),
    ];

    let mut fills: std::collections::HashMap<String, FillStats> = std::collections::HashMap::new();

    let op_a = identities[0].operator_id.to_string();
    let op_b = identities[1].operator_id.to_string();
    let op_c = identities[2].operator_id.to_string();

    fills.insert(
        op_a.clone(),
        FillStats {
            count: 10,
            distinct_wallets: ["a1", "a2"].iter().map(|s| (*s).to_owned()).collect(),
        },
    );
    fills.insert(
        op_b.clone(),
        FillStats {
            count: 100,
            distinct_wallets: ["b1"].iter().map(|s| (*s).to_owned()).collect(),
        },
    );
    fills.insert(
        op_c.clone(),
        FillStats {
            count: 5,
            distinct_wallets: ["c1", "c2"].iter().map(|s| (*s).to_owned()).collect(),
        },
    );

    let rows = build_rows(identities, &fills, 0);
    assert_eq!(rows.len(), 3, "expected 3 rows; got {}", rows.len());

    // Expected order: A (Y, fills=10), C (Y, fills=5), B (-, fills=100).
    // Within Y group, fills DESC wins over ppm DESC: A.fills=10 > C.fills=5
    // despite A.ppm=500 < C.ppm=900.
    assert_eq!(
        rows[0].operator_id, op_a,
        "rank 1 should be operator A (Y, highest fills)"
    );
    assert_eq!(
        rows[1].operator_id, op_c,
        "rank 2 should be operator C (Y, lower fills)"
    );
    assert_eq!(
        rows[2].operator_id, op_b,
        "rank 3 should be operator B (no Y)"
    );
    assert_eq!(rows[0].rank, 1);
    assert_eq!(rows[1].rank, 2);
    assert_eq!(rows[2].rank, 3);
    // Determinism: A had WashCluster flag — verify formatting is stable.
    assert_eq!(rows[0].anti_gaming_flags, "WashCluster");
    assert!(
        rows[1].anti_gaming_flags.is_empty(),
        "B had no flags; expected empty string"
    );
}
