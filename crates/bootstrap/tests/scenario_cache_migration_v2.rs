#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario: sealed v1 cache, exact frozen proof, finalized v2, and fixed-path cutover (#544, #545).
//!
//! PASS: a row committed while the input is in WAL survives sealing; legacy rows
//! remain audit-readable only in `*_v1_sealed`; v2 typed reads contain no legacy
//! identity; frozen active/freshness evidence matches; finalization removes
//! sidecars; activation keeps an immutable v1 main and installs the exact hash.
//! FAIL: any generation crosses, a checkpoint is incomplete, a manifest is
//! missing, a stale sidecar survives, or the installed hash changes.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::cache_migration::{
    CacheActivationRequest, CacheV2BuildManifest, FrozenCacheFreshness, FrozenPayloadReference,
    PriorCacheBinding, PublicationConsumptionProbe, activate_cache_v2, finalize_cache_v2,
    migrate_cache_v2, populate_activity_fresh_v2, populate_activity_v2, restore_prior_cache,
    sha256_file, verify_frozen_payload_v1,
};
use pe_bootstrap::clob::ClobFetcher;
use pe_bootstrap::pile::SRC_TRADES;
use pe_core_types::{ReceivedAt, ReconstructionQuality, SourceId, SourceTimestamp, WalletAddress};
use pe_position_ledger::{
    EntryClassification, LedgerMutation, PositionLedger, SecondVerdict,
    classify_complete_second_legacy,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, CLOB_RESOLUTION_PARSER_VERSION,
    CLOB_RESOLUTION_SCHEMA_VERSION, ClobCoverageManifest, ClobCoveragePage, FixtureFetcher,
    PageFetcher, parse_activity_response,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

const WALLET: &str = "0x1111111111111111111111111111111111111111";

struct FixedPublicationProbe(bool);

impl PublicationConsumptionProbe for FixedPublicationProbe {
    fn was_published<'a>(
        &'a self,
        _publish_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, pe_bootstrap::error::BootstrapError>> + Send + 'a>>
    {
        Box::pin(async move { Ok(self.0) })
    }
}

struct UnavailablePublicationProbe;

impl PublicationConsumptionProbe for UnavailablePublicationProbe {
    fn was_published<'a>(
        &'a self,
        _publish_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, pe_bootstrap::error::BootstrapError>> + Send + 'a>>
    {
        Box::pin(async {
            Err(pe_bootstrap::error::BootstrapError::Invalid {
                message: "publication authority unavailable".to_owned(),
            })
        })
    }
}

fn write_pending_publication(
    dir: &TempDir,
    name: &str,
    side: &std::path::Path,
    expected_installed: &std::path::Path,
    fixed: &std::path::Path,
    prior: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let request_path = dir.path().join(format!("{name}-request.json"));
    let pending_path = dir.path().join(format!("{name}-pending"));
    let activation = serde_json::json!({
        "side_path": side,
        "fixed_path": fixed,
        "prior_cache_backup_path": prior,
        "expected_sha256": sha256_file(expected_installed).unwrap(),
    });
    let batch = serde_json::json!({"config_hash": null});
    let entries = serde_json::json!([{"rank": 1, "wallet_hex": WALLET}]);
    let identity = serde_json::json!({
        "batch": batch,
        "cache_activation": activation,
        "entries": entries,
    });
    let publish_key = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&identity).unwrap())
    );
    std::fs::write(
        &request_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "batch": identity["batch"],
            "entries": identity["entries"],
            "keep_batches": 1080,
            "cache_activation": identity["cache_activation"],
            "publish_key": publish_key,
        }))
        .unwrap(),
    )
    .unwrap();
    let relative = request_path
        .strip_prefix(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::write(&pending_path, format!("{}\n", relative.display())).unwrap();
    (request_path, pending_path)
}

fn seed_v1(path: &std::path::Path, timestamp: i64) -> WalletCache {
    let mut cache = WalletCache::open(path).unwrap();
    cache
        .upsert_wallets_bulk(&[(WALLET.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
    cache.conn_for_test_set_active(WALLET, 1);
    cache.conn_for_test_insert_trade(WALLET, "0xlegacy", timestamp);
    cache
        .raw_conn_for_test()
        .execute(
            "INSERT INTO market_resolutions
                 (market_id, winning_outcome_id, resolved_at_unix, fetched_at_unix, source)
             VALUES ('m', 0, ?1, ?2, 'clob')",
            params![timestamp - 10, timestamp],
        )
        .unwrap();
    cache
        .raw_conn_for_test()
        .execute(
            "INSERT INTO source_cursor (key, value, updated_at)
             VALUES ('clob_closed', '', ?1)",
            params![timestamp],
        )
        .unwrap();
    // Deliberately do not checkpoint: migration must preserve a committed row
    // that is present only in the WAL at entry.
    cache
}

fn write_build_manifest(dir: &TempDir, cache_path: &std::path::Path) -> std::path::PathBuf {
    let path = dir.path().join("cache-build.json");
    let mut hashes = BTreeMap::from([("online_backup".to_owned(), "fixture".to_owned())]);
    let wal_path = cache_path.with_extension("db-wal");
    if wal_path.is_file() && wal_path.metadata().unwrap().len() != 0 {
        hashes.insert(
            "backup_wal_sha256".to_owned(),
            sha256_file(&wal_path).unwrap(),
        );
    }
    let manifest = CacheV2BuildManifest {
        manifest_version: 1,
        backup_sha256: sha256_file(cache_path).unwrap(),
        source_bounds: serde_json::json!({"activity_end": 1_800_000_000_i64}),
        cursors: serde_json::json!({"clob_closed": ""}),
        hashes,
        sealed_at_unix: 1_800_000_010,
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    path
}

fn write_frozen_reference(
    dir: &TempDir,
    watermark: i64,
    active_wallets: Vec<String>,
) -> std::path::PathBuf {
    let path = dir.path().join("frozen.json");
    let frozen = FrozenPayloadReference {
        version: 1,
        process_now_unix: watermark + 60,
        active_window_hours: 72,
        max_cache_staleness_hours: 24,
        ranked_wallets: active_wallets.clone(),
        active_wallets,
        freshness: FrozenCacheFreshness {
            newest_trade_unix: watermark,
            newest_resolution_fetch_unix: watermark,
            clob_cursor: String::new(),
            clob_cursor_updated_at: watermark,
        },
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&frozen).unwrap()).unwrap();
    path
}

#[derive(Clone, Default)]
struct YieldingFetcher {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl PageFetcher for YieldingFetcher {
    fn fetch_page(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SourceError>> + Send {
        let active = Arc::clone(&self.active);
        let maximum = Arc::clone(&self.maximum);
        let calls = Arc::clone(&self.calls);
        let url = url.to_owned();
        async move {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(now, Ordering::SeqCst);
            calls.lock().unwrap().push(url);
            tokio::task::yield_now().await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(b"[]".to_vec())
        }
    }
}

fn install_payout_manifest(path: &std::path::Path) {
    let manifest = ClobCoverageManifest::complete(
        1,
        vec![ClobCoveragePage {
            ordinal: 0,
            request_cursor: None,
            returned_next_cursor: Some("LTE=".to_owned()),
            raw_sha256: "a".repeat(64),
            market_count: 0,
            closed_market_count: 0,
            resolved_payout_count: 0,
            unresolved_payout_count: 0,
            explicit_fifty_fifty_count: 0,
        }],
    )
    .unwrap();
    let connection = Connection::open(path).unwrap();
    connection
        .execute(
            "INSERT INTO clob_payout_coverage_manifests_v2
                 (generation, manifest_json, walked_start_cursor, walked_end_cursor,
                  page_count, market_count, closed_market_count, resolved_payout_count,
                  unresolved_payout_count, explicit_fifty_fifty_count, terminal_kind,
                  terminal_page_sha256, schema_version, parser_version, completed_at_unix)
             VALUES (1, ?1, NULL, 'LTE=', 1, 0, 0, 0, 0, 0, 'end_cursor',
                     ?2, ?3, ?4, 1800000020)",
            params![
                serde_json::to_string(&manifest).unwrap(),
                "a".repeat(64),
                i64::from(CLOB_RESOLUTION_SCHEMA_VERSION),
                i64::from(CLOB_RESOLUTION_PARSER_VERSION),
            ],
        )
        .unwrap();
}

async fn finalize_empty_activity_side(
    dir: &TempDir,
    side: &std::path::Path,
    watermark: i64,
) -> String {
    drop(seed_v1(side, watermark));
    let manifest = write_build_manifest(dir, side);
    migrate_cache_v2(side, &manifest).unwrap();
    let frozen_path = write_frozen_reference(dir, watermark, vec![WALLET.to_owned()]);
    verify_frozen_payload_v1(side, &frozen_path, watermark + 70).unwrap();
    let activity_url = format!(
        "https://data.example/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end={watermark}"
    );
    populate_activity_v2(
        side,
        &FixtureFetcher::new(HashMap::from([(activity_url, b"[]".to_vec())])),
        "https://data.example",
        &frozen_path,
        watermark,
        1,
        watermark + 80,
    )
    .await
    .unwrap();
    install_payout_manifest(side);
    finalize_cache_v2(
        side,
        &dir.path().join("cache-v2-final.json"),
        watermark + 90,
    )
    .unwrap()
    .cache_sha256
}

#[tokio::test]
async fn migration_is_resumable_and_activation_installs_only_the_finalized_main() {
    let dir = tempfile::Builder::new()
        .prefix("pe-cache-v2-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(data.join("eval-results")).unwrap();
    let side = data.join("wallet_cache.v2.side.db");
    let fixed = data.join("wallet_cache.db");
    let v1_backup = data.join("wallet_cache.v1.sha.db");
    let manifest_path;
    let watermark = 1_800_000_000_i64;
    {
        let wal_owner = seed_v1(&side, watermark);
        manifest_path = write_build_manifest(&dir, &side);
        assert!(side.with_extension("db-wal").metadata().unwrap().len() > 0);
        let first = migrate_cache_v2(&side, &manifest_path).unwrap();
        drop(wal_owner);
        assert!(!first.resumed);
        assert_eq!(first.legacy_trade_count, 1);
    }
    let second = migrate_cache_v2(&side, &manifest_path).unwrap();
    assert!(second.resumed);

    let cache = WalletCache::open_configured(&pe_bootstrap::BootstrapConfig {
        cache_path: side.clone(),
        cache_page_cache_mib: 7,
        cache_mmap_mib: 4096,
        ..pe_bootstrap::BootstrapConfig::default()
    })
    .unwrap();
    let conn = cache.raw_conn_for_test();
    assert_eq!(
        conn.pragma_query_value(None, "cache_size", |row| row.get::<_, i32>(0))
            .unwrap(),
        -7 * 1024
    );
    let effective_mmap: i64 = conn
        .query_row("PRAGMA mmap_size", [], |row| row.get(0))
        .optional()
        .unwrap()
        .unwrap_or(0);
    assert!((0..=4096 * (1_i64 << 20)).contains(&effective_mmap));
    let mmap_disabled: bool = conn
        .query_row(
            "SELECT sqlite_compileoption_used('MAX_MMAP_SIZE=0')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    if !mmap_disabled {
        assert!(effective_mmap > 0);
    }
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' \
             AND name IN ('trades', 'market_resolutions', 'source_cursor')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        0,
        "configured v2 open must not recreate legacy owners"
    );
    assert_eq!(cache.schema_version().unwrap(), 2);
    assert!(
        cache.activity_aggregates_v2().unwrap().is_empty(),
        "sealed generation must not appear in the v2 typed read"
    );
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row("SELECT COUNT(*) FROM trades_v1_sealed", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1,
        "WAL-only committed row was lost"
    );
    drop(cache);

    let frozen_path = write_frozen_reference(&dir, watermark, vec![WALLET.to_owned()]);
    let verified = verify_frozen_payload_v1(&side, &frozen_path, watermark + 70).unwrap();
    assert_eq!(verified.active_wallets, vec![WALLET]);
    assert_eq!(verified.cross_generation_matches, 0);

    let activity_url = format!(
        "https://data.example/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end={watermark}"
    );
    let activity = populate_activity_v2(
        &side,
        &FixtureFetcher::new(HashMap::from([(activity_url, b"[]".to_vec())])),
        "https://data.example",
        &frozen_path,
        watermark,
        1,
        watermark + 80,
    )
    .await
    .unwrap();
    assert_eq!(activity.source_row_count, 0);
    assert_eq!(activity.group_count, 0);
    install_payout_manifest(&side);
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("cache-v2-final.json"),
        watermark + 90,
    )
    .unwrap();
    assert!(!side.with_extension("db-wal").exists());

    drop(seed_v1(&fixed, watermark - 100));
    let activation_request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: v1_backup.clone(),
        expected_side_sha256: stage.cache_sha256.clone(),
    };
    let mut interrupted_backup = v1_backup.as_os_str().to_owned();
    interrupted_backup.push(".pending");
    std::fs::write(
        std::path::PathBuf::from(interrupted_backup),
        b"partial-copy",
    )
    .unwrap();
    let (temporary_request, _) =
        write_pending_publication(&dir, "wrapper", &side, &side, &fixed, &v1_backup);
    let request_dir = data.join("eval-results/cron-wrapper");
    std::fs::create_dir(&request_dir).unwrap();
    let request_path = request_dir.join("ranking_publish_request.json");
    std::fs::rename(temporary_request, &request_path).unwrap();
    std::fs::write(
        request_dir.join("latency_shift_ranked.csv"),
        format!("wallet,survives\n{WALLET},true\n"),
    )
    .unwrap();
    let pending_path = data.join("eval-results/rank_and_push.pending");
    let relative_request = request_path.strip_prefix(dir.path()).unwrap();
    std::fs::write(&pending_path, format!("{}\n", relative_request.display())).unwrap();
    let scripts = dir.path().join("scripts");
    let release = dir.path().join("target/release");
    std::fs::create_dir(&scripts).unwrap();
    std::fs::create_dir_all(&release).unwrap();
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    std::fs::copy(
        repository.join("scripts/rank_and_push.sh"),
        scripts.join("rank_and_push.sh"),
    )
    .unwrap();
    std::fs::copy(
        repository.join("scripts/partial_backfill_wallets.py"),
        scripts.join("partial_backfill_wallets.py"),
    )
    .unwrap();
    std::fs::copy(
        repository.join("scripts/push_ranking_to_supabase.py"),
        scripts.join("push_ranking_to_supabase.py"),
    )
    .unwrap();
    std::os::unix::fs::symlink(
        env!("CARGO_BIN_EXE_pe-bootstrap"),
        release.join("pe-bootstrap"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join(".env"),
        format!(
            "SUPABASE_URL=https://fixture.invalid\nSUPABASE_SECRET_KEY=fixture-key\nPE_BOOTSTRAP_OUTPUT={}\n",
            dir.path().join("watchlist.json").display()
        ),
    )
    .unwrap();
    let python = Command::new("python3")
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .unwrap();
    assert!(python.status.success());
    let python = String::from_utf8(python.stdout).unwrap();
    let site = dir.path().join("python-fixture");
    std::fs::create_dir(&site).unwrap();
    std::fs::write(
        site.join("sitecustomize.py"),
        r#"import json
import urllib.request
class Response:
    status = 200
    def __init__(self, value): self.value = value
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self): return json.dumps(self.value).encode()
def urlopen(request, timeout=30):
    url = request.full_url
    if "/rpc/publish_ranking_batch" in url: return Response(7)
    if "/latest_ranking" in url:
        return Response([{"batch_id": 7, "rank": 1, "survives": None}])
    if "/ranking_batches?" in url: return Response([])
    raise RuntimeError("unexpected fixture URL: " + url)
urllib.request.urlopen = urlopen
"#,
    )
    .unwrap();
    let wrapper = Command::new("bash")
        .args([
            "-c",
            "exec 9<>data/eval-results/.rank_and_push_loop.lock; flock -n 9; \
             printf '%s\\n' \"$$\" > data/eval-results/.rank_and_push_loop.lock; \
             exec bash scripts/rank_and_push.sh --resume-pending",
        ])
        .current_dir(dir.path())
        .env("PE_PYTHON", python.trim())
        .env("PYTHONPATH", &site)
        .output()
        .unwrap();
    assert!(
        wrapper.status.success(),
        "real wrapper/bootstrap activation failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&wrapper.stdout),
        String::from_utf8_lossy(&wrapper.stderr)
    );
    let activation_json = String::from_utf8_lossy(&wrapper.stdout)
        .lines()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
        .unwrap();
    assert_eq!(activation_json["installed_sha256"], stage.cache_sha256);
    assert_eq!(activation_json["resumed"], false);
    assert_eq!(
        activation_json["activation_evidence"]["activation_ready"],
        true
    );
    assert!(!pending_path.exists());

    let resumed = activate_cache_v2(&activation_request).unwrap();
    assert!(resumed.resumed);
    assert!(resumed.activation_evidence.is_none());
    assert!(v1_backup.is_file());
    assert!(!fixed.with_extension("db-wal").exists());
    assert_eq!(
        WalletCache::open(&fixed).unwrap().schema_version().unwrap(),
        2
    );
    let first_binding = PriorCacheBinding {
        sha256: resumed.prior_cache_sha256,
        schema_version: resumed.prior_cache_schema,
    };

    let next_side = dir.path().join("wallet_cache.next.v2.db");
    std::fs::copy(&fixed, &next_side).unwrap();
    let next = Connection::open(&next_side).unwrap();
    next.execute(
        "UPDATE cache_v2_migration_state SET updated_at_unix = updated_at_unix + 1",
        [],
    )
    .unwrap();
    next.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(next);
    let next_hash = sha256_file(&next_side).unwrap();
    let prior_v2 = dir.path().join("wallet_cache.prior.v2.db");
    let v2_to_v2 = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: next_side,
        prior_cache_backup_path: prior_v2.clone(),
        expected_side_sha256: next_hash,
    })
    .unwrap();
    assert_eq!(v2_to_v2.prior_cache_schema, 2);
    assert!(v2_to_v2.activation_evidence.is_none());
    let v2_binding = PriorCacheBinding {
        sha256: v2_to_v2.prior_cache_sha256,
        schema_version: v2_to_v2.prior_cache_schema,
    };
    let current_hash = sha256_file(&fixed).unwrap();
    let consumed_side = dir.path().join("consumed-side.db");
    let (v2_request, v2_pending) =
        write_pending_publication(&dir, "v2", &consumed_side, &fixed, &fixed, &prior_v2);
    let refused = restore_prior_cache(
        &fixed,
        &prior_v2,
        &dir.path().join("must-not-exist.db"),
        &v2_binding,
        &v2_request,
        &v2_pending,
        &FixedPublicationProbe(true),
    )
    .await
    .unwrap_err();
    assert!(refused.to_string().contains("publication was consumed"));
    assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
    let unavailable = restore_prior_cache(
        &fixed,
        &prior_v2,
        &dir.path().join("must-not-exist.db"),
        &v2_binding,
        &v2_request,
        &v2_pending,
        &UnavailablePublicationProbe,
    )
    .await
    .unwrap_err();
    assert!(unavailable.to_string().contains("authority unavailable"));
    assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
    let displaced_next = dir.path().join("wallet_cache.displaced.next.v2.db");
    restore_prior_cache(
        &fixed,
        &prior_v2,
        &displaced_next,
        &v2_binding,
        &v2_request,
        &v2_pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap();
    assert_eq!(
        WalletCache::open(&fixed).unwrap().schema_version().unwrap(),
        2
    );

    let displaced_v2 = dir.path().join("wallet_cache.displaced.v2.db");
    let (v1_request, v1_pending) =
        write_pending_publication(&dir, "v1", &consumed_side, &fixed, &fixed, &v1_backup);
    restore_prior_cache(
        &fixed,
        &v1_backup,
        &displaced_v2,
        &first_binding,
        &v1_request,
        &v1_pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap();
    assert_eq!(
        WalletCache::open(&fixed).unwrap().schema_version().unwrap(),
        1
    );
    assert_eq!(
        WalletCache::open(&displaced_v2)
            .unwrap()
            .schema_version()
            .unwrap(),
        2
    );
    assert!(!fixed.with_extension("db-wal").exists());
}

#[test]
fn migration_refuses_reclamation_marker_missing_index_and_tampered_hash() {
    let watermark = 1_800_000_000_i64;
    for defect in ["marker", "index", "hash", "wal_unbound"] {
        let dir = TempDir::new().unwrap();
        let side = dir.path().join(format!("{defect}.db"));
        let cache = seed_v1(&side, watermark);
        match defect {
            "marker" => {
                cache
                    .raw_conn_for_test()
                    .execute(
                        "INSERT INTO meta (key, value) VALUES ('reclamation_pending', '1')",
                        [],
                    )
                    .unwrap();
            }
            "index" => {
                cache
                    .raw_conn_for_test()
                    .execute("DROP INDEX idx_trades_market_id", [])
                    .unwrap();
            }
            "hash" => {}
            "wal_unbound" => {}
            _ => unreachable!(),
        }
        let manifest = write_build_manifest(&dir, &side);
        if defect == "hash" {
            cache
                .raw_conn_for_test()
                .execute(
                    "INSERT INTO wallets (wallet_hex, is_active) VALUES (?1, 0)",
                    params!["0x2222222222222222222222222222222222222222"],
                )
                .unwrap();
            cache
                .raw_conn_for_test()
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }
        if defect == "wal_unbound" {
            let mut value: CacheV2BuildManifest =
                serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
            value.hashes.remove("backup_wal_sha256");
            std::fs::write(&manifest, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        }
        let error = migrate_cache_v2(&side, &manifest).unwrap_err().to_string();
        match defect {
            "marker" => assert!(error.contains("reclamation_pending"), "{error}"),
            "index" => assert!(error.contains("required trades indexes"), "{error}"),
            "hash" => assert!(error.contains("online-backup hash mismatch"), "{error}"),
            "wal_unbound" => assert!(error.contains("uncheckpointed WAL frames"), "{error}"),
            _ => unreachable!(),
        }
        drop(cache);
    }
}

#[test]
fn migration_refuses_a_busy_nonzero_checkpoint() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("busy.db");
    let watermark = 1_800_000_000_i64;
    drop(seed_v1(&side, watermark));

    let reader = Connection::open(&side).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    reader
        .query_row("SELECT COUNT(*) FROM trades", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let writer = Connection::open(&side).unwrap();
    writer
        .execute(
            "INSERT INTO wallets (wallet_hex, is_active) VALUES (?1, 0)",
            params!["0x2222222222222222222222222222222222222222"],
        )
        .unwrap();
    drop(writer);
    let manifest = write_build_manifest(&dir, &side);

    let error = migrate_cache_v2(&side, &manifest).unwrap_err().to_string();
    assert!(error.contains("checkpoint incomplete: busy="), "{error}");
    drop(reader);
}

#[test]
fn activation_refuses_a_side_main_changed_after_finalization() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    let fixed = dir.path().join("wallet_cache.db");
    let backup = dir.path().join("v1.db");
    let watermark = 1_800_000_000_i64;
    drop(seed_v1(&side, watermark));
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();
    let expected = sha256_file(&side).unwrap();
    Connection::open(&side)
        .unwrap()
        .execute(
            "UPDATE cache_v2_migration_state SET updated_at_unix = updated_at_unix + 1",
            [],
        )
        .unwrap();
    drop(seed_v1(&fixed, watermark));
    let error = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side,
        prior_cache_backup_path: backup,
        expected_side_sha256: expected,
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("hash changed"), "{error}");
    assert_eq!(
        WalletCache::open(&fixed).unwrap().schema_version().unwrap(),
        1
    );
}

/// PASS: a post-finalization commit retained only in an open side WAL is rejected before the
/// immutable side is opened or checkpointed, and the fixed cache remains byte-for-byte unchanged.
/// FAIL: activation checkpoints the unmanifested table, replaces the fixed cache, or reports the
/// stale-evidence failure only after replacement.
#[tokio::test]
async fn activation_rejects_unmanifested_side_wal_before_replacing_fixed_cache() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    let fixed = dir.path().join("wallet_cache.db");
    let backup = dir.path().join("v1.db");
    let watermark = 1_800_000_000_i64;
    let expected = finalize_empty_activity_side(&dir, &side, watermark).await;
    drop(seed_v1(&fixed, watermark - 100));
    let fixed_before = sha256_file(&fixed).unwrap();

    let wal_owner = Connection::open(&side).unwrap();
    wal_owner
        .execute_batch(
            "PRAGMA wal_autocheckpoint = 0;
             BEGIN IMMEDIATE;
             CREATE TABLE unmanifested_activation_write(value TEXT NOT NULL);
             INSERT INTO unmanifested_activation_write(value) VALUES ('stale');
             COMMIT;",
        )
        .unwrap();
    let wal = side.with_extension("db-wal");
    let shm = side.with_extension("db-shm");
    assert!(wal.metadata().unwrap().len() > 0);
    assert!(shm.metadata().unwrap().len() > 0);
    assert_eq!(sha256_file(&side).unwrap(), expected);

    let error = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: backup,
        expected_side_sha256: expected,
    })
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("non-empty SQLite activation sidecar"),
        "{error}"
    );
    assert_eq!(sha256_file(&fixed).unwrap(), fixed_before);
    assert_eq!(
        WalletCache::open(&fixed).unwrap().schema_version().unwrap(),
        1
    );
    assert!(side.is_file());
    drop(wal_owner);
}

#[tokio::test]
async fn v2_payout_walk_never_touches_sealed_resolution_or_cursor_rows() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let watermark = 1_800_000_000_i64;
    drop(seed_v1(&side, watermark));
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();

    let responses = HashMap::from([(
        "https://clob.example/markets?closed=true&limit=1000".to_owned(),
        br#"{"data":[{"condition_id":"0xsecondwalk","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"1","winner":true},{"token_id":"2","winner":false}]}],"next_cursor":"LTE="}"#.to_vec(),
    )]);
    let fetcher = ClobFetcher::new(
        "https://clob.example".to_owned(),
        FixtureFetcher::new(responses),
    );
    let mut cache = WalletCache::open(&side).unwrap();
    let report = fetcher.fetch_closed_markets(&mut cache).await.unwrap();
    assert!(report.coverage_manifest.is_some());
    assert!(
        cache
            .clob_payout_evidence_v2("0xsecondwalk")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        cache
            .clob_payout_evidence_v2("0xsecondwalk")
            .unwrap()
            .unwrap()
            .end_date_unix,
        Some(1_730_678_400)
    );
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row(
                "SELECT COUNT(*) FROM market_resolutions_v1_sealed",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .unwrap(),
        1
    );
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row(
                "SELECT value FROM source_cursor_v1_sealed WHERE key = 'clob_closed'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        ""
    );
}

#[tokio::test]
async fn v2_activity_population_is_complete_idempotent_and_generation_isolated() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let fixed_end = 1_800_000_000_i64;
    drop(seed_v1(&side, fixed_end - 10));
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();
    let frozen_path = write_frozen_reference(&dir, fixed_end - 10, vec![WALLET.to_owned()]);

    let url = format!(
        "https://data.example/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end={fixed_end}"
    );
    // Source order is newest first. The causal replay is therefore split ->
    // merge -> entry -> funded redemption -> RequiresAnchor -> later buy. The
    // final buy has complete payout evidence and would be projected if the
    // wallet fence were accidentally treated as a no-op.
    let body = format!(
        r#"[
          {{"proxyWallet":"{WALLET}","type":"TRADE","conditionId":"0xafter","asset":"789","outcome":"No","side":"BUY","size":"1","usdcSize":"0.4","price":"0.4","timestamp":{},"transactionHash":"0xafter","outcomeIndex":"1"}},
          {{"proxyWallet":"{WALLET}","type":"REDEEM","conditionId":"0xanchor","asset":"","side":"","size":"0","usdcSize":"0","price":"0","timestamp":{},"transactionHash":"0xanchor","outcomeIndex":"0"}},
          {{"proxyWallet":"{WALLET}","type":"REDEEM","conditionId":"0xcondition","asset":"123","outcome":"Yes","side":"","size":"1","usdcSize":"1","price":"1","timestamp":{},"transactionHash":"0xredeem","outcomeIndex":"0"}},
          {{"proxyWallet":"{WALLET}","type":"TRADE","conditionId":"0xcondition","asset":"123","outcome":"Yes","side":"BUY","size":"5.25","usdcSize":"2.625","price":"0.5","timestamp":{},"transactionHash":"0xabc","outcomeIndex":"0"}},
          {{"proxyWallet":"{WALLET}","type":"MERGE","conditionId":"0xeffects","asset":"","side":"","size":"2","usdcSize":"2","price":"1","timestamp":{},"transactionHash":"0xmerge"}},
          {{"proxyWallet":"{WALLET}","type":"SPLIT","conditionId":"0xeffects","asset":"","side":"","size":"2","usdcSize":"2","price":"1","timestamp":{},"transactionHash":"0xsplit"}}
        ]"#,
        fixed_end - 1,
        fixed_end - 2,
        fixed_end - 3,
        fixed_end - 4,
        fixed_end - 5,
        fixed_end - 6,
    );
    let fetcher = FixtureFetcher::new(HashMap::from([(url, body.into_bytes())]));
    let first = populate_activity_v2(
        &side,
        &fetcher,
        "https://data.example",
        &frozen_path,
        fixed_end,
        7,
        fixed_end + 1,
    )
    .await
    .unwrap();
    assert_eq!(first.generation, 7);
    let second = populate_activity_v2(
        &side,
        &fetcher,
        "https://data.example",
        &frozen_path,
        fixed_end,
        7,
        fixed_end + 1,
    )
    .await
    .unwrap();
    assert_eq!(second, first);

    let staging = Connection::open(&side).unwrap();
    assert_eq!(
        staging
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(staging);
    let payout_fetcher = ClobFetcher::new(
        "https://clob.example".to_owned(),
        FixtureFetcher::new(HashMap::from([(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[{"condition_id":"0xcondition","active":true,"closed":true,"end_date_iso":"2027-01-16T00:00:00Z","is_50_50_outcome":false,"tokens":[{"token_id":"123","outcome":"Yes","price":1,"winner":true},{"token_id":"456","outcome":"No","price":0,"winner":false}]},{"condition_id":"0xafter","active":true,"closed":true,"end_date_iso":"2027-01-17T00:00:00Z","is_50_50_outcome":true,"tokens":[{"token_id":"321","outcome":"Yes","price":"0.5","winner":false},{"token_id":"789","outcome":"No","price":"0.5","winner":false}]}],"next_cursor":"LTE="}"#.to_vec(),
        )])),
    );
    payout_fetcher
        .fetch_closed_markets(&mut WalletCache::open(&side).unwrap())
        .await
        .unwrap();
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("activity-final.json"),
        fixed_end + 2,
    )
    .unwrap();
    assert_eq!(stage.version, 2);
    assert_eq!(stage.ranker_projection_count, 1);

    let cache = WalletCache::open(&side).unwrap();
    let active = cache.activity_aggregates_v2().unwrap();
    assert_eq!(active.len(), 6);
    assert!(
        active
            .iter()
            .all(|aggregate| aggregate.source_trade_id.0.starts_with("g2:"))
    );
    let projected: (String, String, String, i64, i64) = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT groups_v2.share_amount_str,
                    groups_v2.price_weighted_share_amount_str,
                    groups_v2.source_usdc_amount_str,
                    payout.end_date_unix,
                    ranker.classifier_version
             FROM ranker_entries_v2 ranker
             JOIN activity_groups_v2 groups_v2 USING (source_trade_id)
             JOIN clob_payout_evidence_v2 payout ON payout.market_id = groups_v2.condition_id
             WHERE groups_v2.condition_id = '0xcondition'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(projected.0, "5.25");
    assert_eq!(projected.1, "2.625");
    assert_eq!(projected.2, "2.625");
    assert_eq!(projected.3, 1_800_057_600);
    assert_eq!(projected.4, 2);
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row(
                "SELECT COUNT(*) FROM ranker_entries_v2 ranker
                 JOIN activity_groups_v2 groups_v2 USING (source_trade_id)
                 WHERE groups_v2.condition_id = '0xafter'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "RequiresAnchor must stop every later bucket for the wallet"
    );
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "activity staging must be deleted atomically with its manifest"
    );
    assert_eq!(
        cache
            .raw_conn_for_test()
            .query_row("SELECT COUNT(*) FROM trades_v1_sealed", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

const CLASSIFIER_FIXED_END: i64 = 1_800_000_000;

async fn retained_classifier_activity(dir: &TempDir, side: &std::path::Path) -> std::path::PathBuf {
    retained_classifier_activity_at_version(dir, side, 2).await
}

async fn retained_classifier_activity_at_version(
    dir: &TempDir,
    side: &std::path::Path,
    classifier_version: u32,
) -> std::path::PathBuf {
    let fixed_end = CLASSIFIER_FIXED_END;
    drop(seed_v1(side, fixed_end - 10));
    let manifest = write_build_manifest(dir, side);
    migrate_cache_v2(side, &manifest).unwrap();
    let frozen = write_frozen_reference(dir, fixed_end - 10, vec![WALLET.to_owned()]);
    let row = |kind: &str, id: &str, market: &str, epoch: i64| {
        serde_json::json!({
            "proxyWallet": WALLET, "type": kind, "conditionId": market, "asset": "123",
            "outcome": "Yes", "side": "BUY", "size": if kind == "CONVERSION" { "0" } else { "1" },
            "usdcSize": if kind == "CONVERSION" { "0" } else { "0.5" }, "price": "0.5",
            "timestamp": epoch, "transactionHash": id, "outcomeIndex": "0",
        })
    };
    let mut rows = vec![
        row("TRADE", "0xlater-b", "0xlater-b", fixed_end - 1),
        row("TRADE", "0xlater-a", "0xlater-a", fixed_end - 2),
    ];
    if classifier_version == 2 {
        rows.extend(
            (0..5).map(|index| row("TRADE", &format!("0xwide-{index}"), "0xwide", fixed_end - 3)),
        );
        rows.push(row("CONVERSION", "0xzero", "0xconversion", fixed_end - 4));
    }
    let raw = serde_json::to_vec(&rows).unwrap();
    let url = format!(
        "https://data.example/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end={fixed_end}"
    );
    populate_activity_v2(
        side,
        &FixtureFetcher::new(HashMap::from([(url, raw.clone())])),
        "https://data.example",
        &frozen,
        fixed_end,
        7,
        fixed_end + 1,
    )
    .await
    .unwrap();
    let markets = ["0xlater-a", "0xlater-b"].map(|market| {
        serde_json::json!({
            "condition_id": market, "active": true, "closed": true,
            "end_date_iso": "2027-01-16T00:00:00Z", "is_50_50_outcome": false,
            "tokens": [{"token_id":"123","outcome":"Yes","price":1,"winner":true},
                       {"token_id":"456","outcome":"No","price":0,"winner":false}],
        })
    });
    ClobFetcher::new(
        "https://clob.example".to_owned(),
        FixtureFetcher::new(HashMap::from([(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            serde_json::to_vec(&serde_json::json!({"data": markets, "next_cursor": "LTE="}))
                .unwrap(),
        )])),
    )
    .fetch_closed_markets(&mut WalletCache::open(side).unwrap())
    .await
    .unwrap();
    if classifier_version == 1 {
        // These two singleton BUY buckets have identical projections in both
        // generations. Prove the expected source IDs with the retained legacy
        // classifier before asking the normal finalizer to certify them.
        let wallet = WalletAddress::from_hex(WALLET).unwrap();
        let observed = time::OffsetDateTime::from_unix_timestamp(fixed_end + 1).unwrap();
        let mut aggregates = parse_activity_response(
            &raw,
            wallet,
            &ActivityParseContext {
                source_id: SourceId("polymarket-data-api".to_owned()),
                observed_at: SourceTimestamp(observed),
                received_at: ReceivedAt(observed),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap()
        .aggregates()
        .unwrap();
        aggregates.sort_by_key(|aggregate| aggregate.source_time.0);
        let mut ledger = PositionLedger::new();
        let mut expected = Vec::new();
        for aggregate in aggregates {
            let mutation = LedgerMutation::from_activity(&aggregate).unwrap();
            let SecondVerdict::OrderIndependent { decisions, .. } =
                classify_complete_second_legacy(
                    &ledger,
                    wallet,
                    std::slice::from_ref(&mutation),
                    ReconstructionQuality::new(100).unwrap(),
                    &Default::default(),
                    true,
                    &|_| false,
                )
                .unwrap()
            else {
                panic!("legacy singleton projection refused");
            };
            assert_eq!(decisions.len(), 1);
            assert_eq!(decisions[0].entry, EntryClassification::Admitted);
            expected.push((decisions[0].source_trade_id.0.clone(), 7, 1));
            ledger.apply(&mutation).unwrap();
        }
        expected.sort();
        assert_eq!(expected.len(), 2);
        let connection = Connection::open(side).unwrap();
        // Insert classifier-one rows from the outset, before the finalizer
        // computes their digest. Never relabel an already-built projection.
        // The state trigger retains the finalizer's real count and digest.
        connection
            .execute_batch(
                "CREATE TRIGGER classifier_one_projection BEFORE INSERT ON ranker_entries_v2
             WHEN NEW.classifier_version = 2
             BEGIN
                 INSERT INTO ranker_entries_v2
                     (source_trade_id, activity_generation, classifier_version)
                 VALUES (NEW.source_trade_id, NEW.activity_generation, 1);
                 SELECT RAISE(IGNORE);
             END;
             CREATE TRIGGER classifier_one_state BEFORE UPDATE OF ranker_classifier_version
             ON cache_v2_migration_state WHEN NEW.ranker_classifier_version = 2
             BEGIN
                 UPDATE cache_v2_migration_state SET phase = NEW.phase,
                     ranker_projection_count = NEW.ranker_projection_count,
                     ranker_projection_digest = NEW.ranker_projection_digest,
                     ranker_classifier_version = 1, updated_at_unix = NEW.updated_at_unix
                 WHERE singleton = NEW.singleton;
                 SELECT RAISE(IGNORE);
             END;",
            )
            .unwrap();
        drop(connection);
        let stage =
            finalize_cache_v2(side, &dir.path().join("initial-stage.json"), fixed_end + 2).unwrap();
        assert_eq!(stage.ranker_projection_count, 2);
        assert_eq!(classifier_projection_rows(side), expected);
        let connection = Connection::open(side).unwrap();
        assert_eq!(connection.query_row(
            "SELECT ranker_classifier_version, ranker_projection_count, ranker_projection_digest
             FROM cache_v2_migration_state WHERE phase = 'finalized'", [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
        ).unwrap(), (1, 2, stage.ranker_projection_digest));
        connection
            .execute_batch(
                "DROP TRIGGER classifier_one_projection;
             DROP TRIGGER classifier_one_state;",
            )
            .unwrap();
    } else {
        finalize_cache_v2(side, &dir.path().join("initial-stage.json"), fixed_end + 2).unwrap();
    }
    frozen
}

fn classifier_projection_rows(path: &std::path::Path) -> Vec<(String, i64, i64)> {
    Connection::open(path)
        .unwrap()
        .prepare(
            "SELECT source_trade_id, activity_generation, classifier_version
         FROM ranker_entries_v2 ORDER BY source_trade_id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn retained_activity_rows(
    path: &std::path::Path,
) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let connection = Connection::open(path).unwrap();
    ["activity_groups_v2", "activity_coverage_manifests_v2"]
        .into_iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get(index))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (table.to_owned(), rows)
        })
        .collect()
}

// Build the authentic classifier-one result for this retained history: its first
// zero conversion refused, so no later entry was projected. Preserve all source proof.
fn retain_legacy_empty_projection(path: &std::path::Path) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute("DELETE FROM ranker_entries_v2", [])
        .unwrap();
    connection
        .execute(
            "UPDATE cache_v2_migration_state SET ranker_projection_count = 0,
        ranker_projection_digest = ?1, ranker_classifier_version = 1 WHERE singleton = 1",
            params![format!("{:x}", Sha256::digest(b"[]"))],
        )
        .unwrap();
}

async fn assert_no_activity_recollection(side: &std::path::Path, frozen: &std::path::Path) {
    let fetcher = YieldingFetcher::default();
    populate_activity_v2(
        side,
        &fetcher,
        "https://data.example",
        frozen,
        CLASSIFIER_FIXED_END,
        7,
        CLASSIFIER_FIXED_END + 3,
    )
    .await
    .unwrap();
    assert!(fetcher.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn classifier_v2_rebuilds_retained_activity_without_recollection() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let frozen = retained_classifier_activity(&dir, &side).await;
    retain_legacy_empty_projection(&side);
    let retained = retained_activity_rows(&side);
    let connection = Connection::open(&side).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER abort_projection_rebuild BEFORE INSERT ON ranker_entries_v2
        BEGIN SELECT RAISE(ABORT, 'forced classifier rebuild crash'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(
        finalize_cache_v2(
            &side,
            &dir.path().join("failed-rebuild-stage.json"),
            CLASSIFIER_FIXED_END + 3
        )
        .is_err()
    );
    let connection = Connection::open(&side).unwrap();
    assert_eq!(connection.query_row("SELECT ranker_classifier_version, ranker_projection_count FROM cache_v2_migration_state", [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).unwrap(), (1, 0));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM ranker_entries_v2", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    connection
        .execute_batch("DROP TRIGGER abort_projection_rebuild")
        .unwrap();
    drop(connection);
    assert_eq!(retained_activity_rows(&side), retained);
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("rebuilt-stage.json"),
        CLASSIFIER_FIXED_END + 3,
    )
    .unwrap();
    assert_eq!(stage.version, 2);
    assert_eq!(stage.ranker_classifier_version, 2);
    assert_eq!(stage.ranker_projection_count, 2);
    assert_eq!(stage.cache_sha256, sha256_file(&side).unwrap());
    let connection = Connection::open(&side).unwrap();
    let rows = connection
        .prepare(
            "SELECT groups_v2.condition_id, ranker.classifier_version
        FROM ranker_entries_v2 ranker JOIN activity_groups_v2 groups_v2 USING (source_trade_id)
        ORDER BY groups_v2.condition_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![("0xlater-a".to_owned(), 2), ("0xlater-b".to_owned(), 2)]
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT ranker_classifier_version FROM cache_v2_migration_state",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    drop(connection);
    assert_eq!(retained_activity_rows(&side), retained);
    assert_no_activity_recollection(&side, &frozen).await;
}

#[tokio::test]
async fn stale_classifier_projection_cannot_be_certified() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    let side = dir.path().join("side.db");
    retained_classifier_activity(&dir, &fixed).await;
    let frozen = retained_classifier_activity(&dir, &side).await;
    // Old certification alone cannot install as a current candidate, even with
    // authentic count/digest proof for its empty legacy projection.
    retain_legacy_empty_projection(&side);
    let request = |hash| CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: dir.path().join(format!("prior-{hash}.db")),
        expected_side_sha256: hash,
    };
    let error = activate_cache_v2(&request(sha256_file(&side).unwrap())).unwrap_err();
    assert!(
        error.to_string().contains("frozen/activity/ranker proof"),
        "{error}"
    );
    finalize_cache_v2(
        &side,
        &dir.path().join("rebuilt-stage.json"),
        CLASSIFIER_FIXED_END + 3,
    )
    .unwrap();
    // Historical classifier 1 is permitted, but rows still certified by their
    // unchanged classifier-2 digest cannot be relabeled by changing only state.
    let connection = Connection::open(&fixed).unwrap();
    connection
        .execute(
            "UPDATE cache_v2_migration_state SET ranker_classifier_version = 1",
            [],
        )
        .unwrap();
    drop(connection);
    let error = activate_cache_v2(&request(sha256_file(&side).unwrap())).unwrap_err();
    assert!(
        error.to_string().contains("frozen/activity/ranker proof"),
        "{error}"
    );
    finalize_cache_v2(
        &fixed,
        &dir.path().join("fixed-rebuilt-stage.json"),
        CLASSIFIER_FIXED_END + 3,
    )
    .unwrap();
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("accepted-stage.json"),
        CLASSIFIER_FIXED_END + 3,
    )
    .unwrap();
    assert_no_activity_recollection(&side, &frozen).await;
    assert_eq!(
        activate_cache_v2(&request(stage.cache_sha256.clone()))
            .unwrap()
            .installed_sha256,
        stage.cache_sha256
    );
}

#[tokio::test]
async fn null_classifier_state_cannot_be_certified_with_empty_or_nonempty_projection() {
    for empty_projection in [true, false] {
        for historical in [true, false] {
            let dir = TempDir::new().unwrap();
            std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
            let fixed = dir.path().join("fixed.db");
            let side = dir.path().join("side.db");
            retained_classifier_activity(&dir, &fixed).await;
            retained_classifier_activity(&dir, &side).await;
            let target = if historical { &fixed } else { &side };
            if empty_projection {
                retain_legacy_empty_projection(target);
            }
            let connection = Connection::open(target).unwrap();
            connection
                .execute(
                    "UPDATE cache_v2_migration_state SET ranker_classifier_version = NULL",
                    [],
                )
                .unwrap();
            assert_eq!(connection.query_row(
                "SELECT ranker_classifier_version, ranker_projection_count FROM cache_v2_migration_state",
                [], |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, i64>(1)?))
            ).unwrap(), (None, if empty_projection { 0 } else { 2 }));
            drop(connection);
            assert_eq!(
                classifier_projection_rows(target).is_empty(),
                empty_projection
            );
            let fixed_hash = sha256_file(&fixed).unwrap();
            let side_hash = sha256_file(&side).unwrap();
            let error = activate_cache_v2(&CacheActivationRequest {
                fixed_path: fixed.clone(),
                side_path: side.clone(),
                prior_cache_backup_path: dir.path().join("prior.db"),
                expected_side_sha256: side_hash.clone(),
            })
            .unwrap_err();
            assert!(
                error.to_string().contains("frozen/activity/ranker proof"),
                "{error}"
            );
            assert_eq!(sha256_file(&fixed).unwrap(), fixed_hash);
            assert_eq!(sha256_file(&side).unwrap(), side_hash);
        }
    }
}

#[tokio::test]
async fn missing_side_resume_rejects_classifier_one_installed_cache() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    let side = dir.path().join("missing-side.db");
    let prior = dir.path().join("prior.db");
    retained_classifier_activity_at_version(&dir, &fixed, 1).await;
    std::fs::copy(&fixed, &prior).unwrap();
    assert!(!side.exists());
    let fixed_hash = sha256_file(&fixed).unwrap();
    let error = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: prior.clone(),
        expected_side_sha256: fixed_hash.clone(),
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("frozen/activity/ranker proof"),
        "{error}"
    );
    assert_eq!(sha256_file(&fixed).unwrap(), fixed_hash);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_hash);
    assert!(!side.exists());
}

#[tokio::test]
async fn refinalization_resumes_after_commit_before_stage_receipt() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let frozen = retained_classifier_activity(&dir, &side).await;
    retain_legacy_empty_projection(&side);
    let retained = retained_activity_rows(&side);
    let blocked_parent = dir.path().join("stage-parent-is-file");
    std::fs::write(&blocked_parent, b"blocks stage creation").unwrap();
    let blocked_stage = blocked_parent.join("stage.json");
    assert!(finalize_cache_v2(&side, &blocked_stage, CLASSIFIER_FIXED_END + 3).is_err());
    assert!(!blocked_stage.exists());
    let connection = Connection::open(&side).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT ranker_classifier_version, ranker_projection_count,
        phase FROM cache_v2_migration_state",
                [],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?
                ))
            )
            .unwrap(),
        (2, 2, "finalized".to_owned())
    );
    drop(connection);
    let stage_path = dir.path().join("resumed-stage.json");
    let stage = finalize_cache_v2(&side, &stage_path, CLASSIFIER_FIXED_END + 3).unwrap();
    assert_eq!(stage.cache_sha256, sha256_file(&side).unwrap());
    let receipt: Value = serde_json::from_slice(&std::fs::read(stage_path).unwrap()).unwrap();
    assert_eq!(receipt["cache_sha256"], stage.cache_sha256);
    assert_eq!(receipt["ranker_classifier_version"], 2);
    assert_eq!(retained_activity_rows(&side), retained);
    assert_no_activity_recollection(&side, &frozen).await;
}

#[tokio::test]
async fn classifier_upgrade_activation_preserves_authentic_prior_cache() {
    let dir = tempfile::Builder::new()
        .prefix("pe-classifier-upgrade-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    let side = dir.path().join("side.db");
    retained_classifier_activity_at_version(&dir, &fixed, 1).await;
    let prior_hash = sha256_file(&fixed).unwrap();
    let prior_bytes = std::fs::read(&fixed).unwrap();
    let prior_rows = retained_activity_rows(&fixed);
    let prior_projection = classifier_projection_rows(&fixed);
    assert_eq!(prior_projection.len(), 2);
    assert!(prior_projection.iter().all(|row| row.2 == 1));
    // Upgrade a copy of exactly the same source history through the normal finalizer.
    std::fs::copy(&fixed, &side).unwrap();
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("upgraded-stage.json"),
        CLASSIFIER_FIXED_END + 3,
    )
    .unwrap();
    assert_eq!(stage.ranker_classifier_version, 2);
    assert_eq!(stage.ranker_projection_count, 2);
    let upgraded_bytes = std::fs::read(&side).unwrap();
    let upgraded_projection = classifier_projection_rows(&side);
    assert!(upgraded_projection.iter().all(|row| row.2 == 2));
    let request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: dir.path().join("prior.db"),
        expected_side_sha256: stage.cache_sha256.clone(),
    };
    let installed = activate_cache_v2(&request).unwrap();
    assert!(!installed.resumed);
    assert_eq!(installed.prior_cache_schema, 2);
    assert_eq!(installed.prior_cache_sha256, prior_hash);
    assert_eq!(
        sha256_file(&request.prior_cache_backup_path).unwrap(),
        prior_hash
    );
    assert_eq!(
        retained_activity_rows(&request.prior_cache_backup_path),
        prior_rows
    );
    assert_eq!(
        classifier_projection_rows(&request.prior_cache_backup_path),
        prior_projection
    );
    assert_eq!(
        std::fs::read(&request.prior_cache_backup_path).unwrap(),
        prior_bytes
    );
    let prior = Connection::open(&request.prior_cache_backup_path).unwrap();
    assert_eq!(
        prior
            .query_row(
                "SELECT ranker_classifier_version FROM cache_v2_migration_state",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    drop(prior);
    let resumed = activate_cache_v2(&request).unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.installed_sha256, stage.cache_sha256);
    assert_eq!(resumed.prior_cache_sha256, prior_hash);
    assert_eq!(
        sha256_file(&request.prior_cache_backup_path).unwrap(),
        prior_hash
    );
    let binding = PriorCacheBinding {
        sha256: installed.prior_cache_sha256,
        schema_version: installed.prior_cache_schema,
    };
    let (publication_request, pending) = write_pending_publication(
        &dir,
        "classifier-upgrade",
        &side,
        &fixed,
        &fixed,
        &request.prior_cache_backup_path,
    );
    let displaced = dir.path().join("displaced-classifier-2.db");
    restore_prior_cache(
        &fixed,
        &request.prior_cache_backup_path,
        &displaced,
        &binding,
        &publication_request,
        &pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap();
    assert_eq!(sha256_file(&fixed).unwrap(), prior_hash);
    assert_eq!(std::fs::read(&fixed).unwrap(), prior_bytes);
    assert_eq!(classifier_projection_rows(&fixed), prior_projection);
    assert_eq!(retained_activity_rows(&fixed), prior_rows);
    assert_eq!(
        Connection::open(&fixed)
            .unwrap()
            .query_row(
                "SELECT ranker_classifier_version FROM cache_v2_migration_state",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(sha256_file(&displaced).unwrap(), stage.cache_sha256);
    assert_eq!(std::fs::read(&displaced).unwrap(), upgraded_bytes);
    assert_eq!(classifier_projection_rows(&displaced), upgraded_projection);
    assert_eq!(retained_activity_rows(&displaced), prior_rows);

    let fresh_side = dir.path().join("fresh-side.db");
    std::fs::copy(&fixed, &fresh_side).unwrap();
    let fresh_stage = finalize_cache_v2(
        &fresh_side,
        &dir.path().join("fresh-stage.json"),
        CLASSIFIER_FIXED_END + 4,
    )
    .unwrap();
    assert_eq!(fresh_stage.ranker_classifier_version, 2);
    let fresh_request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: fresh_side,
        prior_cache_backup_path: dir.path().join("fresh-prior.db"),
        expected_side_sha256: fresh_stage.cache_sha256.clone(),
    };
    let fresh_install = activate_cache_v2(&fresh_request).unwrap();
    assert!(!fresh_install.resumed);
    assert_eq!(fresh_install.installed_sha256, fresh_stage.cache_sha256);
    assert_eq!(fresh_install.prior_cache_sha256, prior_hash);
    assert_eq!(sha256_file(&fixed).unwrap(), fresh_stage.cache_sha256);
    assert_eq!(classifier_projection_rows(&fixed), upgraded_projection);
    assert_eq!(retained_activity_rows(&fixed), prior_rows);
    assert_eq!(
        std::fs::read(&fresh_request.prior_cache_backup_path).unwrap(),
        prior_bytes
    );
    assert_eq!(std::fs::read(&displaced).unwrap(), upgraded_bytes);
}

/// PASS: the activity fan-out reaches but never exceeds 16 in-flight wallets;
/// a receipt-insert crash rolls back its wallet transaction, restart fetches
/// only an exactly missing wallet, malformed receipt identity/digest fail, and
/// manifest installation plus staging deletion is atomic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_wallet_receipts_bound_resume_and_finalize_atomically() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let fixed_end = 1_800_000_000_i64;
    let mut cache = seed_v1(&side, fixed_end - 10);
    let mut wallets = vec![WALLET.to_owned()];
    for ordinal in 2_u64..=17 {
        let wallet = format!("0x{ordinal:040x}");
        cache
            .upsert_wallets_bulk(&[(wallet.clone(), SRC_TRADES, false, None, None, None, 0)])
            .unwrap();
        cache.conn_for_test_set_active(&wallet, 1);
        cache.conn_for_test_insert_trade(&wallet, &format!("0xlegacy{ordinal}"), fixed_end - 10);
        wallets.push(wallet);
    }
    drop(cache);
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();
    let frozen = write_frozen_reference(&dir, fixed_end - 10, wallets.clone());

    let connection = Connection::open(&side).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER abort_activity_receipt
             BEFORE INSERT ON activity_wallet_coverage_staging_v2
             BEGIN SELECT RAISE(ABORT, 'forced receipt crash'); END;",
        )
        .unwrap();
    drop(connection);
    let failed = populate_activity_v2(
        &side,
        &YieldingFetcher::default(),
        "https://data.example",
        &frozen,
        fixed_end,
        9,
        fixed_end + 1,
    )
    .await
    .unwrap_err();
    assert!(failed.to_string().contains("forced receipt crash"));
    let connection = Connection::open(&side).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    connection
        .execute_batch("DROP TRIGGER abort_activity_receipt")
        .unwrap();
    drop(connection);

    let fetcher = YieldingFetcher::default();
    let preview = populate_activity_v2(
        &side,
        &fetcher,
        "https://data.example",
        &frozen,
        fixed_end,
        9,
        fixed_end + 2,
    )
    .await
    .unwrap();
    assert_eq!(preview.wallet_count, 17);
    assert_eq!(preview.group_count, 0);
    assert_eq!(fetcher.maximum.load(Ordering::SeqCst), 16);
    assert_eq!(fetcher.calls.lock().unwrap().len(), 17);

    let missing = wallets[7].clone();
    let connection = Connection::open(&side).unwrap();
    let original_digest: String = connection
        .query_row(
            "SELECT ordered_aggregate_digest
             FROM activity_wallet_coverage_staging_v2
             WHERE generation = 9 AND wallet_hex = ?1",
            params![missing],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "UPDATE activity_wallet_coverage_staging_v2 SET fixed_end_unix = fixed_end_unix + 1
             WHERE generation = 9 AND wallet_hex = ?1",
            params![missing],
        )
        .unwrap();
    drop(connection);
    assert!(
        populate_activity_v2(
            &side,
            &YieldingFetcher::default(),
            "https://data.example",
            &frozen,
            fixed_end,
            9,
            fixed_end + 3,
        )
        .await
        .is_err()
    );
    let connection = Connection::open(&side).unwrap();
    connection
        .execute(
            "UPDATE activity_wallet_coverage_staging_v2
             SET fixed_end_unix = ?1, ordered_aggregate_digest = ?2
             WHERE generation = 9 AND wallet_hex = ?3",
            params![fixed_end, "f".repeat(64), missing],
        )
        .unwrap();
    drop(connection);
    assert!(
        populate_activity_v2(
            &side,
            &YieldingFetcher::default(),
            "https://data.example",
            &frozen,
            fixed_end,
            9,
            fixed_end + 4,
        )
        .await
        .is_err()
    );
    let connection = Connection::open(&side).unwrap();
    connection
        .execute(
            "UPDATE activity_wallet_coverage_staging_v2 SET ordered_aggregate_digest = ?1
             WHERE generation = 9 AND wallet_hex = ?2",
            params![original_digest, missing],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM activity_wallet_coverage_staging_v2
             WHERE generation = 9 AND wallet_hex = ?1",
            params![missing],
        )
        .unwrap();
    drop(connection);
    let resumed = YieldingFetcher::default();
    populate_activity_v2(
        &side,
        &resumed,
        "https://data.example",
        &frozen,
        fixed_end,
        9,
        fixed_end + 5,
    )
    .await
    .unwrap();
    let calls = resumed.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].contains(&format!("user={missing}")));
    drop(calls);

    install_payout_manifest(&side);
    let connection = Connection::open(&side).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER abort_activity_staging_delete
             BEFORE DELETE ON activity_wallet_coverage_staging_v2
             BEGIN SELECT RAISE(ABORT, 'forced finalize crash'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(
        finalize_cache_v2(&side, &dir.path().join("failed-stage.json"), fixed_end + 6).is_err()
    );
    let connection = Connection::open(&side).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM activity_coverage_manifests_v2",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        17
    );
    connection
        .execute_batch("DROP TRIGGER abort_activity_staging_delete")
        .unwrap();
    drop(connection);
    finalize_cache_v2(&side, &dir.path().join("stage.json"), fixed_end + 7).unwrap();
    let connection = Connection::open(&side).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

// ── #588: fresh private-generation collection and cycle staging ──────────────
//
// PASS: a fresh generation binds the union of current acquisition candidates
// and retained histories, resumes only missing wallets, keeps the immutable
// prior byte-identical, certifies classifier two without a frozen reference,
// and staging copies the checkpointed fixed main exactly under the cache lock.
// FAIL: a wallet outside the union is fetched, a retry refetches or clears
// progress, a stale/tampered identity certifies, or a staged copy differs.

const WALLET_B: &str = "0x2222222222222222222222222222222222222222";
const WALLET_C: &str = "0x3333333333333333333333333333333333333333";
const WALLET_D: &str = "0x4444444444444444444444444444444444444444";
const WALLET_E: &str = "0x5555555555555555555555555555555555555555";
const FRESH_END: i64 = 1_800_000_000;

// Fresh collection reaches full history: the exclusive bound 0 is `start=1`
// on the wire, where an omitted `start` returns only the venue's recent window.
fn activity_url(wallet: &str, end: i64) -> String {
    format!(
        "https://data.example/activity?user={wallet}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end={end}&start=1"
    )
}

fn activity_rows(wallet: &str, epochs: &[i64], revised: bool) -> Vec<u8> {
    let rows = epochs
        .iter()
        .map(|epoch| {
            serde_json::json!({
                "proxyWallet": wallet, "type": "TRADE", "conditionId": format!("market-{epoch}"),
                "asset": "123", "outcome": "Yes", "side": "BUY",
                "size": if revised { "2" } else { "1" },
                "usdcSize": if revised { "1" } else { "0.5" }, "price": "0.5",
                "timestamp": epoch, "transactionHash": format!("trade-{epoch}"), "outcomeIndex": "0",
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&rows).unwrap()
}

fn fresh_fetcher(wallets: &[&str], end: i64, epochs: &[i64], revised: bool) -> FixtureFetcher {
    FixtureFetcher::new(
        wallets
            .iter()
            .map(|wallet| {
                (
                    activity_url(wallet, end),
                    activity_rows(wallet, epochs, revised),
                )
            })
            .collect(),
    )
}

fn fresh_record(path: &std::path::Path) -> Value {
    let stored: String = Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT fresh_collection_json FROM cache_v2_migration_state",
            [],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&stored).unwrap()
}

fn generation_rows(path: &std::path::Path, generation: i64) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = ?1",
            [generation],
            |row| row.get(0),
        )
        .unwrap()
}

fn count(path: &std::path::Path, sql: &str) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

/// Initial candidate: schema-one history with an active wallet, an inactive
/// wallet with retained trades, an infrastructure-flagged wallet with retained
/// trades and an active wallet without any history.
fn seed_initial_candidate(side: &std::path::Path) {
    let mut cache = seed_v1(side, FRESH_END - 10);
    for (wallet, active, infra, trade) in [
        (WALLET_B, 0, false, true),
        (WALLET_C, 1, true, true),
        (WALLET_D, 1, false, false),
    ] {
        cache
            .upsert_wallets_bulk(&[(wallet.to_owned(), SRC_TRADES, infra, None, None, None, 0)])
            .unwrap();
        cache.conn_for_test_set_active(wallet, active);
        if trade {
            cache.conn_for_test_insert_trade(wallet, &format!("legacy-{wallet}"), FRESH_END - 10);
        }
    }
    drop(cache);
}

async fn finalize_fresh_initial(dir: &TempDir, side: &std::path::Path) -> String {
    seed_initial_candidate(side);
    let manifest = write_build_manifest(dir, side);
    migrate_cache_v2(side, &manifest).unwrap();
    let union = [WALLET, WALLET_B, WALLET_C, WALLET_D];
    let manifest = populate_activity_fresh_v2(
        side,
        &fresh_fetcher(&union, FRESH_END, &[FRESH_END - 1, FRESH_END - 101], false),
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    assert_eq!(manifest.generation, 1);
    assert_eq!(manifest.wallet_count, 4);
    install_payout_manifest(side);
    finalize_cache_v2(
        side,
        &dir.path().join("fresh-initial-stage.json"),
        FRESH_END + 2,
    )
    .unwrap()
    .cache_sha256
}

#[tokio::test]
async fn fresh_generation_on_initial_base_binds_the_union_and_certifies_without_frozen_rows() {
    let dir = tempfile::Builder::new()
        .prefix("pe-fresh-initial-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    let side_sha256 = finalize_fresh_initial(&dir, &side).await;

    let record = fresh_record(&side);
    assert_eq!(record["version"], 1);
    assert_eq!(record["generation"], 1);
    assert_eq!(record["fixed_end_unix"], FRESH_END);
    assert_eq!(
        record["wallets"],
        serde_json::json!([WALLET, WALLET_B, WALLET_C, WALLET_D])
    );
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM cache_frozen_payload_verifications"
        ),
        0
    );
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(DISTINCT reference_sha256) FROM activity_coverage_manifests_v2"
        ),
        1
    );
    let bound: String = Connection::open(&side)
        .unwrap()
        .query_row(
            "SELECT reference_sha256 FROM activity_coverage_manifests_v2 WHERE generation = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(Value::String(bound), record["digest"]);
    assert_eq!(generation_rows(&side, 1), 8);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM ranker_entries_v2 WHERE classifier_version = 2"
        ),
        count(&side, "SELECT COUNT(*) FROM ranker_entries_v2")
    );

    // A completed generation is returned without any source call, and an older
    // or equal generation cannot be started.
    let silent = FixtureFetcher::new(HashMap::new());
    let repeated = populate_activity_fresh_v2(
        &side,
        &silent,
        "https://data.example",
        1,
        FRESH_END + 500,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(repeated.generation, 1);
    assert_eq!(repeated.wallet_count, 4);
    let stale = populate_activity_fresh_v2(
        &side,
        &silent,
        "https://data.example",
        0,
        FRESH_END,
        FRESH_END + 3,
    )
    .await
    .unwrap_err();
    assert!(stale.to_string().contains("must exceed"), "{stale}");

    // Activation certifies the fresh identity with no frozen verification row.
    let fixed = dir.path().join("fixed.db");
    drop(seed_v1(&fixed, FRESH_END - 100));
    let request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: dir.path().join("prior.db"),
        expected_side_sha256: side_sha256.clone(),
    };
    let installed = activate_cache_v2(&request).unwrap();
    assert!(!installed.resumed);
    assert_eq!(installed.installed_sha256, side_sha256);
    assert_eq!(installed.prior_cache_schema, 1);
    assert!(activate_cache_v2(&request).unwrap().resumed);
}

struct InterruptAfterFirst {
    side: std::path::PathBuf,
    generation: i64,
    first: &'static str,
}

impl PageFetcher for InterruptAfterFirst {
    fn fetch_page(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SourceError>> + Send {
        let side = self.side.clone();
        let generation = self.generation;
        let first = self.first;
        let url = url.to_owned();
        async move {
            if url.contains(first) {
                return Ok(activity_rows(first, &[FRESH_END + 50], false));
            }
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let committed: i64 = Connection::open(&side)
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2
                         WHERE generation = ?1 AND wallet_hex = ?2",
                        params![generation, first],
                        |row| row.get(0),
                    )
                    .unwrap();
                if committed == 1 {
                    return Err(SourceError::Transient {
                        message: "controlled interruption after the first receipt".to_owned(),
                    });
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "first wallet never committed"
                );
                tokio::task::yield_now().await;
            }
        }
    }
}

#[derive(Clone, Default)]
struct RecordingFetcher {
    calls: Arc<Mutex<Vec<String>>>,
    revised: bool,
}

impl PageFetcher for RecordingFetcher {
    fn fetch_page(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SourceError>> + Send {
        self.calls.lock().unwrap().push(url.to_owned());
        let wallet = [WALLET, WALLET_B, WALLET_C, WALLET_D, WALLET_E]
            .into_iter()
            .find(|wallet| url.contains(wallet))
            .unwrap();
        let body = activity_rows(wallet, &[FRESH_END + 50, FRESH_END - 101], self.revised);
        async move { Ok(body) }
    }
}

#[tokio::test]
async fn fresh_generation_on_recurring_base_preserves_the_prior_and_resumes_only_missing_wallets() {
    let dir = tempfile::Builder::new()
        .prefix("pe-fresh-recurring-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let prior = dir.path().join("prior.db");
    finalize_fresh_initial(&dir, &prior).await;
    let prior_sha256 = sha256_file(&prior).unwrap();
    let side = dir.path().join("side.db");
    std::fs::copy(&prior, &side).unwrap();

    // Current acquisition changes on the candidate: a new active wallet, the
    // original wallet now infrastructure-flagged, one retained history whose
    // wallet row is gone. Every retained history must stay in the union.
    let mut cache = WalletCache::open(&side).unwrap();
    cache
        .upsert_wallets_bulk(&[(WALLET_E.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
    cache.conn_for_test_set_active(WALLET_E, 1);
    cache.mark_infra(WALLET).unwrap();
    cache
        .raw_conn_for_test()
        .execute(
            "DELETE FROM wallets WHERE wallet_hex = ?1",
            params![WALLET_C],
        )
        .unwrap();
    drop(cache);
    let next_end = FRESH_END + 100;

    let interrupted = populate_activity_fresh_v2(
        &side,
        &InterruptAfterFirst {
            side: side.clone(),
            generation: 2,
            first: WALLET_B,
        },
        "https://data.example",
        2,
        next_end,
        next_end + 1,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            interrupted,
            pe_bootstrap::error::BootstrapError::TransientSource { .. }
        ),
        "{interrupted}"
    );
    assert_eq!(interrupted.exit_code(), 75);
    let record = fresh_record(&side);
    assert_eq!(record["generation"], 2);
    assert_eq!(record["fixed_end_unix"], next_end);
    assert_eq!(
        record["wallets"],
        serde_json::json!([WALLET, WALLET_B, WALLET_C, WALLET_D, WALLET_E])
    );
    assert_eq!(
        generation_rows(&side, 1),
        0,
        "superseded activity must be cleared"
    );
    assert_eq!(generation_rows(&side, 2), 1);
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        0
    );
    assert_eq!(count(&side, "SELECT COUNT(*) FROM ranker_entries_v2"), 0);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        1
    );
    assert_eq!(sha256_file(&prior).unwrap(), prior_sha256);
    assert_eq!(generation_rows(&prior, 1), 8);

    // An unfinished generation can only be resumed.
    let skipped = populate_activity_fresh_v2(
        &side,
        &RecordingFetcher::default(),
        "https://data.example",
        3,
        next_end + 10,
        next_end + 2,
    )
    .await
    .unwrap_err();
    assert!(skipped.to_string().contains("incomplete"), "{skipped}");
    assert_eq!(fresh_record(&side)["generation"], 2);

    // The retry fetches exactly the missing wallets at the recorded end and
    // accepts a revised historical row in this private generation.
    let resumed = RecordingFetcher {
        revised: true,
        ..RecordingFetcher::default()
    };
    let manifest = populate_activity_fresh_v2(
        &side,
        &resumed,
        "https://data.example",
        2,
        next_end + 999,
        next_end + 3,
    )
    .await
    .unwrap();
    let mut calls = resumed.calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(
        calls,
        [WALLET, WALLET_C, WALLET_D, WALLET_E]
            .map(|wallet| activity_url(wallet, next_end))
            .to_vec()
    );
    assert_eq!(manifest.generation, 2);
    assert_eq!(manifest.wallet_count, 5);
    assert_eq!(generation_rows(&side, 2), 9);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_groups_v2 WHERE share_amount_str = '2'"
        ),
        8,
        "revised historical rows are accepted in the private generation"
    );

    let silent = FixtureFetcher::new(HashMap::new());
    let repeated = populate_activity_fresh_v2(
        &side,
        &silent,
        "https://data.example",
        2,
        next_end + 5,
        next_end + 3,
    )
    .await
    .unwrap();
    assert_eq!(
        repeated, manifest,
        "a complete generation is revalidated, not refetched"
    );

    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("recurring-stage.json"),
        next_end + 5,
    )
    .unwrap();
    assert_eq!(stage.activity_coverage_generation, 2);
    assert_eq!(stage.ranker_classifier_version, 2);

    // The fresh-identity prior validates as the historical fixed cache and is
    // preserved byte for byte by activation.
    let fixed = dir.path().join("fixed.db");
    std::fs::copy(&prior, &fixed).unwrap();
    let request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: dir.path().join("prior-backup.db"),
        expected_side_sha256: stage.cache_sha256.clone(),
    };
    let installed = activate_cache_v2(&request).unwrap();
    assert_eq!(installed.prior_cache_schema, 2);
    assert_eq!(installed.prior_cache_sha256, prior_sha256);
    assert_eq!(
        sha256_file(&request.prior_cache_backup_path).unwrap(),
        prior_sha256
    );
    assert_eq!(sha256_file(&fixed).unwrap(), stage.cache_sha256);
}

#[tokio::test]
async fn fresh_generation_supersedes_a_legacy_frozen_identity_and_refuses_tampering() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    retained_classifier_activity(&dir, &side).await;
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM cache_frozen_payload_verifications"
        ),
        1
    );
    let older = populate_activity_fresh_v2(
        &side,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        7,
        FRESH_END + 100,
        FRESH_END + 10,
    )
    .await
    .unwrap_err();
    assert!(older.to_string().contains("must exceed"), "{older}");

    let fresh = RecordingFetcher::default();
    let manifest = populate_activity_fresh_v2(
        &side,
        &fresh,
        "https://data.example",
        8,
        FRESH_END + 100,
        FRESH_END + 11,
    )
    .await
    .unwrap();
    assert_eq!(manifest.generation, 8);
    assert_eq!(
        fresh.calls.lock().unwrap().as_slice(),
        [activity_url(WALLET, FRESH_END + 100)]
    );
    assert_eq!(generation_rows(&side, 7), 0);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM cache_frozen_payload_verifications"
        ),
        1,
        "legacy verification history is retained"
    );
    let stage =
        finalize_cache_v2(&side, &dir.path().join("superseded.json"), FRESH_END + 12).unwrap();
    assert_eq!(stage.activity_coverage_generation, 8);

    // A record whose digest no longer matches its fields cannot certify or resume.
    let connection = Connection::open(&side).unwrap();
    let stored: String = connection
        .query_row(
            "SELECT fresh_collection_json FROM cache_v2_migration_state",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut tampered: Value = serde_json::from_str(&stored).unwrap();
    tampered["wallets"]
        .as_array_mut()
        .unwrap()
        .push(Value::String(WALLET_B.to_owned()));
    connection
        .execute(
            "UPDATE cache_v2_migration_state SET fresh_collection_json = ?1",
            params![tampered.to_string()],
        )
        .unwrap();
    drop(connection);
    let refused =
        finalize_cache_v2(&side, &dir.path().join("tampered.json"), FRESH_END + 13).unwrap_err();
    assert!(refused.to_string().contains("digest mismatch"), "{refused}");
    let refused = populate_activity_fresh_v2(
        &side,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        8,
        FRESH_END + 100,
        FRESH_END + 14,
    )
    .await
    .unwrap_err();
    assert!(refused.to_string().contains("digest mismatch"), "{refused}");
}

#[tokio::test]
async fn legacy_identity_still_requires_its_frozen_verification_row() {
    let dir = tempfile::Builder::new()
        .prefix("pe-legacy-frozen-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    let side_sha256 = finalize_empty_activity_side(&dir, &side, FRESH_END).await;
    let connection = Connection::open(&side).unwrap();
    connection
        .execute("DELETE FROM cache_frozen_payload_verifications", [])
        .unwrap();
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(connection);
    for suffix in ["db-wal", "db-shm"] {
        let _ = std::fs::remove_file(side.with_extension(suffix));
    }
    let fixed = dir.path().join("fixed.db");
    drop(seed_v1(&fixed, FRESH_END - 100));
    let refused = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed,
        side_path: side.clone(),
        prior_cache_backup_path: dir.path().join("prior.db"),
        expected_side_sha256: sha256_file(&side).unwrap(),
    })
    .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("frozen activity identity is missing"),
        "{refused}"
    );
    assert_ne!(sha256_file(&side).unwrap(), side_sha256);
}

#[test]
fn cycle_staging_copies_the_checkpointed_fixed_main_exactly_and_resumes_its_own_candidate() {
    use pe_bootstrap::cache_migration::stage_cache_cycle_v2;
    let dir = TempDir::new().unwrap();
    let fixed = dir.path().join("wallet_cache.db");
    let prior = dir.path().join("wallet_cache.cron-1.prior.db");
    let side = dir.path().join("wallet_cache.cron-1.side.db");
    // The seeded row lives only in the WAL while this handle stays open.
    let wal_owner = seed_v1(&fixed, FRESH_END - 10);
    assert!(fixed.with_extension("db-wal").metadata().unwrap().len() > 0);

    let manifest = dir.path().join("cache_build_manifest.json");
    let held = pe_bootstrap::lock::CacheMutationLock::acquire(&fixed).unwrap();
    let locked = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap_err();
    assert!(
        locked.to_string().contains("cache mutation lock"),
        "{locked}"
    );
    assert!(!prior.exists() && !side.exists() && !manifest.exists());
    drop(held);

    let interrupted = std::path::PathBuf::from(format!("{}.pending", side.display()));
    std::fs::write(&interrupted, b"interrupted private staging").unwrap();
    let staged = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    drop(wal_owner);
    assert!(!staged.resumed);
    assert_eq!(staged.prior_schema, 1);
    assert!(!interrupted.exists());
    let fixed_sha256 = sha256_file(&fixed).unwrap();
    assert_eq!(staged.prior_sha256.as_deref(), Some(fixed_sha256.as_str()));
    assert_eq!(staged.side_sha256.as_deref(), Some(fixed_sha256.as_str()));
    assert_eq!(sha256_file(&prior).unwrap(), fixed_sha256);
    assert_eq!(sha256_file(&side).unwrap(), fixed_sha256);
    assert_eq!(
        count(&prior, "SELECT COUNT(*) FROM trades"),
        1,
        "the WAL-only committed row must reach the immutable prior"
    );
    // The staged build manifest is the authentic hash-bound input the initial
    // migration seals against; it is written once.
    let manifest_bytes = std::fs::read(&manifest).unwrap();
    let build: CacheV2BuildManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(build.backup_sha256, fixed_sha256);
    assert_eq!(build.source_bounds["newest_trade_unix"], FRESH_END - 10);
    assert_eq!(build.cursors["clob_closed"], "");
    let migrated = migrate_cache_v2(&side, &manifest).unwrap();
    assert!(!migrated.resumed);
    assert_eq!(migrated.legacy_trade_count, 1);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_sha256);

    // A retry returns the cycle's own candidate untouched and never rewrites
    // the completed prior.
    let mut candidate = WalletCache::open(&side).unwrap();
    candidate
        .upsert_wallets_bulk(&[(WALLET_B.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
    drop(candidate);
    let mutated = sha256_file(&side).unwrap();
    assert_ne!(mutated, fixed_sha256);
    let resumed = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.prior_sha256, None);
    assert_eq!(sha256_file(&side).unwrap(), mutated);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_sha256);
    assert_eq!(std::fs::read(&manifest).unwrap(), manifest_bytes);

    // A candidate without its prior is not a resumable cycle.
    std::fs::remove_file(&prior).unwrap();
    let orphan = stage_cache_cycle_v2(&fixed, &prior, &side, None).unwrap_err();
    assert!(
        orphan.to_string().contains("without its immutable prior"),
        "{orphan}"
    );
    assert_eq!(sha256_file(&side).unwrap(), mutated);

    // The three roles must be independent files: same path, a hard link and
    // a symbolic link are refused before anything is copied.
    let fixed_sha256_now = sha256_file(&fixed).unwrap();
    let same = stage_cache_cycle_v2(&fixed, &fixed, &side, None).unwrap_err();
    assert!(
        same.to_string().contains("not an independent file"),
        "{same}"
    );
    let linked = dir.path().join("wallet_cache.cron-2.prior.db");
    std::fs::hard_link(&fixed, &linked).unwrap();
    let hard = stage_cache_cycle_v2(&fixed, &linked, &dir.path().join("cron-2.side.db"), None)
        .unwrap_err();
    assert!(
        hard.to_string().contains("not an independent file"),
        "{hard}"
    );
    let symlinked = dir.path().join("wallet_cache.cron-3.prior.db");
    std::os::unix::fs::symlink(&fixed, &symlinked).unwrap();
    let soft = stage_cache_cycle_v2(&fixed, &symlinked, &dir.path().join("cron-3.side.db"), None)
        .unwrap_err();
    assert!(
        soft.to_string().contains("not an independent file"),
        "{soft}"
    );
    assert_eq!(sha256_file(&fixed).unwrap(), fixed_sha256_now);
}

#[tokio::test]
async fn activation_refuses_a_fixed_cache_changed_after_its_prior_was_staged() {
    use pe_bootstrap::cache_migration::stage_cache_cycle_v2;
    let dir = tempfile::Builder::new()
        .prefix("pe-fixed-drift-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    let prior = dir.path().join("fixed.cron-1.prior.db");
    let side = dir.path().join("fixed.cron-1.side.db");
    drop(seed_v1(&fixed, FRESH_END - 10));
    let staged = stage_cache_cycle_v2(&fixed, &prior, &side, None).unwrap();
    let prior_sha256 = staged.prior_sha256.unwrap();

    // Build a finalized candidate elsewhere and place it at the side path.
    let candidate = dir.path().join("candidate.db");
    let side_sha256 = finalize_fresh_initial(&dir, &candidate).await;
    std::fs::copy(&candidate, &side).unwrap();
    let request = CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: prior.clone(),
        expected_side_sha256: side_sha256.clone(),
    };

    // A fixed cache written after the prior was captured is refused; the
    // candidate and the staged prior are preserved for the operator.
    let mut drifting = WalletCache::open(&fixed).unwrap();
    drifting
        .upsert_wallets_bulk(&[(WALLET_E.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
    drop(drifting);
    let refused = activate_cache_v2(&request).unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("existing prior-cache backup differs from the fixed cache"),
        "{refused}"
    );
    assert!(side.exists());
    assert_eq!(sha256_file(&prior).unwrap(), prior_sha256);
    assert_ne!(sha256_file(&fixed).unwrap(), prior_sha256);

    // Restaging from the drifted fixed would need a new cycle: the completed
    // prior is never rewritten beneath the candidate.
    let resumed = stage_cache_cycle_v2(&fixed, &prior, &side, None).unwrap();
    assert!(resumed.resumed);
    assert_eq!(sha256_file(&prior).unwrap(), prior_sha256);

    // The unchanged control installs the candidate and keeps the prior.
    let fixed_control = dir.path().join("control.db");
    let prior_control = dir.path().join("control.cron-1.prior.db");
    let side_control = dir.path().join("control.cron-1.side.db");
    drop(seed_v1(&fixed_control, FRESH_END - 10));
    let control =
        stage_cache_cycle_v2(&fixed_control, &prior_control, &side_control, None).unwrap();
    std::fs::copy(&candidate, &side_control).unwrap();
    let installed = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed_control.clone(),
        side_path: side_control.clone(),
        prior_cache_backup_path: prior_control.clone(),
        expected_side_sha256: side_sha256.clone(),
    })
    .unwrap();
    assert!(!installed.resumed);
    assert_eq!(installed.prior_cache_sha256, control.prior_sha256.unwrap());
    assert_eq!(sha256_file(&fixed_control).unwrap(), side_sha256);
    assert!(!side_control.exists());
}

#[tokio::test]
async fn historical_cache_without_the_fresh_column_keeps_its_legacy_identity() {
    let dir = tempfile::Builder::new()
        .prefix("pe-legacy-column-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    retained_classifier_activity(&dir, &fixed).await;
    // Reproduce a cache finalized before the column existed: rebuild the
    // singleton without it (SQLite cannot drop a column in place here).
    let connection = Connection::open(&fixed).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE cache_v2_migration_state_legacy AS
                 SELECT singleton, phase, input_manifest_sha256, ranker_projection_count,
                        ranker_projection_digest, ranker_classifier_version, updated_at_unix
                 FROM cache_v2_migration_state;
             DROP TABLE cache_v2_migration_state;
             ALTER TABLE cache_v2_migration_state_legacy RENAME TO cache_v2_migration_state;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    drop(connection);
    for suffix in ["db-wal", "db-shm"] {
        let _ = std::fs::remove_file(fixed.with_extension(suffix));
    }
    assert_eq!(
        count(
            &fixed,
            "SELECT COUNT(*) FROM pragma_table_info('cache_v2_migration_state')
             WHERE name = 'fresh_collection_json'"
        ),
        0
    );
    let fixed_sha256 = sha256_file(&fixed).unwrap();

    // The historical fixed cache validates through its legacy identity during
    // activation of a fresh-identity candidate, without being upgraded.
    let side = dir.path().join("side.db");
    let side_sha256 = finalize_fresh_initial(&dir, &side).await;
    let installed = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: side,
        prior_cache_backup_path: dir.path().join("prior.db"),
        expected_side_sha256: side_sha256,
    })
    .unwrap();
    assert_eq!(installed.prior_cache_schema, 2);
    assert_eq!(installed.prior_cache_sha256, fixed_sha256);
    assert_eq!(
        sha256_file(&installed.prior_cache_backup_path).unwrap(),
        fixed_sha256
    );

    // A fatal source answer keeps the permanent classification.
    let candidate = dir.path().join("fatal.db");
    seed_initial_candidate(&candidate);
    let manifest = write_build_manifest(&dir, &candidate);
    migrate_cache_v2(&candidate, &manifest).unwrap();
    let fatal = populate_activity_fresh_v2(
        &candidate,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            fatal,
            pe_bootstrap::error::BootstrapError::Polymarket { .. }
        ),
        "{fatal}"
    );
    assert_eq!(fatal.exit_code(), 1);
}
