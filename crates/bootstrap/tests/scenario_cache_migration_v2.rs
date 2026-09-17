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

use pe_bootstrap::cache::{RankerPageStatus, RankerPricePage, WalletCache};
use pe_bootstrap::cache_migration::{
    CacheActivationRequest, CacheFinalStageRecord, CacheV2BuildManifest, FrozenCacheFreshness,
    FrozenPayloadReference, PriorCacheBinding, PublicationConsumptionProbe, activate_cache_v2,
    finalize_cache_v2, migrate_cache_v2, populate_activity_fresh_v2, populate_activity_v2,
    restore_prior_cache, sha256_file, verify_frozen_payload_v1,
};
use pe_bootstrap::clob::ClobFetcher;
use pe_bootstrap::pile::SRC_TRADES;
use pe_core_types::{
    LeaderAction, ReceivedAt, ReconstructionQuality, ShareAmount, SourceId, SourceTimestamp,
    WalletAddress,
};
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

// Damage a b-tree page in a table the lifecycle never reads. The database header,
// schema and domain tables remain readable: only a whole-file scan finds this.
fn damage_unused_page(path: &std::path::Path) {
    use std::io::{Seek as _, SeekFrom, Write as _};
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE structural_probe (payload BLOB);
         INSERT INTO structural_probe VALUES (zeroblob(32));
         PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    let root: u64 = connection
        .query_row(
            "SELECT rootpage FROM sqlite_schema WHERE name = 'structural_probe'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let page_size: u64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .unwrap();
    connection.close().unwrap();
    assert!(root > 1);
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start((root - 1) * page_size)).unwrap();
    file.write_all(&[0xff]).unwrap(); // Invalid b-tree page type, not a file-header defect.
    file.sync_all().unwrap();
}

fn assert_structural_error(error: &pe_bootstrap::error::BootstrapError) {
    let text = error.to_string();
    assert!(
        text.contains("quick_check") || text.contains("malformed"),
        "{text}"
    );
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
    // Private finalization may certify domain content despite unrelated page damage.
    // Activation must reject even when its expected hash includes that damage.
    drop(seed_v1(&fixed, watermark - 100));
    let v1_hash = sha256_file(&fixed).unwrap();
    let damaged_side = data.join("damaged-side.db");
    let damaged_backup = data.join("damaged-backup.db");
    std::fs::copy(&side, &damaged_side).unwrap();
    damage_unused_page(&damaged_side);
    let damaged_stage = finalize_cache_v2(
        &damaged_side,
        &dir.path().join("damaged-stage.json"),
        watermark + 90,
    )
    .unwrap();
    assert_eq!(
        damaged_stage.cache_sha256,
        sha256_file(&damaged_side).unwrap()
    );
    let refused = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: damaged_side.clone(),
        prior_cache_backup_path: damaged_backup.clone(),
        expected_side_sha256: damaged_stage.cache_sha256.clone(),
    })
    .unwrap_err();
    assert_structural_error(&refused);
    assert_eq!(sha256_file(&fixed).unwrap(), v1_hash);
    assert_eq!(sha256_file(&damaged_backup).unwrap(), v1_hash);
    assert_eq!(
        sha256_file(&damaged_side).unwrap(),
        damaged_stage.cache_sha256
    );
    let stage = finalize_cache_v2(
        &side,
        &dir.path().join("cache-v2-final.json"),
        watermark + 90,
    )
    .unwrap();
    assert!(!side.with_extension("db-wal").exists());

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
    assert_eq!(sha256_file(&v1_backup).unwrap(), v1_hash);
    assert_eq!(activation_json["prior_cache_sha256"], v1_hash);

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

    let first_v2_hash = sha256_file(&fixed).unwrap();
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
    assert_eq!(sha256_file(&prior_v2).unwrap(), first_v2_hash);
    assert_eq!(v2_to_v2.prior_cache_sha256, first_v2_hash);
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
    // Restore writes the displaced copy through its `.pending` name and
    // removes the fixed cache's sidecars, so those names may not be roles: a
    // prior named like the displaced copy's staging file is refused intact.
    let saved_pending = dir.path().join("saved.pending");
    std::fs::rename(&prior_v2, &saved_pending).unwrap();
    let (colliding_request, colliding_pending) =
        write_pending_publication(&dir, "v2c", &consumed_side, &fixed, &fixed, &saved_pending);
    let colliding = restore_prior_cache(
        &fixed,
        &saved_pending,
        &dir.path().join("saved"),
        &v2_binding,
        &colliding_request,
        &colliding_pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap_err();
    assert!(
        colliding.to_string().contains("not an independent file"),
        "{colliding}"
    );
    assert_eq!(sha256_file(&saved_pending).unwrap(), v2_binding.sha256);
    assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
    assert!(!dir.path().join("saved").exists());
    std::fs::rename(&saved_pending, &prior_v2).unwrap();
    // The lock stack rewrites its lock files and the displaced copy's sidecars
    // are written through their own `.pending` names, so those names are
    // refused before any lock is taken and the prior stays intact.
    let lock_named = pe_bootstrap::lock::lock_path_for(&fixed);
    let _ = std::fs::remove_file(&lock_named);
    let wal_pending = dir.path().join("saved-wal.pending");
    for colliding_prior in [&lock_named, &wal_pending] {
        std::fs::rename(&prior_v2, colliding_prior).unwrap();
        let refused = restore_prior_cache(
            &fixed,
            colliding_prior,
            &dir.path().join("saved"),
            &v2_binding,
            &v2_request,
            &v2_pending,
            &FixedPublicationProbe(false),
        )
        .await
        .unwrap_err();
        assert!(
            refused.to_string().contains("not an independent file"),
            "{}: {refused}",
            colliding_prior.display()
        );
        assert_eq!(sha256_file(colliding_prior).unwrap(), v2_binding.sha256);
        assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
        assert!(!dir.path().join("saved").exists());
        std::fs::rename(colliding_prior, &prior_v2).unwrap();
    }
    // The publication request and its pending pointer are evidence restore
    // reads and must never write over: naming the displaced copy after the
    // pointer is refused with the pointer unchanged.
    let pointer_bytes = std::fs::read(&v2_pending).unwrap();
    let evidence_role = restore_prior_cache(
        &fixed,
        &prior_v2,
        &v2_pending,
        &v2_binding,
        &v2_request,
        &v2_pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap_err();
    assert!(
        evidence_role
            .to_string()
            .contains("not an independent file"),
        "{evidence_role}"
    );
    assert_eq!(std::fs::read(&v2_pending).unwrap(), pointer_bytes);
    assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
    let damaged_prior = dir.path().join("damaged-prior.db");
    let damaged_displaced = dir.path().join("must-not-displace.db");
    std::fs::copy(&prior_v2, &damaged_prior).unwrap();
    damage_unused_page(&damaged_prior);
    let damaged_binding = PriorCacheBinding {
        sha256: sha256_file(&damaged_prior).unwrap(),
        schema_version: 2,
    };
    let (damaged_request, damaged_pending) = write_pending_publication(
        &dir,
        "damaged-prior",
        &consumed_side,
        &fixed,
        &fixed,
        &damaged_prior,
    );
    let refused = restore_prior_cache(
        &fixed,
        &damaged_prior,
        &damaged_displaced,
        &damaged_binding,
        &damaged_request,
        &damaged_pending,
        &FixedPublicationProbe(false),
    )
    .await
    .unwrap_err();
    assert_structural_error(&refused);
    assert_eq!(sha256_file(&fixed).unwrap(), current_hash);
    assert_eq!(sha256_file(&damaged_prior).unwrap(), damaged_binding.sha256);
    assert!(!damaged_displaced.exists());
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

    assert_eq!(sha256_file(&displaced_next).unwrap(), current_hash);
    assert_eq!(sha256_file(&fixed).unwrap(), v2_binding.sha256);
    assert!(
        count(
            &fixed,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ) > 0,
        "restoring a schema-two prior retains its verified receipt rows"
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
    assert_eq!(sha256_file(&fixed).unwrap(), v1_hash);
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
    assert_eq!(sha256_file(&displaced_v2).unwrap(), first_v2_hash);
}

#[test]
fn migration_refuses_reclamation_marker_missing_index_and_tampered_hash() {
    let watermark = 1_800_000_000_i64;
    let damaged_dir = TempDir::new().unwrap();
    let damaged = damaged_dir.path().join("damaged.db");
    drop(seed_v1(&damaged, watermark));
    damage_unused_page(&damaged);
    let manifest = write_build_manifest(&damaged_dir, &damaged);
    let damaged_hash = sha256_file(&damaged).unwrap();
    assert_structural_error(&migrate_cache_v2(&damaged, &manifest).unwrap_err());
    assert_eq!(sha256_file(&damaged).unwrap(), damaged_hash);
    assert_eq!(count(&damaged, "PRAGMA user_version"), 1);
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
    assert_eq!(
        stage.ranker_projection_digest,
        reference_projection_digest(&side)
    );

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
        1,
        "finalization retains the generation's receipt evidence"
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
        assert_eq!(
            stage.ranker_projection_digest,
            reference_projection_digest(side)
        );
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
    if classifier_version == 1 {
        install_legacy_receipt_manifest(side, 7);
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

#[tokio::test]
async fn refinalization_reuses_projection_after_targeted_price_write() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    let frozen = retained_classifier_activity(&dir, &side).await;
    let stage_path = dir.path().join("initial-stage.json");
    let first: CacheFinalStageRecord =
        serde_json::from_slice(&std::fs::read(&stage_path).unwrap()).unwrap();
    let rows_before = serde_json::to_vec(&classifier_projection_rows(&side)).unwrap();
    assert_eq!(first.ranker_projection_count, 2);
    assert_eq!(
        first.ranker_projection_digest,
        reference_projection_digest(&side)
    );

    let connection = Connection::open(&side).unwrap();
    // A rebuild deletes these two real entries before classifying. Exercise the
    // guard now so later success cannot be a vacuous test of an empty projection.
    connection
        .execute_batch(
            "CREATE TRIGGER forbid_projection_rebuild BEFORE DELETE ON ranker_entries_v2
         BEGIN SELECT RAISE(ABORT, 'projection rebuild forbidden by scenario'); END;",
        )
        .unwrap();
    let error = connection
        .execute("DELETE FROM ranker_entries_v2", [])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("projection rebuild forbidden by scenario")
    );
    // Removing a real receipt makes full activity verification impossible while
    // leaving its installed manifest and every projected row intact.
    assert_eq!(
        connection
            .execute("DELETE FROM activity_wallet_coverage_staging_v2", [])
            .unwrap(),
        1
    );
    drop(connection);
    let verification = populate_activity_v2(
        &side,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        &frozen,
        CLASSIFIER_FIXED_END,
        7,
        CLASSIFIER_FIXED_END + 3,
    )
    .await;
    assert!(
        verification.is_err(),
        "full activity verification must need the deleted receipt"
    );
    let before_price_sha256 = sha256_file(&side).unwrap();

    let mut cache = WalletCache::open(&side).unwrap();
    cache
        .commit_ranker_price_page(
            &RankerPricePage {
                token_id: "123".to_owned(),
                start_ts: CLASSIFIER_FIXED_END - 60,
                end_ts: CLASSIFIER_FIXED_END,
                fidelity_minutes: 1,
                status: RankerPageStatus::Complete,
                point_count: 1,
                raw_sha256: "ab".repeat(32),
                source_id: "polymarket-clob-prices-history".to_owned(),
                schema_version: 1,
                parser_version: 1,
                observed_at_unix: CLASSIFIER_FIXED_END + 3,
                fetched_at_unix: CLASSIFIER_FIXED_END + 3,
                request_envelope: "https://clob.example/prices-history?market=123".to_owned(),
            },
            &[(CLASSIFIER_FIXED_END - 30, "0.55".to_owned())],
        )
        .unwrap();
    drop(cache);
    assert_ne!(
        sha256_file(&side).unwrap(),
        before_price_sha256,
        "the price write must change the artifact"
    );

    let second = finalize_cache_v2(&side, &stage_path, CLASSIFIER_FIXED_END + 4).unwrap();
    let recorded: CacheFinalStageRecord =
        serde_json::from_slice(&std::fs::read(stage_path).unwrap()).unwrap();
    assert_eq!(recorded, second);
    assert_ne!(second.cache_sha256, first.cache_sha256);
    assert_eq!(second.cache_sha256, sha256_file(&side).unwrap());
    assert_eq!(
        second.ranker_projection_count,
        first.ranker_projection_count
    );
    assert_eq!(
        second.ranker_projection_digest,
        first.ranker_projection_digest
    );
    assert_eq!(
        second.ranker_projection_digest,
        reference_projection_digest(&side)
    );
    assert_eq!(
        serde_json::to_vec(&classifier_projection_rows(&side)).unwrap(),
        rows_before
    );
    assert!(!side.with_extension("db-wal").exists());
    assert!(!side.with_extension("db-shm").exists());
}

#[tokio::test]
async fn refinalization_refuses_newly_eligible_payout_without_manifest_change() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    prepare_fresh_initial(&dir, &side).await;
    let connection = Connection::open(&side).unwrap();
    let original_end: i64 = connection
        .query_row(
            "SELECT end_date_unix FROM clob_payout_evidence_v2
             WHERE market_id = '0xsame' AND payout_status = 'resolved'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        connection
            .execute(
                "UPDATE clob_payout_evidence_v2 SET end_date_unix = NULL
                 WHERE market_id = '0xsame'",
                [],
            )
            .unwrap(),
        1
    );
    drop(connection);
    let stage_path = dir.path().join("first-stage.json");
    let first = finalize_cache_v2(&side, &stage_path, FRESH_END + 2).unwrap();
    let stage_bytes = std::fs::read(&stage_path).unwrap();
    let rows_before = projected_entries(&side);
    let newly_eligible = (WALLET.to_owned(), "0xsame".to_owned(), FRESH_END - 201);
    assert_eq!(first.ranker_projection_count, 4);
    assert_eq!(rows_before.len(), 4);
    assert!(!rows_before.contains(&newly_eligible));
    let activity_before = retained_activity_rows(&side);
    let payout_coverage = || {
        Connection::open(&side)
            .unwrap()
            .query_row(
                "SELECT generation, manifest_json, market_count,
                        (SELECT COUNT(*) FROM clob_payout_evidence_v2
                         WHERE coverage_generation = manifest.generation)
                 FROM clob_payout_coverage_manifests_v2 manifest
                 ORDER BY generation DESC LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap()
    };
    let coverage_before = payout_coverage();
    assert_eq!(coverage_before.2, 5);
    assert_eq!(coverage_before.3, 5);

    // Only this previously excluded market changes. The old manifest binding
    // and projection digest cannot see the new membership requirement.
    assert_eq!(
        Connection::open(&side)
            .unwrap()
            .execute(
                "UPDATE clob_payout_evidence_v2 SET end_date_unix = ?1
                 WHERE market_id = '0xsame' AND end_date_unix IS NULL
                   AND payout_status = 'resolved' AND payout_vector_json = '[\"1\",\"0\"]'",
                params![original_end],
            )
            .unwrap(),
        1
    );
    assert_eq!(payout_coverage(), coverage_before);
    assert_eq!(retained_activity_rows(&side), activity_before);
    assert_eq!(projected_entries(&side), rows_before);
    assert_eq!(
        reference_projection_digest(&side),
        first.ranker_projection_digest
    );
    let hash_before_refusal = sha256_file(&side).unwrap();
    let error = finalize_cache_v2(&side, &stage_path, FRESH_END + 3).unwrap_err();
    assert!(
        error.to_string().contains("input binding changed"),
        "{error}"
    );
    assert_eq!(std::fs::read(&stage_path).unwrap(), stage_bytes);
    assert_eq!(sha256_file(&side).unwrap(), hash_before_refusal);
    assert_eq!(projected_entries(&side), rows_before);

    // Changed inputs fail closed rather than silently rebuilding a finalized
    // generation. A normal fresh collection rebuilds from the corrected payout
    // inputs and proves the missing entry was otherwise eligible all along. The
    // successor advances the end and carries the same history with an empty delta.
    populate_activity_fresh_v2(
        &side,
        &DatasetFetcher::default(),
        "https://data.example",
        2,
        FRESH_END + 1,
        FRESH_END + 4,
    )
    .await
    .unwrap();
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM cache_v2_migration_state WHERE ranker_projection_inputs_json IS NOT NULL"
        ),
        0
    );
    let rebuilt = finalize_cache_v2(&side, &stage_path, FRESH_END + 5).unwrap();
    assert_eq!(rebuilt.activity_coverage_generation, 2);
    assert_eq!(
        rebuilt.ranker_classifier_version,
        first.ranker_classifier_version
    );
    assert_eq!(rebuilt.ranker_projection_count, 5);
    let mut expected = rows_before;
    expected.push(newly_eligible);
    expected.sort();
    assert_eq!(projected_entries(&side), expected);
    assert_eq!(payout_coverage(), coverage_before);
    assert_eq!(
        rebuilt.ranker_projection_digest,
        reference_projection_digest(&side)
    );
    assert_eq!(rebuilt.cache_sha256, sha256_file(&side).unwrap());
}

#[tokio::test]
async fn refinalization_refuses_changed_or_missing_projection_proof() {
    let dir = TempDir::new().unwrap();
    let original = dir.path().join("original.db");
    retained_classifier_activity(&dir, &original).await;
    for (name, sql, expected_error) in [
        (
            "activity_generation",
            "UPDATE activity_coverage_manifests_v2 SET generation = 8",
            "activity manifest",
        ),
        (
            "activity_reference",
            "UPDATE activity_coverage_manifests_v2 SET reference_sha256 = printf('%064d', 0)",
            "input binding changed",
        ),
        (
            "activity_digest",
            "UPDATE activity_coverage_manifests_v2 SET aggregate_digest = printf('%064d', 0)",
            "input binding changed",
        ),
        (
            "activity_marker",
            "UPDATE activity_coverage_manifests_v2 SET cursors_json = '{}'",
            "input binding changed",
        ),
        (
            "payout_generation",
            "BEGIN; PRAGMA defer_foreign_keys=ON;
             UPDATE clob_payout_evidence_v2 SET coverage_generation = coverage_generation + 1;
             UPDATE clob_payout_coverage_manifests_v2 SET generation = generation + 1,
             manifest_json = json_set(manifest_json, '$.generation', generation + 1); COMMIT;",
            "input binding changed",
        ),
        (
            "payout_proof",
            "UPDATE clob_payout_coverage_manifests_v2 SET manifest_json = json_set(manifest_json,
             '$.pages[0].raw_sha256', printf('%064d', 0),
             '$.terminal_proof.terminal_page_sha256', printf('%064d', 0)),
             terminal_page_sha256 = printf('%064d', 0)",
            "input binding changed",
        ),
        (
            "payout_coverage",
            "UPDATE clob_payout_coverage_manifests_v2 SET market_count = market_count + 1",
            "CLOB payout coverage manifest",
        ),
        (
            "payout_evidence",
            "DELETE FROM clob_payout_evidence_v2 WHERE market_id = '0xlater-a'",
            "CLOB payout coverage is incomplete",
        ),
        (
            "payout_market",
            "UPDATE clob_payout_evidence_v2 SET market_id = '0xother'
             WHERE market_id = '0xlater-a'",
            "input binding changed",
        ),
        (
            "payout_status",
            "UPDATE clob_payout_evidence_v2
             SET payout_status = 'unresolved_incomplete', payout_vector_json = NULL
             WHERE market_id = '0xlater-a'",
            "input binding changed",
        ),
        (
            "payout_vector",
            "UPDATE clob_payout_evidence_v2 SET payout_vector_json = '[\"0\",\"1\"]'
             WHERE market_id = '0xlater-a'",
            "input binding changed",
        ),
        (
            "projection_row",
            "UPDATE ranker_entries_v2 SET source_trade_id = 'g2:' || printf('%064d', 0)
             WHERE source_trade_id = (SELECT MIN(source_trade_id) FROM ranker_entries_v2)",
            "projection digest mismatch",
        ),
        (
            "projected_activity",
            "UPDATE activity_groups_v2 SET share_amount_str = '9'
             WHERE source_trade_id IN (SELECT source_trade_id FROM ranker_entries_v2)",
            "projection digest mismatch",
        ),
        (
            "missing_digest",
            "UPDATE cache_v2_migration_state SET ranker_projection_digest = NULL",
            "recorded projection count or digest",
        ),
        (
            "missing_count",
            "UPDATE cache_v2_migration_state SET ranker_projection_count = NULL",
            "recorded projection count or digest",
        ),
        (
            "wrong_count",
            "UPDATE cache_v2_migration_state SET ranker_projection_count = ranker_projection_count + 1",
            "projection count mismatch",
        ),
        (
            "missing_binding",
            "UPDATE cache_v2_migration_state SET ranker_projection_inputs_json = NULL",
            "input binding is missing",
        ),
        (
            "malformed_binding",
            "UPDATE cache_v2_migration_state SET ranker_projection_inputs_json = '{}'",
            "input binding is invalid",
        ),
        (
            "missing_payout_evidence_binding",
            "UPDATE cache_v2_migration_state SET ranker_projection_inputs_json =
             json_remove(ranker_projection_inputs_json, '$.payout_evidence_digest')",
            "input binding is invalid",
        ),
        (
            "missing_classifier",
            "UPDATE cache_v2_migration_state SET ranker_classifier_version = NULL",
            "classifier version",
        ),
    ] {
        let side = dir.path().join(format!("{name}.db"));
        std::fs::copy(&original, &side).unwrap();
        Connection::open(&side).unwrap().execute_batch(sql).unwrap();
        let rows = classifier_projection_rows(&side);
        let hash = sha256_file(&side).unwrap();
        let stage_path = dir.path().join(format!("{name}-stage.json"));
        let error = finalize_cache_v2(&side, &stage_path, CLASSIFIER_FIXED_END + 3).unwrap_err();
        assert!(
            error.to_string().contains(expected_error),
            "{name}: {error}"
        );
        assert!(
            !stage_path.exists(),
            "{name}: refusal must not write a stage record"
        );
        assert_eq!(
            classifier_projection_rows(&side),
            rows,
            "{name}: no rebuild on refusal"
        );
        assert_eq!(
            sha256_file(&side).unwrap(),
            hash,
            "{name}: no mutation on refusal"
        );
    }
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
        WHEN (SELECT COUNT(*) FROM ranker_entries_v2) = 1
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
    assert_eq!(
        stage.ranker_projection_digest,
        reference_projection_digest(&side)
    );
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

    let current = dir.path().join("current.db");
    retained_classifier_activity(&dir, &current).await;
    damage_unused_page(&current);
    let current_hash = sha256_file(&current).unwrap();
    let refused = activate_cache_v2(&CacheActivationRequest {
        fixed_path: current.clone(),
        side_path: side.clone(),
        prior_cache_backup_path: prior.clone(),
        expected_side_sha256: current_hash.clone(),
    })
    .unwrap_err();
    assert_structural_error(&refused);
    assert_eq!(sha256_file(&current).unwrap(), current_hash);
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
/// manifest installation plus projection/state replacement is atomic.
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
    // Collection completion now installs the manifest. A missing receipt under
    // a completed manifest is corruption; only an unfinished receipt set resumes.
    Connection::open(&side)
        .unwrap()
        .execute(
            "DELETE FROM activity_coverage_manifests_v2 WHERE generation = 9",
            [],
        )
        .unwrap();
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
            "CREATE TRIGGER abort_activity_final_state
             BEFORE UPDATE OF phase ON cache_v2_migration_state
             WHEN NEW.phase = 'finalized'
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
        1
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
        .execute_batch("DROP TRIGGER abort_activity_final_state")
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
        17
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

// Every wallet other than the original trades one distinct market per epoch.
fn market_for(wallet: &str, epoch: i64) -> String {
    format!("market-{}-{epoch}", &wallet[2..6])
}

// The original wallet always returns one scripted history on one market: an
// old entry, a complete exit, and a recent re-entry. Full history makes the
// recent buy a re-entry; the venue's default window would show it as a first
// entry. Its `epochs` are ignored.
fn activity_rows(wallet: &str, epochs: &[i64], revised: bool) -> Vec<u8> {
    let size = if revised { "2" } else { "1" };
    let usdc = if revised { "1" } else { "0.5" };
    let row = |market: String, side: &str, epoch: i64| {
        serde_json::json!({
            "proxyWallet": wallet, "type": "TRADE", "conditionId": market,
            "asset": "123", "outcome": "Yes", "side": side, "size": size,
            "usdcSize": usdc, "price": "0.5", "timestamp": epoch,
            "transactionHash": format!("trade-{epoch}"), "outcomeIndex": "0",
        })
    };
    let rows = if wallet == WALLET {
        vec![
            row("0xsame".to_owned(), "BUY", FRESH_END - 1),
            row("0xsame".to_owned(), "SELL", FRESH_END - 151),
            row("0xsame".to_owned(), "BUY", FRESH_END - 201),
        ]
    } else {
        epochs
            .iter()
            .map(|epoch| row(market_for(wallet, *epoch), "BUY", *epoch))
            .collect()
    };
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
    prepare_fresh_initial(dir, side).await;
    finalize_cache_v2(
        side,
        &dir.path().join("fresh-initial-stage.json"),
        FRESH_END + 2,
    )
    .unwrap()
    .cache_sha256
}

async fn prepare_fresh_initial(dir: &TempDir, side: &std::path::Path) {
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
    install_fresh_payouts(side).await;
}

/// Resolved payout evidence for the shared market and the wallets C and D, so
/// the classifier projects real first entries while the inactive retained
/// wallet B keeps zero projected entries (retention is not projection).
async fn install_fresh_payouts(side: &std::path::Path) {
    let markets = [
        "0xsame".to_owned(),
        market_for(WALLET_C, FRESH_END - 1),
        market_for(WALLET_C, FRESH_END - 101),
        market_for(WALLET_D, FRESH_END - 1),
        market_for(WALLET_D, FRESH_END - 101),
    ]
    .map(|market| {
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
}

fn projected_entries(path: &std::path::Path) -> Vec<(String, String, i64)> {
    Connection::open(path)
        .unwrap()
        .prepare(
            "SELECT groups_v2.wallet_hex, groups_v2.condition_id, groups_v2.source_time_unix
             FROM ranker_entries_v2 ranker
             JOIN activity_groups_v2 groups_v2 USING (source_trade_id)
             ORDER BY 1, 2, 3",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
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
    assert_eq!(record["version"], 2);
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
    assert_eq!(generation_rows(&side, 1), 9);
    // Full history makes the old buy the only first entry on the shared market:
    // the recent buy after a complete exit is a re-entry and is not projected.
    // C and D project both of their markets; the inactive retained wallet B has
    // no payout evidence and therefore no projected entry at all.
    let mut expected = vec![(WALLET.to_owned(), "0xsame".to_owned(), FRESH_END - 201)];
    for wallet in [WALLET_C, WALLET_D] {
        for epoch in [FRESH_END - 101, FRESH_END - 1] {
            expected.push((wallet.to_owned(), market_for(wallet, epoch), epoch));
        }
    }
    expected.sort();
    assert_eq!(projected_entries(&side), expected);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM ranker_entries_v2 WHERE classifier_version = 2"
        ),
        5
    );
    // The same fixture through the real ledger and classifier: the old buy is
    // an admitted entry, the sell is an exit that empties the position, and the
    // recent buy is an entry (not an add-on) refused as not-first-entry.
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let observed = time::OffsetDateTime::from_unix_timestamp(FRESH_END + 1).unwrap();
    let mut aggregates = parse_activity_response(
        &activity_rows(WALLET, &[], false),
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
    let mut history = std::collections::BTreeSet::<String>::new();
    let mut classified = Vec::new();
    for aggregate in &aggregates {
        let mutation = LedgerMutation::from_activity(aggregate).unwrap();
        let SecondVerdict::OrderIndependent { decisions, .. } =
            pe_position_ledger::classify_complete_historical_second(
                &ledger,
                wallet,
                std::slice::from_ref(&mutation),
                ReconstructionQuality::new(100).unwrap(),
                &|market| history.contains(&market.to_string()),
            )
            .unwrap()
        else {
            panic!("scripted history is order independent");
        };
        assert_eq!(decisions.len(), 1);
        classified.push((decisions[0].action, decisions[0].entry));
        ledger
            .apply_all_or_none(std::slice::from_ref(&mutation))
            .unwrap();
        for key in mutation.touched_keys() {
            history.insert(key.market().to_string());
        }
        if decisions[0].action == LeaderAction::Exit {
            assert!(
                ledger.position(&wallet).is_some_and(|snapshot| {
                    snapshot.positions.values().all(|state| {
                        state.long_contracts == ShareAmount::ZERO
                            && state.short_contracts == ShareAmount::ZERO
                    })
                }),
                "the exit must leave no inventory: {:?}",
                ledger.position(&wallet)
            );
        }
    }
    assert_eq!(
        classified,
        vec![
            (LeaderAction::Entry, EntryClassification::Admitted),
            (LeaderAction::Exit, EntryClassification::NotBuy),
            (LeaderAction::Entry, EntryClassification::NotFirstEntry),
        ]
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
        let start = reqwest::Url::parse(url)
            .unwrap()
            .query_pairs()
            .find(|(key, _)| key == "start")
            .unwrap()
            .1
            .parse::<i64>()
            .unwrap();
        let rows: Vec<Value> = serde_json::from_slice(&body).unwrap();
        let body = serde_json::to_vec(
            &rows
                .into_iter()
                .filter(|row| row["timestamp"].as_i64().unwrap() >= start)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        async move { Ok(body) }
    }
}

// Acquire the lock only after collection startup, inside its first source call.
// No sleeps or production hooks: the coordinator releases it once every request
// has actually been polled on the same single-threaded runtime as the collector.
struct WriteLockedFetcher {
    side: std::path::PathBuf,
    lock: Mutex<Option<Connection>>,
    calls: Mutex<Vec<String>>,
    requested: tokio::sync::Notify,
    fail_last_read: bool,
}

impl WriteLockedFetcher {
    fn new(side: &std::path::Path, fail_last_read: bool) -> Self {
        Self {
            side: side.to_owned(),
            lock: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
            requested: tokio::sync::Notify::new(),
            fail_last_read,
        }
    }

    async fn release_after_requests(&self) {
        let requested = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while self.calls.lock().unwrap().len() < 4 {
                self.requested.notified().await;
            }
        })
        .await;
        // Always release, even on a failed concurrency assertion, so the test
        // cannot strand the writer behind its real five-second busy timeout.
        let lock = self.lock.lock().unwrap().take().unwrap();
        let receipts: i64 = lock
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        lock.execute_batch("ROLLBACK").unwrap();
        requested.expect("reads stopped while the first wallet commit held a write lock");
        assert_eq!(receipts, 0, "the write lock must prevent all commits");
        assert_eq!(
            *self.calls.lock().unwrap(),
            [WALLET, WALLET_B, WALLET_C, WALLET_D]
                .map(|wallet| activity_url(wallet, FRESH_END + 100))
        );
    }
}

impl PageFetcher for WriteLockedFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if url.contains(WALLET) {
            let connection = Connection::open(&self.side).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            *self.lock.lock().unwrap() = Some(connection);
        }
        self.calls.lock().unwrap().push(url.to_owned());
        self.requested.notify_one();
        if self.fail_last_read && url.contains(WALLET_D) {
            return Err(SourceError::Transient {
                message: "read failed while wallet commits were held".to_owned(),
            });
        }
        let wallet = [WALLET, WALLET_B, WALLET_C, WALLET_D]
            .into_iter()
            .find(|wallet| url.contains(wallet))
            .unwrap();
        Ok(activity_rows(wallet, &[FRESH_END + 50], false))
    }
}

/// The old inline consumer cannot pass: WALLET returns before any other wallet
/// is polled, and its synchronous commit blocks on this lock before reads.next().
/// Here all four requests arrive while the lock is held, then all receipts commit.
#[tokio::test]
async fn activity_reads_advance_while_writer_commit_is_locked() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    seed_initial_candidate(&side);
    migrate_cache_v2(&side, &write_build_manifest(&dir, &side)).unwrap();
    let fetcher = WriteLockedFetcher::new(&side, false);
    let (result, ()) = tokio::join!(
        populate_activity_fresh_v2(
            &side,
            &fetcher,
            "https://data.example",
            1,
            FRESH_END + 100,
            FRESH_END + 101,
        ),
        fetcher.release_after_requests(),
    );
    let manifest = result.unwrap();
    assert_eq!(manifest.wallet_count, 4);
    assert_eq!(manifest.group_count, 6);
    assert_eq!(generation_rows(&side, 1), 6);
    for wallet in [WALLET, WALLET_B, WALLET_C, WALLET_D] {
        let rows = if wallet == WALLET { 3 } else { 1 };
        assert_eq!(
            receipt(&side, 1, wallet),
            Some((rows, rows, u64::try_from(rows).unwrap()))
        );
    }
}

/// The read fails with three completions already sent behind a held writer.
/// Joining drains them, or stops at a later receipt failure with that wallet's
/// inserts rolled back. In both cases the earlier transient error wins and a
/// fresh invocation completes by fetching exactly the missing wallets.
#[tokio::test]
async fn activity_read_error_joins_writer_and_preserves_first_error() {
    for fail_writer_during_drain in [false, true] {
        let dir = TempDir::new().unwrap();
        let side = dir.path().join("side.db");
        seed_initial_candidate(&side);
        migrate_cache_v2(&side, &write_build_manifest(&dir, &side)).unwrap();
        if fail_writer_during_drain {
            Connection::open(&side)
                .unwrap()
                .execute_batch(&format!(
                    "CREATE TRIGGER abort_activity_receipt
                     BEFORE INSERT ON activity_wallet_coverage_staging_v2
                     WHEN NEW.wallet_hex = '{WALLET_B}'
                     BEGIN SELECT RAISE(ABORT, 'later writer failure'); END;"
                ))
                .unwrap();
        }
        let fetcher = WriteLockedFetcher::new(&side, true);
        let (result, ()) = tokio::join!(
            populate_activity_fresh_v2(
                &side,
                &fetcher,
                "https://data.example",
                1,
                FRESH_END + 100,
                FRESH_END + 101,
            ),
            fetcher.release_after_requests(),
        );
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            pe_bootstrap::error::BootstrapError::TransientSource { .. }
        ));
        assert_eq!(error.exit_code(), 75);
        assert!(
            error
                .to_string()
                .contains("read failed while wallet commits were held")
        );
        assert_eq!(receipt(&side, 1, WALLET), Some((3, 3, 3)));
        for wallet in [WALLET_B, WALLET_C] {
            assert_eq!(
                receipt(&side, 1, wallet),
                if fail_writer_during_drain {
                    None
                } else {
                    Some((1, 1, 1))
                }
            );
        }
        assert_eq!(receipt(&side, 1, WALLET_D), None);
        assert_eq!(
            generation_rows(&side, 1),
            if fail_writer_during_drain { 3 } else { 5 }
        );
        assert_eq!(
            count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
            0
        );
        Connection::open(&side)
            .unwrap()
            .execute_batch("DROP TRIGGER IF EXISTS abort_activity_receipt")
            .unwrap();
        let resumed = RecordingFetcher::default();
        let manifest = populate_activity_fresh_v2(
            &side,
            &resumed,
            "https://data.example",
            1,
            FRESH_END + 999,
            FRESH_END + 102,
        )
        .await
        .unwrap();
        assert_eq!(manifest.wallet_count, 4);
        let missing = if fail_writer_during_drain {
            vec![WALLET_B, WALLET_C, WALLET_D]
        } else {
            vec![WALLET_D]
        };
        assert_eq!(
            *resumed.calls.lock().unwrap(),
            missing
                .into_iter()
                .map(|wallet| activity_url(wallet, FRESH_END + 100))
                .collect::<Vec<_>>()
        );
    }
}

struct PendingActivityReads {
    started: AtomicUsize,
    dropped: AtomicUsize,
}

struct PendingActivityRead<'a>(&'a AtomicUsize);

impl Drop for PendingActivityRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl PageFetcher for PendingActivityReads {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        if url.contains(WALLET) {
            // Ensure the other three reads are in flight before the writer
            // can fail. They never answer, so only writer failure can wake us.
            tokio::task::yield_now().await;
            return Ok(activity_rows(WALLET, &[], false));
        }
        let _pending = PendingActivityRead(&self.dropped);
        std::future::pending().await
    }
}

#[tokio::test]
async fn activity_writer_error_cancels_pending_reads_and_rolls_back_wallet() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    seed_initial_candidate(&side);
    migrate_cache_v2(&side, &write_build_manifest(&dir, &side)).unwrap();
    Connection::open(&side)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER abort_activity_receipt
             BEFORE INSERT ON activity_wallet_coverage_staging_v2
             BEGIN SELECT RAISE(ABORT, 'writer receipt failure'); END;",
        )
        .unwrap();
    let fetcher = PendingActivityReads {
        started: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
    };
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        populate_activity_fresh_v2(
            &side,
            &fetcher,
            "https://data.example",
            1,
            FRESH_END,
            FRESH_END + 1,
        ),
    )
    .await
    .expect("writer failure must interrupt pending reads")
    .unwrap_err();
    assert!(matches!(
        error,
        pe_bootstrap::error::BootstrapError::Sqlite(_)
    ));
    assert!(error.to_string().contains("writer receipt failure"));
    assert_eq!(error.exit_code(), 1);
    assert_eq!(fetcher.started.load(Ordering::SeqCst), 4);
    assert_eq!(fetcher.dropped.load(Ordering::SeqCst), 3);
    assert_eq!(generation_rows(&side, 1), 0);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        0
    );
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        0
    );
    // Immediate write access after return also checks that rollback/join released
    // the connection; there can be no later commit from a detached writer.
    Connection::open(&side)
        .unwrap()
        .execute_batch("BEGIN IMMEDIATE; DROP TRIGGER abort_activity_receipt; COMMIT")
        .unwrap();
}

async fn interrupt_fresh_candidate_after_one_receipt(dir: &TempDir, side: &std::path::Path) {
    seed_initial_candidate(side);
    migrate_cache_v2(side, &write_build_manifest(dir, side)).unwrap();
    let error = populate_activity_fresh_v2(
        side,
        &InterruptAfterFirst {
            side: side.to_owned(),
            generation: 1,
            first: WALLET_B,
        },
        "https://data.example",
        1,
        FRESH_END + 100,
        FRESH_END + 101,
    )
    .await
    .unwrap_err();
    assert_eq!(error.exit_code(), 75);
    assert_eq!(receipt(side, 1, WALLET_B), Some((1, 1, 1)));
    assert_eq!(
        count(
            side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        1
    );
}

#[tokio::test]
async fn activity_resume_validates_receipt_identity_shape_and_counts_before_source_io() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    interrupt_fresh_candidate_after_one_receipt(&dir, &side).await;
    let connection = Connection::open(&side).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE saved_receipts AS SELECT * FROM activity_wallet_coverage_staging_v2;
             PRAGMA ignore_check_constraints = ON;",
        )
        .unwrap();
    for change in [
        format!("wallet_hex = '{WALLET_E}'"),
        "reference_sha256 = 'wrong reference'".to_owned(),
        "fixed_end_unix = fixed_end_unix + 1".to_owned(),
        "schema_version = schema_version + 1".to_owned(),
        "parser_version = parser_version + 1".to_owned(),
        "ordered_aggregate_digest = 'not a sha256'".to_owned(),
        "page_evidence_json = 'malformed json'".to_owned(),
        "page_evidence_json = '{}'".to_owned(),
        "page_evidence_json = json_set(page_evidence_json, '$[0].schema_version', 999)".to_owned(),
        "page_evidence_json = json_set(page_evidence_json, '$[0].parser_version', 999)".to_owned(),
        "source_row_count = -1".to_owned(),
        "aggregate_count = -1".to_owned(),
        "source_row_count = 1.5".to_owned(),
        "aggregate_count = 9223372036854775808".to_owned(),
        "aggregate_count = 0".to_owned(), // nonzero source rows cannot describe zero groups
    ] {
        connection
            .execute(
                &format!("UPDATE activity_wallet_coverage_staging_v2 SET {change}"),
                [],
            )
            .unwrap();
        let fetcher = RecordingFetcher::default();
        let error = populate_activity_fresh_v2(
            &side,
            &fetcher,
            "https://data.example",
            1,
            FRESH_END + 999,
            FRESH_END + 102,
        )
        .await
        .expect_err(&change);
        assert_eq!(error.exit_code(), 1, "{change}: {error}");
        assert!(
            fetcher.calls.lock().unwrap().is_empty(),
            "{change}: {error}"
        );
        assert_eq!(generation_rows(&side, 1), 1);
        assert_eq!(
            count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
            0
        );
        connection
            .execute_batch(
                "DELETE FROM activity_wallet_coverage_staging_v2;
                 INSERT INTO activity_wallet_coverage_staging_v2 (generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json, ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, completed_at_unix, acquisition_json, exclusion_reason)
                 SELECT generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json, ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, completed_at_unix, acquisition_json, exclusion_reason FROM saved_receipts;",
            )
            .unwrap();
    }
    drop(connection);
    let fetcher = RecordingFetcher::default();
    let manifest = populate_activity_fresh_v2(
        &side,
        &fetcher,
        "https://data.example",
        1,
        FRESH_END + 999,
        FRESH_END + 103,
    )
    .await
    .unwrap();
    assert_eq!(manifest.wallet_count, 4);
    assert_eq!(
        *fetcher.calls.lock().unwrap(),
        [WALLET, WALLET_C, WALLET_D].map(|wallet| activity_url(wallet, FRESH_END + 100))
    );
}

#[tokio::test]
async fn activity_resume_defers_completed_wallet_content_corruption_to_completion() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    interrupt_fresh_candidate_after_one_receipt(&dir, &side).await;
    Connection::open(&side)
        .unwrap()
        .execute(
            "DELETE FROM activity_groups_v2 WHERE wallet_hex = ?1",
            [WALLET_B],
        )
        .unwrap();
    assert_eq!(receipt(&side, 1, WALLET_B), Some((1, 1, 1)));
    let fetcher = RecordingFetcher::default();
    let error = populate_activity_fresh_v2(
        &side,
        &fetcher,
        "https://data.example",
        1,
        FRESH_END + 999,
        FRESH_END + 102,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains(&format!(
        "activity receipt aggregate mismatch for {WALLET_B}"
    )));
    // Missing wallets were collected before validation failed; B was skipped
    // despite its deleted group. The old startup content scan makes no calls.
    assert_eq!(
        *fetcher.calls.lock().unwrap(),
        [WALLET, WALLET_C, WALLET_D].map(|wallet| activity_url(wallet, FRESH_END + 100))
    );
    assert_eq!(generation_rows(&side, 1), 7);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        4
    );
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        0
    );
    install_payout_manifest(&side);
    let stage = dir.path().join("corrupt-stage.json");
    let finalization = finalize_cache_v2(&side, &stage, FRESH_END + 103).unwrap_err();
    assert!(
        finalization
            .to_string()
            .contains("activity receipt aggregate mismatch")
    );
    assert!(!stage.exists());
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        0
    );
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
    assert_successor_rejects_damaged_predecessor(&dir, &prior, 1).await;
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
        7,
        "only the committed wallet moves out of the predecessor"
    );
    assert_eq!(generation_rows(&side, 2), 3);
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        1
    );
    assert_eq!(count(&side, "SELECT COUNT(*) FROM ranker_entries_v2"), 0);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        5
    );
    assert_eq!(sha256_file(&prior).unwrap(), prior_sha256);
    assert_eq!(generation_rows(&prior, 1), 9);

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
    // keeps historical rows and admits revisions only in the newly observed window.
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
            .map(|wallet| if wallet == WALLET_E {
                activity_url(wallet, next_end)
            } else {
                activity_url(wallet, next_end)
                    .replace("start=1", &format!("start={}", FRESH_END + 1))
            })
            .to_vec()
    );
    assert_eq!(manifest.generation, 2);
    assert_eq!(manifest.wallet_count, 5);
    assert_eq!(generation_rows(&side, 2), 14);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_groups_v2 WHERE share_amount_str = '2'"
        ),
        4,
        "only fetched delta and new-wallet rows can contain the revision"
    );
    // Retained histories stay in the union independently of ranker membership:
    // the inactive wallet B has retained rows but no projected entry in the
    // prior, and it was collected anyway.
    let prior_projection = projected_entries(&prior);
    assert!(
        !prior_projection
            .iter()
            .any(|(wallet, _, _)| wallet == WALLET_B)
    );
    assert_eq!(
        prior_projection
            .iter()
            .filter(|(wallet, _, _)| wallet == WALLET)
            .count(),
        1
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

/// One fill reported as two rows with different venue timestamps (the shape
/// the aggregator refuses as causally ambiguous; observed on Forge for
/// `0x04902c…` on 2026-09-17).
fn ambiguous_fill_rows(wallet: &str) -> Vec<u8> {
    let rows = [FRESH_END - 1, FRESH_END - 3].map(|epoch| {
        serde_json::json!({
            "proxyWallet": wallet, "type": "TRADE", "conditionId": market_for(wallet, FRESH_END - 1),
            "asset": "123", "outcome": "Yes", "side": "BUY", "size": "210",
            "usdcSize": "112.41", "price": "0.5352857143", "timestamp": epoch,
            "transactionHash": "trade-shared", "outcomeIndex": "0",
        })
    });
    serde_json::to_vec(&rows).unwrap()
}

/// A TRADE row priced outside the unit interval: the shape the parser refuses
/// (observed on Forge for `0x1b5f1f…` on 2026-09-17, price 3.1968021978).
fn unparseable_price_rows(wallet: &str) -> Vec<u8> {
    let rows = [serde_json::json!({
        "proxyWallet": wallet, "type": "TRADE", "conditionId": market_for(wallet, FRESH_END - 1),
        "asset": "123", "outcome": "Yes", "side": "BUY", "size": "210",
        "usdcSize": "112.41", "price": "3.1968021978", "timestamp": FRESH_END - 1,
        "transactionHash": "trade-unparseable", "outcomeIndex": "0",
    })];
    serde_json::to_vec(&rows).unwrap()
}

/// Serves an unparseable history for wallet C and ordinary rows for the rest.
struct UnparseableWalletFetcher;

impl PageFetcher for UnparseableWalletFetcher {
    fn fetch_page(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SourceError>> + Send {
        let body = if url.contains(WALLET_C) {
            unparseable_price_rows(WALLET_C)
        } else {
            let wallet = [WALLET, WALLET_B, WALLET_D]
                .into_iter()
                .find(|wallet| url.contains(wallet))
                .unwrap();
            activity_rows(wallet, &[FRESH_END - 1], false)
        };
        async move { Ok(body) }
    }
}

/// A wallet whose venue payload the parser refuses is excluded from the
/// generation with the reason on its receipt, which is the whole record because
/// a read that failed while parsing kept no page evidence: the other wallets
/// complete, the resume does not refetch it, finalization projects nothing for
/// it, and the next generation's union still contains it.
#[tokio::test]
async fn fresh_generation_excludes_a_wallet_whose_history_cannot_be_parsed() {
    let dir = tempfile::Builder::new()
        .prefix("pe-fresh-unparseable-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    seed_initial_candidate(&side);
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();

    let manifest = populate_activity_fresh_v2(
        &side,
        &UnparseableWalletFetcher,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    assert_eq!((manifest.generation, manifest.wallet_count), (1, 4));
    assert_eq!(
        receipt(&side, 1, WALLET_C),
        Some((0, 0, 0)),
        "a parse failure keeps no page evidence"
    );
    let reason: Option<String> = Connection::open(&side)
        .unwrap()
        .query_row(
            "SELECT exclusion_reason FROM activity_wallet_coverage_staging_v2
             WHERE generation = 1 AND wallet_hex = ?1",
            [WALLET_C],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        reason
            .as_deref()
            .is_some_and(|reason| reason.contains("price")),
        "the receipt records why the wallet was excluded: {reason:?}"
    );
    let proofs = stored_receipt_proofs(&Connection::open(&side).unwrap(), 1);
    let failed = proofs
        .iter()
        .find(|proof| proof["wallet_hex"] == WALLET_C)
        .unwrap();
    assert_eq!(failed["acquisition"]["aggregation_status"], "not_attempted");
    assert_eq!(
        failed["acquisition"]["exclusion_reason"],
        "acquisition_failure"
    );
    assert_eq!(failed["acquisition"]["fetched_source_row_count"], 0);
    assert!(failed["acquisition"]["fetched_aggregate_count"].is_null());
    assert!(failed["acquisition"]["fetched_aggregate_digest"].is_null());
    assert_eq!(failed["pages"], serde_json::json!([]));
    assert!(
        proofs
            .iter()
            .filter(|proof| proof["wallet_hex"] != WALLET_C)
            .all(|proof| proof.get("exclusion_reason").is_none())
    );
    for wallet in [WALLET, WALLET_B, WALLET_D] {
        assert!(receipt(&side, 1, wallet).is_some_and(|counts| counts.0 > 0));
    }
    assert_eq!(
        count(
            &side,
            &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{WALLET_C}'")
        ),
        0
    );

    // A resume performs no read at all: every wallet has a receipt.
    let silent = FixtureFetcher::new(HashMap::new());
    populate_activity_fresh_v2(
        &side,
        &silent,
        "https://data.example",
        1,
        FRESH_END + 500,
        FRESH_END + 2,
    )
    .await
    .unwrap();

    install_fresh_payouts(&side).await;
    finalize_cache_v2(
        &side,
        &dir.path().join("unparseable-stage.json"),
        FRESH_END + 3,
    )
    .unwrap();
    assert_eq!(
        count(
            &side,
            &format!(
                "SELECT COUNT(*) FROM ranker_entries_v2 ranker
                 JOIN activity_groups_v2 groups ON groups.source_trade_id = ranker.source_trade_id
                 WHERE groups.wallet_hex = '{WALLET_C}'"
            )
        ),
        0
    );

    // The excluded wallet stays in the next generation's union even though the
    // next prior retains no history for it.
    let next = dir.path().join("next.db");
    std::fs::copy(&side, &next).unwrap();
    let next_end = FRESH_END + 100;
    populate_activity_fresh_v2(
        &next,
        &FixtureFetcher::new(
            [WALLET, WALLET_B, WALLET_C, WALLET_D]
                .into_iter()
                .map(|wallet| {
                    if wallet == WALLET_C {
                        (
                            activity_url(wallet, next_end),
                            activity_rows(wallet, &[FRESH_END - 1, FRESH_END - 101], false),
                        )
                    } else {
                        (
                            activity_url(wallet, next_end)
                                .replace("start=1", &format!("start={}", FRESH_END + 1)),
                            b"[]".to_vec(),
                        )
                    }
                })
                .collect(),
        ),
        "https://data.example",
        2,
        next_end,
        next_end + 1,
    )
    .await
    .unwrap();
    assert_eq!(
        fresh_record(&next)["wallets"],
        serde_json::json!([WALLET, WALLET_B, WALLET_C, WALLET_D])
    );
    assert!(receipt(&next, 2, WALLET_C).is_some_and(|counts| counts.0 > 0));
}

/// Serves the ambiguous retained wallet B and the ordinary wallet D, then
/// interrupts every other read transiently once both receipts are durable.
struct InterruptAfterAmbiguous {
    side: std::path::PathBuf,
}

impl PageFetcher for InterruptAfterAmbiguous {
    fn fetch_page(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SourceError>> + Send {
        let side = self.side.clone();
        let url = url.to_owned();
        async move {
            if url.contains(WALLET_B) {
                return Ok(ambiguous_fill_rows(WALLET_B));
            }
            if url.contains(WALLET_D) {
                return Ok(activity_rows(WALLET_D, &[FRESH_END - 1], false));
            }
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let committed: i64 = Connection::open(&side)
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2
                         WHERE generation = 1 AND wallet_hex IN (?1, ?2)",
                        params![WALLET_B, WALLET_D],
                        |row| row.get(0),
                    )
                    .unwrap();
                if committed == 2 {
                    return Err(SourceError::Transient {
                        message: "controlled interruption after the excluded receipt".to_owned(),
                    });
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the served wallets never committed"
                );
                tokio::task::yield_now().await;
            }
        }
    }
}

/// `(aggregate_count, source_row_count, rows across the page evidence)` of a
/// wallet's receipt in `generation`.
fn receipt(side: &std::path::Path, generation: i64, wallet: &str) -> Option<(i64, i64, u64)> {
    Connection::open(side)
        .unwrap()
        .query_row(
            "SELECT aggregate_count, source_row_count, page_evidence_json
             FROM activity_wallet_coverage_staging_v2
             WHERE generation = ?1 AND wallet_hex = ?2",
            params![generation, wallet],
            |row| {
                let pages: Value = serde_json::from_str(&row.get::<_, String>(2)?).unwrap();
                let page_rows = pages
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|page| page["row_count"].as_u64().unwrap())
                    .sum::<u64>();
                Ok((row.get(0)?, row.get(1)?, page_rows))
            },
        )
        .optional()
        .unwrap()
}

/// A wallet whose fetched history cannot be aggregated deterministically is
/// excluded from the generation instead of failing it: its receipt keeps the
/// page evidence with zero aggregates, the other wallets keep collecting, the
/// resume fetches only wallets without a receipt, validation and finalization
/// accept the receipt set, the ranker projects nothing for the wallet, and the
/// next generation's union keeps the wallet even though it is inactive and
/// retains no history, so a valid history is collected again.
#[tokio::test]
async fn fresh_generation_excludes_a_wallet_whose_history_cannot_be_aggregated() {
    let dir = tempfile::Builder::new()
        .prefix("pe-fresh-excluded-")
        .tempdir_in(std::env::current_dir().unwrap())
        .unwrap();
    std::fs::create_dir_all(dir.path().join("eval-results")).unwrap();
    let side = dir.path().join("side.db");
    seed_initial_candidate(&side);
    let manifest = write_build_manifest(&dir, &side);
    migrate_cache_v2(&side, &manifest).unwrap();

    let interrupted = populate_activity_fresh_v2(
        &side,
        &InterruptAfterAmbiguous { side: side.clone() },
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap_err();
    assert_eq!(interrupted.exit_code(), 75, "{interrupted}");
    assert_eq!(
        receipt(&side, 1, WALLET_B),
        Some((0, 0, 2)),
        "excluded receipt keeps the page evidence with zero aggregates"
    );
    assert_eq!(receipt(&side, 1, WALLET_D), Some((1, 1, 1)));
    assert_eq!(receipt(&side, 1, WALLET), None);
    let groups_for = |wallet: &str| {
        count(
            &side,
            &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{wallet}'"),
        )
    };
    assert_eq!(groups_for(WALLET_B), 0);

    // The resume fetches only the wallets without a receipt: this fetcher has
    // no response for the excluded wallet, so a refetch would fail the resume.
    let manifest = populate_activity_fresh_v2(
        &side,
        &fresh_fetcher(
            &[WALLET, WALLET_C],
            FRESH_END,
            &[FRESH_END - 1, FRESH_END - 101],
            false,
        ),
        "https://data.example",
        1,
        FRESH_END + 500,
        FRESH_END + 2,
    )
    .await
    .unwrap();
    assert_eq!((manifest.generation, manifest.wallet_count), (1, 4));
    assert_eq!(receipt(&side, 1, WALLET_B), Some((0, 0, 2)));
    assert_eq!(generation_rows(&side, 1), 3 + 2 + 1);

    install_fresh_payouts(&side).await;
    finalize_cache_v2(
        &side,
        &dir.path().join("excluded-stage.json"),
        FRESH_END + 3,
    )
    .unwrap();
    assert_eq!(
        count(
            &side,
            &format!(
                "SELECT COUNT(*) FROM ranker_entries_v2 ranker
                 JOIN activity_groups_v2 groups ON groups.source_trade_id = ranker.source_trade_id
                 WHERE groups.wallet_hex = '{WALLET_B}'"
            )
        ),
        0
    );
    assert!(count(&side, "SELECT COUNT(*) FROM ranker_entries_v2") > 0);

    for legacy in [false, true] {
        // The finalized cache is the next cycle's prior: the excluded wallet is
        // inactive and retains no history there, and only its excluded receipt
        // can carry it into the next union.
        let next = dir.path().join(format!("next-{legacy}.db"));
        std::fs::copy(&side, &next).unwrap();
        if legacy {
            // Authentic pre-649 exclusion: page rows with no aggregates and no
            // reason column. Recompute the old envelope before embedding it.
            convert_root_to_v1(&next);
            Connection::open(&next)
                .unwrap()
                .execute_batch(
                    "ALTER TABLE activity_wallet_coverage_staging_v2 DROP COLUMN exclusion_reason",
                )
                .unwrap();
            convert_root_to_v1(&next);
            install_legacy_receipt_manifest(&next, 1);
        }
        assert_eq!(
            count(
                &next,
                &format!(
                    "SELECT COUNT(*) FROM active_tradeable_wallets WHERE wallet_hex = '{WALLET_B}'"
                )
            ),
            0
        );
        assert_eq!(
            count(
                &next,
                &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{WALLET_B}'")
            ),
            0
        );
        let next_end = FRESH_END + 100;
        let manifest = populate_activity_fresh_v2(
            &next,
            &FixtureFetcher::new(
                [WALLET, WALLET_B, WALLET_C, WALLET_D]
                    .into_iter()
                    .map(|wallet| {
                        if wallet == WALLET_B {
                            (
                                activity_url(wallet, next_end),
                                activity_rows(wallet, &[FRESH_END - 1, FRESH_END - 101], false),
                            )
                        } else {
                            (
                                activity_url(wallet, next_end)
                                    .replace("start=1", &format!("start={}", FRESH_END + 1)),
                                b"[]".to_vec(),
                            )
                        }
                    })
                    .collect(),
            ),
            "https://data.example",
            2,
            next_end,
            next_end + 1,
        )
        .await
        .unwrap();
        assert_eq!((manifest.generation, manifest.wallet_count), (2, 4));
        assert_eq!(
            fresh_record(&next)["wallets"],
            serde_json::json!([WALLET, WALLET_B, WALLET_C, WALLET_D])
        );
        assert_eq!(receipt(&next, 2, WALLET_B), Some((2, 2, 2)));
        assert_eq!(
            count(
                &next,
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2 WHERE generation = 1"
            ),
            if legacy { 0 } else { 4 }
        );
        assert_eq!(generation_rows(&next, 1), 0);
    }
}

async fn assert_successor_rejects_damaged_predecessor(
    dir: &TempDir,
    prior: &std::path::Path,
    generation: i64,
) {
    for (name, sql, expected) in [
        (
            "marker_as_array",
            "UPDATE activity_coverage_manifests_v2 SET cursors_json = '[]'",
            "legacy activity manifest retained staging receipts",
        ),
        (
            "receipt_digest",
            "UPDATE activity_wallet_coverage_staging_v2 SET ordered_aggregate_digest = printf('%064d', 0)",
            "activity receipt aggregate mismatch",
        ),
        (
            "missing_receipts",
            "DELETE FROM activity_wallet_coverage_staging_v2",
            "missing frozen wallets",
        ),
        (
            "foreign_generation",
            "INSERT INTO activity_coverage_manifests_v2
                 (generation, reference_sha256, wallet_count, receipt_set_digest,
                  aggregate_digest, source_row_count, source_bounds_json, cursors_json,
                  page_hashes_json, group_count, schema_version, parser_version, completed_at_unix)
             SELECT generation + 1, reference_sha256, wallet_count, receipt_set_digest,
                    aggregate_digest, source_row_count, source_bounds_json, cursors_json,
                    page_hashes_json, group_count, schema_version, parser_version, completed_at_unix
             FROM activity_coverage_manifests_v2",
            "prior activity manifest generation mismatch",
        ),
    ] {
        let damaged = dir.path().join(format!("damaged-{generation}-{name}.db"));
        std::fs::copy(prior, &damaged).unwrap();
        let connection = Connection::open(&damaged).unwrap();
        connection.execute_batch(sql).unwrap();
        let state = || {
            connection
                .query_row(
                    "SELECT phase, fresh_collection_json FROM cache_v2_migration_state",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .unwrap()
        };
        let before_state = state();
        assert_eq!(before_state.0, "finalized");
        let before_activity = retained_activity_rows(&damaged);
        assert!(before_activity.values().all(|rows| !rows.is_empty()));
        let before_receipts = stored_receipt_proofs(&connection, generation);
        assert_eq!(before_receipts.is_empty(), name == "missing_receipts");
        let before_projection = classifier_projection_rows(&damaged);
        let fetcher = RecordingFetcher::default();
        let refused = populate_activity_fresh_v2(
            &damaged,
            &fetcher,
            "https://data.example",
            u64::try_from(generation + 2).unwrap(),
            FRESH_END + 100,
            FRESH_END + 11,
        )
        .await
        .unwrap_err();
        assert!(
            refused.to_string().contains(expected)
                || (name == "marker_as_array"
                    && refused
                        .to_string()
                        .contains("identity and receipt marker versions disagree")),
            "{name}: {refused}"
        );
        assert!(fetcher.calls.lock().unwrap().is_empty(), "{name}");
        assert_eq!(retained_activity_rows(&damaged), before_activity, "{name}");
        assert_eq!(
            stored_receipt_proofs(&connection, generation),
            before_receipts,
            "{name}"
        );
        assert_eq!(
            classifier_projection_rows(&damaged),
            before_projection,
            "{name}"
        );
        assert_eq!(state(), before_state, "{name}");
    }
}

#[tokio::test]
async fn fresh_generation_supersedes_a_legacy_frozen_identity_and_refuses_tampering() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("side.db");
    retained_classifier_activity(&dir, &side).await;
    assert_successor_rejects_damaged_predecessor(&dir, &side, 7).await;
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

    let legacy = dir.path().join("legacy-frozen-receipts.db");
    std::fs::copy(&side, &legacy).unwrap();
    install_legacy_receipt_manifest(&legacy, 7);
    let legacy_manifest = populate_activity_fresh_v2(
        &legacy,
        &RecordingFetcher::default(),
        "https://data.example",
        8,
        FRESH_END + 100,
        FRESH_END + 11,
    )
    .await
    .unwrap();
    assert_eq!(legacy_manifest.generation, 8);
    assert_eq!(generation_rows(&legacy, 7), 0);
    assert_eq!(generation_rows(&legacy, 8), 3);

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
    assert_eq!(generation_rows(&side, 8), 3);
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

    // A manifest that carries the recorded generation but another identity does
    // not count as completion for starting a later generation.
    let connection = Connection::open(&side).unwrap();
    connection
        .execute(
            "UPDATE activity_coverage_manifests_v2 SET reference_sha256 = ?1 WHERE generation = 8",
            params!["f".repeat(64)],
        )
        .unwrap();
    drop(connection);
    let foreign = populate_activity_fresh_v2(
        &side,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        9,
        FRESH_END + 200,
        FRESH_END + 12,
    )
    .await
    .unwrap_err();
    assert!(
        foreign.to_string().contains("identity mismatch"),
        "{foreign}"
    );
    assert_eq!(fresh_record(&side)["generation"], 8);
    assert_eq!(
        generation_rows(&side, 8),
        3,
        "no clearing on a refused start"
    );
    let connection = Connection::open(&side).unwrap();
    connection
        .execute(
            "UPDATE activity_coverage_manifests_v2 SET reference_sha256 = ?1 WHERE generation = 8",
            params![fresh_record(&side)["digest"].as_str().unwrap()],
        )
        .unwrap();
    drop(connection);

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
    // Keep the root's all-full membership shape valid so this specifically
    // exercises the unchanged digest refusing modified identity fields.
    tampered["full_read_wallets"]
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
    let damaged = dir.path().join("damaged-fixed.db");
    drop(seed_v1(&damaged, FRESH_END - 10));
    damage_unused_page(&damaged);
    let damaged_hash = sha256_file(&damaged).unwrap();
    let manifest = dir.path().join("cache_build_manifest.json");
    assert_structural_error(
        &stage_cache_cycle_v2(&damaged, &prior, &side, Some(&manifest)).unwrap_err(),
    );
    assert_eq!(sha256_file(&damaged).unwrap(), damaged_hash);
    assert!(!prior.exists() && !side.exists() && !manifest.exists());
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
    // Checked before any test connection reopens a role: a later read-write
    // close would tidy sidecars that staging itself left behind.
    for role in [&prior, &side] {
        for suffix in ["db-wal", "db-shm"] {
            assert!(
                !role.with_extension(suffix).exists(),
                "staging left a sidecar beside {}",
                role.display()
            );
        }
    }
    // Spellings with repeated separators name the same files; the immutable
    // prior read builds its SQLite URI from the canonical path.
    let doubled = |path: &std::path::Path| std::path::PathBuf::from(format!("/{}", path.display()));
    let respelled = stage_cache_cycle_v2(
        &doubled(&fixed),
        &doubled(&prior),
        &doubled(&side),
        Some(&doubled(&manifest)),
    )
    .unwrap();
    assert!(respelled.resumed);
    assert_eq!(respelled.prior_schema, 1);
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
    // migration seals against; it is written before the candidate is adopted.
    let build: CacheV2BuildManifest =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(build.backup_sha256, fixed_sha256);
    assert_eq!(build.source_bounds["newest_trade_unix"], FRESH_END - 10);
    assert_eq!(build.cursors["clob_closed"], "");
    assert_eq!(staged.side_schema, 1);
    // A manifest lost before the seal is recreated from the immutable prior
    // and still seals the candidate.
    std::fs::remove_file(&manifest).unwrap();
    let recovered = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert!(recovered.resumed);
    let rebuilt: CacheV2BuildManifest =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(rebuilt.backup_sha256, build.backup_sha256);
    assert_eq!(rebuilt.source_bounds, build.source_bounds);
    // A stale but valid write-ahead log beside the immutable prior (never
    // produced by staging) is ignored: the manifest is rebuilt from the main
    // file's bytes, the prior is not checkpointed and no index is created.
    let scratch = dir.path().join("scratch.db");
    std::fs::copy(&prior, &scratch).unwrap();
    let mut stale = WalletCache::open(&scratch).unwrap();
    stale
        .raw_conn_for_test()
        .execute_batch("PRAGMA wal_autocheckpoint = 0")
        .unwrap();
    stale.conn_for_test_insert_trade(WALLET, "0xstale", FRESH_END + 100);
    std::fs::copy(
        scratch.with_extension("db-wal"),
        prior.with_extension("db-wal"),
    )
    .unwrap();
    drop(stale);
    // The copied log is one SQLite honors for these exact main bytes: an
    // ordinary read-write open of an identical copy sees the extra trade.
    let probe = dir.path().join("probe.db");
    std::fs::copy(&prior, &probe).unwrap();
    std::fs::copy(
        prior.with_extension("db-wal"),
        probe.with_extension("db-wal"),
    )
    .unwrap();
    assert_eq!(count(&probe, "SELECT COUNT(*) FROM trades"), 2);
    std::fs::remove_file(&manifest).unwrap();
    let ignoring = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert!(ignoring.resumed);
    assert_eq!(ignoring.prior_schema, 1);
    let from_main: CacheV2BuildManifest =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(from_main.source_bounds["newest_trade_unix"], FRESH_END - 10);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_sha256);
    assert!(!prior.with_extension("db-shm").exists());
    std::fs::remove_file(prior.with_extension("db-wal")).unwrap();
    let migrated = migrate_cache_v2(&side, &manifest).unwrap();
    assert!(!migrated.resumed);
    assert_eq!(migrated.legacy_trade_count, 1);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_sha256);
    // Once sealed, the candidate records the manifest hash itself: a resume
    // reports the sealed schema and does not fabricate a manifest that the
    // seal could no longer verify.
    std::fs::remove_file(&manifest).unwrap();
    let sealed = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert!(sealed.resumed);
    assert_eq!(sealed.side_schema, 2);
    assert!(!manifest.exists());
    for suffix in ["db-wal", "db-shm"] {
        assert!(!prior.with_extension(suffix).exists());
    }

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
    assert!(!manifest.exists());

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
    // A role that is another role's `.pending` staging name would be deleted
    // by the copy; refused before any checkpoint or copy.
    let colliding_prior = dir.path().join("cron-4.side.db.pending");
    let colliding_side = dir.path().join("cron-4.side.db");
    let pending_role =
        stage_cache_cycle_v2(&fixed, &colliding_prior, &colliding_side, None).unwrap_err();
    assert!(
        pending_role.to_string().contains("not an independent file"),
        "{pending_role}"
    );
    let fixed_named_as_pending = dir.path().join("cron-5.prior.db.pending");
    std::fs::copy(&fixed, &fixed_named_as_pending).unwrap();
    let pending_fixed = stage_cache_cycle_v2(
        &fixed_named_as_pending,
        &dir.path().join("cron-5.prior.db"),
        &dir.path().join("cron-5.side.db"),
        None,
    )
    .unwrap_err();
    assert!(
        pending_fixed
            .to_string()
            .contains("not an independent file"),
        "{pending_fixed}"
    );
    assert!(
        fixed_named_as_pending.is_file(),
        "the fixed cache must survive a refused staging"
    );
    assert!(!colliding_prior.exists() && !colliding_side.exists());
    // The build manifest is written before the copies, so it must not be a
    // role or a staging name either.
    let manifest_as_pending = dir.path().join("cron-6.side.db.pending");
    let manifest_role = stage_cache_cycle_v2(
        &fixed,
        &dir.path().join("cron-6.prior.db"),
        &dir.path().join("cron-6.side.db"),
        Some(&manifest_as_pending),
    )
    .unwrap_err();
    assert!(
        manifest_role
            .to_string()
            .contains("not an independent file"),
        "{manifest_role}"
    );
    assert!(!dir.path().join("cron-6.prior.db").exists());
    // Staging never creates directories: a manifest whose directory does not
    // exist yet is refused before any copy, whether spelled directly, through
    // `..`, or through a directory that would have to appear at a role.
    for spelling in [
        "new-dir/nested/build.json",
        "missing-b/../build.json",
        "cron-7.side.db/../build.json",
        "cron-7.side.db/build.json",
    ] {
        let manifest = dir.path().join(spelling);
        let refused = stage_cache_cycle_v2(
            &fixed,
            &dir.path().join("cron-7.prior.db"),
            &dir.path().join("cron-7.side.db"),
            Some(&manifest),
        )
        .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("parent directory is not available"),
            "{spelling}: {refused}"
        );
        assert!(!dir.path().join("cron-7.prior.db").exists(), "{spelling}");
        assert!(!dir.path().join("cron-7.side.db").exists(), "{spelling}");
    }
    assert!(!dir.path().join("new-dir").exists());
    assert!(!dir.path().join("missing-b").exists());
    // `..` through an existing directory resolves through the file system, so
    // a manifest spelled that way onto a role is a collision.
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    let manifest_on_role = dir.path().join("sub/../cron-8.side.db");
    let dotted_role = stage_cache_cycle_v2(
        &fixed,
        &dir.path().join("cron-8.prior.db"),
        &dir.path().join("cron-8.side.db"),
        Some(&manifest_on_role),
    )
    .unwrap_err();
    assert!(
        dotted_role.to_string().contains("not an independent file"),
        "{dotted_role}"
    );
    assert!(!dir.path().join("cron-8.prior.db").exists());
    // A link without a target is not an existing directory either, so nothing
    // is created that could later give it one.
    let link = dir.path().join("link");
    std::os::unix::fs::symlink("missing-c/..", &link).unwrap();
    let manifest_through_link = link.join(fixed.file_name().unwrap());
    let broken = stage_cache_cycle_v2(
        &fixed,
        &dir.path().join("cron-10.prior.db"),
        &dir.path().join("cron-10.side.db"),
        Some(&manifest_through_link),
    )
    .unwrap_err();
    assert!(
        broken
            .to_string()
            .contains("parent directory is not available"),
        "{broken}"
    );
    assert!(!dir.path().join("missing-c").exists());
    assert!(!dir.path().join("cron-10.prior.db").exists());
    // A manifest name that is itself a link without a target is refused: the
    // prior copied later could give it a target and make it an alias.
    let dangling_manifest = dir.path().join("dangling.json");
    std::os::unix::fs::symlink("cron-11.prior.db", &dangling_manifest).unwrap();
    let dangling = stage_cache_cycle_v2(
        &fixed,
        &dir.path().join("cron-11.prior.db"),
        &dir.path().join("cron-11.side.db"),
        Some(&dangling_manifest),
    )
    .unwrap_err();
    assert!(
        dangling
            .to_string()
            .contains("symbolic link without a target"),
        "{dangling}"
    );
    assert!(!dir.path().join("cron-11.prior.db").exists());
    // A spelling that names a directory (a trailing `/` or `/.`) is refused:
    // the manifest is read again under the same spelling by the migration,
    // which cannot open it that way once a file exists there.
    for spelling in ["spelled.json/.", "spelled.json/"] {
        let spelled = std::path::PathBuf::from(format!("{}/{spelling}", dir.path().display()));
        let refused = stage_cache_cycle_v2(
            &fixed,
            &dir.path().join("cron-12.prior.db"),
            &dir.path().join("cron-12.side.db"),
            Some(&spelled),
        )
        .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("names a directory, not a file"),
            "{spelling}: {refused}"
        );
        assert!(!dir.path().join("spelled.json").exists());
        assert!(!dir.path().join("cron-12.prior.db").exists());
    }
    // The writer's temporary file beside the manifest is a role as well: a
    // candidate named like it would receive the manifest bytes first.
    let temp_named_side = dir
        .path()
        .join(format!(".build.json.{}.tmp", std::process::id()));
    let temp_role = stage_cache_cycle_v2(
        &fixed,
        &dir.path().join("cron-13.prior.db"),
        &temp_named_side,
        Some(&dir.path().join("build.json")),
    )
    .unwrap_err();
    assert!(
        temp_role.to_string().contains("not an independent file"),
        "{temp_role}"
    );
    assert!(!dir.path().join("cron-13.prior.db").exists());
    assert!(!dir.path().join("build.json").exists());
    // The candidate's and the fixed cache's SQLite sidecar names are roles:
    // a prior named like the candidate's shared-memory index would be
    // truncated when staging reads the candidate.
    let sidecar_named_prior = dir.path().join("cron-14.side.db-shm");
    let sidecar_role = stage_cache_cycle_v2(
        &fixed,
        &sidecar_named_prior,
        &dir.path().join("cron-14.side.db"),
        Some(&dir.path().join("build.json")),
    )
    .unwrap_err();
    assert!(
        sidecar_role.to_string().contains("not an independent file"),
        "{sidecar_role}"
    );
    assert!(!sidecar_named_prior.exists());
    assert!(!dir.path().join("cron-14.side.db").exists());
    // A rollback journal SQLite finds beside a database it opens is played
    // back and deleted, so that name is a role too.
    let journal_named_prior = dir.path().join("cron-15.side.db-journal");
    let journal_role = stage_cache_cycle_v2(
        &fixed,
        &journal_named_prior,
        &dir.path().join("cron-15.side.db"),
        Some(&dir.path().join("build.json")),
    )
    .unwrap_err();
    assert!(
        journal_role.to_string().contains("not an independent file"),
        "{journal_role}"
    );
    assert!(!journal_named_prior.exists());
    assert!(!dir.path().join("cron-15.side.db").exists());
    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    assert_eq!(sha256_file(&fixed).unwrap(), fixed_sha256_now);
}

#[test]
fn cycle_staging_checks_the_lock_file_name_before_taking_the_lock() {
    use pe_bootstrap::cache_migration::stage_cache_cycle_v2;
    let dir = TempDir::new().unwrap();
    let fixed = dir.path().join("wallet_cache.db");
    drop(seed_v1(&fixed, FRESH_END - 10));
    let lock = pe_bootstrap::lock::lock_path_for(&fixed);
    let before = std::fs::read(&lock).ok();
    // Taking the lock creates and rewrites the lock file, so a role named
    // like it is refused before the lock is taken.
    let refused =
        stage_cache_cycle_v2(&fixed, &lock, &dir.path().join("cron-1.side.db"), None).unwrap_err();
    assert!(
        refused.to_string().contains("not an independent file"),
        "{refused}"
    );
    assert_eq!(std::fs::read(&lock).ok(), before);
    assert!(!dir.path().join("cron-1.side.db").exists());
    // The commands after staging take the candidate's own lock, so a prior
    // named like it is refused as well.
    let side = dir.path().join("cron-1.side.db");
    let side_lock = pe_bootstrap::lock::lock_path_for(&side);
    let refused_side = stage_cache_cycle_v2(&fixed, &side_lock, &side, None).unwrap_err();
    assert!(
        refused_side.to_string().contains("not an independent file"),
        "{refused_side}"
    );
    assert!(!side_lock.exists());
    assert!(!side.exists());

    // The documented alias layout links the physical lock name to the
    // repository's lock file before that file exists: the link's target is
    // the reserved name, staging proceeds and taking the lock creates it.
    let phys = dir.path().join("phys");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&phys).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    let physical = phys.join("wallet_cache.db");
    drop(seed_v1(&physical, FRESH_END - 10));
    let _ = std::fs::remove_file(phys.join("wallet_cache.db.lock"));
    let repo_lock = data.join("wallet_cache.db.lock");
    std::os::unix::fs::symlink(&repo_lock, phys.join("wallet_cache.db.lock")).unwrap();
    assert!(!repo_lock.exists());
    let staged = stage_cache_cycle_v2(
        &physical,
        &phys.join("cron-2.prior.db"),
        &phys.join("cron-2.side.db"),
        None,
    )
    .unwrap();
    assert!(!staged.resumed);
    assert!(repo_lock.is_file());

    // A lock link whose target is another name of the fixed cache is refused
    // by identity: taking the lock would rewrite the fixed cache.
    let other = dir.path().join("other");
    std::fs::create_dir_all(&other).unwrap();
    let victim = other.join("wallet_cache.db");
    drop(seed_v1(&victim, FRESH_END - 10));
    let _ = std::fs::remove_file(other.join("wallet_cache.db.lock"));
    let victim_sha256 = sha256_file(&victim).unwrap();
    std::fs::hard_link(&victim, other.join("alias.db")).unwrap();
    std::os::unix::fs::symlink("alias.db", other.join("wallet_cache.db.lock")).unwrap();
    let aliased = stage_cache_cycle_v2(
        &victim,
        &other.join("cron-3.prior.db"),
        &other.join("cron-3.side.db"),
        None,
    )
    .unwrap_err();
    assert!(
        aliased.to_string().contains("not an independent file"),
        "{aliased}"
    );
    assert_eq!(sha256_file(&victim).unwrap(), victim_sha256);
    assert!(!other.join("cron-3.prior.db").exists());
}

#[test]
fn cycle_staging_honors_a_seal_committed_only_to_the_write_ahead_log() {
    use pe_bootstrap::cache_migration::stage_cache_cycle_v2;
    let dir = TempDir::new().unwrap();
    let fixed = dir.path().join("wallet_cache.db");
    let prior = dir.path().join("wallet_cache.cron-1.prior.db");
    let side = dir.path().join("wallet_cache.cron-1.side.db");
    let manifest = dir.path().join("cache_build_manifest.json");
    drop(seed_v1(&fixed, FRESH_END - 10));
    let staged = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert_eq!(staged.side_schema, 1);
    // A seal whose `user_version` commit sits in the write-ahead log while the
    // main header still says one (interrupted before its checkpoint) must be
    // seen as sealed: no manifest is fabricated for it.
    let sealing = Connection::open(&side).unwrap();
    sealing
        .execute_batch("PRAGMA journal_mode = WAL; PRAGMA user_version = 2;")
        .unwrap();
    std::fs::remove_file(&manifest).unwrap();
    let resumed = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.side_schema, 2);
    assert!(!manifest.exists());
    assert!(
        side.with_extension("db-wal").metadata().unwrap().len() > 0,
        "the committed write-ahead log must be left intact"
    );
    // A connection that stays open across a resume keeps its shared-memory
    // index and truncated log: staging never unlinks sidecars itself, so the
    // held connection still commits and its write is visible afterwards.
    sealing
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert_eq!(side.with_extension("db-wal").metadata().unwrap().len(), 0);
    let held = stage_cache_cycle_v2(&fixed, &prior, &side, Some(&manifest)).unwrap();
    assert_eq!(held.side_schema, 2);
    assert!(side.with_extension("db-shm").exists());
    let changed = sealing
        .execute(
            "UPDATE source_cursor SET value = 'live' WHERE key = 'clob_closed'",
            [],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(sealing);
    let value: String = Connection::open(&side)
        .unwrap()
        .query_row(
            "SELECT value FROM source_cursor WHERE key = 'clob_closed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, "live");
    for suffix in ["db-wal", "db-shm"] {
        assert!(!side.with_extension(suffix).exists());
    }
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

// Compile the private pure commitment owner into this focused binary as well,
// so parity tests need neither a public test API nor a separate lib-test run.
#[path = "../src/cache_migration/digests.rs"]
mod digests;

fn whole_json_digest(value: &impl serde::Serialize) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}

#[test]
fn streamed_aggregate_digest_matches_whole_typed_vector() {
    use pe_source_polymarket_public::{ActivityAggregate, SourceActivityGroupId};
    let observed = time::OffsetDateTime::from_unix_timestamp(FRESH_END).unwrap();
    let make = |wallet: &str| {
        let raw = activity_rows(wallet, &[FRESH_END - 1, FRESH_END - 2], false);
        let mut groups = parse_activity_response(
            &raw,
            WalletAddress::from_hex(wallet).unwrap(),
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(observed),
                received_at: ReceivedAt(observed),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap()
        .aggregates()
        .unwrap();
        // Equal seconds with different keys, exact decimals and escaped text.
        groups[1].source_time = groups[0].source_time.clone();
        let mut components = groups[1].group_id.components().clone();
        components.transaction_hash = "quote\" newline\n slash\\ unicode é".to_owned();
        groups[1].group_id = SourceActivityGroupId::derive(components).unwrap();
        groups[1].share_sum =
            ShareAmount::from_decimal_exact(rust_decimal_macros::dec!(1.250001)).unwrap();
        groups[1].price_weighted_share_sum.0 = rust_decimal_macros::dec!(0.500000400);
        groups
    };
    let populated_b = make(WALLET_B);
    let populated_e = make(WALLET_E);
    let cases: Vec<Vec<(&str, Vec<ActivityAggregate>)>> = vec![
        vec![],
        vec![(WALLET, vec![]), (WALLET_C, vec![])], // empty and excluded
        // The last wallet is empty/excluded after a populated wallet.
        vec![(WALLET_B, populated_b.clone()), (WALLET_E, vec![])],
        vec![
            (WALLET_E, populated_e.clone()),
            (WALLET_C, vec![]),
            (WALLET_B, populated_b.clone()),
            (WALLET_D, vec![]),
            (WALLET, vec![]),
        ],
        vec![
            (WALLET_E, populated_e),
            (WALLET_B, populated_b),
            (WALLET_D, make(WALLET_D)),
        ],
    ];
    let key = |a: &ActivityAggregate| {
        (
            a.group_id.components().wallet.to_string(),
            a.source_time.0.unix_timestamp(),
            a.group_id.key().0.clone(),
        )
    };
    for wallets in cases {
        // The old whole-generation computation is retained only as a reference.
        let mut all = wallets
            .iter()
            .flat_map(|(_, groups)| groups.clone())
            .collect::<Vec<_>>();
        all.sort_by_key(&key);
        let mut ordered = wallets.into_iter().collect::<BTreeMap<_, _>>();
        let mut streamed = digests::JsonArrayDigest::new();
        for groups in ordered.values_mut() {
            groups.sort_by_key(&key);
            let json = serde_json::to_string(groups).unwrap();
            assert_eq!(
                whole_json_digest(groups),
                format!("{:x}", Sha256::digest(json.as_bytes()))
            );
            streamed.extend_array(&json).unwrap();
        }
        assert_eq!(streamed.finish(), whole_json_digest(&all));
    }
}

#[test]
fn streamed_receipt_digest_matches_whole_value_envelope() {
    #[derive(serde::Serialize)]
    struct Receipt {
        wallet_hex: String,
        pages: Vec<Value>,
        ordered_aggregate_digest: String,
        source_row_count: u64,
        aggregate_count: u64,
        schema_version: u32,
        parser_version: u32,
    }
    let receipts = [WALLET, WALLET_B, WALLET_C].map(|wallet| Receipt {
        wallet_hex: wallet.to_owned(),
        pages: vec![
            serde_json::json!({"raw_page_hash": "f".repeat(64), "row_count": 1,
            "request_cursor": "quote\"\n\\é", "schema_version": 2, "parser_version": 2}),
        ],
        ordered_aggregate_digest: "a".repeat(64),
        source_row_count: 1,
        aggregate_count: 1,
        schema_version: 2,
        parser_version: 2,
    });
    for len in 0..=receipts.len() {
        let reference = "escaped\"\n\\é";
        let expected = whole_json_digest(&serde_json::json!({
            "generation": 7, "reference_sha256": reference, "fixed_end_unix": FRESH_END,
            "receipts": &receipts[..len],
        }));
        let mut streamed = digests::ReceiptSetDigest::new(7, reference, FRESH_END).unwrap();
        for receipt in &receipts[..len] {
            streamed.push(receipt).unwrap();
        }
        assert_eq!(streamed.finish(), expected);
    }
}

fn assert_bounded_activity_cli(path: &std::path::Path, legacy: bool) {
    let output = Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .env_clear()
        .env(
            "PE_BOOTSTRAP_OUTPUT",
            path.parent().unwrap().join("watchlist.json"),
        )
        .env("RUST_LOG", "off")
        .args(["cache-populate-activity-v2", "--db"])
        .arg(path)
        .args(["--fresh-generation", "1"])
        .current_dir(path.parent().unwrap())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "status={} stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(output.stdout.len() < 1024);
    assert_eq!(report["wallet_count"], 4);
    assert_eq!(report["page_hashes"], serde_json::json!([]));
    if legacy {
        assert_eq!(report["cursors"], Value::Null);
    } else {
        assert_eq!(
            report["cursors"]["receipt_storage"],
            "activity_wallet_coverage_staging_v2"
        );
    }
}

fn stored_receipt_proofs(connection: &Connection, generation: i64) -> Vec<Value> {
    // Fixtures that build the table from an older snapshot have no reason column.
    let has_reason: bool = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('activity_wallet_coverage_staging_v2')
             WHERE name = 'exclusion_reason'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
        > 0;
    let has_acquisition: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('activity_wallet_coverage_staging_v2') WHERE name = 'acquisition_json')", [], |row| row.get(0)).unwrap();
    let acquisition_column = if has_acquisition {
        "acquisition_json"
    } else {
        "NULL"
    };
    let reason_column = if has_reason {
        "exclusion_reason"
    } else {
        "NULL"
    };
    connection
        .prepare(&format!(
            "SELECT wallet_hex, page_evidence_json, ordered_aggregate_digest, source_row_count,
                aggregate_count, schema_version, parser_version, {reason_column}, {acquisition_column}
         FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1 ORDER BY wallet_hex"
        ))
        .unwrap()
        .query_map([generation], |row| {
            let mut proof = serde_json::Map::new();
            proof.insert("wallet_hex".to_owned(), Value::String(row.get(0)?));
            proof.insert(
                "pages".to_owned(),
                serde_json::from_str::<Value>(&row.get::<_, String>(1)?).unwrap(),
            );
            proof.insert(
                "ordered_aggregate_digest".to_owned(),
                Value::String(row.get(2)?),
            );
            for (key, index) in [
                ("source_row_count", 3),
                ("aggregate_count", 4),
                ("schema_version", 5),
                ("parser_version", 6),
            ] {
                proof.insert(key.to_owned(), Value::from(row.get::<_, i64>(index)?));
            }
            // An ordinary receipt serializes without the field at all.
            if let Some(reason) = row.get::<_, Option<String>>(7)? {
                proof.insert("exclusion_reason".to_owned(), Value::String(reason));
            }
            if let Some(json) = row.get::<_, Option<String>>(8)? {
                proof.insert("acquisition".to_owned(), serde_json::from_str(&json).unwrap());
            }
            Ok(Value::Object(proof))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

// Install exactly the old stored representation and remove its staging rows.
// Its receipt digest is independently checked against the original envelope;
// this fixture does not let the new writer redefine the legacy format.
fn install_legacy_receipt_manifest(path: &std::path::Path, generation: i64) {
    if Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT json_extract(fresh_collection_json, '$.version') FROM cache_v2_migration_state",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .unwrap()
        == Some(2)
    {
        convert_root_to_v1(path);
    }
    let mut connection = Connection::open(path).unwrap();
    let transaction = connection.transaction().unwrap();
    let receipts = stored_receipt_proofs(&transaction, generation);
    let (reference, bounds, digest): (String, String, String) = transaction
        .query_row(
            "SELECT reference_sha256, source_bounds_json, receipt_set_digest
         FROM activity_coverage_manifests_v2 WHERE generation = ?1",
            [generation],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let bounds: Value = serde_json::from_str(&bounds).unwrap();
    assert_eq!(
        digest,
        whole_json_digest(&serde_json::json!({
            "generation": generation, "reference_sha256": reference,
            "fixed_end_unix": bounds["end_inclusive"], "receipts": receipts,
        }))
    );
    let mut hashes = receipts
        .iter()
        .flat_map(|receipt| {
            receipt["pages"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |page| {
                    format!(
                        "{}:{}",
                        receipt["wallet_hex"].as_str().unwrap(),
                        page["raw_page_hash"].as_str().unwrap()
                    )
                })
        })
        .collect::<Vec<_>>();
    hashes.sort();
    transaction.execute("UPDATE activity_coverage_manifests_v2 SET cursors_json = ?1, page_hashes_json = ?2 WHERE generation = ?3",
        params![serde_json::to_string(&receipts).unwrap(), serde_json::to_string(&hashes).unwrap(), generation]).unwrap();
    transaction
        .execute(
            "DELETE FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1",
            [generation],
        )
        .unwrap();
    transaction.commit().unwrap();
}

fn reference_projection_digest(path: &std::path::Path) -> String {
    let connection = Connection::open(path).unwrap();
    let names = [
        "source_trade_id",
        "activity_generation",
        "classifier_version",
        "wallet_hex",
        "condition_id",
        "asset",
        "outcome_id",
        "side",
        "share_amount_str",
        "price_weighted_share_amount_str",
        "source_usdc_amount_str",
        "source_time_unix",
        "payout_vector_json",
        "end_date_unix",
    ];
    let rows = connection.prepare(
        "SELECT ranker.source_trade_id, ranker.activity_generation, ranker.classifier_version,
                groups_v2.wallet_hex, groups_v2.condition_id, groups_v2.asset, groups_v2.outcome_id,
                groups_v2.side, groups_v2.share_amount_str, groups_v2.price_weighted_share_amount_str,
                groups_v2.source_usdc_amount_str, groups_v2.source_time_unix,
                payout.payout_vector_json, payout.end_date_unix
         FROM ranker_entries_v2 ranker JOIN activity_groups_v2 groups_v2
           ON groups_v2.source_trade_id = ranker.source_trade_id
          AND groups_v2.coverage_generation = ranker.activity_generation
         JOIN clob_payout_evidence_v2 payout ON payout.market_id = groups_v2.condition_id
         ORDER BY ranker.source_trade_id"
    ).unwrap().query_map([], |row| {
        let mut value = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            value.insert((*name).to_owned(), match i {
                1 | 2 | 6 | 11 | 13 => Value::from(row.get::<_, i64>(i)?),
                _ => Value::from(row.get::<_, String>(i)?),
            });
        }
        Ok(Value::Object(value))
    }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    whole_json_digest(&rows)
}

#[tokio::test]
async fn retained_receipts_and_legacy_manifests_fail_closed_on_corruption() {
    let dir = TempDir::new().unwrap();
    let side = dir.path().join("retained.db");
    prepare_fresh_initial(&dir, &side).await;
    let unfinalized = dir.path().join("unfinalized.db");
    std::fs::copy(&side, &unfinalized).unwrap();
    finalize_cache_v2(&side, &dir.path().join("retained.json"), FRESH_END + 2).unwrap();
    // Collection already installed the manifest, before payout/projection finalization.
    let unfinalized_legacy = dir.path().join("unfinalized-legacy.db");
    std::fs::copy(&unfinalized, &unfinalized_legacy).unwrap();
    install_legacy_receipt_manifest(&unfinalized_legacy, 1);
    let legacy = dir.path().join("legacy.db");
    std::fs::copy(&unfinalized_legacy, &legacy).unwrap();
    // Both representations are installed before their first finalization, so
    // each input binding records the authentic manifest that will be reused.
    finalize_cache_v2(&legacy, &dir.path().join("legacy.json"), FRESH_END + 2).unwrap();
    assert_bounded_activity_cli(&legacy, true);
    let connection = Connection::open(&side).unwrap();
    let (cursors, hashes, digest): (String, String, String) = connection.query_row(
        "SELECT cursors_json, page_hashes_json, receipt_set_digest FROM activity_coverage_manifests_v2",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&cursors).unwrap(),
        serde_json::json!({
            "receipt_storage": "activity_wallet_coverage_staging_v2", "version": 2
        })
    );
    assert_eq!(hashes, "[]");
    assert!(cursors.len() < 100);
    let receipts = stored_receipt_proofs(&connection, 1);
    assert_eq!(receipts.len(), 4);
    assert!(
        count(
            &side,
            &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{WALLET_B}'")
        ) > 0
    );
    assert!(
        !projected_entries(&side)
            .iter()
            .any(|(wallet, _, _)| wallet == WALLET_B)
    );
    assert_eq!(
        digest,
        whole_json_digest(&serde_json::json!({
            "generation": 1, "reference_sha256": fresh_record(&side)["digest"],
            "fixed_end_unix": FRESH_END, "receipts": receipts,
        }))
    );
    let projection: String = connection
        .query_row(
            "SELECT ranker_projection_digest FROM cache_v2_migration_state",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(projection, reference_projection_digest(&side));
    drop(connection);
    assert_bounded_activity_cli(&side, false);
    // Re-finalization verifies the projection; a completed collection still
    // validates the full retained receipt/content evidence without source I/O.
    finalize_cache_v2(&side, &dir.path().join("again.json"), FRESH_END + 3).unwrap();
    let no_reads = YieldingFetcher::default();
    let manifest = populate_activity_fresh_v2(
        &side,
        &no_reads,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 4,
    )
    .await
    .unwrap();
    assert!(no_reads.calls.lock().unwrap().is_empty());
    assert!(serde_json::to_string(&manifest).unwrap().len() < 1024);
    std::fs::create_dir(dir.path().join("eval-results")).unwrap();
    let fixed = dir.path().join("fixed.db");
    let prior = dir.path().join("prior.db");
    drop(seed_v1(&fixed, FRESH_END));
    let fixed_hash = sha256_file(&fixed).unwrap();
    for (name, sql) in [
        (
            "missing",
            "DELETE FROM activity_wallet_coverage_staging_v2 WHERE wallet_hex = '0x2222222222222222222222222222222222222222'",
        ),
        (
            "extra",
            "UPDATE activity_wallet_coverage_staging_v2 SET wallet_hex = '0x5555555555555555555555555555555555555555' WHERE wallet_hex = '0x2222222222222222222222222222222222222222'",
        ),
        (
            "page",
            "UPDATE activity_wallet_coverage_staging_v2 SET page_evidence_json = '[]'",
        ),
        (
            "receipt_digest",
            "UPDATE activity_wallet_coverage_staging_v2 SET ordered_aggregate_digest = printf('%064d', 0)",
        ),
        (
            "receipt_count",
            "UPDATE activity_wallet_coverage_staging_v2 SET aggregate_count = aggregate_count + 1",
        ),
        (
            "receipt_identity",
            "UPDATE activity_wallet_coverage_staging_v2 SET fixed_end_unix = fixed_end_unix + 1",
        ),
        (
            "marker",
            "UPDATE activity_coverage_manifests_v2 SET cursors_json = '{\"receipt_storage\":\"activity_wallet_coverage_staging_v2\",\"version\":999}'",
        ),
        (
            "marker_extra",
            "UPDATE activity_coverage_manifests_v2 SET cursors_json = '{\"receipt_storage\":\"activity_wallet_coverage_staging_v2\",\"version\":1,\"extra\":true}'",
        ),
        (
            "hashes",
            "UPDATE activity_coverage_manifests_v2 SET page_hashes_json = '[\"unexpected\"]'",
        ),
        (
            "component",
            "UPDATE activity_groups_v2 SET components_json = json_set(components_json, '$.transaction_hash', 'changed')",
        ),
        (
            "key",
            "UPDATE activity_groups_v2 SET source_trade_id = 'g2:' || printf('%064d', 0) WHERE rowid = (SELECT MIN(rowid) FROM activity_groups_v2)",
        ),
        (
            "amount",
            "UPDATE activity_groups_v2 SET share_amount_str = '9.000001'",
        ),
        (
            "unprojected_amount",
            "UPDATE activity_groups_v2 SET share_amount_str = '9.000001'
             WHERE wallet_hex = '0x2222222222222222222222222222222222222222'",
        ),
        (
            "unprojected_delete",
            "DELETE FROM activity_groups_v2
             WHERE wallet_hex = '0x2222222222222222222222222222222222222222'",
        ),
        (
            "count",
            "UPDATE activity_groups_v2 SET row_count = row_count + 1",
        ),
        (
            "aggregate_digest",
            "UPDATE activity_coverage_manifests_v2 SET aggregate_digest = printf('%064d', 0)",
        ),
    ] {
        for legacy in [false, true] {
            // Marker/receipt-table damage tests apply to the retained representation.
            if legacy
                && ![
                    "component",
                    "key",
                    "amount",
                    "unprojected_amount",
                    "unprojected_delete",
                    "count",
                    "aggregate_digest",
                ]
                .contains(&name)
            {
                continue;
            }
            let before_first = dir.path().join(format!("before-first-{name}-{legacy}.db"));
            std::fs::copy(
                if legacy {
                    &unfinalized_legacy
                } else {
                    &unfinalized
                },
                &before_first,
            )
            .unwrap();
            Connection::open(&before_first)
                .unwrap()
                .execute_batch(sql)
                .unwrap();
            let first_hash = sha256_file(&before_first).unwrap();
            let failed_stage = dir
                .path()
                .join(format!("before-first-{name}-{legacy}.json"));
            let first_error = finalize_cache_v2(&before_first, &failed_stage, FRESH_END + 5)
                .expect_err("corruption before the first finalization must be rejected");
            assert!(!failed_stage.exists());
            assert_eq!(sha256_file(&before_first).unwrap(), first_hash);

            let damaged = dir.path().join(format!("{name}-{legacy}.db"));
            let finalized = if legacy {
                dir.path().join("legacy.db")
            } else {
                side.clone()
            };
            std::fs::copy(&finalized, &damaged).unwrap();
            Connection::open(&damaged)
                .unwrap()
                .execute_batch(sql)
                .unwrap();
            // Receipt and unprojected-content damage does not change the saved
            // inputs or projection digest. Certify these bytes again, then
            // prove activation still rejects their original content error.
            // Manifest or projected-value changes are covered by the separate
            // refinalization refusal scenario; exercise activation for them too.
            if ![
                "marker",
                "marker_extra",
                "hashes",
                "aggregate_digest",
                "key",
                "amount",
            ]
            .contains(&name)
            {
                let stage_path = dir.path().join(format!("again-{name}-{legacy}.json"));
                let stage = finalize_cache_v2(&damaged, &stage_path, FRESH_END + 5)
                    .unwrap_or_else(|error| panic!("{name} legacy={legacy}: {error}"));
                assert_eq!(stage.cache_sha256, sha256_file(&damaged).unwrap());
                assert_eq!(stage.ranker_projection_digest, projection);
                let recorded: CacheFinalStageRecord =
                    serde_json::from_slice(&std::fs::read(stage_path).unwrap()).unwrap();
                assert_eq!(recorded, stage);
            }
            assert_eq!(
                Connection::open(&damaged)
                    .unwrap()
                    .query_row::<String, _, _>("PRAGMA quick_check", [], |row| row.get(0))
                    .unwrap(),
                "ok"
            );
            let damaged_hash = sha256_file(&damaged).unwrap();
            let error = activate_cache_v2(&CacheActivationRequest {
                fixed_path: fixed.clone(),
                side_path: damaged.clone(),
                prior_cache_backup_path: prior.clone(),
                expected_side_sha256: damaged_hash.clone(),
            })
            .unwrap_err();
            assert!(
                matches!(error, pe_bootstrap::error::BootstrapError::Invalid { .. }),
                "{name}: {error}"
            );
            assert!(
                !error.to_string().contains("hash changed"),
                "{name}: {error}"
            );
            assert!(
                !error.to_string().contains("quick_check"),
                "{name}: {error}"
            );
            assert_eq!(
                error.to_string(),
                first_error.to_string(),
                "{name} legacy={legacy}"
            );
            assert_eq!(sha256_file(&fixed).unwrap(), fixed_hash);
            assert_eq!(sha256_file(&prior).unwrap(), fixed_hash);
            assert_eq!(sha256_file(&damaged).unwrap(), damaged_hash);
        }
    }
    finalize_cache_v2(&legacy, &dir.path().join("legacy.json"), FRESH_END + 5).unwrap();
    let connection = Connection::open(&legacy).unwrap();
    // A legacy array requires *no* retained receipts; it cannot hide table damage.
    connection
        .execute(
            "INSERT INTO activity_wallet_coverage_staging_v2
            (generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
             ordered_aggregate_digest, source_row_count, aggregate_count, schema_version,
             parser_version, completed_at_unix)
         VALUES (1, ?1, ?2, ?3, '[]', ?4, 0, 0, 2, 2, ?3)",
            params![
                WALLET,
                fresh_record(&legacy)["digest"].as_str().unwrap(),
                FRESH_END,
                whole_json_digest(&Vec::<Value>::new())
            ],
        )
        .unwrap();
    drop(connection);
    let stage =
        finalize_cache_v2(&legacy, &dir.path().join("bad-legacy.json"), FRESH_END + 6).unwrap();
    assert_eq!(stage.cache_sha256, sha256_file(&legacy).unwrap());
    let error = activate_cache_v2(&CacheActivationRequest {
        fixed_path: fixed.clone(),
        side_path: legacy.clone(),
        prior_cache_backup_path: prior.clone(),
        expected_side_sha256: stage.cache_sha256.clone(),
    })
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("legacy activity manifest retained staging receipts")
    );
    assert_eq!(sha256_file(&fixed).unwrap(), fixed_hash);
    assert_eq!(sha256_file(&prior).unwrap(), fixed_hash);
    assert_eq!(sha256_file(&legacy).unwrap(), stage.cache_sha256);
}

#[derive(Clone, Default)]
struct CheckLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CheckLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CheckLog {
    fn take(&self) -> Vec<Value> {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["fields"]["message"] == "SQLite quick_check completed")
            .collect()
    }
}

fn assert_check_event(
    events: &[Value],
    role: &str,
    path: &std::path::Path,
    size: u64,
    success: bool,
) {
    assert_eq!(events.len(), 1, "{events:?}");
    let fields = &events[0]["fields"];
    assert_eq!(fields["role"], role);
    assert_eq!(fields["path"], path.to_str().unwrap());
    assert_eq!(fields["file_size_bytes"], size);
    assert!(fields["elapsed_ms"].as_u64().is_some(), "{fields}");
    assert_eq!(fields["success"], success);
}

#[tokio::test]
async fn lifecycle_check_counts_and_diagnostics_preserve_json_reports() {
    for initial_schema in [1, 2] {
        let dir = tempfile::Builder::new()
            .prefix("pe-check-counts-")
            .tempdir_in(std::env::current_dir().unwrap())
            .unwrap();
        std::fs::create_dir(dir.path().join("eval-results")).unwrap();
        let fixed = dir.path().join("fixed.db");
        let prior = dir.path().join("prior.db");
        let side = dir.path().join("side.db");
        let build = dir.path().join("build.json");
        let stage_path = dir.path().join("stage.json");
        if initial_schema == 1 {
            drop(seed_v1(&fixed, FRESH_END));
        } else {
            finalize_empty_activity_side(&dir, &fixed, FRESH_END).await;
        }
        // Real CLI stdout must stay exactly one JSON report even at INFO level.
        let staged = Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
            .arg("cache-stage-v2")
            .arg("--db")
            .arg(&fixed)
            .arg("--prior")
            .arg(&prior)
            .arg("--side")
            .arg(&side)
            .arg("--manifest")
            .arg(&build)
            .env("RUST_LOG", "info")
            .env("PE_BOOTSTRAP_OUTPUT", dir.path().join("watchlist.json"))
            .output()
            .unwrap();
        assert!(
            staged.status.success(),
            "{}",
            String::from_utf8_lossy(&staged.stderr)
        );
        let report: Value = serde_json::from_slice(&staged.stdout).unwrap();
        assert_eq!(report["resumed"], false);
        let events: Vec<Value> = String::from_utf8(staged.stderr)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["fields"]["message"] == "SQLite quick_check completed")
            .collect();
        assert_check_event(
            &events,
            "staging_fixed",
            &fixed,
            fixed.metadata().unwrap().len(),
            true,
        );
        let mut cycle_checks = events.len();
        let log = CheckLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        pe_bootstrap::cache_migration::stage_cache_cycle_v2(&fixed, &prior, &side, Some(&build))
            .unwrap();
        assert!(log.take().is_empty(), "staging resume must not rescan");
        if initial_schema == 1 {
            let size = side.metadata().unwrap().len();
            migrate_cache_v2(&side, &build).unwrap();
            let events = log.take();
            assert_check_event(&events, "migration_input", &side, size, true);
            cycle_checks += events.len();
            migrate_cache_v2(&side, &build).unwrap();
            assert!(log.take().is_empty(), "migration resume must not rescan");
            install_payout_manifest(&side);
        }
        populate_activity_fresh_v2(
            &side,
            &FixtureFetcher::new(HashMap::from([(
                activity_url(WALLET, FRESH_END),
                b"[]".to_vec(),
            )])),
            "https://data.example",
            u64::try_from(initial_schema).unwrap(),
            FRESH_END,
            FRESH_END + 1,
        )
        .await
        .unwrap();
        finalize_cache_v2(&side, &stage_path, FRESH_END + 2).unwrap();
        let stage = finalize_cache_v2(&side, &stage_path, FRESH_END + 3).unwrap();
        assert!(
            log.take().is_empty(),
            "both finalizations must omit structural scans"
        );
        let request = CacheActivationRequest {
            fixed_path: fixed.clone(),
            side_path: side.clone(),
            prior_cache_backup_path: prior.clone(),
            expected_side_sha256: stage.cache_sha256,
        };
        let size = side.metadata().unwrap().len();
        let activation = activate_cache_v2(&request).unwrap();
        let events = log.take();
        assert_check_event(&events, "activation_candidate", &side, size, true);
        cycle_checks += events.len();
        assert_eq!(cycle_checks, if initial_schema == 1 { 3 } else { 2 });
        assert!(activate_cache_v2(&request).unwrap().resumed);
        assert_check_event(&log.take(), "activation_missing_side", &fixed, size, true);
        let (publish, pending) =
            write_pending_publication(&dir, "restore", &side, &fixed, &fixed, &prior);
        let prior_size = prior.metadata().unwrap().len();
        restore_prior_cache(
            &fixed,
            &prior,
            &dir.path().join("displaced.db"),
            &PriorCacheBinding {
                sha256: activation.prior_cache_sha256,
                schema_version: activation.prior_cache_schema,
            },
            &publish,
            &pending,
            &FixedPublicationProbe(false),
        )
        .await
        .unwrap();
        assert_check_event(&log.take(), "restore_prior", &prior, prior_size, true);
        // A failed pragma emits one completion too, and keeps its original error.
        damage_unused_page(&fixed);
        let size = fixed.metadata().unwrap().len();
        let error =
            pe_bootstrap::cache_migration::stage_cache_cycle_v2(&fixed, &prior, &side, None)
                .unwrap_err();
        assert_structural_error(&error);
        assert_check_event(&log.take(), "staging_fixed", &fixed, size, false);
    }
}

// Authentic pre-648 full-root encoding, independently constructed from the
// version-one fields at 641edf6. No incremental rows are ever relabeled here.
fn convert_root_to_v1(path: &std::path::Path) {
    let mut identity = fresh_record(path);
    assert!(identity["base_generation"].is_null());
    for key in [
        "base_generation",
        "base_manifest_sha256",
        "start_exclusive",
        "full_read_wallets",
        "digest",
    ] {
        identity.as_object_mut().unwrap().remove(key);
    }
    identity["version"] = Value::from(1);
    let digest = whole_json_digest(&identity);
    identity["digest"] = Value::from(digest.clone());
    let connection = Connection::open(path).unwrap();
    connection
        .execute(
            "UPDATE cache_v2_migration_state SET fresh_collection_json = ?1",
            [identity.to_string()],
        )
        .unwrap();
    connection.execute("UPDATE activity_wallet_coverage_staging_v2 SET acquisition_json = NULL, reference_sha256 = ?1", [&digest]).unwrap();
    let receipts = stored_receipt_proofs(&connection, identity["generation"].as_i64().unwrap());
    let receipts_digest = whole_json_digest(
        &serde_json::json!({"generation": identity["generation"], "reference_sha256":digest,
        "fixed_end_unix":identity["fixed_end_unix"], "receipts":receipts}),
    );
    connection.execute("UPDATE activity_coverage_manifests_v2 SET reference_sha256 = ?1, receipt_set_digest = ?2,
        cursors_json = '{\"receipt_storage\":\"activity_wallet_coverage_staging_v2\",\"version\":1}', collection_identity_json = NULL",
        params![digest, receipts_digest]).unwrap();
}

#[derive(Default)]
struct DatasetFetcher {
    rows: Vec<Value>,
    calls: Mutex<Vec<(String, i64, i64)>>,
}

impl PageFetcher for DatasetFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        let url = reqwest::Url::parse(url).unwrap();
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        let wallet = query["user"].clone();
        let start = query["start"].parse::<i64>().unwrap();
        let end = query["end"].parse::<i64>().unwrap();
        let offset = query["offset"].parse::<usize>().unwrap();
        self.calls
            .lock()
            .unwrap()
            .push((wallet.clone(), start, end));
        let mut rows = self
            .rows
            .iter()
            .filter(|row| {
                row["proxyWallet"] == wallet
                    && row["timestamp"].as_i64().unwrap() >= start
                    && row["timestamp"].as_i64().unwrap() <= end
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| {
            (
                std::cmp::Reverse(row["timestamp"].as_i64().unwrap()),
                row["transactionHash"].as_str().unwrap().to_owned(),
            )
        });
        Ok(
            serde_json::to_vec(&rows.into_iter().skip(offset).take(500).collect::<Vec<_>>())
                .unwrap(),
        )
    }
}

fn dataset_row(wallet: &str, market: &str, id: &str, side: &str, epoch: i64) -> Value {
    serde_json::json!({"proxyWallet": wallet, "type":"TRADE", "conditionId":market,
        "asset":"123", "outcome":"Yes", "side":side, "size":"1.250001", "usdcSize":"0.625001",
        "price":"0.500000", "timestamp":epoch, "transactionHash":id, "outcomeIndex":"0"})
}

fn dataset_candidate(dir: &TempDir, name: &str, wallets: &[&str]) -> std::path::PathBuf {
    let side = dir.path().join(name);
    let mut cache = seed_v1(&side, FRESH_END - 10);
    for wallet in wallets {
        cache
            .upsert_wallets_bulk(&[(wallet.to_string(), SRC_TRADES, false, None, None, None, 0)])
            .unwrap();
        cache.conn_for_test_set_active(wallet, 1);
    }
    drop(cache);
    migrate_cache_v2(&side, &write_build_manifest(dir, &side)).unwrap();
    side
}

fn admit_dataset_wallet(side: &std::path::Path, wallet: &str) {
    let mut cache = WalletCache::open(side).unwrap();
    cache
        .upsert_wallets_bulk(&[(wallet.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
    cache.conn_for_test_set_active(wallet, 1);
}

async fn dataset_payouts(side: &std::path::Path, rows: &[Value]) {
    let markets = rows
        .iter()
        .map(|r| r["conditionId"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let data = markets.into_iter().map(|market| serde_json::json!({
        "condition_id":market, "active":true, "closed":true, "end_date_iso":"2027-01-16T00:00:00Z",
        "is_50_50_outcome":false, "tokens":[{"token_id":"123","outcome":"Yes","price":1,"winner":true},
        {"token_id":"456","outcome":"No","price":0,"winner":false}]})).collect::<Vec<_>>();
    ClobFetcher::new(
        "https://clob.example".to_owned(),
        FixtureFetcher::new(HashMap::from([(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            serde_json::to_vec(&serde_json::json!({"data":data,"next_cursor":"LTE="})).unwrap(),
        )])),
    )
    .fetch_closed_markets(&mut WalletCache::open(side).unwrap())
    .await
    .unwrap();
}

fn query_values(side: &std::path::Path, sql: &str) -> Vec<Vec<rusqlite::types::Value>> {
    let connection = Connection::open(side).unwrap();
    let mut statement = connection.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|i| row.get(i))
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn classify_dataset(rows: &[pe_bootstrap::cache::StoredActivityAggregateV2]) -> Vec<String> {
    use pe_source_polymarket_public::{
        ActivityAggregate, PriceWeightedShareAmount, SourceActivityGroupId,
    };
    let mut result = Vec::new();
    for wallet in [WALLET, WALLET_B, WALLET_C, WALLET_D, WALLET_E] {
        let wallet_address = WalletAddress::from_hex(wallet).unwrap();
        let mut buckets = BTreeMap::<i64, Vec<LedgerMutation>>::new();
        for row in rows.iter().filter(|row| row.wallet_hex == wallet) {
            let aggregate = ActivityAggregate {
                group_id: SourceActivityGroupId::derive(
                    serde_json::from_str(&row.components_json).unwrap(),
                )
                .unwrap(),
                semantic_revision: serde_json::from_value(Value::from(
                    row.semantic_revision.clone(),
                ))
                .unwrap(),
                row_count: row.row_count,
                share_sum: ShareAmount::from_decimal_exact(row.share_amount).unwrap(),
                price_weighted_share_sum: PriceWeightedShareAmount(row.price_weighted_share_amount),
                source_usdc_sum: pe_core_types::CollateralAmount::from_decimal_exact(
                    row.source_usdc_amount,
                )
                .unwrap(),
                source_time: SourceTimestamp(
                    time::OffsetDateTime::from_unix_timestamp(row.source_time_unix).unwrap(),
                ),
                is_combo: row.is_combo,
            };
            buckets
                .entry(row.source_time_unix)
                .or_default()
                .push(LedgerMutation::from_activity(&aggregate).unwrap());
        }
        let wallet = wallet_address;
        let mut ledger = PositionLedger::new();
        let mut history = std::collections::BTreeSet::new();
        for mutations in buckets.into_values() {
            let verdict = pe_position_ledger::classify_complete_historical_second(
                &ledger,
                wallet,
                &mutations,
                ReconstructionQuality::new(100).unwrap(),
                &|market| history.contains(&market.to_string()),
            )
            .unwrap();
            result.push(format!("{wallet}:{verdict:?}"));
            ledger.apply_all_or_none(&mutations).unwrap();
            for mutation in mutations {
                for key in mutation.touched_keys() {
                    history.insert(key.market().to_string());
                }
            }
        }
    }
    result
}

#[tokio::test]
async fn incremental_full_read_equivalence_through_aggregation_certification_and_projection() {
    let dir = TempDir::new().unwrap();
    let full = dataset_candidate(&dir, "full.db", &[WALLET_B, WALLET_C, WALLET_D, WALLET_E]);
    let incremental = dataset_candidate(&dir, "incremental.db", &[WALLET_B, WALLET_C, WALLET_D]);
    let e1 = FRESH_END;
    let e2 = e1 + 10;
    let mut dataset = vec![
        dataset_row(WALLET, "0xsame", "old-buy", "BUY", e1 - 10),
        dataset_row(WALLET, "0xsame", "sell", "SELL", e1 + 1),
        dataset_row(WALLET, "0xsame", "re-entry", "BUY", e1 + 2),
        dataset_row(WALLET, "0xequal", "equal-a", "BUY", e1 + 3),
        dataset_row(WALLET, "0xequal", "equal-b", "BUY", e1 + 3),
        dataset_row(WALLET_B, "0xempty-delta", "at-e1", "BUY", e1),
        dataset_row(WALLET_C, "0xnew-entry", "at-e2", "BUY", e2),
        dataset_row(
            WALLET_E,
            "0xnew-wallet-old",
            "new-wallet-old",
            "BUY",
            e1 - 1,
        ),
        dataset_row(WALLET_E, "0xnew-wallet-new", "new-wallet-new", "BUY", e2),
    ];
    for (kind, id, epoch) in [("SPLIT", "split", e1 - 5), ("MERGE", "merge", e1 + 4)] {
        dataset.push(serde_json::json!({"proxyWallet":WALLET, "type":kind, "conditionId":"0xeffects",
            "asset":"", "side":"", "size":"2", "usdcSize":"2", "price":"1", "timestamp":epoch, "transactionHash":id}));
    }
    let source = DatasetFetcher {
        rows: dataset.clone(),
        ..Default::default()
    };
    let full_manifest =
        populate_activity_fresh_v2(&full, &source, "https://data.example", 7, e2, e2 + 1)
            .await
            .unwrap();
    let root =
        populate_activity_fresh_v2(&incremental, &source, "https://data.example", 1, e1, e1 + 1)
            .await
            .unwrap();
    assert_eq!(
        count(
            &incremental,
            "SELECT COUNT(*) FROM activity_coverage_manifests_v2"
        ),
        1
    );
    assert_eq!(
        count(
            &incremental,
            "SELECT COUNT(*) FROM clob_payout_coverage_manifests_v2"
        ),
        0
    );
    assert_eq!(
        count(
            &incremental,
            "SELECT COUNT(*) FROM cache_v2_migration_state WHERE phase = 'finalized'"
        ),
        0
    );
    admit_dataset_wallet(&incremental, WALLET_E);
    source.calls.lock().unwrap().clear();
    let delta_manifest = pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
        &incremental,
        &source,
        "https://data.example",
        7,
        &[],
        || {
            assert_eq!(fresh_record(&incremental)["generation"], 1);
            Ok(e2)
        },
        e2 + 1,
    )
    .await
    .unwrap();
    let calls = source.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 5);
    for (wallet, start, end) in calls {
        assert_eq!(start, if wallet == WALLET_E { 1 } else { e1 + 1 });
        assert_eq!(end, e2);
    }
    assert_eq!(fresh_record(&incremental)["base_generation"], 1);
    assert_eq!(generation_rows(&incremental, 1), 0);
    assert_eq!(
        full_manifest.aggregate_digest,
        delta_manifest.aggregate_digest
    );
    assert_eq!(
        (
            full_manifest.group_count,
            full_manifest.source_row_count,
            full_manifest.wallet_count
        ),
        (
            delta_manifest.group_count,
            delta_manifest.source_row_count,
            delta_manifest.wallet_count
        )
    );
    assert_ne!(
        full_manifest.reference_sha256,
        delta_manifest.reference_sha256
    );
    assert_ne!(
        full_manifest.receipt_set_digest,
        delta_manifest.receipt_set_digest
    );
    for sql in [
        "SELECT * FROM activity_groups_v2 WHERE coverage_generation = 7 ORDER BY wallet_hex, source_time_unix, source_trade_id",
        "SELECT wallet_hex, ordered_aggregate_digest, aggregate_count, source_row_count FROM activity_wallet_coverage_staging_v2 WHERE generation = 7 ORDER BY wallet_hex",
        "SELECT wallet_hex, MAX(source_time_unix) FROM activity_groups_v2 WHERE coverage_generation = 7 AND activity_type = 'TRADE' GROUP BY wallet_hex ORDER BY wallet_hex",
    ] {
        assert_eq!(
            query_values(&full, sql),
            query_values(&incremental, sql),
            "{sql}"
        );
    }
    let full_typed = WalletCache::open_read_only(&full)
        .unwrap()
        .activity_aggregates_v2()
        .unwrap();
    let incremental_typed = WalletCache::open_read_only(&incremental)
        .unwrap()
        .activity_aggregates_v2()
        .unwrap();
    assert_eq!(format!("{full_typed:?}"), format!("{incremental_typed:?}"));
    assert_eq!(
        classify_dataset(&full_typed),
        classify_dataset(&incremental_typed)
    );
    for side in [&full, &incremental] {
        dataset_payouts(side, &dataset).await;
    }
    let a = finalize_cache_v2(&full, &dir.path().join("full-stage.json"), e2 + 2).unwrap();
    let b = finalize_cache_v2(
        &incremental,
        &dir.path().join("incremental-stage.json"),
        e2 + 2,
    )
    .unwrap();
    assert_eq!(a.ranker_projection_digest, b.ranker_projection_digest);
    assert_eq!(a.ranker_projection_count, b.ranker_projection_count);
    assert_eq!(
        classifier_projection_rows(&full),
        classifier_projection_rows(&incremental)
    );
    assert_eq!(projected_entries(&full), projected_entries(&incremental));
    assert!(
        projected_entries(&full)
            .iter()
            .any(|(_, market, _)| market == "0xsame")
    );
    assert!(
        !projected_entries(&full)
            .iter()
            .any(|(_, market, _)| market == "0xequal")
    );
    assert_eq!(root.group_count + 8, delta_manifest.group_count);
    assert_python_consumer_parity(&full, &incremental);

    // The old row-selection contract is unchanged: MAX(completed generation)
    // selects every effective row, including the old empty-delta wallet.
    assert_eq!(
        query_values(
            &incremental,
            "SELECT source_trade_id FROM activity_groups_v2 WHERE coverage_generation = (SELECT MAX(generation) FROM activity_coverage_manifests_v2) ORDER BY source_trade_id"
        ),
        query_values(
            &full,
            "SELECT source_trade_id FROM activity_groups_v2 WHERE coverage_generation = 7 ORDER BY source_trade_id"
        )
    );
}

fn assert_python_consumer_parity(full: &std::path::Path, incremental: &std::path::Path) {
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let parity = Command::new("python3").current_dir(repository).args(["-c",
        "import sys; sys.path.insert(0, 'scripts'); from test_ranker_duck_parity import assert_certified_full_incremental_equivalence; assert_certified_full_incremental_equivalence(sys.argv[1], sys.argv[2])"])
        .arg(full).arg(incremental).output().unwrap();
    assert!(
        parity.status.success(),
        "Python consumer parity: {}\n{}",
        String::from_utf8_lossy(&parity.stdout),
        String::from_utf8_lossy(&parity.stderr)
    );
}

#[tokio::test]
async fn incremental_collision_exclusion_then_full_replacement_and_empty_replacement() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "collisions.db", &[WALLET_B]);
    let initial = dataset_row(WALLET, "0xmarket", "shared-id", "BUY", FRESH_END - 1);
    let mut source = DatasetFetcher {
        rows: vec![
            initial.clone(),
            dataset_row(WALLET_B, "0xb", "b", "BUY", FRESH_END),
        ],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    let retained = query_values(
        &side,
        &format!("SELECT * FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'"),
    );
    let mut collision = initial;
    collision["timestamp"] = Value::from(FRESH_END + 1);
    source.rows.push(collision);
    let excluded = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(excluded.group_count, 1);
    assert_eq!(receipt(&side, 7, WALLET), Some((0, 0, 1)));
    assert_eq!(
        query_values(
            &side,
            &format!("SELECT * FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'")
        ),
        retained
    );
    let proofs = stored_receipt_proofs(&Connection::open(&side).unwrap(), 7);
    assert_eq!(
        proofs[0]["acquisition"]["exclusion_reason"],
        "cross_boundary_collision"
    );
    assert_eq!(proofs[0]["acquisition"]["aggregation_status"], "complete");
    assert_eq!(proofs[0]["acquisition"]["fetched_aggregate_count"], 1);
    assert_eq!(proofs[0]["acquisition"]["predecessor"]["carried"], false);
    dataset_payouts(&side, &source.rows).await;
    let excluded_stage = finalize_cache_v2(
        &side,
        &dir.path().join("excluded-stage.json"),
        FRESH_END + 4,
    )
    .unwrap();
    assert_eq!(excluded_stage.ranker_projection_count, 1);
    assert_eq!(
        projected_entries(&side),
        vec![(WALLET_B.to_owned(), "0xb".to_owned(), FRESH_END)]
    );
    let export = Command::new("python3")
        .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .args(["-c", "import sys; sys.path.insert(0, 'scripts'); from test_ranker_duck_parity import assert_certified_export_wallets; assert_certified_export_wallets(sys.argv[1], {sys.argv[2]})"])
        .arg(&side).arg(WALLET_B).output().unwrap();
    assert!(
        export.status.success(),
        "excluded-wallet export: {}\n{}",
        String::from_utf8_lossy(&export.stdout),
        String::from_utf8_lossy(&export.stderr)
    );
    let no_reads = DatasetFetcher::default();
    assert_eq!(
        populate_activity_fresh_v2(
            &side,
            &no_reads,
            "https://data.example",
            7,
            FRESH_END + 99,
            FRESH_END + 4
        )
        .await
        .unwrap(),
        excluded
    );
    assert!(no_reads.calls.lock().unwrap().is_empty());
    // The exclusion itself preserves the wallet even when acquisition eligibility disappears.
    Connection::open(&side)
        .unwrap()
        .execute("DELETE FROM wallets WHERE wallet_hex = ?1", [WALLET])
        .unwrap();
    source.rows.retain(|row| row["proxyWallet"] != WALLET);
    let mut revised = dataset_row(WALLET, "0xmarket", "shared-id", "BUY", FRESH_END - 1);
    revised["size"] = Value::from("2.5");
    source.rows.push(revised);
    source.calls.lock().unwrap().clear();
    let recovered = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        8,
        FRESH_END + 4,
        FRESH_END + 5,
    )
    .await
    .unwrap();
    assert!(
        source
            .calls
            .lock()
            .unwrap()
            .contains(&(WALLET.to_owned(), 1, FRESH_END + 4))
    );
    assert_eq!(
        count(
            &side,
            &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'")
        ),
        1
    );
    assert_eq!(generation_rows(&side, 1), 0);
    assert_eq!(receipt(&side, 8, WALLET), Some((1, 1, 1)));
    let full = dataset_candidate(&dir, "repaired-full.db", &[WALLET_B]);
    let fresh = populate_activity_fresh_v2(
        &full,
        &source,
        "https://data.example",
        8,
        FRESH_END + 4,
        FRESH_END + 5,
    )
    .await
    .unwrap();
    assert_eq!(fresh.aggregate_digest, recovered.aggregate_digest);
    assert_eq!(fresh.group_count, recovered.group_count);
    assert_eq!(fresh.source_row_count, recovered.source_row_count);
    assert_eq!(
        classify_dataset(
            &WalletCache::open_read_only(&full)
                .unwrap()
                .activity_aggregates_v2()
                .unwrap()
        ),
        classify_dataset(
            &WalletCache::open_read_only(&side)
                .unwrap()
                .activity_aggregates_v2()
                .unwrap()
        ),
    );
    dataset_payouts(&full, &source.rows).await;
    let full_stage = finalize_cache_v2(
        &full,
        &dir.path().join("repaired-full-stage.json"),
        FRESH_END + 6,
    )
    .unwrap();
    let recovered_stage = finalize_cache_v2(
        &side,
        &dir.path().join("recovered-stage.json"),
        FRESH_END + 6,
    )
    .unwrap();
    assert_eq!(full_stage.ranker_projection_count, 2);
    assert_eq!(
        full_stage.ranker_projection_count,
        recovered_stage.ranker_projection_count
    );
    assert_eq!(
        full_stage.ranker_projection_digest,
        recovered_stage.ranker_projection_digest
    );
    assert_eq!(
        classifier_projection_rows(&full),
        classifier_projection_rows(&side)
    );
    assert_eq!(projected_entries(&full), projected_entries(&side));
    assert_python_consumer_parity(&full, &side);
    // An explicit empty full read deletes every retained row; an empty delta on B carries.
    source.rows.retain(|row| row["proxyWallet"] != WALLET);
    pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
        &side,
        &source,
        "https://data.example",
        9,
        &[WALLET.to_owned()],
        || Ok(FRESH_END + 6),
        FRESH_END + 7,
    )
    .await
    .unwrap();
    assert_eq!(receipt(&side, 9, WALLET), Some((0, 0, 0)));
    assert_eq!(
        count(
            &side,
            &format!("SELECT COUNT(*) FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'")
        ),
        0
    );
    assert_eq!(receipt(&side, 9, WALLET_B), Some((1, 1, 0)));
    let receipts = stored_receipt_proofs(&Connection::open(&side).unwrap(), 9);
    assert_eq!(receipts[0]["acquisition"]["disposition"], "complete");
}

#[tokio::test]
async fn incremental_revision_before_boundary_requires_explicit_full_read() {
    let dir = TempDir::new().unwrap();
    let full = dataset_candidate(&dir, "revision-full.db", &[]);
    let side = dataset_candidate(&dir, "revision-incremental.db", &[]);
    let mut source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "old", "BUY", FRESH_END - 1)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    source.rows[0]["size"] = Value::from("2.5");
    let a = populate_activity_fresh_v2(
        &full,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    let b = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_ne!(
        a.aggregate_digest, b.aggregate_digest,
        "incremental reads cannot discover a pre-boundary revision"
    );
    let c = pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
        &side,
        &source,
        "https://data.example",
        8,
        &[WALLET.to_owned()],
        || Ok(FRESH_END + 4),
        FRESH_END + 5,
    )
    .await
    .unwrap();
    assert_eq!(a.aggregate_digest, c.aggregate_digest);
    assert_eq!(count(&side, "SELECT COUNT(*) FROM activity_groups_v2"), 1);
}

#[tokio::test]
async fn incremental_wallet_and_manifest_transactions_roll_back_at_every_write_boundary() {
    let dir = TempDir::new().unwrap();
    let prior = dataset_candidate(&dir, "crash-prior.db", &[]);
    let mut source = DatasetFetcher::default();
    for index in 0..1030 {
        source.rows.push(dataset_row(
            WALLET,
            "0xmarket",
            &format!("old-{index}"),
            "BUY",
            FRESH_END - 2000 + index,
        ));
    }
    populate_activity_fresh_v2(
        &prior,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    source.rows.push(dataset_row(
        WALLET,
        "0xmarket",
        "delta",
        "BUY",
        FRESH_END + 1,
    ));
    let fixtures = [
        (
            "before_carry",
            "BEFORE UPDATE OF coverage_generation ON activity_groups_v2 WHEN NEW.coverage_generation = 7",
        ),
        (
            "mid_carry",
            "BEFORE UPDATE OF coverage_generation ON activity_groups_v2 WHEN NEW.coverage_generation = 7 AND (SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = 7) = 513",
        ),
        (
            "after_delta",
            "AFTER INSERT ON activity_groups_v2 WHEN NEW.coverage_generation = 7",
        ),
        (
            "before_receipt",
            "BEFORE INSERT ON activity_wallet_coverage_staging_v2 WHEN NEW.generation = 7",
        ),
        (
            "after_receipt",
            "AFTER INSERT ON activity_wallet_coverage_staging_v2 WHEN NEW.generation = 7",
        ),
        (
            "before_manifest",
            "BEFORE INSERT ON activity_coverage_manifests_v2 WHEN NEW.generation = 7",
        ),
        (
            "after_manifest",
            "AFTER INSERT ON activity_coverage_manifests_v2 WHEN NEW.generation = 7",
        ),
    ];
    for (name, trigger) in fixtures {
        let side = dir.path().join(format!("{name}.db"));
        std::fs::copy(&prior, &side).unwrap();
        Connection::open(&side).unwrap().execute_batch(&format!("CREATE TRIGGER inject_failure {trigger} BEGIN SELECT RAISE(ABORT, 'injected wallet/manifest failure'); END;")).unwrap();
        let error = populate_activity_fresh_v2(
            &side,
            &source,
            "https://data.example",
            7,
            FRESH_END + 2,
            FRESH_END + 3,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected"), "{name}: {error}");
        let manifest_failure = name.contains("manifest");
        assert_eq!(
            generation_rows(&side, 1),
            if manifest_failure { 0 } else { 1030 },
            "{name}"
        );
        assert_eq!(
            generation_rows(&side, 7),
            if manifest_failure { 1031 } else { 0 },
            "{name}"
        );
        assert_eq!(
            receipt(&side, 7, WALLET).is_some(),
            manifest_failure,
            "{name}"
        );
        assert_eq!(
            count(
                &side,
                "SELECT COUNT(*) FROM activity_coverage_manifests_v2 WHERE generation = 7"
            ),
            0
        );
        Connection::open(&side)
            .unwrap()
            .execute_batch("DROP TRIGGER inject_failure")
            .unwrap();
        source.calls.lock().unwrap().clear();
        let manifest = populate_activity_fresh_v2(
            &side,
            &source,
            "https://data.example",
            7,
            FRESH_END + 999,
            FRESH_END + 4,
        )
        .await
        .unwrap();
        assert_eq!(manifest.group_count, 1031);
        assert_eq!(source.calls.lock().unwrap().is_empty(), manifest_failure);
        assert_eq!(fresh_record(&side)["fixed_end_unix"], FRESH_END + 2);
        source.calls.lock().unwrap().clear();
        assert_eq!(
            populate_activity_fresh_v2(
                &side,
                &source,
                "https://data.example",
                7,
                FRESH_END + 999,
                FRESH_END + 9
            )
            .await
            .unwrap(),
            manifest
        );
        assert!(
            source.calls.lock().unwrap().is_empty(),
            "after commit/before output retry"
        );
    }
}

#[tokio::test]
async fn incremental_carry_uses_advancing_wallet_index_and_preserves_rowids_across_integer_widths()
{
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "carry-progress.db", &[WALLET_B]);
    let mut source = DatasetFetcher::default();
    for wallet in [WALLET, WALLET_B] {
        for index in 0..1100 {
            source.rows.push(dataset_row(
                wallet,
                "0xmarket",
                &format!("{wallet}-{index}"),
                "BUY",
                FRESH_END - 2000 + index,
            ));
        }
    }
    let root = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    let connection = Connection::open(&side).unwrap();
    // Interleave physical rows while retaining the exact table and indexes.
    connection.execute_batch("CREATE TEMP TABLE interleaved AS SELECT * FROM activity_groups_v2;
        DELETE FROM activity_groups_v2; INSERT INTO activity_groups_v2 SELECT * FROM interleaved ORDER BY source_time_unix, wallet_hex;
        CREATE TABLE carry_updates(source_trade_id TEXT, old_rowid INTEGER, new_rowid INTEGER, generation INTEGER);
        CREATE TRIGGER record_carry AFTER UPDATE OF coverage_generation ON activity_groups_v2
        BEGIN INSERT INTO carry_updates VALUES (NEW.source_trade_id, OLD.rowid, NEW.rowid, NEW.coverage_generation); END;").unwrap();
    for query in [
        "SELECT source_trade_id FROM activity_groups_v2 WHERE wallet_hex = 'wallet' AND coverage_generation = 1 AND (source_time_unix, source_trade_id) > (10, 'id') ORDER BY source_time_unix, source_trade_id LIMIT 512",
        "UPDATE activity_groups_v2 SET coverage_generation = 2 WHERE wallet_hex = 'wallet' AND coverage_generation = 1 AND (source_time_unix, source_trade_id) > (10, 'id') AND (source_time_unix, source_trade_id) <= (20, 'last')",
    ] {
        let plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("idx_activity_groups_v2_wallet_time"),
            "{plan}"
        );
        assert!(
            !plan.contains("SCAN activity_groups") && !plan.contains("TEMP B-TREE"),
            "{plan}"
        );
    }
    drop(connection);
    for (index, generation) in [2, 7, 127, 128].into_iter().enumerate() {
        let end = FRESH_END + i64::try_from(index).unwrap() + 2;
        let manifest = populate_activity_fresh_v2(
            &side,
            &source,
            "https://data.example",
            generation,
            end,
            end + 1,
        )
        .await
        .unwrap();
        assert_eq!(manifest.aggregate_digest, root.aggregate_digest);
        assert_eq!(manifest.group_count, 2200);
        assert_eq!(
            count(
                &side,
                &format!("SELECT COUNT(*) FROM carry_updates WHERE generation = {generation}")
            ),
            2200
        );
        assert_eq!(
            count(
                &side,
                "SELECT COUNT(*) FROM carry_updates WHERE old_rowid != new_rowid"
            ),
            0
        );
        assert_eq!(
            count(
                &side,
                &format!(
                    "SELECT COUNT(*) FROM (SELECT source_trade_id FROM carry_updates WHERE generation = {generation} GROUP BY source_trade_id HAVING COUNT(*) != 1)"
                )
            ),
            0
        );
    }
}

#[tokio::test]
async fn incremental_versions_windows_and_predecessor_corruption_fail_before_source_io() {
    let dir = TempDir::new().unwrap();
    let base = dataset_candidate(&dir, "proof-base.db", &[]);
    let source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "base", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &base,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    // Freeze N but fail the receipt, keeping B's rows in place for restart tests.
    Connection::open(&base)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER stop_receipt BEFORE INSERT ON activity_wallet_coverage_staging_v2
        WHEN NEW.generation = 7 BEGIN SELECT RAISE(ABORT, 'pause'); END;",
        )
        .unwrap();
    populate_activity_fresh_v2(
        &base,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap_err();
    Connection::open(&base)
        .unwrap()
        .execute_batch("DROP TRIGGER stop_receipt")
        .unwrap();
    for (name, sql) in [
        (
            "future_identity",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.version', 99)",
        ),
        (
            "identity_downgrade",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.version', 1)",
        ),
        (
            "changed_end",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.fixed_end_unix', 2)",
        ),
        (
            "changed_start",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.start_exclusive', 2)",
        ),
        (
            "full_membership",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.full_read_wallets', json('[\"0x1111111111111111111111111111111111111111\"]'))",
        ),
        (
            "base_generation",
            "UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.base_generation', 6)",
        ),
        (
            "missing_base_identity",
            "UPDATE activity_coverage_manifests_v2 SET collection_identity_json = NULL WHERE generation = 1",
        ),
        (
            "changed_base_receipt",
            "UPDATE activity_wallet_coverage_staging_v2 SET ordered_aggregate_digest = printf('%064d', 0) WHERE generation = 1",
        ),
        (
            "missing_base_receipt",
            "DELETE FROM activity_wallet_coverage_staging_v2 WHERE generation = 1",
        ),
        (
            "base_manifest",
            "UPDATE activity_coverage_manifests_v2 SET completed_at_unix = completed_at_unix + 1 WHERE generation = 1",
        ),
    ] {
        let side = dir.path().join(format!("{name}.db"));
        std::fs::copy(&base, &side).unwrap();
        Connection::open(&side).unwrap().execute_batch(sql).unwrap();
        let fetcher = DatasetFetcher::default();
        let result = populate_activity_fresh_v2(
            &side,
            &fetcher,
            "https://data.example",
            7,
            FRESH_END + 99,
            FRESH_END + 4,
        )
        .await;
        assert!(result.is_err(), "{name}");
        assert!(fetcher.calls.lock().unwrap().is_empty(), "{name}");
        assert_eq!(generation_rows(&side, 1), 1);
    }
    // An unreceipted row at N is corruption, not something a retry deletes.
    let side = dir.path().join("unreceipted.db");
    std::fs::copy(&base, &side).unwrap();
    Connection::open(&side)
        .unwrap()
        .execute("UPDATE activity_groups_v2 SET coverage_generation = 7", [])
        .unwrap();
    assert!(
        populate_activity_fresh_v2(
            &side,
            &source,
            "https://data.example",
            7,
            FRESH_END + 99,
            FRESH_END + 4
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("unexpected current-generation")
    );
    assert_eq!(generation_rows(&side, 7), 1);
    // The clock is not sampled on a damaged predecessor, and no successor identity is written.
    let damaged = dataset_candidate(&dir, "clock.db", &[]);
    populate_activity_fresh_v2(
        &damaged,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    Connection::open(&damaged)
        .unwrap()
        .execute("DELETE FROM activity_groups_v2", [])
        .unwrap();
    let sampled = AtomicUsize::new(0);
    assert!(
        pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
            &damaged,
            &source,
            "https://data.example",
            2,
            &[],
            || {
                sampled.fetch_add(1, Ordering::SeqCst);
                Ok(FRESH_END + 10)
            },
            FRESH_END + 11
        )
        .await
        .is_err()
    );
    assert_eq!(sampled.load(Ordering::SeqCst), 0);
    assert_eq!(fresh_record(&damaged)["generation"], 1);
}

#[tokio::test]
async fn incremental_receipt_resume_validates_new_metadata_without_loading_completed_rows() {
    let dir = TempDir::new().unwrap();
    let base = dataset_candidate(&dir, "receipt-proof.db", &[]);
    let source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "base", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &base,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    Connection::open(&base)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER stop_manifest BEFORE INSERT ON activity_coverage_manifests_v2
        WHEN NEW.generation = 7 BEGIN SELECT RAISE(ABORT, 'pause'); END;",
        )
        .unwrap();
    populate_activity_fresh_v2(
        &base,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap_err();
    Connection::open(&base)
        .unwrap()
        .execute_batch("DROP TRIGGER stop_manifest")
        .unwrap();
    for (index, change) in [
        "NULL",
        "'{}'",
        "json_set(acquisition_json, '$.version', 3)",
        "json_set(acquisition_json, '$.start_exclusive', 0)",
        "json_set(acquisition_json, '$.fixed_end_unix', 9)",
        "json_set(acquisition_json, '$.mode', 'full')",
        "json_set(acquisition_json, '$.read_sha256', printf('%064d', 0))",
        "json_set(acquisition_json, '$.predecessor.wallet_receipt_sha256', printf('%064d', 0))",
        "json_set(acquisition_json, '$.predecessor.carried', json('false'))",
        "json_set(acquisition_json, '$.disposition', 'excluded')",
        "json_set(acquisition_json, '$.fetched_source_row_count', 1)",
        "json_set(acquisition_json, '$.fetched_aggregate_count', json('null'))",
    ]
    .into_iter()
    .enumerate()
    {
        let side = dir.path().join(format!("receipt-{index}.db"));
        std::fs::copy(&base, &side).unwrap();
        Connection::open(&side).unwrap().execute_batch(&format!("UPDATE activity_wallet_coverage_staging_v2 SET acquisition_json = {change} WHERE generation = 7")).unwrap();
        let empty = DatasetFetcher::default();
        assert!(
            populate_activity_fresh_v2(
                &side,
                &empty,
                "https://data.example",
                7,
                FRESH_END + 99,
                FRESH_END + 4
            )
            .await
            .is_err(),
            "{change}"
        );
        assert!(empty.calls.lock().unwrap().is_empty());
    }
    Connection::open(&base)
        .unwrap()
        .execute(
            "DELETE FROM activity_groups_v2 WHERE coverage_generation = 7",
            [],
        )
        .unwrap();
    let empty = DatasetFetcher::default();
    let error = populate_activity_fresh_v2(
        &base,
        &empty,
        "https://data.example",
        7,
        FRESH_END + 99,
        FRESH_END + 4,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("aggregate mismatch"), "{error}");
    assert!(empty.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn incremental_authentic_v1_resume_and_old_reader_proof_boundary() {
    // Exact strict shape at 641edf6: unknown incremental keys fail before the
    // old collector could start a destructive generation. Plain row readers
    // are exercised in the full/incremental equivalence fixture above.
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct OldIdentity {
        version: u32,
        generation: u64,
        fixed_end_unix: i64,
        wallets: Vec<String>,
        digest: String,
    }
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "v1-resume.db", &[]);
    let source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "base", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    assert!(serde_json::from_value::<OldIdentity>(fresh_record(&side)).is_err());
    convert_root_to_v1(&side);
    Connection::open(&side)
        .unwrap()
        .execute("DELETE FROM activity_coverage_manifests_v2", [])
        .unwrap();
    let old = fresh_record(&side);
    let decoded: OldIdentity = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.generation, 1);
    assert_eq!(decoded.fixed_end_unix, FRESH_END);
    assert_eq!(decoded.wallets, [WALLET]);
    assert_eq!(decoded.digest, old["digest"].as_str().unwrap());
    let before = stored_receipt_proofs(&Connection::open(&side).unwrap(), 1);
    let empty = DatasetFetcher::default();
    let resumed = populate_activity_fresh_v2(
        &side,
        &empty,
        "https://data.example",
        1,
        FRESH_END + 99,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert!(empty.calls.lock().unwrap().is_empty());
    assert_eq!(fresh_record(&side), old);
    assert_eq!(
        stored_receipt_proofs(&Connection::open(&side).unwrap(), 1),
        before
    );
    assert_eq!(resumed.cursors["version"], 1);
    let successor = populate_activity_fresh_v2(
        &side,
        &empty,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 4,
    )
    .await
    .unwrap();
    assert_eq!(successor.cursors["version"], 2);
    assert_ne!(
        successor.cursors,
        serde_json::json!({"receipt_storage":"activity_wallet_coverage_staging_v2","version":1}),
        "old marker decoder refuses the new proof"
    );
    assert_eq!(successor.aggregate_digest, resumed.aggregate_digest);
    let archive: String = Connection::open(&side).unwrap().query_row("SELECT collection_identity_json FROM activity_coverage_manifests_v2 WHERE generation = 1", [], |row| row.get(0)).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&archive).unwrap(), old);
}

#[tokio::test]
async fn incremental_saturated_pages_commit_probe_evidence_without_double_counting() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "saturated.db", &[]);
    let source = DatasetFetcher {
        rows: (0..5501)
            .map(|index| {
                dataset_row(
                    WALLET,
                    "0xmarket",
                    &format!("row-{index}"),
                    "BUY",
                    FRESH_END - 6000 + index,
                )
            })
            .collect(),
        ..Default::default()
    };
    let root = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    assert_eq!((root.group_count, root.source_row_count), (5501, 5501));
    let proofs = stored_receipt_proofs(&Connection::open(&side).unwrap(), 1);
    let pages = proofs[0]["pages"].as_array().unwrap();
    assert!(
        pages
            .iter()
            .map(|p| p["row_count"].as_u64().unwrap())
            .sum::<u64>()
            > 5501
    );
    let acquisition = &proofs[0]["acquisition"];
    let reference = serde_json::json!({"version":2,"wallet_hex":WALLET,"mode":acquisition["mode"],
        "start_exclusive":0,"fixed_end_unix":FRESH_END,"pages":pages,
        "aggregation_status":acquisition["aggregation_status"],
        "ordered_aggregate_digest":acquisition["fetched_aggregate_digest"],
        "aggregate_count":5501,"source_row_count":5501});
    assert_eq!(acquisition["read_sha256"], whole_json_digest(&reference));
    let delta = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(delta.aggregate_digest, root.aggregate_digest);
    assert_eq!((delta.group_count, delta.source_row_count), (5501, 5501));
}

#[tokio::test]
async fn incremental_full_replacement_rolls_back_deletion_and_keeps_failed_history_excluded() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "replace.db", &[]);
    let mut source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "old", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    let old = query_values(&side, "SELECT * FROM activity_groups_v2");
    Connection::open(&side).unwrap().execute_batch("CREATE TRIGGER fail_replacement BEFORE INSERT ON activity_groups_v2 WHEN NEW.coverage_generation = 7 BEGIN SELECT RAISE(ABORT, 'injected replacement failure'); END").unwrap();
    source.rows[0]["size"] = Value::from("3.25");
    let replace = || {
        pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
            &side,
            &source,
            "https://data.example",
            7,
            &[],
            || Ok(FRESH_END + 99),
            FRESH_END + 4,
        )
    };
    assert!(
        pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
            &side,
            &source,
            "https://data.example",
            7,
            &[WALLET.to_owned()],
            || Ok(FRESH_END + 2),
            FRESH_END + 3
        )
        .await
        .is_err()
    );
    assert_eq!(query_values(&side, "SELECT * FROM activity_groups_v2"), old);
    assert!(receipt(&side, 7, WALLET).is_none());
    Connection::open(&side)
        .unwrap()
        .execute_batch("DROP TRIGGER fail_replacement")
        .unwrap();
    replace().await.unwrap();
    assert_eq!(fresh_record(&side)["fixed_end_unix"], FRESH_END + 2);
    assert_eq!(generation_rows(&side, 1), 0);
    let repaired = query_values(&side, "SELECT * FROM activity_groups_v2");
    // Two new rows with the same semantic group but different times are unbucketable.
    source.rows = vec![
        dataset_row(WALLET, "0xmarket", "bad", "BUY", FRESH_END + 3),
        dataset_row(WALLET, "0xmarket", "bad", "BUY", FRESH_END + 4),
    ];
    let excluded = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        8,
        FRESH_END + 5,
        FRESH_END + 6,
    )
    .await
    .unwrap();
    assert_eq!(excluded.group_count, 0);
    assert_eq!(
        query_values(&side, "SELECT * FROM activity_groups_v2"),
        repaired
    );
    let proofs = stored_receipt_proofs(&Connection::open(&side).unwrap(), 8);
    assert_eq!(
        proofs[0]["acquisition"]["exclusion_reason"],
        "aggregation_failure"
    );
    assert_eq!(proofs[0]["acquisition"]["fetched_source_row_count"], 2);
    assert!(proofs[0]["acquisition"]["fetched_aggregate_digest"].is_null());
    source.rows.clear();
    source.calls.lock().unwrap().clear();
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        9,
        FRESH_END + 7,
        FRESH_END + 8,
    )
    .await
    .unwrap();
    assert_eq!(source.calls.lock().unwrap()[0].1, 1);
    assert_eq!(count(&side, "SELECT COUNT(*) FROM activity_groups_v2"), 0);
}

#[tokio::test]
async fn incremental_wal_reader_blocks_checkpoint_without_exposing_partial_carry() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "wal.db", &[]);
    let source = DatasetFetcher {
        rows: (0..1100)
            .map(|index| {
                dataset_row(
                    WALLET,
                    "0xmarket",
                    &format!("wal-{index}"),
                    "BUY",
                    FRESH_END - index,
                )
            })
            .collect(),
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    let writer = Connection::open(&side).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    let reader = Connection::open(&side).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let old: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old, 1100);
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        2,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(
        reader
            .query_row::<i64, _, _>(
                "SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = 1",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        1100
    );
    assert_eq!(generation_rows(&side, 2), 1100);
    writer.busy_timeout(std::time::Duration::ZERO).unwrap();
    let blocked: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(blocked.0, 1);
    assert!(blocked.1 > blocked.2);
    reader.execute_batch("ROLLBACK").unwrap();
    drop(reader);
    let released: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(released, (0, 0, 0));
    assert_eq!(generation_rows(&side, 2), 1100);
}

#[tokio::test]
async fn incremental_equal_revision_collision_still_excludes_instead_of_upserting() {
    // The inherited certificate deliberately has the same revision as the new
    // row. A normally parsed revision binds time, but cache collision handling
    // must not rely on that incidental difference to reject an equal revision.
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "equal-revision.db", &[]);
    let old = dataset_row(WALLET, "0xmarket", "shared", "BUY", FRESH_END);
    let next = dataset_row(WALLET, "0xmarket", "shared", "BUY", FRESH_END + 1);
    let source = DatasetFetcher {
        rows: vec![old.clone()],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    convert_root_to_v1(&side);
    let parse = |row: Value| {
        let time = time::OffsetDateTime::from_unix_timestamp(FRESH_END + 2).unwrap();
        parse_activity_response(
            &serde_json::to_vec(&vec![row]).unwrap(),
            WalletAddress::from_hex(WALLET).unwrap(),
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(time),
                received_at: ReceivedAt(time),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap()
        .aggregates()
        .unwrap()
        .remove(0)
    };
    let mut inherited = parse(old);
    inherited.semantic_revision = parse(next.clone()).semantic_revision;
    let aggregate_digest = whole_json_digest(&vec![inherited.clone()]);
    let connection = Connection::open(&side).unwrap();
    connection
        .execute(
            "UPDATE activity_groups_v2 SET semantic_revision = ?1",
            [inherited.semantic_revision.as_str()],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE activity_wallet_coverage_staging_v2 SET ordered_aggregate_digest = ?1",
            [&aggregate_digest],
        )
        .unwrap();
    let identity = fresh_record(&side);
    let receipts = stored_receipt_proofs(&connection, 1);
    let set_digest = whole_json_digest(
        &serde_json::json!({"fixed_end_unix":FRESH_END,"generation":1,"reference_sha256":identity["digest"],"receipts":receipts}),
    );
    connection.execute("UPDATE activity_coverage_manifests_v2 SET aggregate_digest = ?1, receipt_set_digest = ?2", params![aggregate_digest, set_digest]).unwrap();
    drop(connection);
    let before = query_values(&side, "SELECT * FROM activity_groups_v2");
    let next_source = DatasetFetcher {
        rows: vec![next],
        ..Default::default()
    };
    let result = populate_activity_fresh_v2(
        &side,
        &next_source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(result.group_count, 0);
    assert_eq!(
        query_values(&side, "SELECT * FROM activity_groups_v2"),
        before
    );
    assert_eq!(
        stored_receipt_proofs(&Connection::open(&side).unwrap(), 7)[0]["acquisition"]["exclusion_reason"],
        "cross_boundary_collision"
    );
}

#[tokio::test]
async fn incremental_admission_rejects_invalid_bounds_generation_and_full_read_selection() {
    let dir = TempDir::new().unwrap();
    for (index, (generation, end, full)) in [
        (0, FRESH_END, vec![]),
        (u64::MAX, FRESH_END, vec![]),
        (1, -1, vec![]),
        (1, 0, vec![]),
        (1, i64::MAX, vec![]),
        (1, FRESH_END, vec!["invalid".to_owned()]),
        (1, FRESH_END, vec![WALLET_B.to_owned()]),
    ]
    .into_iter()
    .enumerate()
    {
        let side = dataset_candidate(&dir, &format!("bad-admission-{index}.db"), &[]);
        let source = DatasetFetcher::default();
        assert!(
            pe_bootstrap::cache_migration::populate_activity_fresh_v2_with_clock(
                &side,
                &source,
                "https://data.example",
                generation,
                &full,
                || Ok(end),
                FRESH_END
            )
            .await
            .is_err(),
            "case {index}"
        );
        assert!(source.calls.lock().unwrap().is_empty());
        assert_eq!(
            count(
                &side,
                "SELECT COUNT(*) FROM cache_v2_migration_state WHERE fresh_collection_json IS NOT NULL"
            ),
            0
        );
        assert_eq!(
            count(
                &side,
                "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
            ),
            0
        );
    }
}

#[tokio::test]
async fn incremental_proof_loading_refuses_external_commit_before_wallet_writes() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "proof-loading-race.db", &[]);
    let source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "old", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    // Admit the successor, but leave it without any completed wallets.
    populate_activity_fresh_v2(
        &side,
        &FixtureFetcher::new(HashMap::new()),
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap_err();
    source.calls.lock().unwrap().clear();
    let connection = trace_collection_interleaving(
        &side,
        "SELECT MAX(generation) FROM activity_coverage_manifests_v2 WHERE generation < 7",
        // Skip receipt-only startup's proof load; target the collector's proof.
        1,
        // This changes the manifest commitment after it was read, while leaving
        // historical receipt validation valid against the cached manifest.
        "UPDATE activity_coverage_manifests_v2 SET completed_at_unix = completed_at_unix + 1 WHERE generation = 1",
    );
    let error = pe_bootstrap::cache_migration::collect_activity_v2_for_test(
        connection,
        &source,
        "https://data.example",
        FRESH_END + 3,
    )
    .await
    .unwrap_err();
    assert_collection_interleaving_committed();
    assert_eq!(
        generation_rows(&side, 7),
        0,
        "stale proof must not carry rows"
    );
    assert_eq!(receipt(&side, 7, WALLET), None);
    assert_eq!(generation_rows(&side, 1), 1);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_coverage_manifests_v2 WHERE generation = 7"
        ),
        0
    );
    assert!(source.calls.lock().unwrap().is_empty());
    assert!(
        error
            .to_string()
            .contains("collection changed externally while loading proof"),
        "{error}"
    );
}

#[tokio::test]
async fn incremental_completion_refuses_external_commit_after_validation() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "completion-race.db", &[]);
    let source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "old", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    // Resume with all wallet receipts present and only manifest installation left.
    Connection::open(&side)
        .unwrap()
        .execute("DELETE FROM activity_coverage_manifests_v2", [])
        .unwrap();
    let connection = trace_collection_interleaving(
        &side,
        "FROM activity_coverage_manifests_v2 WHERE generation = 1",
        // Skip the initial completed-manifest probe. The next lookup begins
        // installation, after every receipt, row digest and total was validated.
        1,
        "DELETE FROM activity_groups_v2 WHERE coverage_generation = 1",
    );
    let no_reads = DatasetFetcher::default();
    let result = pe_bootstrap::cache_migration::collect_activity_v2_for_test(
        connection,
        &no_reads,
        "https://data.example",
        FRESH_END + 1,
    )
    .await;
    assert_collection_interleaving_committed();
    assert_eq!(generation_rows(&side, 1), 0, "external deletion committed");
    assert!(no_reads.calls.lock().unwrap().is_empty());
    assert_eq!(
        count(&side, "SELECT COUNT(*) FROM activity_coverage_manifests_v2"),
        0,
        "changed rows must not acquire a completed manifest"
    );
    let error = result.unwrap_err();
    assert!(
        matches!(error, pe_bootstrap::error::BootstrapError::Sqlite(
        rusqlite::Error::SqliteFailure(ref failure, _)
    ) if failure.extended_code == rusqlite::ffi::SQLITE_BUSY_SNAPSHOT),
        "{error}"
    );
}

struct CollectionInterleaving {
    path: std::path::PathBuf,
    sql_fragment: &'static str,
    skip: usize,
    mutation: &'static str,
    committed: bool,
}

thread_local! {
    // The current-thread Tokio scenarios each own their trace state. The
    // collector's writer thread has none; unrelated parallel tests are isolated.
    static COLLECTION_INTERLEAVING: std::cell::RefCell<Option<CollectionInterleaving>> = const { std::cell::RefCell::new(None) };
}

fn trace_collection_interleaving(
    path: &std::path::Path,
    sql_fragment: &'static str,
    skip: usize,
    mutation: &'static str,
) -> Connection {
    let mut connection = Connection::open(path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    COLLECTION_INTERLEAVING.with(|state| {
        assert!(state.borrow().is_none());
        *state.borrow_mut() = Some(CollectionInterleaving {
            path: path.to_owned(),
            sql_fragment,
            skip,
            mutation,
            committed: false,
        });
    });
    connection.trace(Some(|sql| {
        COLLECTION_INTERLEAVING.with(|state| {
            let mut state = state.borrow_mut();
            let Some(state) = state.as_mut() else { return };
            if state.committed || !sql.contains(state.sql_fragment) {
                return;
            }
            if state.skip > 0 {
                state.skip -= 1;
                return;
            }
            let external = Connection::open(&state.path).unwrap();
            external.busy_timeout(std::time::Duration::ZERO).unwrap();
            assert_eq!(external.execute(state.mutation, []).unwrap(), 1);
            state.committed = true;
        });
    }));
    connection
}

fn assert_collection_interleaving_committed() {
    // rusqlite catches trace callback panics; assert outside the callback that
    // the scheduled external mutation really committed before checking refusal.
    COLLECTION_INTERLEAVING.with(|state| assert!(state.borrow_mut().take().unwrap().committed));
}

#[tokio::test]
async fn incremental_writer_rechecks_identity_after_external_commit_during_fetch() {
    struct ChangingFetcher {
        path: std::path::PathBuf,
    }
    impl PageFetcher for ChangingFetcher {
        async fn fetch_page(&self, _: &str) -> Result<Vec<u8>, SourceError> {
            Connection::open(&self.path).unwrap().execute("UPDATE cache_v2_migration_state SET fresh_collection_json = json_set(fresh_collection_json, '$.fixed_end_unix', 1)", []).unwrap();
            Ok(b"[]".to_vec())
        }
    }
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "external-commit.db", &[]);
    let error = populate_activity_fresh_v2(
        &side,
        &ChangingFetcher { path: side.clone() },
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("collection changed externally"),
        "{error}"
    );
    assert_eq!(count(&side, "SELECT COUNT(*) FROM activity_groups_v2"), 0);
    assert_eq!(
        count(
            &side,
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2"
        ),
        0
    );
}

#[tokio::test]
async fn incremental_exclusion_cannot_hide_missing_predecessor_rows_on_restart() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "excluded-corruption.db", &[]);
    let mut source = DatasetFetcher {
        rows: vec![dataset_row(WALLET, "0xmarket", "old", "BUY", FRESH_END)],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    source.rows = vec![
        dataset_row(WALLET, "0xmarket", "bad", "BUY", FRESH_END + 1),
        dataset_row(WALLET, "0xmarket", "bad", "BUY", FRESH_END + 2),
    ];
    Connection::open(&side).unwrap().execute_batch("CREATE TRIGGER stop_exclusion BEFORE INSERT ON activity_wallet_coverage_staging_v2 WHEN NEW.generation = 7 BEGIN SELECT RAISE(ABORT, 'pause'); END").unwrap();
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 3,
        FRESH_END + 4,
    )
    .await
    .unwrap_err();
    Connection::open(&side)
        .unwrap()
        .execute_batch("DROP TRIGGER stop_exclusion; DELETE FROM activity_groups_v2")
        .unwrap();
    let error = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 99,
        FRESH_END + 5,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match predecessor receipt"),
        "{error}"
    );
    assert!(receipt(&side, 7, WALLET).is_none());
}

#[tokio::test]
async fn acquisition_failure_authentic_649_resume_preserves_proofs_and_recovers_exclusions() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "649-resume.db", &[WALLET_B, WALLET_C]);
    let mut source = DatasetFetcher {
        rows: vec![
            dataset_row(WALLET, "0xa", "a", "BUY", FRESH_END),
            dataset_row(WALLET_B, "0xb", "b", "BUY", FRESH_END),
            dataset_row(WALLET_C, "0xc", "c", "BUY", FRESH_END),
        ],
        ..Default::default()
    };
    source.rows[1]["price"] = Value::from("3.1968021978");
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    // Reconstruct #649's authentic identity/proof/schema, then its interrupted
    // state: the failed wallet and one ordinary wallet completed, C did not.
    convert_root_to_v1(&side);
    let connection = Connection::open(&side).unwrap();
    connection
        .execute_batch(
            "DELETE FROM activity_coverage_manifests_v2;
        ALTER TABLE activity_wallet_coverage_staging_v2 DROP COLUMN acquisition_json;",
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM activity_wallet_coverage_staging_v2 WHERE wallet_hex = ?1",
            [WALLET_C],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM activity_groups_v2 WHERE wallet_hex = ?1",
            [WALLET_C],
        )
        .unwrap();
    let before = stored_receipt_proofs(&connection, 1);
    let bytes = serde_json::to_vec(&before).unwrap();
    assert!(before[0].get("exclusion_reason").is_none());
    assert!(
        before
            .iter()
            .all(|proof| proof.get("acquisition").is_none())
    );
    assert!(
        before[1]["exclusion_reason"]
            .as_str()
            .unwrap()
            .contains("price")
    );
    assert_eq!(before[1]["pages"], serde_json::json!([]));
    let identity = fresh_record(&side);
    source.calls.lock().unwrap().clear();
    let resumed = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END + 99,
        FRESH_END + 2,
    )
    .await
    .unwrap();
    assert_eq!(
        *source.calls.lock().unwrap(),
        vec![(WALLET_C.to_owned(), 1, FRESH_END)]
    );
    assert_eq!(fresh_record(&side), identity);
    let after = stored_receipt_proofs(&connection, 1);
    assert_eq!(serde_json::to_vec(&after[..2]).unwrap(), bytes);
    assert_eq!(
        resumed.receipt_set_digest,
        whole_json_digest(&serde_json::json!({
            "generation":1, "reference_sha256":identity["digest"], "fixed_end_unix":FRESH_END, "receipts":after,
        }))
    );
    drop(connection);
    source.rows[1]["price"] = Value::from("0.500000");
    for embedded in [false, true] {
        let next = dir.path().join(format!("649-next-{embedded}.db"));
        std::fs::copy(&side, &next).unwrap();
        if embedded {
            install_legacy_receipt_manifest(&next, 1);
        }
        // No retained rows or active membership can hide a lost exclusion.
        Connection::open(&next)
            .unwrap()
            .execute("DELETE FROM wallets WHERE wallet_hex = ?1", [WALLET_B])
            .unwrap();
        dataset_payouts(&next, &source.rows).await;
        let stage = finalize_cache_v2(
            &next,
            &dir.path().join(format!("649-before-{embedded}.json")),
            FRESH_END + 3,
        )
        .unwrap();
        assert_eq!(stage.ranker_projection_count, 2);
        source.calls.lock().unwrap().clear();
        populate_activity_fresh_v2(
            &next,
            &source,
            "https://data.example",
            7,
            FRESH_END + 10,
            FRESH_END + 11,
        )
        .await
        .unwrap();
        let calls = source.calls.lock().unwrap().clone();
        assert!(calls.contains(&(WALLET_B.to_owned(), 1, FRESH_END + 10)));
        assert!(calls.contains(&(WALLET.to_owned(), FRESH_END + 1, FRESH_END + 10)));
        assert_eq!(
            count(
                &next,
                "SELECT COUNT(*) FROM cache_v2_migration_state WHERE ranker_projection_inputs_json IS NOT NULL"
            ),
            0
        );
        let first = finalize_cache_v2(
            &next,
            &dir.path().join(format!("649-first-{embedded}.json")),
            FRESH_END + 12,
        )
        .unwrap();
        let second = finalize_cache_v2(
            &next,
            &dir.path().join(format!("649-second-{embedded}.json")),
            FRESH_END + 13,
        )
        .unwrap();
        assert_eq!(first.ranker_projection_count, 3);
        assert_eq!(
            first.ranker_projection_digest,
            second.ranker_projection_digest
        );
    }
}

#[tokio::test]
async fn acquisition_failure_keeps_retained_history_excluded_until_full_recovery() {
    let dir = TempDir::new().unwrap();
    let side = dataset_candidate(&dir, "failed-delta.db", &[WALLET_B]);
    let mut source = DatasetFetcher {
        rows: vec![
            dataset_row(WALLET, "0xa", "old", "BUY", FRESH_END),
            dataset_row(WALLET_B, "0xb", "b", "BUY", FRESH_END),
        ],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        1,
        FRESH_END,
        FRESH_END + 1,
    )
    .await
    .unwrap();
    let retained = query_values(
        &side,
        &format!("SELECT * FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'"),
    );
    source
        .rows
        .push(dataset_row(WALLET, "0xc", "bad", "BUY", FRESH_END + 1));
    source.rows[2]["price"] = Value::from("3.1968021978");
    let excluded = populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    assert_eq!(excluded.group_count, 1);
    assert_eq!(receipt(&side, 7, WALLET), Some((0, 0, 0)));
    assert_eq!(
        query_values(
            &side,
            &format!("SELECT * FROM activity_groups_v2 WHERE wallet_hex = '{WALLET}'")
        ),
        retained
    );
    let proof = &stored_receipt_proofs(&Connection::open(&side).unwrap(), 7)[0];
    assert_eq!(proof["acquisition"]["aggregation_status"], "not_attempted");
    assert_eq!(
        proof["acquisition"]["exclusion_reason"],
        "acquisition_failure"
    );
    assert_eq!(proof["acquisition"]["predecessor"]["carried"], false);
    // Failed reads cannot lose their reason, be relabeled as empty success,
    // or acquire invented page evidence.
    for (index, change) in [
        "exclusion_reason = NULL",
        "acquisition_json = json_set(acquisition_json, '$.disposition', 'complete')",
        "acquisition_json = json_set(acquisition_json, '$.aggregation_status', 'complete', '$.fetched_aggregate_count', 0)",
        "page_evidence_json = (SELECT page_evidence_json FROM activity_wallet_coverage_staging_v2 WHERE generation = 7 AND wallet_hex != '0x1111111111111111111111111111111111111111' LIMIT 1)",
    ].into_iter().enumerate() {
        let damaged = dir.path().join(format!("failed-tamper-{index}.db"));
        std::fs::copy(&side, &damaged).unwrap();
        let connection = Connection::open(&damaged).unwrap();
        connection.execute(&format!("UPDATE activity_wallet_coverage_staging_v2 SET {change} WHERE generation = 7 AND wallet_hex = ?1"), [WALLET]).unwrap();
        let no_reads = DatasetFetcher::default();
        assert!(populate_activity_fresh_v2(&damaged, &no_reads, "https://data.example", 7, FRESH_END + 99, FRESH_END + 4).await.is_err());
        assert!(no_reads.calls.lock().unwrap().is_empty());
    }
    let no_reads = DatasetFetcher::default();
    assert_eq!(
        populate_activity_fresh_v2(
            &side,
            &no_reads,
            "https://data.example",
            7,
            FRESH_END + 99,
            FRESH_END + 4
        )
        .await
        .unwrap(),
        excluded
    );
    assert!(no_reads.calls.lock().unwrap().is_empty());
    dataset_payouts(&side, &source.rows).await;
    let first =
        finalize_cache_v2(&side, &dir.path().join("failed-first.json"), FRESH_END + 4).unwrap();
    let second =
        finalize_cache_v2(&side, &dir.path().join("failed-second.json"), FRESH_END + 5).unwrap();
    assert_eq!(first.ranker_projection_count, 1);
    assert_eq!(
        first.ranker_projection_digest,
        second.ranker_projection_digest
    );
    assert_eq!(
        projected_entries(&side),
        vec![(WALLET_B.to_owned(), "0xb".to_owned(), FRESH_END)]
    );
    // Compare the actual certified export against a fresh cache with only the
    // admitted wallet; retained excluded rows must not enter any consumer.
    let excluded_full = dataset_candidate(&dir, "excluded-full.db", &[WALLET_B]);
    let admitted = DatasetFetcher {
        rows: vec![source.rows[1].clone()],
        ..Default::default()
    };
    populate_activity_fresh_v2(
        &excluded_full,
        &admitted,
        "https://data.example",
        7,
        FRESH_END + 2,
        FRESH_END + 3,
    )
    .await
    .unwrap();
    dataset_payouts(&excluded_full, &source.rows).await;
    finalize_cache_v2(
        &excluded_full,
        &dir.path().join("excluded-full.json"),
        FRESH_END + 4,
    )
    .unwrap();
    assert_python_consumer_parity(&excluded_full, &side);
    source.rows[2]["price"] = Value::from("0.500000");
    source.calls.lock().unwrap().clear();
    populate_activity_fresh_v2(
        &side,
        &source,
        "https://data.example",
        8,
        FRESH_END + 10,
        FRESH_END + 11,
    )
    .await
    .unwrap();
    assert!(
        source
            .calls
            .lock()
            .unwrap()
            .contains(&(WALLET.to_owned(), 1, FRESH_END + 10))
    );
    let full = dataset_candidate(&dir, "recovered-full.db", &[WALLET_B]);
    populate_activity_fresh_v2(
        &full,
        &source,
        "https://data.example",
        8,
        FRESH_END + 10,
        FRESH_END + 11,
    )
    .await
    .unwrap();
    dataset_payouts(&full, &source.rows).await;
    let full_stage = finalize_cache_v2(
        &full,
        &dir.path().join("recovered-full.json"),
        FRESH_END + 12,
    )
    .unwrap();
    let recovered =
        finalize_cache_v2(&side, &dir.path().join("recovered.json"), FRESH_END + 12).unwrap();
    assert_eq!(
        full_stage.ranker_projection_digest,
        recovered.ranker_projection_digest
    );
    assert_eq!(full_stage.ranker_projection_count, 3);
    assert_python_consumer_parity(&full, &side);
}
