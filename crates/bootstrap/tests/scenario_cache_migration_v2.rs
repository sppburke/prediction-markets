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
    migrate_cache_v2, populate_activity_v2, restore_prior_cache, sha256_file,
    verify_frozen_payload_v1,
};
use pe_bootstrap::clob::ClobFetcher;
use pe_bootstrap::pile::SRC_TRADES;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, ClobCoverageManifest,
    ClobCoveragePage, FixtureFetcher, PageFetcher,
};
use rusqlite::{Connection, params};
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

    let cache = WalletCache::open(&side).unwrap();
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
    assert_eq!(projected.4, 1);
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
