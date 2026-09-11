//! Process-level scenarios for destructive/audited `pe-bootstrap` mutators.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::process::{Command, Output};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::lock::CacheMutationLock;
use pe_bootstrap::pile::SRC_TRADES;
use tempfile::TempDir;

fn wallet_hex(byte: u8) -> String {
    format!("0x{byte:040x}")
}

fn run_cli(root: &Path, cache_path: &Path, args: &[&str]) -> Output {
    run_cli_with_env(root, cache_path, args, &[])
}

fn run_cli_with_env(
    root: &Path,
    cache_path: &Path,
    args: &[&str],
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"));
    command
        .args(args)
        .env("PE_BOOTSTRAP_OUTPUT", root.join("watchlist.json"))
        .env("PE_BOOTSTRAP_CACHE_PATH", cache_path)
        .env("PE_BOOTSTRAP_PURGE_ARCHIVE_ENABLED", "false");
    command.envs(extra_env.iter().copied()).output().unwrap()
}

fn upsert(cache: &mut WalletCache, wallet: &str) {
    cache
        .upsert_wallets_bulk(&[(wallet.to_owned(), SRC_TRADES, false, None, None, None, 0)])
        .unwrap();
}

#[test]
fn activate_next_cli_is_audited_idempotent_and_locks_before_open() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let audit_path = dir.path().join("activation.csv");
    let regular = wallet_hex(0x41);
    let infra = wallet_hex(0x42);
    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        upsert(&mut cache, &regular);
        upsert(&mut cache, &infra);
        cache.mark_infra(&infra).unwrap();
    }

    let first = run_cli(
        dir.path(),
        &cache_path,
        &[
            "activate-next",
            "--batch-id",
            "cli-batch",
            "--audit-csv",
            audit_path.to_str().unwrap(),
        ],
    );
    assert!(
        first.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_csv = std::fs::read_to_string(&audit_path).unwrap();
    assert!(first_csv.contains(&regular));
    assert!(!first_csv.contains(&infra));

    let second = run_cli(
        dir.path(),
        &cache_path,
        &[
            "activate-next",
            "--batch-id=cli-batch",
            "--audit-csv",
            audit_path.to_str().unwrap(),
        ],
    );
    assert!(second.status.success());
    assert_eq!(std::fs::read_to_string(&audit_path).unwrap(), first_csv);
    let cache = WalletCache::open(&cache_path).unwrap();
    let (active_regular, active_infra, batches): (i64, i64, i64) = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT \
               (SELECT is_active FROM wallets WHERE wallet_hex=?1), \
               (SELECT is_active FROM wallets WHERE wallet_hex=?2), \
               (SELECT COUNT(*) FROM wallet_activation_batches WHERE batch_id='cli-batch')",
            [&regular, &infra],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((active_regular, active_infra, batches), (1, 0, 1));
    drop(cache);

    let blocked_path = dir.path().join("blocked.db");
    let _holder = CacheMutationLock::acquire(&blocked_path).unwrap();
    let blocked = run_cli(
        dir.path(),
        &blocked_path,
        &["activate-next", "--batch-id", "blocked"],
    );
    assert_eq!(blocked.status.code(), Some(1));
    assert!(
        !blocked_path.exists(),
        "CLI opened/created the cache before acquiring its mutation lock"
    );
}

#[test]
fn purge_infra_cli_is_report_only_until_shared_purge_flag_is_enabled() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let infra = wallet_hex(0x51);
    let regular = wallet_hex(0x52);
    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        upsert(&mut cache, &infra);
        upsert(&mut cache, &regular);
        cache.mark_infra(&infra).unwrap();
        cache.conn_for_test_insert_trade(&infra, "infra", 1_000);
        cache.conn_for_test_insert_trade(&regular, "regular", 1_001);
    }

    let preview = run_cli(dir.path(), &cache_path, &["purge-infra", "--dry-run"]);
    assert!(preview.status.success());
    assert!(
        WalletCache::open(&cache_path)
            .unwrap()
            .conn_for_test_wallet_exists(&infra)
    );

    let disabled = run_cli(dir.path(), &cache_path, &["purge-infra"]);
    assert!(disabled.status.success());
    assert!(
        WalletCache::open(&cache_path)
            .unwrap()
            .conn_for_test_wallet_exists(&infra),
        "PE_BOOTSTRAP_PURGE_ENABLED=false deleted infrastructure"
    );

    let deleted = run_cli_with_env(
        dir.path(),
        &cache_path,
        &["purge-infra"],
        &[("PE_BOOTSTRAP_PURGE_ENABLED", "true")],
    );
    assert!(
        deleted.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&deleted.stderr)
    );
    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(!cache.conn_for_test_wallet_exists(&infra));
    assert!(cache.conn_for_test_wallet_exists(&regular));
    assert!(cache.is_purged(&infra).unwrap());

    let status =
        std::fs::read_to_string(dir.path().join("eval-results/purge_status.jsonl")).unwrap();
    assert_eq!(status.lines().count(), 3);
    assert!(
        status
            .lines()
            .all(|line| line.contains("\"stage\":\"purge-infra\""))
    );
}

#[test]
fn ordinary_purge_cli_is_report_only_when_shared_flag_is_disabled() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let wallet = wallet_hex(0x53);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        upsert(&mut cache, &wallet);
        cache.conn_for_test_insert_trade(&wallet, "fresh", now);
        cache
            .raw_conn_for_test()
            .execute(
                "UPDATE wallets SET is_active=1, last_polymarket_fetch_at=?1 \
                 WHERE wallet_hex=?2",
                rusqlite::params![now, wallet],
            )
            .unwrap();
    }
    let decision_csv = dir.path().join("decisions.csv");
    std::fs::write(
        &decision_csv,
        format!("wallet,tstat_net,mean_net,n_eff,eligible\n{wallet},-3.0,-0.5,50,True\n"),
    )
    .unwrap();

    let output = run_cli_with_env(
        dir.path(),
        &cache_path,
        &["purge"],
        &[(
            "PE_BOOTSTRAP_PURGE_DECISION_CSV",
            decision_csv.to_str().unwrap(),
        )],
    );
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(cache.conn_for_test_wallet_exists(&wallet));
    assert!(!cache.is_purged(&wallet).unwrap());
    let status =
        std::fs::read_to_string(dir.path().join("eval-results/purge_status.jsonl")).unwrap();
    assert!(status.contains("\"stage\":\"purge\""));
    assert!(status.contains("\"exit_code\":0"));
}

#[test]
fn every_locking_cli_entry_refuses_before_creating_the_cache() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("blocked.db");
    let _holder = CacheMutationLock::acquire(&cache_path).unwrap();
    let subcommands = [
        "all",
        "fetch",
        "watchlist",
        "resolutions",
        "schedules",
        "events",
        "backfill",
        "classify-infra",
        "winner-discovery",
        "prices-history",
        "purge",
        "activate-next",
        "purge-infra",
        "clear-infra-exclusion",
        "recover-reclamation",
        "reclamation-evidence",
    ];

    for subcommand in subcommands {
        let output = run_cli(dir.path(), &cache_path, &[subcommand]);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{subcommand} did not fail on the held cache lock; stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !cache_path.exists(),
            "{subcommand} created/opened the cache before lock acquisition"
        );
    }

    let no_argument_all = run_cli(dir.path(), &cache_path, &[]);
    assert_eq!(no_argument_all.status.code(), Some(1));
    assert!(
        !cache_path.exists(),
        "no-argument all created the cache before lock acquisition"
    );
}

#[test]
fn reclamation_evidence_cli_reports_and_enforces_activation_gate() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    WalletCache::open(&cache_path).unwrap();
    let eval_results = dir.path().join("eval-results");
    std::fs::create_dir(&eval_results).unwrap();
    std::fs::write(
        eval_results.join("purge_status.jsonl"),
        "{\"stage\":\"purge\",\"exit_code\":0}\n",
    )
    .unwrap();
    let sqlite_tmpdir = dir.path().join("sqlite-tmp");
    std::fs::create_dir(&sqlite_tmpdir).unwrap();

    let ready = run_cli_with_env(
        dir.path(),
        &cache_path,
        &["reclamation-evidence"],
        &[("SQLITE_TMPDIR", sqlite_tmpdir.to_str().unwrap())],
    );
    assert!(
        ready.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&ready.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&ready.stdout).unwrap();
    assert_eq!(report["activation_ready"], true);
    assert_eq!(report["reclamation_pending"], false);
    assert_eq!(report["latest_purge_status_records"][0]["stage"], "purge");
    assert_eq!(
        report["sqlite_tmpdir"]["resolved_path"],
        sqlite_tmpdir.to_str().unwrap()
    );

    rusqlite::Connection::open(&cache_path)
        .unwrap()
        .execute_batch("DROP INDEX idx_trades_market_id;")
        .unwrap();
    let blocked = run_cli_with_env(
        dir.path(),
        &cache_path,
        &["reclamation-evidence"],
        &[("SQLITE_TMPDIR", sqlite_tmpdir.to_str().unwrap())],
    );
    assert_eq!(blocked.status.code(), Some(2));
    let report: serde_json::Value = serde_json::from_slice(&blocked.stdout).unwrap();
    assert_eq!(report["activation_ready"], false);
    assert_eq!(
        report["missing_required_trades_indexes"],
        serde_json::json!(["idx_trades_market_id"])
    );
}

#[test]
fn clear_infra_exclusion_cli_requires_confirmation_and_exact_reason() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let infra = wallet_hex(0x61);
    let wrong_reason = wallet_hex(0x62);
    let missing = wallet_hex(0x63);
    // The cold-probe shape since purge retirement: a live flag, no tombstone.
    let flagged = wallet_hex(0x64);
    {
        let cache = WalletCache::open(&cache_path).unwrap();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'infra')",
                [&infra],
            )
            .unwrap();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'proven_loser')",
                [&wrong_reason],
            )
            .unwrap();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO wallets (wallet_hex, is_active, is_infra) VALUES (?1, 1, 1)",
                [&flagged],
            )
            .unwrap();
    }

    let unconfirmed = run_cli(
        dir.path(),
        &cache_path,
        &["clear-infra-exclusion", "--wallet", &infra],
    );
    assert_eq!(unconfirmed.status.code(), Some(1));
    let wrong = run_cli(
        dir.path(),
        &cache_path,
        &[
            "clear-infra-exclusion",
            "--wallet",
            &wrong_reason,
            "--confirm",
        ],
    );
    assert_eq!(wrong.status.code(), Some(1));
    let absent = run_cli(
        dir.path(),
        &cache_path,
        &["clear-infra-exclusion", "--wallet", &missing, "--confirm"],
    );
    assert_eq!(absent.status.code(), Some(1));
    let cleared = run_cli(
        dir.path(),
        &cache_path,
        &["clear-infra-exclusion", "--wallet", &infra, "--confirm"],
    );
    assert!(cleared.status.success());

    let flag_cleared = run_cli(
        dir.path(),
        &cache_path,
        &["clear-infra-exclusion", "--wallet", &flagged, "--confirm"],
    );
    assert!(flag_cleared.status.success());
    let flag_again = run_cli(
        dir.path(),
        &cache_path,
        &["clear-infra-exclusion", "--wallet", &flagged, "--confirm"],
    );
    assert_eq!(
        flag_again.status.code(),
        Some(1),
        "second clearance finds nothing"
    );

    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(!cache.is_purged(&infra).unwrap());
    assert!(cache.is_purged(&wrong_reason).unwrap());
    assert!(!cache.conn_for_test_is_infra(&flagged), "live flag cleared");
    let still_active: i64 = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT is_active FROM wallets WHERE wallet_hex = ?1",
            [&flagged],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(still_active, 1, "clearance never changes is_active");
}
