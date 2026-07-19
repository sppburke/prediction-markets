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
    Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .args(args)
        .env("PE_BOOTSTRAP_OUTPUT", root.join("watchlist.json"))
        .env("PE_BOOTSTRAP_CACHE_PATH", cache_path)
        .env("PE_BOOTSTRAP_PURGE_ARCHIVE_ENABLED", "false")
        .output()
        .unwrap()
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
fn purge_infra_cli_reports_then_deletes_only_infrastructure() {
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

    let deleted = run_cli(dir.path(), &cache_path, &["purge-infra"]);
    assert!(
        deleted.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&deleted.stderr)
    );
    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(!cache.conn_for_test_wallet_exists(&infra));
    assert!(cache.conn_for_test_wallet_exists(&regular));
    assert!(cache.is_purged(&infra).unwrap());
}

#[test]
fn clear_infra_exclusion_cli_requires_confirmation_and_exact_reason() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let infra = wallet_hex(0x61);
    let wrong_reason = wallet_hex(0x62);
    let missing = wallet_hex(0x63);
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

    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(!cache.is_purged(&infra).unwrap());
    assert!(cache.is_purged(&wrong_reason).unwrap());
}
