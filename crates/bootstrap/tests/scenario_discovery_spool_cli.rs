//! Real dispatch and opener precedence. Every enabled network URL is deliberately invalid.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::lock::CacheMutationLock;
use serde_json::{Value, json};
use std::path::Path;
use std::process::{Command, Output};

fn run(root: &Path, db: &Path, args: &[&str], enabled: bool) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .current_dir(root)
        .env_clear()
        .args(args)
        .env("RUST_LOG", "info")
        .env("PE_BOOTSTRAP_OUTPUT", root.join("watchlist.json"))
        .env("PE_BOOTSTRAP_CACHE_PATH", db)
        .env("PE_BOOTSTRAP_LEADERBOARD_TOP_N", "0")
        .env("PE_BOOTSTRAP_DATADASH_API_URL", "")
        .env(
            "PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS",
            if enabled { "100" } else { "0" },
        )
        .env(
            "PE_BOOTSTRAP_GAMMA_BASE_URL",
            "invalid URL fails before a request",
        )
        .output()
        .unwrap()
}
fn logs(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}
fn old_candidate(path: &Path) {
    let mut cache = WalletCache::open(path).unwrap();
    cache
        .upsert_wallets_bulk(&[(format!("0x{:040x}", 1), 16, false, None, None, None, 0)])
        .unwrap();
    cache.begin_or_resume_clob_payout_walk_v2(100).unwrap();
    cache
        .raw_conn_for_test()
        .execute_batch(
            "PRAGMA user_version=2; ALTER TABLE clob_payout_evidence_v2 DROP COLUMN neg_risk;",
        )
        .unwrap();
}
fn state(path: &Path) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    let c = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let tables:Vec<String>=c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name IN ('wallets','purged_wallets','meta') OR name LIKE 'clob_payout%') ORDER BY name").unwrap().query_map([],|r|r.get(0)).unwrap().map(Result::unwrap).collect();
    let mut result = vec![];
    let schema: Vec<Vec<rusqlite::types::Value>> = c
        .prepare("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
        .unwrap()
        .query_map([], |r| Ok(vec![r.get(0)?, r.get(1)?, r.get(2)?]))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    result.push(("schema".into(), schema));
    for table in tables {
        let mut stmt = c
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
            .unwrap();
        let n = stmt.column_count();
        let rows = stmt
            .query_map([], |r| (0..n).map(|i| r.get(i)).collect())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        result.push((table, rows));
    }
    result
}
fn collect(root: &Path, fixed: &Path, spool: &Path, enabled: bool) -> Value {
    let out = run(
        root,
        fixed,
        &[
            "winner-discovery",
            "--defer-activation",
            "--collect-to",
            spool.to_str().unwrap(),
            "--coverage-from",
            fixed.to_str().unwrap(),
            "--not-before",
            "0",
        ],
        enabled,
    );
    assert_eq!(out.status.code(), Some(0), "{}", logs(&out));
    serde_json::from_slice(&std::fs::read(spool).unwrap()).unwrap()
}

#[test]
fn discovery_spool_cli_grammar_refuses_before_lock_open_or_requests() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cache.db");
    old_candidate(&db);
    let before = state(&db);
    let bytes = std::fs::read(&db).unwrap();
    let _lock = CacheMutationLock::acquire(&db).unwrap();
    let cases: Vec<Vec<&str>> = vec![
        vec!["winner-discovery", "--collect-to", "s"],
        vec![
            "winner-discovery",
            "--defer-activation",
            "--collect-to",
            "s",
            "--coverage-from",
            "fixed",
        ],
        vec![
            "winner-discovery",
            "--defer-activation",
            "--collect-to",
            "s",
            "--coverage-from",
            "fixed",
            "--not-before",
            "bad",
        ],
        vec![
            "winner-discovery",
            "--defer-activation",
            "--collect-to=s",
            "--coverage-from=fixed",
            "--not-before=",
        ],
        vec!["winner-discovery", "--defer-activation", "--apply-from"],
        vec!["winner-discovery", "--apply-from", "s"],
        vec![
            "winner-discovery",
            "--collect-to",
            "s",
            "--coverage-from",
            "fixed",
            "--not-before",
            "0",
        ],
        vec![
            "winner-discovery",
            "--defer-activation",
            "--apply-from=s",
            "--collect-to=s",
            "--coverage-from=fixed",
            "--not-before=0",
        ],
        vec![
            "winner-discovery",
            "--defer-activation",
            "--apply-from=s",
            "--not-before=0",
        ],
        vec!["events", "--collect-to", "s"],
        vec!["pipeline-versions", "--apply-from=s"],
        vec!["bootstrap.toml", "--coverage-from=fixed"],
    ];
    for args in cases {
        let out = run(dir.path(), &db, &args, true);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {}", logs(&out));
        assert!(logs(&out).contains("invalid mode"));
        assert!(!logs(&out).contains("cache mutation lock failed"));
        assert_eq!(state(&db), before);
        assert_eq!(std::fs::read(&db).unwrap(), bytes);
    }
    let absent = dir.path().join("absent.db");
    let out = run(dir.path(), &absent, &["events", "--apply-from=s"], true);
    assert_eq!(out.status.code(), Some(1));
    assert!(!absent.exists());
    assert!(!pe_bootstrap::lock::lock_path_for(&absent).exists());
}

#[test]
fn discovery_spool_cli_collector_ignores_held_lock_and_preserves_fixed() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fixed.db");
    old_candidate(&db);
    let before = state(&db);
    let bytes = std::fs::read(&db).unwrap();
    let _lock = CacheMutationLock::acquire(&db).unwrap();
    std::fs::write(dir.path().join("bootstrap.toml"), "").unwrap();
    let spool = dir.path().join("spool.json");
    let out = run(
        dir.path(),
        &db,
        &[
            "winner-discovery",
            "--defer-activation",
            "--collect-to",
            spool.to_str().unwrap(),
            "--coverage-from",
            db.to_str().unwrap(),
            "--not-before",
            "9223372036854775807",
            "bootstrap.toml",
        ],
        false,
    );
    assert_eq!(out.status.code(), Some(0), "{}", logs(&out));
    assert!(spool.exists());
    assert_eq!(state(&db), before);
    assert_eq!(std::fs::read(&db).unwrap(), bytes);
    assert!(!logs(&out).contains("connection tuning applied"));
    assert!(logs(&out).contains("sha256"));
}

#[test]
fn discovery_spool_cli_refusals_precede_old_schema_two_migration() {
    let dir = tempfile::tempdir().unwrap();
    let fixed = dir.path().join("fixed.db");
    old_candidate(&fixed);
    let side = dir.path().join("side.db");
    std::fs::copy(&fixed, &side).unwrap();
    let spool = dir.path().join("spool.json");
    let original = collect(dir.path(), &fixed, &spool, true);
    let before = state(&side);
    let bytes = std::fs::read(&side).unwrap();
    let mut invalid = vec![];
    let mut flag = original.clone();
    flag["coverage_read"] = json!(false);
    invalid.push(flag);
    let mut version = original.clone();
    version["version"] = json!(9);
    invalid.push(version);
    for (key, value) in original["settings"].as_object().unwrap() {
        let mut v = original.clone();
        v["settings"][key] = match value {
            Value::Number(n) => json!(n.as_u64().unwrap() + 1),
            Value::Array(_) => json!(["different"]),
            Value::String(s) => json!(format!("{s}different")),
            Value::Null => json!("different"),
            _ => unreachable!(),
        };
        invalid.push(v);
    }
    let mut mismatch = original.clone();
    mismatch["prior_coverage_raw"] = json!("different");
    invalid.push(mismatch);
    for value in invalid {
        std::fs::write(&spool, serde_json::to_vec(&value).unwrap()).unwrap();
        let out = run(
            dir.path(),
            &side,
            &[
                "winner-discovery",
                "--defer-activation",
                "--apply-from",
                spool.to_str().unwrap(),
            ],
            true,
        );
        assert_eq!(out.status.code(), Some(77), "{}", logs(&out));
        assert!(!logs(&out).contains("connection tuning applied"));
        assert_eq!(state(&side), before);
        assert_eq!(std::fs::read(&side).unwrap(), bytes);
    }
    std::fs::write(&spool, "malformed").unwrap();
    for path in [
        spool,
        side.clone(),
        side.with_extension("db-wal"),
        side.with_extension("db-shm"),
        side.with_extension("db-journal"),
        pe_bootstrap::lock::lock_path_for(&side),
    ] {
        let out = run(
            dir.path(),
            &side,
            &[
                "winner-discovery",
                "--defer-activation",
                "--apply-from",
                path.to_str().unwrap(),
            ],
            true,
        );
        assert_eq!(out.status.code(), Some(77));
        assert_eq!(state(&side), before);
        assert_eq!(std::fs::read(&side).unwrap(), bytes);
    }
}

#[test]
fn discovery_spool_cli_bound_opener_migrates_before_terminal_and_migration_failure_wins() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        old_candidate(&fixed);
        let side = dir.path().join("side.db");
        std::fs::copy(&fixed, &side).unwrap();
        let spool = dir.path().join("spool.json");
        let value = collect(dir.path(), &fixed, &spool, true);
        assert_eq!(value["outcomes"][2]["result"]["exit_code"], 75);
        if fail {
            let c = rusqlite::Connection::open(&side).unwrap();
            c.execute_batch("CREATE TRIGGER stop_migration BEFORE DELETE ON clob_payout_walk_state_v2 BEGIN SELECT RAISE(ABORT, 'migration fixture'); END;").unwrap();
        }
        let before = state(&side);
        let out = run(
            dir.path(),
            &side,
            &[
                "winner-discovery",
                "--defer-activation",
                "--apply-from",
                spool.to_str().unwrap(),
            ],
            true,
        );
        assert_eq!(
            out.status.code(),
            Some(if fail { 1 } else { 75 }),
            "{}",
            logs(&out)
        );
        if fail {
            assert!(logs(&out).contains("migration fixture"));
            assert_eq!(state(&side), before);
        } else {
            assert!(logs(&out).contains("connection tuning applied"));
            assert_ne!(state(&side), before);
            let c = rusqlite::Connection::open(&side).unwrap();
            assert_eq!(
                c.query_row("SELECT COUNT(*) FROM clob_payout_walk_state_v2", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap(),
                0
            );
        }
    }
}

#[test]
fn discovery_spool_cli_opener_failure_precedes_recorded_participant_tempfail() {
    let dir = tempfile::tempdir().unwrap();
    let fixed = dir.path().join("fixed.db");
    old_candidate(&fixed);
    let side = dir.path().join("side.db");
    std::fs::copy(&fixed, &side).unwrap();
    let spool = dir.path().join("spool.json");
    let value = collect(dir.path(), &fixed, &spool, true);
    assert_eq!(value["outcomes"][2]["result"]["exit_code"], 75);
    let c = rusqlite::Connection::open(&side).unwrap();
    c.execute_batch("PRAGMA user_version=999").unwrap();
    drop(c);
    let before = state(&side);
    let out = run(
        dir.path(),
        &side,
        &[
            "winner-discovery",
            "--defer-activation",
            "--apply-from",
            spool.to_str().unwrap(),
        ],
        true,
    );
    assert_eq!(out.status.code(), Some(1), "{}", logs(&out));
    assert!(logs(&out).contains("999"));
    assert_eq!(state(&side), before);
}
