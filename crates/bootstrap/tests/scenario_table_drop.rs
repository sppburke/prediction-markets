//! Scenario: `WalletCache::open()` drops the operator/funder/delta tables (#326 PR4).
//!
//! The reclaim removes `counterparty_edges` / `funder_edges` / `funder_lookup_done`
//! / `delta_audit` (≈ half the production cache — `counterparty_edges` alone was
//! ~275M rows) via an idempotent `DROP TABLE IF EXISTS` migration, while KEEPING
//! `market_fees` + `token_conditions` (still written by the surviving `events`
//! sweep). These pin that an existing cache carrying the legacy tables is
//! reclaimed on the next open, and that fresh opens never recreate them.
//!
//! Determinism: pure in-process; fresh `TempDir` per test, no network/clock.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use rusqlite::Connection;
use tempfile::TempDir;

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        > 0
}

const DROPPED: [&str; 4] = [
    "counterparty_edges",
    "funder_edges",
    "funder_lookup_done",
    "delta_audit",
];

/// PASS: after `WalletCache::open()`, the four operator/funder/delta tables are
///       GONE even though they existed (with data) beforehand, while the kept
///       tables `market_fees` + `token_conditions` + `wallets` remain.
/// FAIL: any dropped table survives, OR a kept table is missing, OR `open()`
///       errors on the legacy DB.
#[test]
fn open_drops_operator_tables_and_keeps_market_fees_and_token_conditions() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");

    // 1. Seed a legacy-shaped DB carrying the four doomed tables + a row each.
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE counterparty_edges (tx_hash TEXT PRIMARY KEY, log_index INTEGER);
             CREATE TABLE funder_edges (funder_hex TEXT, funded_hex TEXT, PRIMARY KEY (funder_hex, funded_hex));
             CREATE TABLE funder_lookup_done (wallet_hex TEXT PRIMARY KEY, fetched_at_unix INTEGER);
             CREATE TABLE delta_audit (run_at_unix INTEGER, wallet_hex TEXT, PRIMARY KEY (run_at_unix, wallet_hex));
             INSERT INTO counterparty_edges VALUES ('0xtx', 1);
             INSERT INTO funder_edges VALUES ('0xf', '0xw');
             INSERT INTO funder_lookup_done VALUES ('0xw', 1);
             INSERT INTO delta_audit VALUES (1, '0xw');",
        )
        .unwrap();
        for t in DROPPED {
            assert!(
                table_exists(&conn, t),
                "{t} must exist before the migration"
            );
        }
    }

    // 2. Open through WalletCache — runs the #326 PR4 DROP migration.
    {
        let _cache = WalletCache::open(&path).unwrap();
    }

    // 3. The four tables are gone; the kept tables are present.
    let conn = Connection::open(&path).unwrap();
    for dropped in DROPPED {
        assert!(
            !table_exists(&conn, dropped),
            "{dropped} must be DROPPED by WalletCache::open()"
        );
    }
    for kept in ["market_fees", "token_conditions", "wallets"] {
        assert!(
            table_exists(&conn, kept),
            "{kept} must survive the migration (created from SCHEMA / kept)"
        );
    }
    println!(
        "PASS: open_drops_operator_tables_and_keeps_market_fees_and_token_conditions \
         — 4 dropped, market_fees+token_conditions+wallets kept"
    );
}

/// PASS: opening a FRESH DB never creates the four dropped tables (they are out
///       of SCHEMA), and a second open is a clean idempotent no-op.
/// FAIL: a fresh open recreates a dropped table, OR the second open errors.
#[test]
fn fresh_open_never_creates_dropped_tables_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");

    WalletCache::open(&path).unwrap();
    WalletCache::open(&path).unwrap(); // idempotent second open must not error

    let conn = Connection::open(&path).unwrap();
    for dropped in DROPPED {
        assert!(
            !table_exists(&conn, dropped),
            "fresh open must NOT create {dropped}"
        );
    }
    println!("PASS: fresh_open_never_creates_dropped_tables_and_is_idempotent");
}
