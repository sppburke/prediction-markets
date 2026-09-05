//! Scenario: `WalletCache::open()` drops the operator/funder/delta tables (#326 PR4)
//! and the orphan `idx_wallets_weekly` index (#521).
//!
//! The reclaim removes `counterparty_edges` / `funder_edges` / `funder_lookup_done`
//! / `delta_audit` (≈ half the production cache — `counterparty_edges` alone was
//! ~275M rows) via an idempotent `DROP TABLE IF EXISTS` migration. The retired
//! `market_fees` table is left unread in existing caches but is not created in a
//! fresh cache. These pin both compatibility and fresh-schema retirement.
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

fn index_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
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
///       legacy `market_fees` and current `token_conditions` + `wallets` remain.
/// FAIL: any dropped table survives, OR a kept table is missing, OR `open()`
///       errors on the legacy DB.
#[test]
fn open_drops_operator_tables_and_leaves_retired_market_fees_unread() {
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
             CREATE TABLE market_fees (condition_id TEXT PRIMARY KEY, taker_base_fee_bps INTEGER, maker_base_fee_bps INTEGER, fee_active_from_unix INTEGER, fetched_at_unix INTEGER);
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
    println!("PASS: existing market_fees remains unread while current tables survive");
}

/// PASS: opening a FRESH DB creates neither the four dropped tables nor retired
///       `market_fees`, and a second open is a clean idempotent no-op.
/// FAIL: a fresh open creates a retired table, OR the second open errors.
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
    assert!(
        !table_exists(&conn, "market_fees"),
        "fresh open must NOT create retired market_fees"
    );
    println!("PASS: fresh_open_never_creates_dropped_tables_and_is_idempotent");
}

/// PASS: an existing cache carrying the legacy `idx_wallets_weekly` index loses
///       it on the next `WalletCache::open()` (#521), and a fresh database
///       never creates it — while the `last_funder_fetch_at` column the index
///       covered remains in schema, so a rolled-back pre-#521 binary can still
///       recreate the index.
/// FAIL: a fresh open creates the index, the legacy index survives a reopen,
///       or the column no longer supports the legacy index definition.
#[test]
fn open_drops_orphan_weekly_index_and_fresh_open_never_creates_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");

    // Fresh open: modern schema must not contain the orphan index.
    {
        let _cache = WalletCache::open(&path).unwrap();
    }
    {
        let conn = Connection::open(&path).unwrap();
        assert!(
            !index_exists(&conn, "idx_wallets_weekly"),
            "fresh open must NOT create idx_wallets_weekly"
        );
        // Recreate the index exactly as the pre-#521 schema did — this is the
        // statement a rolled-back binary runs, so it must still succeed against
        // the retained `last_funder_fetch_at` column.
        conn.execute_batch(
            "CREATE INDEX idx_wallets_weekly \
             ON wallets(is_active, last_funder_fetch_at) WHERE is_active = 1;",
        )
        .unwrap();
        assert!(index_exists(&conn, "idx_wallets_weekly"));
    }

    // Reopen through WalletCache — the #521 migration drops the orphan index.
    {
        let _cache = WalletCache::open(&path).unwrap();
    }
    let conn = Connection::open(&path).unwrap();
    assert!(
        !index_exists(&conn, "idx_wallets_weekly"),
        "idx_wallets_weekly must be DROPPED by WalletCache::open()"
    );
    println!("PASS: open_drops_orphan_weekly_index_and_fresh_open_never_creates_it");
}
