#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::{EventSeq, WalletAddress};
use pe_paper_state::{PaperStateDb, PaperStateError, WalletRetentionWait};
use rusqlite::{Connection, params};

fn wallet(n: u8) -> WalletAddress {
    WalletAddress([n; 20])
}

fn fixture() -> (tempfile::TempDir, PaperStateDb, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paper.db");
    let db = PaperStateDb::open(&path).unwrap();
    let sql = Connection::open(path).unwrap();
    (dir, db, sql)
}

fn seed(sql: &Connection, wallet: WalletAddress) {
    let w = wallet.to_string();
    let group = format!("group-{w}");
    let decision = format!("decision-{w}");
    let gate = format!("gate-{w}");
    sql.execute(
        "INSERT INTO activity_groups VALUES (?1, 'tx', ?2, 1, 'r', 'TRADE', 'applied', '{}')",
        params![group, w],
    )
    .unwrap();
    sql.execute("INSERT INTO decision_pending VALUES (?1, 'r', ?2, 1, '{}', '{}', 'terminal', 'no_copy', 1)", params![decision, w]).unwrap();
    sql.execute(
        "INSERT INTO entry_gate_results VALUES (?1, ?2, 'market', 1, 'admitted', 1)",
        params![gate, w],
    )
    .unwrap();
    for id in [&group, &decision, &gate] {
        sql.execute(
            "INSERT INTO seen_trades_v2 VALUES (?1, 2, 'tx')",
            params![id],
        )
        .unwrap();
        // These clocks intentionally lie in the future: retention must never use trade times.
        sql.execute(
            "INSERT INTO no_copy_dispositions VALUES (?1, 'rest_poll', 0, 'stale', 9999999)",
            params![id],
        )
        .unwrap();
        sql.execute(
            "INSERT INTO activity_group_revisions VALUES (?1, 'r', 'tx', 'applied', '{}', 9999999)",
            params![id],
        )
        .unwrap();
    }
    sql.execute(
        "INSERT INTO wallet_market_history_v2 VALUES (?1, 'market', 1, ?2, 'activity_v2')",
        params![w, group],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO wallet_history_status_v2 VALUES (?1, 1, '{}', 1)",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO leader_positions VALUES (?1, 'market', 0, '10', '0')",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO poll_cursors(wallet_hex, last_ts_unix) VALUES (?1, 1)",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO position_anchors VALUES (?1, 1, 1, 1, 'balances', 'hash', '{\"proof\":1}')",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO position_validations VALUES (?1, 'hash', 'proof', '{}', 'g', '{}', 1)",
        params![w],
    )
    .unwrap();
}

fn count(sql: &Connection, table: &str) -> i64 {
    sql.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

#[test]
fn retention_blanks_bounded_batches_keeps_newest_and_skips_blank_rows() {
    let (_dir, db, sql) = fixture();
    for n in [1, 2] {
        for seq in 0..4 {
            sql.execute(
                "INSERT INTO position_anchors VALUES (?1, ?2, 1, 2, 'balances', 'hash', ?3)",
                params![
                    wallet(n).to_string(),
                    seq,
                    if seq == 0 { "{}" } else { "{\"proof\":1}" }
                ],
            )
            .unwrap();
        }
    }
    sql.execute_batch("CREATE TRIGGER forbid_blank_or_newest BEFORE UPDATE OF proof_json ON position_anchors
        WHEN OLD.proof_json = '{}' OR OLD.anchor_seq = 3 BEGIN SELECT RAISE(ABORT, 'must skip row'); END;").unwrap();
    assert_eq!(db.blank_superseded_anchor_proofs(2).unwrap().result, 2);
    // Another caller can start its batch between retention transactions.
    db.begin_batch().unwrap();
    assert!(matches!(
        db.blank_superseded_anchor_proofs(2),
        Err(PaperStateError::RetentionBatchOpen)
    ));
    db.rollback_batch().unwrap();
    assert_eq!(db.blank_superseded_anchor_proofs(2).unwrap().result, 2);
    assert_eq!(db.blank_superseded_anchor_proofs(2).unwrap().result, 0);
    assert_eq!(
        sql.query_row(
            "SELECT COUNT(*) FROM position_anchors WHERE proof_json = '{}'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        6
    );
    assert_eq!(sql.query_row("SELECT COUNT(*) FROM position_anchors WHERE balances_json = 'balances' AND activity_cutoff_unix = 2", [], |row| row.get::<_, i64>(0)).unwrap(), 8);
}

#[test]
fn retention_deletes_all_trade_id_owners_and_preserves_history_and_other_wallet() {
    let (_dir, db, sql) = fixture();
    seed(&sql, wallet(1));
    seed(&sql, wallet(2));
    db.publish_feed_history_frontiers(&serde_json::json!({"version":1,"frontiers":[
        {"wallet":wallet(1)}, {"wallet":wallet(2)}]}))
        .unwrap();
    assert_eq!(
        db.retention_wallets(None, 1).unwrap().result,
        vec![wallet(1)]
    );
    assert_eq!(
        db.retention_wallets(Some(wallet(1)), 1).unwrap().result,
        vec![wallet(2)]
    );
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
    for table in [
        "activity_groups",
        "decision_pending",
        "entry_gate_results",
        "position_anchors",
        "position_validations",
        "wallet_history_status_v2",
        "leader_positions",
        "poll_cursors",
    ] {
        assert_eq!(count(&sql, table), 1, "{table}");
    }
    for table in [
        "seen_trades_v2",
        "no_copy_dispositions",
        "activity_group_revisions",
    ] {
        assert_eq!(count(&sql, table), 3, "{table}");
    }
    assert_eq!(count(&sql, "wallet_market_history_v2"), 2);
    assert_eq!(
        db.feed_history_frontiers().unwrap(),
        serde_json::json!({"version":1,"frontiers":[{"wallet":wallet(2)}]})
    );
    assert_eq!(
        db.retention_wallets(None, 10).unwrap().result,
        vec![wallet(2)]
    );
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
}

#[test]
fn retention_rechecks_fence_open_decision_recorded_clocks_and_financial_marker() {
    let (_dir, db, sql) = fixture();
    seed(&sql, wallet(1));
    let w = wallet(1).to_string();
    for (mutation, undo, expected) in [
        (
            "INSERT INTO wallet_fences VALUES (?1, 'id', 'cause', '{}', 1)",
            "DELETE FROM wallet_fences",
            WalletRetentionWait::Fenced,
        ),
        (
            "UPDATE decision_pending SET state='open' WHERE wallet_hex=?1",
            "UPDATE decision_pending SET state='terminal'",
            WalletRetentionWait::OpenDecision,
        ),
        (
            "UPDATE decision_pending SET updated_at_unix=100 WHERE wallet_hex=?1",
            "UPDATE decision_pending SET updated_at_unix=1",
            WalletRetentionWait::RecentDecision,
        ),
        (
            "UPDATE position_anchors SET anchored_at_unix=100 WHERE wallet_hex=?1",
            "UPDATE position_anchors SET anchored_at_unix=1",
            WalletRetentionWait::RecentAnchor,
        ),
        (
            "UPDATE position_validations SET recorded_at_unix=100 WHERE wallet_hex=?1",
            "UPDATE position_validations SET recorded_at_unix=1",
            WalletRetentionWait::RecentValidation,
        ),
    ] {
        sql.execute(mutation, params![w]).unwrap();
        assert_eq!(
            db.wallet_retention_wait(wallet(1), 100, None).unwrap(),
            Some(expected)
        );
        assert_eq!(
            db.retire_wallet(wallet(1), 100, None).unwrap().result,
            Some(expected)
        );
        assert_eq!(count(&sql, "activity_groups"), 1);
        sql.execute_batch(undo).unwrap();
    }
    assert_eq!(
        db.retire_wallet(wallet(1), 100, Some(EventSeq(7)))
            .unwrap()
            .result,
        Some(WalletRetentionWait::FinancialProjectionBehind)
    );
    sql.execute(
        "INSERT INTO meta VALUES ('financial_last_prepared_seq', 7)",
        [],
    )
    .unwrap();
    assert_eq!(
        db.retire_wallet(wallet(1), 100, Some(EventSeq(7)))
            .unwrap()
            .result,
        None
    );
}

#[test]
fn retention_wallet_deletion_rolls_back_all_tables_and_frontier_on_failure() {
    let (_dir, db, sql) = fixture();
    seed(&sql, wallet(1));
    db.publish_feed_history_frontiers(
        &serde_json::json!({"version":1,"frontiers":[{"wallet":wallet(1)}]}),
    )
    .unwrap();
    sql.execute_batch("CREATE TRIGGER fail_retirement BEFORE DELETE ON poll_cursors BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(db.retire_wallet(wallet(1), 100, None).is_err());
    for table in [
        "activity_groups",
        "decision_pending",
        "entry_gate_results",
        "position_anchors",
        "position_validations",
        "wallet_history_status_v2",
        "leader_positions",
        "poll_cursors",
    ] {
        assert_eq!(count(&sql, table), 1, "{table}");
    }
    assert_eq!(count(&sql, "seen_trades_v2"), 3);
    assert_eq!(count(&sql, "activity_group_revisions"), 3);
    assert_eq!(
        db.feed_history_frontiers().unwrap()["frontiers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    sql.execute_batch("DROP TRIGGER fail_retirement;").unwrap();
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
}

#[test]
fn retention_largest_wallet_fixture_records_single_transaction_lock_time() {
    let (_dir, db, sql) = fixture();
    let w = wallet(1).to_string();
    sql.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 179573)
        INSERT INTO activity_groups SELECT 'group-'||i, 'tx', ?1, i, 'r', 'TRADE', 'applied', '{}' FROM n", params![w]).unwrap();
    sql.execute_batch("INSERT INTO seen_trades_v2 SELECT source_trade_id, 2, transaction_hash FROM activity_groups;
        INSERT INTO activity_group_revisions SELECT source_trade_id, semantic_revision, transaction_hash, disposition, proof_json, source_epoch FROM activity_groups;").unwrap();
    let retired = db.retire_wallet(wallet(1), 999999, None).unwrap();
    eprintln!("179573-group retirement lock_time={:?}", retired.lock_time);
    assert_eq!(retired.result, None);
    assert_eq!(count(&sql, "activity_groups"), 0);
    assert_eq!(count(&sql, "activity_group_revisions"), 0);
    assert_eq!(count(&sql, "seen_trades_v2"), 0);
}
