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
    // The removal takes what makes the wallet known and its decisions' trade rows; its group and
    // gate result stay listed for draining.
    for table in [
        "decision_pending",
        "position_anchors",
        "position_validations",
        "wallet_history_status_v2",
        "leader_positions",
        "poll_cursors",
    ] {
        assert_eq!(count(&sql, table), 1, "{table}");
    }
    assert_eq!(count(&sql, "activity_groups"), 2);
    assert_eq!(count(&sql, "entry_gate_results"), 2);
    for table in [
        "seen_trades_v2",
        "no_copy_dispositions",
        "activity_group_revisions",
    ] {
        assert_eq!(count(&sql, table), 5, "{table}");
    }
    assert_eq!(db.retirement_drains().unwrap().result, vec![wallet(1)]);
    assert_eq!(
        db.feed_history_frontiers().unwrap(),
        serde_json::json!({"version":1,"frontiers":[{"wallet":wallet(2)}]})
    );
    // A wallet that is not listed is never drained.
    let unlisted = db.drain_retired_wallet(wallet(2), 10).unwrap().result;
    assert_eq!((unlisted.trade_ids, unlisted.finished), (0, true));
    assert_eq!(count(&sql, "activity_groups"), 2);
    // Groups first, then gate results, one bounded transaction each; the empty one clears the entry.
    let drains: Vec<_> = (0..3)
        .map(|_| db.drain_retired_wallet(wallet(1), 1).unwrap().result)
        .map(|drain| (drain.trade_ids, drain.finished))
        .collect();
    assert_eq!(drains, vec![(1, false), (1, false), (0, true)]);
    assert!(db.retirement_drains().unwrap().result.is_empty());
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
        db.retention_wallets(None, 10).unwrap().result,
        vec![wallet(2)]
    );
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
    let again = db.drain_retired_wallet(wallet(1), 10).unwrap().result;
    assert_eq!((again.trade_ids, again.finished), (0, true));
    assert!(db.retirement_drains().unwrap().result.is_empty());
}

#[test]
fn retention_candidate_pages_equal_the_sorted_union_of_every_working_state_source() {
    let (_dir, db, sql) = fixture();
    let mut expected = Vec::new();
    for (n, statement) in [
        (
            1,
            "INSERT INTO activity_groups VALUES ('g1', 'tx', ?1, 1, 'r', 'TRADE', 'applied', '{}')",
        ),
        (
            2,
            "INSERT INTO activity_groups VALUES ('g2', 'tx', ?1, 2, 'r', 'TRADE', 'applied', '{}')",
        ),
        (
            3,
            "INSERT INTO decision_pending VALUES ('d3', 'r', ?1, 1, '{}', '{}', 'terminal', 'no_copy', 1)",
        ),
        (
            4,
            "INSERT INTO entry_gate_results VALUES ('e4', ?1, 'market', 1, 'admitted', 1)",
        ),
        (
            5,
            "INSERT INTO position_anchors VALUES (?1, 0, 1, 1, '[]', 'hash', '{}')",
        ),
        (
            6,
            "INSERT INTO position_validations VALUES (?1, 'hash', 'proof', '{}', 'g', '{}', 1)",
        ),
        (
            7,
            "INSERT INTO wallet_history_status_v2 VALUES (?1, 1, '{}', 1)",
        ),
        (
            8,
            "INSERT INTO leader_positions VALUES (?1, 'market', 0, '10', '0')",
        ),
        (
            9,
            "INSERT INTO poll_cursors(wallet_hex, last_ts_unix) VALUES (?1, 1)",
        ),
    ] {
        sql.execute(statement, params![wallet(n).to_string()])
            .unwrap();
        expected.push(wallet(n));
    }
    // Several groups for one wallet still make one candidate; market history alone makes none.
    sql.execute(
        "INSERT INTO activity_groups VALUES ('g1b', 'tx', ?1, 3, 'r', 'TRADE', 'applied', '{}')",
        params![wallet(1).to_string()],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO wallet_market_history_v2 VALUES (?1, 'market', 1, 'x', 'activity_v2')",
        params![wallet(11).to_string()],
    )
    .unwrap();
    db.publish_feed_history_frontiers(
        &serde_json::json!({"version":1,"frontiers":[{"wallet":wallet(10)}]}),
    )
    .unwrap();
    expected.push(wallet(10));
    expected.sort_by_key(ToString::to_string);
    for limit in [1, 2, 4, 128] {
        let mut after = None;
        let mut pages = Vec::new();
        loop {
            let page = db.retention_wallets(after, limit).unwrap().result;
            assert!(page.len() <= limit);
            let Some(last) = page.last().copied() else {
                break;
            };
            pages.extend(page);
            after = Some(last);
        }
        assert_eq!(pages, expected, "limit {limit}");
    }
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
            db.wallet_retention_wait(wallet(1), 100, None)
                .unwrap()
                .result,
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
    assert!(db.retirement_drains().unwrap().result.is_empty());
    assert_eq!(
        db.feed_history_frontiers().unwrap()["frontiers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    sql.execute_batch("DROP TRIGGER fail_retirement;").unwrap();
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
    // A failed drain transaction changes nothing and keeps the wallet listed.
    sql.execute_batch("CREATE TRIGGER fail_drain BEFORE DELETE ON activity_groups BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(db.drain_retired_wallet(wallet(1), 10).is_err());
    assert_eq!(count(&sql, "activity_groups"), 1);
    assert_eq!(count(&sql, "seen_trades_v2"), 2);
    assert_eq!(db.retirement_drains().unwrap().result, vec![wallet(1)]);
    sql.execute_batch("DROP TRIGGER fail_drain;").unwrap();
    while !db
        .drain_retired_wallet(wallet(1), 10)
        .unwrap()
        .result
        .finished
    {}
    assert_eq!(count(&sql, "activity_groups"), 0);
    assert_eq!(count(&sql, "entry_gate_results"), 0);
    assert_eq!(count(&sql, "seen_trades_v2"), 0);
    assert!(db.retirement_drains().unwrap().result.is_empty());
}

#[test]
fn retention_largest_wallet_fixture_drains_in_bounded_transactions() {
    let (_dir, db, sql) = fixture();
    let w = wallet(1).to_string();
    sql.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 179573)
        INSERT INTO activity_groups SELECT 'group-'||i, 'tx', ?1, i, 'r', 'TRADE', 'applied', '{}' FROM n", params![w]).unwrap();
    sql.execute_batch("INSERT INTO seen_trades_v2 SELECT source_trade_id, 2, transaction_hash FROM activity_groups;
        INSERT INTO activity_group_revisions SELECT source_trade_id, semantic_revision, transaction_hash, disposition, proof_json, source_epoch FROM activity_groups;").unwrap();
    let retired = db.retire_wallet(wallet(1), 999999, None).unwrap();
    assert_eq!(retired.result, None);
    let (mut transactions, mut trade_ids, mut longest) = (0, 0, retired.lock_time);
    loop {
        let drain = db.drain_retired_wallet(wallet(1), 500).unwrap();
        assert!(drain.result.trade_ids <= 500);
        transactions += 1;
        trade_ids += drain.result.trade_ids;
        longest = longest.max(drain.lock_time);
        if drain.result.finished {
            break;
        }
    }
    eprintln!(
        "179573-group removal lock_time={:?}; drain transactions={transactions} longest={longest:?}",
        retired.lock_time
    );
    assert_eq!(trade_ids, 179_573);
    assert_eq!(transactions, 179_573_usize.div_ceil(500) + 1);
    assert_eq!(count(&sql, "activity_groups"), 0);
    assert_eq!(count(&sql, "activity_group_revisions"), 0);
    assert_eq!(count(&sql, "seen_trades_v2"), 0);
}

#[test]
fn retention_drain_list_absent_means_empty_and_malformed_fails_closed() {
    let (_dir, db, sql) = fixture();
    seed(&sql, wallet(1));
    // Absent: nothing is listed, and draining an unlisted wallet changes nothing.
    assert!(db.retirement_drains().unwrap().result.is_empty());
    let drain = db.drain_retired_wallet(wallet(1), 500).unwrap().result;
    assert_eq!((drain.trade_ids, drain.finished), (0, true));
    assert_eq!(count(&sql, "activity_groups"), 1);
    // Malformed or unsupported: the list read, a drain and a removal all refuse, and nothing changes.
    for (value, label) in [
        (
            br#"{"version":2,"wallets":[]}"#.to_vec(),
            "unsupported version",
        ),
        (
            br#"{"version":1,"wallets":["not-a-wallet"]}"#.to_vec(),
            "invalid wallet",
        ),
        (br#"{"version":1}"#.to_vec(), "missing wallets"),
        (b"not json".to_vec(), "not json"),
    ] {
        sql.execute(
            "INSERT INTO meta (key, value) VALUES ('wallet_retirement_drains', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![value],
        )
        .unwrap();
        assert!(db.retirement_drains().is_err(), "{label}");
        assert!(db.drain_retired_wallet(wallet(1), 500).is_err(), "{label}");
        assert!(db.retire_wallet(wallet(1), 100, None).is_err(), "{label}");
        for table in [
            "activity_groups",
            "decision_pending",
            "entry_gate_results",
            "position_anchors",
            "poll_cursors",
        ] {
            assert_eq!(count(&sql, table), 1, "{label}: {table}");
        }
    }
}

/// One SQLite value, compared exactly: its storage class, a text's or blob's raw bytes, a real's bit pattern.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

/// Every table's rows, sorted, for comparing two databases table by table.
fn table_rows(sql: &Connection) -> std::collections::BTreeMap<String, Vec<Vec<Cell>>> {
    let tables: Vec<String> = sql
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let mut statement = sql.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = statement.column_count();
            let mut rows: Vec<Vec<Cell>> = statement
                .query_map([], |row| {
                    Ok((0..columns)
                        .map(|i| match row.get_ref(i).unwrap() {
                            rusqlite::types::ValueRef::Null => Cell::Null,
                            rusqlite::types::ValueRef::Integer(v) => Cell::Integer(v),
                            rusqlite::types::ValueRef::Real(v) => Cell::Real(v.to_bits()),
                            rusqlite::types::ValueRef::Text(v) => Cell::Text(v.to_vec()),
                            rusqlite::types::ValueRef::Blob(v) => Cell::Blob(v.to_vec()),
                        })
                        .collect())
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            rows.sort();
            (table, rows)
        })
        .collect()
}

#[test]
fn retention_removal_and_drains_delete_exactly_the_one_transaction_set() {
    // Decision-only, gate-only and overlapping trade ids with several revisions; fences, market
    // history and financial rows that reuse the departing wallet's ids must all survive.
    let (dir, db, sql) = fixture();
    for byte in [1, 2] {
        seed(&sql, wallet(byte));
    }
    let w = wallet(1).to_string();
    let (group, decision) = (format!("group-{w}"), format!("decision-{w}"));
    sql.execute_batch(&format!(
        "INSERT INTO entry_gate_results VALUES ('{group}', '{w}', 'market-2', 1, 'admitted', 1);
         INSERT INTO activity_group_revisions VALUES ('{group}', 'r2', 'tx', 'applied', '{{}}', 1);
         INSERT INTO activity_group_revisions VALUES ('{decision}', 'r2', 'tx', 'applied', '{{}}', 1);
         INSERT INTO wallet_fences VALUES ('{w2}', 'fenced-trade', 'fixture', '{{}}', 1);
         INSERT INTO wallet_fences VALUES ('{w3}', 'fenced-trade', 'fixture', '{{}}', 1);
         INSERT INTO fills VALUES ('{decision}', 'market', 0, 'buy', '10', '0.5', '5', '0', 1, 0, NULL, NULL, NULL);
         INSERT INTO fill_market_snapshots VALUES ('{decision}', NULL, NULL, NULL, NULL, 1);
         INSERT INTO positions VALUES ('market', 0, '10', '0');
         INSERT INTO bankroll VALUES (0, '995');
         INSERT INTO settled_markets VALUES ('market', '[\"0\",\"1\"]', '0', 1, NULL, NULL);",
        w2 = wallet(2),
        w3 = wallet(3),
    ))
    .unwrap();
    db.publish_feed_history_frontiers(&serde_json::json!({"version":1,"frontiers":[
        {"wallet":wallet(1)}, {"wallet":wallet(2)}]}))
        .unwrap();
    // The one-transaction design, applied literally to a copy of the database.
    let copy = dir.path().join("one-transaction.db");
    sql.execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
        .unwrap();
    let expected = Connection::open(&copy).unwrap();
    expected
        .execute_batch(&format!(
            "CREATE TEMP TABLE gone AS
               SELECT source_trade_id FROM activity_groups WHERE wallet_hex = '{w}'
               UNION SELECT source_trade_id FROM decision_pending WHERE wallet_hex = '{w}'
               UNION SELECT source_trade_id FROM entry_gate_results WHERE wallet_hex = '{w}';
             DELETE FROM seen_trades_v2 WHERE source_trade_id IN gone;
             DELETE FROM no_copy_dispositions WHERE source_trade_id IN gone;
             DELETE FROM activity_group_revisions WHERE source_trade_id IN gone;
             DELETE FROM activity_groups WHERE wallet_hex = '{w}';
             DELETE FROM decision_pending WHERE wallet_hex = '{w}';
             DELETE FROM entry_gate_results WHERE wallet_hex = '{w}';
             DELETE FROM position_anchors WHERE wallet_hex = '{w}';
             DELETE FROM position_validations WHERE wallet_hex = '{w}';
             DELETE FROM wallet_history_status_v2 WHERE wallet_hex = '{w}';
             DELETE FROM leader_positions WHERE wallet_hex = '{w}';
             DELETE FROM poll_cursors WHERE wallet_hex = '{w}';"
        ))
        .unwrap();
    // The removal, then drains of one trade id each until the entry clears.
    assert_eq!(db.retire_wallet(wallet(1), 100, None).unwrap().result, None);
    let mut drains = 0;
    while !db
        .drain_retired_wallet(wallet(1), 1)
        .unwrap()
        .result
        .finished
    {
        drains += 1;
        assert!(drains < 10, "the drain never finished");
    }
    assert_eq!(
        drains, 3,
        "the group, its overlapping gate result and the gate-only result"
    );
    assert!(db.retirement_drains().unwrap().result.is_empty());
    let (mut actual, mut wanted) = (table_rows(&sql), table_rows(&expected));
    // `meta` differs only by the frontier hint the removal drops and the drain list left empty.
    assert_eq!(
        db.feed_history_frontiers().unwrap(),
        serde_json::json!({"version":1,"frontiers":[{"wallet":wallet(2)}]})
    );
    let other_meta = |rows: &[Vec<Cell>]| -> Vec<Vec<Cell>> {
        rows.iter()
            .filter(|row| {
                !matches!(row.first(), Some(Cell::Text(key))
                    if key.as_slice() == b"feed_history_frontiers"
                        || key.as_slice() == b"wallet_retirement_drains")
            })
            .cloned()
            .collect()
    };
    assert_eq!(other_meta(&actual["meta"]), other_meta(&wanted["meta"]));
    actual.remove("meta");
    wanted.remove("meta");
    assert_eq!(actual, wanted);
    for table in [
        "wallet_market_history_v2",
        "wallet_fences",
        "fills",
        "fill_market_snapshots",
        "positions",
        "bankroll",
        "settled_markets",
    ] {
        assert!(!actual[table].is_empty(), "{table} was populated");
    }
}
