#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Participant CLI coverage, retry and sweep-import contracts.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;

use pe_bootstrap::cache::WalletCache;
use serde_json::{Value, json};
use tempfile::TempDir;

const CONDITION: &str = "0xabababababababababababababababababababababababababababababababab";
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NEW_WALLET: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn run(root: &Path, args: &[&str], base: &str, lookback: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .env_clear()
        .current_dir(root)
        .args(args)
        .env("RUST_LOG", "info")
        .env("PE_BOOTSTRAP_OUTPUT", root.join("out.json"))
        .env("PE_BOOTSTRAP_CACHE_PATH", root.join("cache.db"))
        .env("PE_BOOTSTRAP_GAMMA_BASE_URL", base)
        .env("PE_POLYMARKET_BASE_URL", base)
        .env("PE_BOOTSTRAP_DATADASH_API_URL", "")
        .env("PE_BOOTSTRAP_LEADERBOARD_TOP_N", "0")
        .env("PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS", lookback)
        .output()
        .unwrap()
}

fn imported(root: &Path, from: i64, through: i64, floor: i64) -> Output {
    run(
        root,
        &[
            "participants-import",
            "--wallets",
            "participants.txt",
            "--series",
            "7:seven",
            "--end-date-min",
            &floor.to_string(),
            "--covered-from",
            &from.to_string(),
            "--covered-through",
            &through.to_string(),
        ],
        "invalid URL must never be fetched",
        "5",
    )
}

fn snapshot(root: &Path) -> (usize, Option<Value>) {
    let cache = WalletCache::open_read_only(&root.join("cache.db")).unwrap();
    (
        cache.wallet_pile_size().unwrap(),
        cache
            .participant_discovery_coverage()
            .unwrap()
            .map(|coverage| serde_json::to_value(coverage).unwrap()),
    )
}

#[test]
fn enabled_cli_failure_is_75_and_disabled_discovery_preserves_coverage() {
    let dir = TempDir::new().unwrap();
    drop(WalletCache::open(&dir.path().join("cache.db")).unwrap());
    std::fs::write(dir.path().join("participants.txt"), WALLET).unwrap();
    assert!(imported(dir.path(), 10, 20, 0).status.success());
    let before = snapshot(dir.path());
    let args = [
        "winner-discovery",
        "--defer-activation",
        "--end-date-min",
        "0",
    ];
    let failed = run(
        dir.path(),
        &[
            args.as_slice(),
            &["--participant-audit", "failed-audit.json"],
        ]
        .concat(),
        "invalid URL fails before network I/O",
        "5",
    );
    assert_eq!(failed.status.code(), Some(75), "{failed:?}");
    assert_eq!(snapshot(dir.path()), before);
    let disabled = run(dir.path(), &args, "invalid URL must never be fetched", "0");
    assert!(disabled.status.success(), "{disabled:?}");
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn import_cli_refusals_change_neither_wallets_nor_coverage_and_rerun_is_idempotent() {
    let dir = TempDir::new().unwrap();
    drop(WalletCache::open(&dir.path().join("cache.db")).unwrap());
    std::fs::write(
        dir.path().join("participants.txt"),
        WALLET.to_uppercase().replacen("0X", "0x", 1),
    )
    .unwrap();
    assert!(imported(dir.path(), 10, 20, 5).status.success());
    let initial = snapshot(dir.path());
    assert!(imported(dir.path(), 10, 20, 5).status.success());
    assert_eq!(snapshot(dir.path()), initial);
    std::fs::write(
        dir.path().join("participants.txt"),
        format!("{NEW_WALLET}\ninvalid"),
    )
    .unwrap();
    assert_eq!(imported(dir.path(), 20, 30, 5).status.code(), Some(1));
    assert_eq!(snapshot(dir.path()), initial);
    std::fs::write(dir.path().join("participants.txt"), NEW_WALLET).unwrap();
    let future = time::OffsetDateTime::now_utc().unix_timestamp() + 10_000;
    for (from, through, floor, reason) in [
        (21, 30, 5, "missing bridge [20, 21)"),
        (15, 25, 4, "requires coverage of [10, 20)"),
        (-1, 30, 5, "invalid"),
        (20, 20, 5, "invalid"),
        (20, future, 5, "future"),
    ] {
        let output = imported(dir.path(), from, through, floor);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(reason),
            "{output:?}"
        );
        assert_eq!(snapshot(dir.path()), initial);
    }
    assert!(imported(dir.path(), 0, 30, 4).status.success());
    let repaired = snapshot(dir.path());
    assert_eq!(repaired.0, 2);
    assert_eq!(repaired.1.unwrap()["market_floor_unix"], 4);
}

fn trade_page(data: Value) -> Value {
    json!({"data": data, "pagination": {"limit":1000, "offset":0, "has_more":false, "next_cursor":null}})
}

#[derive(Default)]
struct Venue {
    requests: Vec<String>,
    mode: u8,
    trade_time: i64,
    closed: bool,
    series_failure: Option<&'static str>,
}

fn serve(venue: Arc<Mutex<Venue>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut buffer = [0u8; 4_096];
                let read = stream.read(&mut buffer).unwrap();
                assert!(read > 0, "incomplete fixture request");
                bytes.extend_from_slice(&buffer[..read]);
            }
            let request = String::from_utf8_lossy(&bytes).into_owned();
            let mut venue = venue.lock().unwrap();
            venue.requests.push(request.clone());
            let (status, body) = if request.starts_with("GET /series?") {
                (200, json!([{"id":"7", "slug":"seven", "recurrence":"15m"}]))
            } else if request.starts_with("GET /series/7 ") {
                match venue.series_failure {
                    Some("gone") => (404, json!({"error":"not found"})),
                    Some("renamed") => {
                        (200, json!({"id":"7", "slug":"renamed", "recurrence":"15m"}))
                    }
                    Some("recurrence") => {
                        (200, json!({"id":"7", "slug":"seven", "recurrence":"daily"}))
                    }
                    None | Some("embedded") => {
                        (200, json!({"id":"7", "slug":"seven", "recurrence":"15m"}))
                    }
                    _ => unreachable!(),
                }
            } else if request.starts_with("GET /events/keyset?") {
                let path = request.split_whitespace().nth(1).unwrap();
                let url = reqwest::Url::parse(&format!("http://fixture{path}")).unwrap();
                let query: std::collections::BTreeMap<_, _> =
                    url.query_pairs().into_owned().collect();
                if request.contains("after_cursor=last")
                    || query["end_date_min"].as_str() > "1970-01-02T00:00:00Z"
                {
                    (200, json!({"events":[]}))
                } else {
                    // #871: the embedded series is what gate F reads; a test slug there fails the walk.
                    let slug = if venue.series_failure == Some("embedded") {
                        "polyv2-soak-btc-up-or-down-15m"
                    } else {
                        "seven"
                    };
                    (
                        200,
                        json!({"events":[{"series":[{"id":"7", "slug":slug, "recurrence":"15m"}], "markets":[{"conditionId":CONDITION, "endDate":"1970-01-02T00:00:00Z", "closed":venue.closed}]}], "next_cursor":"last"}),
                    )
                }
            } else if request.starts_with("GET /v2/trades?") {
                match venue.mode {
                    0 => (400, json!({"error":"fixture failed page"})),
                    1 => (
                        200,
                        trade_page(
                            json!([{"proxy_wallet":WALLET, "timestamp":venue.trade_time, "size":"0.000001"}]),
                        ),
                    ),
                    2 => (
                        200,
                        trade_page(
                            json!([{"proxy_wallet":WALLET, "timestamp":venue.trade_time}, {"timestamp":venue.trade_time}]),
                        ),
                    ),
                    3 => (
                        200,
                        trade_page(
                            json!([{"proxy_wallet":NEW_WALLET, "timestamp":venue.trade_time}]),
                        ),
                    ),
                    _ => panic!("unexpected fixture mode"),
                }
            } else {
                panic!("unexpected request {request}");
            };
            let body = body.to_string();
            stream.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).unwrap();
        }
    });
    base
}

#[test]
fn discovery_retries_pages_and_finds_post_end_trades_before_advancing_coverage() {
    let dir = TempDir::new().unwrap();
    drop(WalletCache::open(&dir.path().join("cache.db")).unwrap());
    let before = time::OffsetDateTime::now_utc().unix_timestamp() - 10;
    std::fs::write(dir.path().join("participants.txt"), "").unwrap();
    assert!(imported(dir.path(), 0, before, 0).status.success());
    let initial = snapshot(dir.path());
    let venue = Arc::new(Mutex::new(Venue {
        trade_time: before + 1,
        ..Venue::default()
    }));
    let base = serve(Arc::clone(&venue));
    let args = [
        "winner-discovery",
        "--defer-activation",
        "--end-date-min",
        "0",
    ];
    let failed = run(
        dir.path(),
        &[
            args.as_slice(),
            &["--participant-audit", "failed-audit.json"],
        ]
        .concat(),
        &base,
        "5",
    );
    assert_eq!(failed.status.code(), Some(75), "{failed:?}");
    assert_eq!(snapshot(dir.path()), initial);
    venue.lock().unwrap().mode = 1;
    let retried = run(
        dir.path(),
        &[
            args.as_slice(),
            &["--participant-audit", "retried-audit.json"],
        ]
        .concat(),
        &base,
        "5",
    );
    assert_eq!(retried.status.code(), Some(0), "{retried:?}");
    let first = snapshot(dir.path());
    assert_eq!(first.0, 1);
    let first_end = first.1.as_ref().unwrap()["covered_through_unix"]
        .as_i64()
        .unwrap();
    {
        let mut venue = venue.lock().unwrap();
        venue.mode = 2;
        venue.closed = true;
        venue.trade_time = first_end;
    }
    let invalid = run(
        dir.path(),
        &[
            args.as_slice(),
            &["--participant-audit", "invalid-audit.json"],
        ]
        .concat(),
        &base,
        "5",
    );
    assert_eq!(invalid.status.code(), Some(75), "{invalid:?}");
    let audit: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("invalid-audit.json")).unwrap())
            .unwrap();
    assert!(audit["pages"].as_array().unwrap().iter().any(|page| {
        page["kind"] == "trades"
            && page["raw_sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
    }));
    assert!(
        !String::from_utf8_lossy(&invalid.stderr).contains("winner_discovery: participant page")
    );
    assert_eq!(snapshot(dir.path()), first);
    venue.lock().unwrap().mode = 3;
    let later = run(
        dir.path(),
        &[
            args.as_slice(),
            &["--participant-audit", "later-audit.json"],
        ]
        .concat(),
        &base,
        "5",
    );
    assert_eq!(later.status.code(), Some(0), "{later:?}");
    let final_state = snapshot(dir.path());
    assert_eq!(final_state.0, 2);
    assert!(
        final_state.1.unwrap()["covered_through_unix"]
            .as_i64()
            .unwrap()
            > first_end
    );
    let cache = WalletCache::open_read_only(&dir.path().join("cache.db")).unwrap();
    assert_eq!(cache.active_wallet_count().unwrap(), 0);
    drop(cache);
    let conn = rusqlite::Connection::open(dir.path().join("cache.db")).unwrap();
    assert_eq!(
        conn.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM wallet_activation_batches",
            [],
            |row| row.get(0)
        )
        .unwrap(),
        0
    );
    let venue = venue.lock().unwrap();
    for request in &venue.requests {
        let path = request.split_whitespace().nth(1).unwrap();
        let url = reqwest::Url::parse(&format!("{base}{path}")).unwrap();
        let query: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        match url.path() {
            "/series" | "/series/7" => {}
            "/events/keyset" => {
                assert_eq!(query["end_date_min"], "1970-01-01T00:00:00Z");
                assert!(!query.contains_key("closed"));
            }
            "/v2/trades" => {
                assert_eq!(query["condition"], CONDITION);
                assert_eq!(query["taker_only"], "false");
                assert_eq!(query["filter_amount"], "0.0000001");
                assert_eq!(query["limit"], "1000");
                assert!(
                    !query.contains_key("start")
                        && !query.contains_key("end")
                        && !query.contains_key("offset")
                );
            }
            _ => panic!("unexpected request {path}"),
        }
    }
}

#[test]
fn discovery_refuses_changed_or_missing_covered_series_before_trades() {
    for failure in ["gone", "renamed", "recurrence", "embedded"] {
        let dir = TempDir::new().unwrap();
        drop(WalletCache::open(&dir.path().join("cache.db")).unwrap());
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        std::fs::write(dir.path().join("participants.txt"), WALLET).unwrap();
        assert!(
            imported(dir.path(), now - 100, now - 10, 0)
                .status
                .success()
        );
        let initial = snapshot(dir.path());
        let venue = Arc::new(Mutex::new(Venue {
            mode: 3,
            trade_time: now,
            series_failure: Some(failure),
            ..Venue::default()
        }));
        let base = serve(Arc::clone(&venue));
        let output = run(
            dir.path(),
            &[
                "winner-discovery",
                "--participant-audit",
                "audit.json",
                "--defer-activation",
                "--end-date-min",
                "0",
            ],
            &base,
            "5",
        );
        assert_eq!(output.status.code(), Some(75), "{failure}: {output:?}");
        assert_eq!(snapshot(dir.path()), initial);
        assert!(
            !venue
                .lock()
                .unwrap()
                .requests
                .iter()
                .any(|request| request.starts_with("GET /v2/trades"))
        );
        let audit: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("audit.json")).unwrap()).unwrap();
        assert_eq!(audit["record"]["status"], "failed:gamma", "{failure}");
    }
}

/// A schema-two candidate, built as the spool CLI scenario builds one (the collector requires schema two).
fn schema_two_candidate(path: &Path) {
    let mut cache = WalletCache::open(path).unwrap();
    cache
        .upsert_wallets_bulk(&[(WALLET.to_owned(), 16, false, None, None, None, 0)])
        .unwrap();
    cache.begin_or_resume_clob_payout_walk_v2(100).unwrap();
    cache
        .raw_conn_for_test()
        .execute_batch(
            "PRAGMA user_version=2; ALTER TABLE clob_payout_evidence_v2 DROP COLUMN neg_risk;",
        )
        .unwrap();
}

fn participant_state(db: &Path) -> (i64, Option<String>) {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    (
        conn.query_row("SELECT COUNT(*) FROM wallets", [], |row| row.get(0))
            .unwrap(),
        conn.query_row(
            "SELECT value FROM meta WHERE key = 'participant_discovery_coverage'",
            [],
            |row| row.get(0),
        )
        .ok(),
    )
}

/// #871 FAIL: a kept series' event embedding a test-series slug lets split collection exit nonzero (the wrapper
/// would then run ordinary discovery) or record anything but terminal 75, its audit loses `failed:gamma`, or apply
/// returns anything but 75 or changes participant state.
#[test]
fn split_collection_records_an_embedded_test_slug_as_terminal_75() {
    let dir = TempDir::new().unwrap();
    let fixed = dir.path().join("fixed.db");
    schema_two_candidate(&fixed);
    let candidate = dir.path().join("cache.db");
    std::fs::copy(&fixed, &candidate).unwrap();
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let venue = Arc::new(Mutex::new(Venue {
        mode: 3,
        trade_time: now,
        series_failure: Some("embedded"),
        ..Venue::default()
    }));
    let base = serve(Arc::clone(&venue));
    let spool = dir.path().join("spool.json");
    let collected = run(
        dir.path(),
        &[
            "winner-discovery",
            "--defer-activation",
            "--collect-to",
            spool.to_str().unwrap(),
            "--participant-audit",
            "audit.json",
            "--coverage-from",
            fixed.to_str().unwrap(),
            "--not-before",
            "0",
            "--end-date-min",
            "0",
        ],
        &base,
        "5",
    );
    assert_eq!(collected.status.code(), Some(0), "{collected:?}");
    let value: Value = serde_json::from_slice(&std::fs::read(&spool).unwrap()).unwrap();
    assert_eq!(value["outcomes"][2]["result"]["exit_code"], 75, "{value}");
    let audit: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("audit.json")).unwrap()).unwrap();
    assert_eq!(audit["record"]["status"], "failed:gamma");
    assert!(
        !venue
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| request.starts_with("GET /v2/trades"))
    );
    let before = participant_state(&candidate);
    let applied = run(
        dir.path(),
        &[
            "winner-discovery",
            "--defer-activation",
            "--apply-from",
            spool.to_str().unwrap(),
            "--end-date-min",
            "0",
        ],
        &base,
        "5",
    );
    assert_eq!(applied.status.code(), Some(75), "{applied:?}");
    assert_eq!(participant_state(&candidate), before);
}

#[test]
fn import_cannot_drop_or_rename_any_of_ten_covered_series_but_accepts_a_superset() {
    let dir = TempDir::new().unwrap();
    drop(WalletCache::open(&dir.path().join("cache.db")).unwrap());
    let series = |n: u64| {
        (1..=n)
            .map(|id| format!("{id}:s{id}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let import = |series: &str| {
        run(
            dir.path(),
            &[
                "participants-import",
                "--wallets",
                "participants.txt",
                "--series",
                series,
                "--end-date-min",
                "0",
                "--covered-from",
                "10",
                "--covered-through",
                "20",
            ],
            "invalid URL must never be fetched",
            "5",
        )
    };
    std::fs::write(dir.path().join("participants.txt"), WALLET).unwrap();
    assert!(import(&series(10)).status.success());
    let initial = snapshot(dir.path());
    std::fs::write(dir.path().join("participants.txt"), NEW_WALLET).unwrap();
    for dropped in [series(9), series(10).replace("10:s10", "10:renamed")] {
        let output = import(&dropped);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("retain every covered id:slug"));
        assert_eq!(snapshot(dir.path()), initial);
    }
    let accepted = import(&series(11));
    assert!(accepted.status.success(), "{accepted:?}");
    let final_state = snapshot(dir.path());
    assert_eq!(final_state.0, 2);
    assert_eq!(
        final_state.1.unwrap()["scope"]["series"]
            .as_array()
            .unwrap()
            .len(),
        11
    );
}
