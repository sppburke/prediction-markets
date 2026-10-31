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
        "--series",
        "7:seven",
        "--end-date-min",
        "0",
    ];
    let failed = run(
        dir.path(),
        &args,
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

#[derive(Default)]
struct Venue {
    requests: Vec<String>,
    mode: u8,
    trade_time: i64,
    closed: bool,
}

fn serve(venue: Arc<Mutex<Venue>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut bytes = [0u8; 8_192];
            let read = stream.read(&mut bytes).unwrap();
            let request = String::from_utf8_lossy(&bytes[..read]).into_owned();
            let mut venue = venue.lock().unwrap();
            venue.requests.push(request.clone());
            let (status, body) = if request.starts_with("GET /series/7 ") {
                (200, json!({"id":"7", "slug":"seven", "recurrence":"15m"}))
            } else if request.starts_with("GET /events/keyset?") {
                if request.contains("after_cursor=last") {
                    (200, json!({"events":[]}))
                } else {
                    (
                        200,
                        json!({"events":[{"series":[{"id":"7", "recurrence":"15m"}], "markets":[{"conditionId":CONDITION, "endDate":"1970-01-02T00:00:00Z", "closed":venue.closed}]}], "next_cursor":"last"}),
                    )
                }
            } else if request.starts_with("GET /trades?") {
                match venue.mode {
                    0 => (400, json!({"error":"fixture failed page"})),
                    1 => (
                        200,
                        json!([{"proxyWallet":WALLET, "timestamp":venue.trade_time}]),
                    ),
                    2 => (200, json!([{"proxyWallet":WALLET}, {}])),
                    3 => (
                        200,
                        json!([{"proxyWallet":NEW_WALLET, "timestamp":venue.trade_time}]),
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
fn pinned_discovery_retries_pages_and_finds_post_end_trades_before_advancing_coverage() {
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
        "--series",
        "7:seven",
        "--end-date-min",
        "0",
    ];
    let failed = run(dir.path(), &args, &base, "5");
    assert_eq!(failed.status.code(), Some(75), "{failed:?}");
    assert_eq!(snapshot(dir.path()), initial);
    venue.lock().unwrap().mode = 1;
    let retried = run(dir.path(), &args, &base, "5");
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
    let invalid = run(dir.path(), &args, &base, "5");
    assert_eq!(invalid.status.code(), Some(75), "{invalid:?}");
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("raw_sha256"));
    assert_eq!(snapshot(dir.path()), first);
    venue.lock().unwrap().mode = 3;
    let later = run(dir.path(), &args, &base, "5");
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
            "/series/7" => {}
            "/events/keyset" => {
                assert_eq!(query["end_date_min"], "1970-01-01T00:00:00Z");
                assert!(!query.contains_key("closed"));
            }
            "/trades" => {
                assert_eq!(query["market"], CONDITION);
                assert_eq!(query["takerOnly"], "false");
                assert!(
                    query["start"].parse::<i64>().unwrap() == before - 5
                        || query["start"].parse::<i64>().unwrap() == first_end - 5
                );
            }
            _ => panic!("unpinned request {path}"),
        }
    }
}
