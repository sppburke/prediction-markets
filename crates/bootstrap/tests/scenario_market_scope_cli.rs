#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! `pe-bootstrap market-scope` (#784) against a local Gamma stub: it writes the scope file, fails
//! closed on a reply anomaly, and never opens or locks the wallet cache.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::thread;

use tempfile::TempDir;

const CONDITION: &str = "0xabababababababababababababababababababababababababababababababab";

fn start_server(events_series_id: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut request = [0u8; 8_192];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]).into_owned();
            let body = if request.starts_with("GET /series?") {
                r#"[{"id":"7","slug":"btc-up-or-down-15m","recurrence":"15m"},{"id":"8","recurrence":"daily"}]"#
                    .to_owned()
            } else if request.starts_with("GET /events/keyset?series_id=7&") {
                format!(
                    r#"{{"events":[{{"series":[{{"id":"{events_series_id}","recurrence":"15m"}}],"markets":[{{"conditionId":"{CONDITION}"}}]}}]}}"#
                )
            } else {
                panic!("unexpected request {request}");
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    format!("http://{address}")
}

fn run(dir: &Path, base_url: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .args(["market-scope", "--out"])
        .arg(dir.join("market_scope.json"))
        .args(["--end-date-min", "1776384000"])
        .env("PE_BOOTSTRAP_CACHE_PATH", dir.join("wallet_cache.db"))
        .env("PE_BOOTSTRAP_OUTPUT", dir.join("out.json"))
        .env("PE_BOOTSTRAP_GAMMA_BASE_URL", base_url)
        .output()
        .unwrap()
}

fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn writes_the_scope_file_without_touching_the_cache() {
    let dir = TempDir::new().unwrap();
    let output = run(dir.path(), &start_server("7"));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let scope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("market_scope.json")).unwrap())
            .unwrap();
    assert_eq!(scope["condition_ids"], serde_json::json!([CONDITION]));
    assert_eq!(
        scope["series"],
        serde_json::json!([{"id": "7", "slug": "btc-up-or-down-15m"}])
    );
    assert_eq!(scope["pages"].as_array().unwrap().len(), 2);
    assert_eq!(scope["end_date_min_unix"], 1_776_384_000);
    assert_eq!(entries(dir.path()), vec!["market_scope.json"]);
}

#[test]
fn a_reply_anomaly_exits_one_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let output = run(dir.path(), &start_server("9"));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(entries(dir.path()).is_empty());
}
