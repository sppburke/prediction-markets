//! Scenario: post-#369 resolution-stage failure modes.
//!
//! After issue #369, CLOB is the **primary, hard-fail** resolution source and
//! the Gamma stages remain soft-fail. Two cases:
//!
//! 1. CLOB returns a fatal response ⇒ `fetch_resolutions_and_schedules`
//!    propagates `Err` and aborts the pipeline (hard-fail).
//! 2. CLOB succeeds while Gamma returns malformed JSON ⇒ the function returns
//!    `Ok` with only `"gamma"` recorded in `stages_failed` (soft-fail).
//!
//! Deterministic, no live network: an in-process HTTP/1.1 server returns either
//! a fatal 404 or one empty terminal CLOB page; the subsequent Gamma request
//! receives malformed JSON so that stage soft-fails without retries.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::{BootstrapConfig, fetch_resolutions_and_schedules};
use tempfile::TempDir;

fn start_server(clob_succeeds: bool, connections: usize) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        // Serve generously past the declared count — a starved accept loop must never wedge a test.
        for _ in 0..connections.max(64) {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4_096];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            let is_clob_page = request.contains("GET /markets?closed=true&limit=1000 ");
            let (status, body) = if clob_succeeds {
                if is_clob_page {
                    ("200 OK", r#"{"data":[],"next_cursor":"LTE="}"#)
                } else {
                    ("200 OK", "{}")
                }
            } else {
                ("404 Not Found", "{}")
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (format!("http://{address}"), handle)
}

/// PASS: a fatal CLOB response makes the primary stage hard-fail, so
/// `fetch_resolutions_and_schedules` returns `Err` and aborts the run.
/// FAIL: the function returns `Ok` despite the fatal CLOB response.
#[tokio::test]
async fn resolutions_hard_fail_on_fatal_clob_response() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();

    let (base_url, server) = start_server(false, 1);
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        ..BootstrapConfig::default()
    };

    let market_ids = vec!["0xmarket0001".to_owned()];

    let result = fetch_resolutions_and_schedules(&config, &mut cache, &market_ids).await;
    drop(server); // detach: exact request counts must never gate completion

    assert!(
        result.is_err(),
        "a fatal CLOB primary response must hard-fail (propagate Err), got Ok"
    );
    println!("PASS: fatal CLOB response ⇒ fetch_resolutions_and_schedules returns Err");
}

/// PASS: with CLOB serving one empty terminal page and Gamma returning
/// malformed JSON, the function returns `Ok` with exactly `["gamma"]`
/// in `stages_failed`.
/// FAIL: the function returns `Err`, or `stages_failed` is not exactly `["gamma"]`.
#[tokio::test]
async fn gamma_only_soft_fails_after_empty_clob_page() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();

    let (base_url, server) = start_server(true, 2);
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        ..BootstrapConfig::default()
    };

    // Non-empty market set so the Gamma open-markets fetch has work to attempt
    // (fresh cache ⇒ the market is unresolved ⇒ it is an open id).
    let market_ids = vec!["0xmarket0001".to_owned()];

    let report = fetch_resolutions_and_schedules(&config, &mut cache, &market_ids)
        .await
        .expect("empty CLOB page + Gamma soft-fail must return Ok, not propagate Err");
    drop(server); // detach: exact request counts must never gate completion

    assert_eq!(
        report.stages_failed,
        vec!["gamma"],
        "only the Gamma stage may soft-fail; CLOB succeeds and the empty \
         null-rewrite / schedule-backfill stages issue no request"
    );
    println!("PASS: empty CLOB page + malformed Gamma JSON ⇒ Ok with stages_failed == [\"gamma\"]");
}
