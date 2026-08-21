#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario regressions for issue #519's rebuild/reset cursor ordering and
//! resolution-audit exit contract.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::{BootstrapConfig, fetch_resolutions_and_schedules};
use tempfile::TempDir;
use time::OffsetDateTime;

struct Route {
    needle: &'static str,
    status: &'static str,
    body: &'static str,
}

fn start_server(
    routes: Vec<Route>,
    connections: usize,
) -> (String, Arc<Mutex<Vec<String>>>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let handle = thread::spawn(move || {
        // Serve generously past the declared count: retries and auxiliary stages may add
        // requests, and a starved accept loop must never wedge a test (issue #519 review).
        for _ in 0..connections.max(64) {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8_192];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]).into_owned();
            observed.lock().unwrap().push(request.clone());
            let route = routes.iter().find(|route| request.contains(route.needle));
            let (status, body) = route
                .map(|route| (route.status, route.body))
                .unwrap_or(("404 Not Found", "{}"));
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (format!("http://{address}"), requests, handle)
}

fn insert_trade(cache: &WalletCache, trade_id: &str, market_id: &str) {
    cache
        .raw_conn_for_test()
        .execute(
            "INSERT INTO trades (source_trade_id, wallet_hex, market_id, outcome_id, \
             side, price_str, contracts, timestamp_unix) \
             VALUES (?1, '0x0000000000000000000000000000000000000001', ?2, 0, \
                     'buy', '0.5', 1, 1)",
            rusqlite::params![trade_id, market_id],
        )
        .unwrap();
}

#[tokio::test]
async fn rebuild_deletes_midwalk_cursor_and_repopulates_from_page_one() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
    cache
        .insert_resolution_with_source("m1", Some(0), 1_000, 1, "clob")
        .unwrap();
    cache
        .insert_resolution_with_source("m2", Some(1), 2_000, 1, "clob")
        .unwrap();
    insert_trade(&cache, "trade-m1", "m1");
    insert_trade(&cache, "trade-m2", "m2");

    let (base_url, requests, server) = start_server(
        vec![
            Route {
                needle: "GET /markets?closed=true&limit=1000 HTTP",
                status: "200 OK",
                body: r#"{"data":[{"condition_id":"m1","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":true},{"winner":false}]}],"next_cursor":"PAGE2"}"#,
            },
            Route {
                needle: "GET /markets?closed=true&limit=1000&next_cursor=PAGE2 HTTP",
                status: "200 OK",
                body: r#"{"data":[{"condition_id":"m2","end_date_iso":"1970-01-01T00:33:20Z","closed":true,"tokens":[{"winner":false},{"winner":true}]}],"next_cursor":"LTE="}"#,
            },
        ],
        2,
    );
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        rebuild_resolutions: true,
        ..BootstrapConfig::default()
    };

    fetch_resolutions_and_schedules(&config, &mut cache, &["m1".into(), "m2".into()])
        .await
        .unwrap();
    drop(server); // detach: fixture thread dies with the test process; exact request counts must never gate completion

    let requests = requests.lock().unwrap();
    assert!(requests[0].contains("GET /markets?closed=true&limit=1000 HTTP"));
    assert_eq!(cache.get_source_cursor("clob_closed").as_deref(), Some(""));
    assert_eq!(cache.resolved_market_ids().len(), 2);
    assert!(cache.resolution_record("m1").is_some());
    assert!(cache.resolution_record("m2").is_some());
}

#[tokio::test]
async fn rebuild_failure_before_page_one_leaves_no_cursor_row() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
    cache
        .insert_resolution_with_source("m1", Some(0), 1_000, 1, "clob")
        .unwrap();
    let (base_url, _, server) = start_server(Vec::new(), 1);
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        rebuild_resolutions: true,
        ..BootstrapConfig::default()
    };

    let result = fetch_resolutions_and_schedules(&config, &mut cache, &[]).await;
    drop(server); // detach: fixture thread dies with the test process; exact request counts must never gate completion

    assert!(result.is_err());
    assert!(cache.get_source_cursor("clob_closed").is_none());
    assert!(cache.resolution_record("m1").is_none());
}

#[test]
fn reset_failure_before_page_one_leaves_no_cursor_row_and_exits_one() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
    drop(cache);
    let (base_url, _, server) = start_server(Vec::new(), 1);

    let output = Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .args(["resolutions", "--reset-clob-cursor"])
        .env("PE_BOOTSTRAP_CACHE_PATH", &cache_path)
        .env("PE_BOOTSTRAP_OUTPUT", dir.path().join("out.json"))
        .env(
            "PE_BOOTSTRAP_WALLET_SET_PATH",
            dir.path().join("wallets.json"),
        )
        .env("PE_BOOTSTRAP_CLOB_BASE_URL", &base_url)
        .env("PE_BOOTSTRAP_GAMMA_BASE_URL", &base_url)
        .output()
        .unwrap();
    drop(server); // detach: fixture thread dies with the test process; exact request counts must never gate completion

    assert_eq!(output.status.code(), Some(1));
    let cache = WalletCache::open(&cache_path).unwrap();
    assert!(cache.get_source_cursor("clob_closed").is_none());
}

#[test]
fn unresolved_audit_exits_75_and_logs_counts() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    insert_trade(&cache, "trade-missing", "missing");
    let now = OffsetDateTime::now_utc().unix_timestamp();
    cache
        .insert_schedule("missing", Some(now.saturating_sub(7_200)), now)
        .unwrap();
    drop(cache);
    let (base_url, _, server) = start_server(
        vec![Route {
            needle: "GET /markets?closed=true&limit=1000 HTTP",
            status: "200 OK",
            body: r#"{"data":[],"next_cursor":"LTE="}"#,
        }],
        3,
    );

    let output = Command::new(env!("CARGO_BIN_EXE_pe-bootstrap"))
        .arg("resolutions")
        .env("RUST_LOG", "info")
        .env("PE_BOOTSTRAP_CACHE_PATH", &cache_path)
        .env("PE_BOOTSTRAP_OUTPUT", dir.path().join("out.json"))
        .env(
            "PE_BOOTSTRAP_WALLET_SET_PATH",
            dir.path().join("wallets.json"),
        )
        .env("PE_BOOTSTRAP_CLOB_BASE_URL", &base_url)
        .env("PE_BOOTSTRAP_GAMMA_BASE_URL", &base_url)
        .output()
        .unwrap();
    drop(server); // detach: fixture thread dies with the test process; exact request counts must never gate completion

    assert_eq!(output.status.code(), Some(75));
    // Tracing writes to stdout; search both streams so the assert is stream-agnostic.
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(logs.contains("resolutions_audit"), "logs were: {logs}");
    assert!(logs.contains("\"still_missing\":1"), "logs were: {logs}");
    assert!(logs.contains("\"clipped\":0"), "logs were: {logs}");
}

#[tokio::test]
async fn audit_rejects_per_market_identity_mismatch() {
    // Issue #519 review: a per-market response whose condition_id differs from
    // the requested market must repair NOTHING and count as still missing.
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    insert_trade(&cache, "trade-a", "audit-a");
    let now = OffsetDateTime::now_utc().unix_timestamp();
    cache
        .insert_schedule("audit-a", Some(now.saturating_sub(7_200)), now)
        .unwrap();
    let (base_url, _, server) = start_server(
        vec![
            Route {
                needle: "GET /markets?closed=true&limit=1000 HTTP",
                status: "200 OK",
                body: r#"{"data":[],"next_cursor":"LTE="}"#,
            },
            Route {
                needle: "GET /markets/audit-a HTTP",
                status: "200 OK",
                body: r#"{"condition_id":"audit-b","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"1","winner":true},{"token_id":"2","winner":false}]}"#,
            },
        ],
        4,
    );
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        ..BootstrapConfig::default()
    };

    let result = fetch_resolutions_and_schedules(&config, &mut cache, &["audit-a".into()]).await;
    drop(server); // detach: exact request counts must never gate completion

    match result {
        Err(pe_bootstrap::error::BootstrapError::ResolutionAuditIncomplete {
            still_missing,
            clipped,
        }) => {
            assert_eq!(still_missing, 1);
            assert_eq!(clipped, 0);
        }
        other => panic!("expected ResolutionAuditIncomplete, got {other:?}"),
    }
    assert!(cache.resolution_record("audit-a").is_none());
    assert!(cache.resolution_record("audit-b").is_none());
}

#[tokio::test]
async fn audit_observes_schedule_inserted_by_gamma_in_the_same_run() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();
    insert_trade(&cache, "trade-same-run", "same-run");
    let (base_url, requests, server) = start_server(
        vec![
            Route {
                needle: "GET /markets?closed=true&limit=1000 HTTP",
                status: "200 OK",
                body: r#"{"data":[],"next_cursor":"LTE="}"#,
            },
            Route {
                needle: "GET /markets?condition_ids=same-run&limit=500 HTTP",
                status: "200 OK",
                body: r#"[{"conditionId":"same-run","closed":false,"endDate":"1970-01-01T00:16:40Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
            },
            Route {
                needle: "GET /markets/same-run HTTP",
                status: "200 OK",
                body: r#"{"condition_id":"same-run","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":true},{"winner":false}]}"#,
            },
        ],
        3,
    );
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: base_url.clone(),
        gamma_base_url: base_url,
        ..BootstrapConfig::default()
    };

    let report = fetch_resolutions_and_schedules(&config, &mut cache, &["same-run".into()])
        .await
        .unwrap();
    drop(server); // detach: fixture thread dies with the test process; exact request counts must never gate completion

    assert!(report.stages_failed.is_empty());
    let resolution = cache
        .resolution_record("same-run")
        .expect("the audit must insert a terminal resolution row");
    assert_eq!(resolution.0, Some(0));
    assert_eq!(resolution.1, 1_000);
    assert_eq!(resolution.3, "clob");
    let requests = requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.contains("GET /markets/same-run HTTP")),
        "the last-stage audit must observe and repair Gamma's same-run schedule"
    );
}
