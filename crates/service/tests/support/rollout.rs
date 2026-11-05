//! The real service process, local HTTP authority, and unchanged production admission gates.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use crate::support::data_api_v2;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, State},
    http::StatusCode,
    response::IntoResponse,
};
use pe_core_types::{
    CollateralAmount, MarketId, OutcomeId, Price, ReceivedAt, ShareAmount, Side, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Scanner, Writer};
use pe_paper_state::{MigrationMetadata, PaperStateDb};
use pe_service::{
    config::ServiceConfig,
    paper_migration::{PaperMigrationBoot, PaperMigrationPaths},
    paper_recovery::{
        FinancialPayload, PaperLogRecord, QualificationStarted, TailBinding, paper_era,
        replay_membership, scan_paper_log,
    },
    runtime_config::{ConfigRow, RuntimeConfig},
    source_log_boot::SourceLogBoot,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{Notify, Semaphore};

fn wallet(n: usize) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{n:040x}")).unwrap()
}
fn condition(n: usize) -> String {
    format!("0x{n:064x}")
}
fn row(w: WalletAddress, market: usize, at: i64, transaction: usize) -> Value {
    let at = OffsetDateTime::from_unix_timestamp(at).unwrap();
    let transaction_hash = format!("0x{transaction:064x}");
    let trade = pe_copy_signal_engine::IncomingTrade {
        wallet: w,
        market_id: MarketId(VenueMarketId(condition(market))),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(dec!(0.5)).unwrap(),
        contracts: ShareAmount::from_whole(100).unwrap(),
        observed_at: at,
        received_at: at,
        source_trade_id: pe_core_types::SourceTradeId(transaction_hash.clone()),
        transaction_hash: Some(transaction_hash),
        provenance: pe_copy_signal_engine::TradeProvenance::RestPoll,
    };
    let mut rows: Vec<Value> =
        serde_json::from_slice(&crate::support::activity_body(&trade)).unwrap();
    let mut row = rows.remove(0);
    row["asset"] = (market * 2 + 101).to_string().into();
    row
}
#[derive(Default)]
struct Authority {
    cash: Decimal,
    progress: Option<u64>,
    fills: HashMap<String, Value>,
    positions: HashMap<String, Decimal>,
    settlements: HashMap<String, Decimal>,
    projection: Vec<Value>,
    projection_generation: u64,
}
struct HttpState {
    boot_waves: bool,
    ranking_batch: std::sync::atomic::AtomicI64,
    scope_drops: Mutex<Vec<Value>>,
    group_spans: Mutex<Vec<Value>>,
    import_reads: Mutex<Vec<i64>>,
    rows: Vec<ConfigRow>,
    now: i64,
    ranked_wallets: usize,
    newcomer: Option<WalletAddress>,
    activity_requests: Mutex<Vec<HashMap<String, String>>>,
    full_history_requests: Mutex<Vec<String>>,
    cut_full_history_replies: std::sync::atomic::AtomicUsize,
    start: Mutex<Option<pe_event_log::AppendReceipt>>,
    authority: Mutex<Authority>,
    activity: Mutex<HashMap<String, Vec<Value>>>,
    positions: Mutex<Vec<Value>>,
    data_api_v2: Mutex<data_api_v2::DataApiV2>,
    activity_page_size_cap: Option<usize>,
    slow_started: Notify,
    slow: Semaphore,
    release_slow: std::sync::atomic::AtomicBool,
    gamma_fails: std::sync::atomic::AtomicBool,
    stale_ranked_wallet_2: std::sync::atomic::AtomicBool,
    gamma_failures: std::sync::atomic::AtomicUsize,
    resolved: std::sync::atomic::AtomicBool,
}
/// The daily-boundary mark the service requests for an open position after a UTC midnight (#761).
fn prices_history(q: &HashMap<String, String>) -> Value {
    assert!(q["market"].parse::<usize>().unwrap() >= 101);
    let end = q["endTs"].parse::<i64>().unwrap();
    assert_eq!(q["startTs"].parse::<i64>().unwrap(), end - 120);
    assert_eq!(q["fidelity"], "1");
    json!({"history":[{"t":end,"p":"0.50"}]})
}

/// The production mark adapter values a position from this fixture's answer at a fixed cutoff.
pub async fn boundary_mark() {
    use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    let state = Arc::new(HttpState {
        boot_waves: false,
        ranking_batch: std::sync::atomic::AtomicI64::new(1),
        scope_drops: Mutex::new(Vec::new()),
        group_spans: Mutex::new(Vec::new()),
        import_reads: Mutex::new(Vec::new()),
        rows: Vec::new(),
        now: 0,
        ranked_wallets: 0,
        newcomer: None,
        activity_requests: Mutex::new(Vec::new()),
        full_history_requests: Mutex::new(Vec::new()),
        cut_full_history_replies: AtomicUsize::new(0),
        start: Mutex::new(None),
        authority: Mutex::new(Authority::default()),
        activity: Mutex::new(HashMap::new()),
        positions: Mutex::new(Vec::new()),
        data_api_v2: Mutex::new(Default::default()),
        activity_page_size_cap: None,
        slow_started: Notify::new(),
        slow: Semaphore::new(0),
        release_slow: AtomicBool::new(true),
        gamma_fails: AtomicBool::new(false),
        stale_ranked_wallet_2: AtomicBool::new(false),
        gamma_failures: AtomicUsize::new(0),
        resolved: AtomicBool::new(false),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(serve).with_state(state))
            .await
            .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(4);
    let (trigger_tx, _triggers) = tokio::sync::mpsc::channel(1);
    tokio::spawn(
        ActivityIngest::poll_only(
            pe_service::source_event_sink::SourceEventSink::open(dir.path().join("source.log"))
                .unwrap(),
            source_rx,
            trigger_tx,
            pe_service::health::new_shared_health_with_ws(false, false, 90),
        )
        .run(),
    );
    let cutoff = 1_790_985_600;
    let (mark, closure) = pe_service::mark_prices::HistoricalMarkAdapter::new(
        reqwest::Client::new(),
        base,
        source_log,
    )
    .fetch_paper(&condition(1), "103", cutoff)
    .await
    .unwrap();
    assert_eq!(mark.price.0, dec!(0.50));
    assert_eq!(mark.sample_unix, cutoff);
    assert!(closure.is_none());
}

async fn serve(
    State(state): State<Arc<HttpState>>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> axum::response::Response {
    use std::sync::atomic::Ordering;
    let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
    let q = url.query_pairs().into_owned().collect::<HashMap<_, _>>();
    let path = url.path();
    if matches!(path, "/activity" | "/v2/activity") {
        let w = q.get("user").unwrap();
        state.activity_requests.lock().unwrap().push(q.clone());
        if q.get("start").is_some_and(|start| start == "1") {
            let reads = {
                let mut requests = state.full_history_requests.lock().unwrap();
                requests.push(w.clone());
                requests.iter().filter(|requested| *requested == w).count()
            };
            // Startup maintenance now runs immediately. Hold its first read for each
            // boot-incomplete wallet so the boot-wave assertions observe boot state.
            if state.boot_waves && w != &wallet(5).to_string() && reads == 4 {
                state.slow.acquire().await.unwrap().forget();
            }
        }
        if w == &wallet(2).to_string()
            && !state.release_slow.load(Ordering::SeqCst)
            // The admission case holds the refresh baseline, leaving ordinary polls available.
            && (state.newcomer.is_none() || q.get("start").is_some_and(|start| start == "1"))
        {
            state.slow_started.notify_one();
            state.slow.acquire().await.unwrap().forget();
        }
        let start = q
            .get("start")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        let end = q
            .get("end")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(i64::MAX);
        let data = state
            .activity
            .lock()
            .unwrap()
            .get(w)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|v| {
                v["timestamp"]
                    .as_i64()
                    .is_some_and(|at| at >= start && at <= end)
            })
            .collect::<Vec<_>>();
        if path == "/v2/activity" {
            let body = match state.data_api_v2.lock().unwrap().reply(
                path,
                &data,
                &q,
                state.activity_page_size_cap,
            ) {
                Ok(body) => body,
                Err(status) => return status.into_response(),
            };
            if q.get("start").is_some_and(|start| start == "1")
                && !q.contains_key("cursor")
                && state
                    .cut_full_history_replies
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                assert_eq!(q["limit"], "1000");
                assert_eq!(q["sort_direction"], "DESC");
                assert_eq!(q["type"], "TRADE,SPLIT,MERGE,REDEEM,CONVERSION");
                assert!(q["end"].parse::<i64>().is_ok());
                let mut body = serde_json::to_string(&body).unwrap();
                body.truncate(body.find("\":\"").unwrap() + 4);
                return (StatusCode::OK, [("content-type", "application/json")], body)
                    .into_response();
            }
            return (StatusCode::OK, Json(body)).into_response();
        }
        if q.get("start").is_some_and(|start| start == "1")
            && q.get("offset").is_some_and(|offset| offset == "0")
            && state
                .cut_full_history_replies
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            assert_eq!(q["limit"], "500");
            assert_eq!(q["sortDirection"], "DESC");
            assert_eq!(q["type"], "TRADE,SPLIT,MERGE,REDEEM,CONVERSION");
            assert!(q["end"].parse::<i64>().is_ok());
            let mut body = serde_json::to_string(&data).unwrap();
            // Retain a strict prefix of the actual reply, ending inside its first string value.
            body.truncate(body.find("\":\"").unwrap() + 4);
            return (StatusCode::OK, [("content-type", "application/json")], body).into_response();
        }
        return (StatusCode::OK, Json(json!(data))).into_response();
    }
    if path == "/v2/positions" {
        return match state.data_api_v2.lock().unwrap().reply(
            path,
            &state.positions.lock().unwrap(),
            &q,
            None,
        ) {
            Ok(body) => (StatusCode::OK, Json(body)).into_response(),
            Err(status) => status.into_response(),
        };
    }
    if path == "/positions" {
        let data = state
            .positions
            .lock()
            .unwrap()
            .iter()
            .filter(|row| {
                row["proxyWallet"] == *q.get("user").unwrap()
                    && row["redeemable"].as_bool().unwrap()
                        == (q.get("redeemable").unwrap() == "true")
            })
            .cloned()
            .collect::<Vec<_>>();
        return (StatusCode::OK, Json(json!(data))).into_response();
    }
    if path == "/markets" {
        let token = q
            .get("clob_token_ids")
            .and_then(|v| v.parse::<usize>().ok());
        let market = token
            .map(|n| (n - 101) / 2)
            .or_else(|| {
                q.get("condition_ids")
                    .and_then(|v| usize::from_str_radix(v.trim_start_matches("0x"), 16).ok())
            })
            .unwrap_or(1);
        if market == 4 && state.gamma_fails.load(Ordering::SeqCst) {
            state.gamma_failures.fetch_add(1, Ordering::SeqCst);
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error":"isolated Gamma outage"})),
            )
                .into_response();
        }
        let mut v: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/golden_stream_v1/gamma_long.json"
        ))
        .unwrap();
        v[0]["conditionId"] = condition(market).into();
        v[0]["clobTokenIds"] = serde_json::to_string(&[
            (market * 2 + 101).to_string(),
            (market * 2 + 102).to_string(),
        ])
        .unwrap()
        .into();
        v[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
        v[0]["feesEnabled"] = true.into();
        v[0]["endDate"] = OffsetDateTime::from_unix_timestamp(last_second_of_day(state.now))
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
            .into();
        return (StatusCode::OK, Json(v)).into_response();
    }
    if let Some(c) = path.strip_prefix("/markets/") {
        let market = usize::from_str_radix(c.trim_start_matches("0x"), 16).unwrap();
        let resolved = market == 5 && state.resolved.load(Ordering::SeqCst);
        let fixture: &[u8] = if resolved {
            include_bytes!("../fixtures/golden_stream_v1/clob_resolution.json")
        } else {
            include_bytes!("../fixtures/golden_stream_v1/clob_long.json")
        };
        let mut v: Value = serde_json::from_slice(fixture).unwrap();
        v["condition_id"] = c.into();
        v["end_date_iso"] = OffsetDateTime::from_unix_timestamp(last_second_of_day(state.now))
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
            .into();
        v["tokens"][0]["token_id"] = (market * 2 + 101).to_string().into();
        v["tokens"][1]["token_id"] = (market * 2 + 102).to_string().into();
        return (StatusCode::OK, Json(v)).into_response();
    }
    if let Some(c) = path.strip_prefix("/clob-markets/") {
        let market = usize::from_str_radix(c.trim_start_matches("0x"), 16).unwrap();
        let mut v: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/golden_stream_v1/clob_compact.json"
        ))
        .unwrap();
        v["c"] = c.into();
        v["t"][0]["t"] = (market * 2 + 101).to_string().into();
        v["t"][1]["t"] = (market * 2 + 102).to_string().into();
        return (StatusCode::OK, Json(v)).into_response();
    }
    if path == "/prices-history" {
        return (StatusCode::OK, Json(prices_history(&q))).into_response();
    }
    if path == "/book" {
        let token = q["token_id"].parse::<usize>().unwrap();
        let market = (token - 101) / 2;
        return (
            StatusCode::OK,
            Json(
                json!({"market":condition(market),"asset_id":token.to_string(),"timestamp":OffsetDateTime::now_utc().unix_timestamp_nanos().checked_div(1_000_000).unwrap().to_string(),"min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{"price":"0.50","size":"100000"}],"asks":[{"price":"0.50","size":"100000"}]}),
            ),
        )
        .into_response();
    }
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    let mut a = state.authority.lock().unwrap();
    let batch = q
        .get("batch_id")
        .and_then(|id| id.strip_prefix("eq."))
        .map(|id| id.parse::<i64>().unwrap())
        .unwrap_or_else(|| state.ranking_batch.load(Ordering::SeqCst));
    let answer=match path {
        "/rest/v1/accounts"|"/rest/v1/account_credentials"=>json!([]),
        "/rest/v1/ranking_scope_drops"=> {
            state.import_reads.lock().unwrap().push(batch);
            json!(state.scope_drops.lock().unwrap().iter().filter(|row| row["batch_id"] == batch).cloned().collect::<Vec<_>>())
        },
        "/rest/v1/ranking_neg_risk_group_spans"=>json!(*state.group_spans.lock().unwrap()),
        "/rest/v1/service_config"=>json!(state.rows),
        "/rest/v1/ranking_batches"=>json!([{"batch_id":batch}]),
        "/rest/v1/ranking_entries"|"/rest/v1/latest_ranking"=>json!((1..=state.ranked_wallets).filter(|n| batch != 1 || state.newcomer != Some(wallet(*n))).map(|n|json!({"batch_id":batch,"rank":n,"wallet_hex":wallet(n).to_string(),"ls_tstat":"3","hit_rate":"0.65","n_trades":100,"last_trade_unix":if n == 2 && state.stale_ranked_wallet_2.load(Ordering::SeqCst) { state.now - 300_000 } else { state.now - 20 },"survives":true})).collect::<Vec<_>>()),
        "/rest/v1/service_runtime"=>json!([{"updated_at":a.projection_generation.to_string(),"watchlist_size":a.projection.len()}]),
        "/rest/v1/rpc/service_watchlist_replace_v1"=> { assert_eq!(body["expected_token"],a.projection_generation.to_string());a.projection=body["entries"].as_array().unwrap().clone();a.projection_generation+=1;json!([{"new_token":a.projection_generation.to_string(),"count":a.projection.len()}]) },
        "/rest/v1/paper_bankroll"=>json!([{"bankroll_str":a.cash.to_string(),"last_prepared_seq":a.progress}]),
        "/rest/v1/paper_positions"=>json!(a.positions.iter().filter(|(_,qty)|!qty.is_zero()).map(|(market,qty)|json!({"market_id":market,"outcome_id":0,"long_contracts":qty.to_string(),"short_contracts":"0"})).collect::<Vec<_>>()),
        "/rest/v1/rpc/seed_financial_start"=> {let start=state.start.lock().unwrap().unwrap();json!({"outcome":"existing","start_seq":start.sequence.0,"start_hash":start.this_hash.to_hex().to_string()})},
        "/rest/v1/rpc/commit_fill_v2"=> {
            let start=state.start.lock().unwrap().unwrap();assert_eq!(body["p_start_seq"],start.sequence.0);assert_eq!(body["p_start_hash"],start.this_hash.to_hex().to_string());
            let key=body["p_idempotency_key"].as_str().unwrap().to_owned();let mut outcome="existing";
            let canonical=if let Some(prior)=a.fills.get(&key){prior.clone()}else{
                assert_eq!(body["p_expected_prior_seq"],json!(a.progress));assert!(!a.settlements.contains_key(body["p_market_id"].as_str().unwrap()));
                let decimal=|key:&str|body[key].as_str().unwrap().parse::<Decimal>().unwrap();let qty=decimal("p_quantity");let principal=decimal("p_principal");let fee=decimal("p_fee");
                assert_eq!(qty*decimal("p_fill_price"),principal);assert!(fee>Decimal::ZERO);a.cash-=principal+fee;
                *a.positions.entry(body["p_market_id"].as_str().unwrap().to_owned()).or_default()+=qty;a.progress=body["p_prepared_seq"].as_u64();outcome="applied";
                let mut canonical=serde_json::Map::new();for field in ["idempotency_key","leader_wallet","source_trade_id","market_id","outcome_id","side","quantity","fill_price","principal","fee","entry_unix","prepared_seq"]{canonical.insert(field.to_owned(),body[format!("p_{field}")].clone());}
                let v=Value::Object(canonical);a.fills.insert(key,v.clone());v
            };json!({"outcome":outcome,"bankroll":a.cash.to_string(),"applied_prepared_seq":body["p_prepared_seq"],"row":canonical})
        },
        "/rest/v1/rpc/apply_resolution_v2"=> {
            assert_eq!(body["p_expected_prior_seq"],json!(a.progress));let c=body["p_condition_id"].as_str().unwrap().to_owned();assert!(!a.settlements.contains_key(&c));assert_eq!(body["p_payout_by_outcome_index"],json!(["1","0"]));
            let credit=a.positions.remove(&c).unwrap();a.cash+=credit;a.settlements.insert(c,credit);a.progress=body["p_prepared_seq"].as_u64();json!({"outcome":"applied","bankroll":a.cash.to_string(),"applied_prepared_seq":a.progress,"credit":credit.to_string()})
        },
        other=>panic!("unexpected production request {other}: {body}"),
    };
    (StatusCode::OK, Json(answer)).into_response()
}
pub struct Child {
    process: std::process::Child,
    root: std::path::PathBuf,
    stdout: Option<tokio::task::JoinHandle<Vec<u8>>>,
    stderr: Option<tokio::task::JoinHandle<Vec<u8>>>,
}
impl Child {
    fn start(config: &Path) -> Self {
        Self::start_checkpoint(config, Path::new(env!("CARGO_BIN_EXE_pe-service")), &[])
    }
    pub fn start_checkpoint(config: &Path, binary: &Path, env: &[(&str, &Path)]) -> Self {
        let mut process = std::process::Command::new(binary)
            .env_clear()
            .envs(env.iter().map(|(key, value)| (key, value)))
            .current_dir(config.parent().unwrap())
            .arg(config)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        fn drain(mut pipe: impl std::io::Read) -> Vec<u8> {
            let mut bytes = Vec::new();
            pipe.read_to_end(&mut bytes).unwrap();
            bytes
        }
        let stdout = process.stdout.take().unwrap();
        let stderr = process.stderr.take().unwrap();
        Self {
            process,
            root: config.parent().unwrap().to_path_buf(),
            stdout: Some(tokio::task::spawn_blocking(move || drain(stdout))),
            stderr: Some(tokio::task::spawn_blocking(move || drain(stderr))),
        }
    }
    pub fn signal_checkpoint(&self, signal: &str) {
        assert!(
            std::process::Command::new("kill")
                .args([signal, &self.process.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
    }
    pub async fn finish_checkpoint(mut self, signal: Option<&str>) -> std::process::Output {
        if let Some(signal) = signal {
            self.signal_checkpoint(signal);
        }
        let status = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                if let Some(status) = self.process.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        std::process::Output {
            status,
            stdout: self.stdout.take().unwrap().await.unwrap(),
            stderr: self.stderr.take().unwrap().await.unwrap(),
        }
    }
    async fn stop(mut self) -> String {
        assert!(
            std::process::Command::new("kill")
                .args(["-INT", &self.process.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.process.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if status.is_err() {
            self.process.kill().unwrap();
            self.process.wait().unwrap();
        }
        let out = self.stdout.take().unwrap().await.unwrap();
        let err = self.stderr.take().unwrap().await.unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&err)
        );
        let status = status.unwrap();
        assert!(status.success(), "{status}: {text}");
        text
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
        if std::thread::panicking() {
            for entry in std::fs::read_dir(&self.root).unwrap().flatten() {
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if (name == "status.json"
                    || (name.starts_with("service.") && name.ends_with(".jsonl")))
                    && let Ok(text) = std::fs::read_to_string(&path)
                {
                    let lines = text.lines().collect::<Vec<_>>();
                    eprintln!(
                        "{}:\n{}",
                        path.display(),
                        lines[lines.len().saturating_sub(60)..].join("\n")
                    );
                }
            }
        }
    }
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(40), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("production observation exceeded its bound");
}
/// A rollout fixture lives inside one UTC day (#778): its markets end in the day's last second, and its Start,
/// recorded at `now - 5`, must not fall on the previous day. The latest accepted start leaves 241 seconds until
/// 23:59:00, where the inclusive 60-second floor is reached; copying must finish within that headroom.
async fn fixture_now() -> i64 {
    loop {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        if (5..86_100).contains(&now.rem_euclid(86_400)) {
            return now;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
fn last_second_of_day(unix: i64) -> i64 {
    unix - unix.rem_euclid(86_400) + 86_399
}
fn envelope(payload: Vec<u8>, source: &str, at: i64, schema: u32, parser: u32) -> EnvelopeIn {
    let at = OffsetDateTime::from_unix_timestamp(at).unwrap();
    EnvelopeIn {
        source_id: SourceId(source.to_owned()),
        schema_version: schema,
        parser_version: parser,
        observed_at: SourceTimestamp(at),
        received_at: ReceivedAt(at),
        content_type: ContentType::Json,
        payload,
    }
}
/// PASS: a real, fully configured binary copies during slow admission, clears only a safe fence,
/// repairs first-entry history, settles once, and restores checkpoint+suffix and exact decisions
/// from schema-7 v2 cursor pages with schema-3 evidence. FAIL: v1 acquisition or restart drift.
pub async fn run() {
    run_case(false, false, false).await;
}

pub async fn run_boot_waves() {
    run_case(true, false, false).await;
}

/// PASS: runtime ranking imports and local anomalies exclude only their scopes; revised BUYs
/// retain their original fill without fencing. Restart preserves drops, imports, dispositions,
/// fills, balances and history, and the sealed CLI verifier reproduces every decision exactly.
/// Schema-7 pages and schema-3 cursor evidence include a forced multi-page read.
/// FAIL: a v1 fetch, missing cursor proof, lost state or inexact sealed replay.
/// This short run does not meet the qualification duration or closed-copy sample thresholds.
pub async fn scope_drops() {
    run_case(false, true, false).await;
}

/// PASS: a newly ranked wallet is admitted and copies before a held routine refresh is released;
/// the refresh retries, and restart plus sealed CLI replay preserve every decision and membership.
/// FAIL: the unflagged refresh retains the bracket lock, its retry never anchors, or replay drifts.
pub async fn newly_ranked_wallet_during_slow_refresh() {
    run_case(false, false, true).await;
}

async fn run_case(boot_waves: bool, scope_drops: bool, waiting_admission: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let now = fixture_now().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let bind = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let cfg = ServiceConfig {
        bind: bind.to_string(),
        event_log_path: root.join("paper.log"),
        source_event_log_path: root.join("source.log"),
        paper_state_db_path: root.join("paper.db"),
        legacy_wallet_history_path: root.join("legacy.json"),
        jsonl_log_path: root.join("service.jsonl"),
        status_path: root.join("status.json"),
        polymarket_base_url: base.clone(),
        gamma_base_url: base.clone(),
        polymarket_clob_base_url: base.clone(),
        supabase_url: base.clone(),
        supabase_secret_key: "synthetic-local-authority".to_owned(),
        supabase_authoritative: true,
        // The unchanged 25-bps cap must cover the venue minimum and its nonzero fee.
        bankroll_usd: "10000".to_owned(),
        maintenance_interval_secs: if boot_waves || scope_drops { 3600 } else { 1 },
        trade_poll_interval_secs: 1,
        gamma_resolution_poll_interval_secs: 1,
        supabase_refresh_interval_secs: 1,
        status_interval_secs: 1,
        ..ServiceConfig::default()
    };
    let mut runtime = RuntimeConfig::from_service_config(&cfg);
    // Keep the monetary cap armed and request a size that fits it, including fees.
    runtime.sizing_mode = pe_strategy_winner_follow::SizingMode::Dollar { usd: dec!(5) };
    runtime.sizing_dollar_usd = dec!(5);
    assert!(runtime.min_resolution_horizon_secs > 0);
    assert!(runtime.max_resolution_horizon_secs > runtime.min_resolution_horizon_secs);
    assert_eq!(runtime.price_impact_cap_bps, 100);
    let state = Arc::new(HttpState {
        boot_waves,
        ranking_batch: std::sync::atomic::AtomicI64::new(1),
        scope_drops: Mutex::new(Vec::new()),
        group_spans: Mutex::new(Vec::new()),
        import_reads: Mutex::new(Vec::new()),
        rows: crate::golden::golden_config_rows(&runtime),
        now,
        ranked_wallets: if boot_waves {
            5
        } else if waiting_admission {
            3
        } else {
            4
        },
        newcomer: waiting_admission.then_some(wallet(3)),
        activity_requests: Mutex::new(Vec::new()),
        full_history_requests: Mutex::new(Vec::new()),
        cut_full_history_replies: std::sync::atomic::AtomicUsize::new(0),
        start: Mutex::new(None),
        authority: Mutex::new(Authority {
            cash: dec!(10000),
            ..Authority::default()
        }),
        activity: Mutex::new(HashMap::from([
            (
                wallet(2).to_string(),
                vec![row(wallet(2), 2, now - 20, 200)],
            ),
            (
                wallet(4).to_string(),
                vec![row(wallet(4), 4, now - 20, 400)],
            ),
        ])),
        positions: Mutex::new(Vec::new()),
        data_api_v2: Mutex::new(Default::default()),
        activity_page_size_cap: (!boot_waves).then_some(1),
        slow_started: Notify::new(),
        slow: Semaphore::new(0),
        release_slow: AtomicBool::new(boot_waves || scope_drops),
        gamma_fails: AtomicBool::new(!boot_waves && !scope_drops && !waiting_admission),
        stale_ranked_wallet_2: AtomicBool::new(false),
        gamma_failures: std::sync::atomic::AtomicUsize::new(0),
        resolved: AtomicBool::new(false),
    });
    if boot_waves || scope_drops || waiting_admission {
        state.activity.lock().unwrap().clear();
    }
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new().fallback(serve).with_state(state.clone()),
        )
        .into_future(),
    );
    rusqlite::Connection::open(&cfg.paper_state_db_path)
        .unwrap()
        .execute_batch(include_str!(
            "../../../paper-state/tests/fixtures/paper_state_v1.sql"
        ))
        .unwrap();
    drop(Writer::open(&cfg.event_log_path).unwrap());
    drop(Writer::open(&cfg.source_event_log_path).unwrap());
    drop(pe_execution_core::LiveJournal::open(root.join("live_journal.log")).unwrap());
    std::fs::write(&cfg.legacy_wallet_history_path, b"{\"wallets\":[]}").unwrap();
    let paths = PaperMigrationPaths {
        fixed_main: cfg.paper_state_db_path.clone(),
        source_log: cfg.source_event_log_path.clone(),
        paper_log: cfg.event_log_path.clone(),
        live_journal: root.join("live_journal.log"),
        legacy_history: cfg.legacy_wallet_history_path.clone(),
        binary_identity: pe_service::build_info::embedded()
            .source_revision
            .to_owned(),
    };
    let boot = PaperMigrationBoot::prepare(paths.clone(), now).unwrap();
    let side = PaperStateDb::open(&boot.active_main).unwrap();
    side.record_migration_activation_facts(&json!({"fixture":"complete"}), &paths.binary_identity)
        .unwrap();
    drop(side);
    boot.session.unwrap().finish().unwrap();
    let paper = Arc::new(PaperStateDb::open(&cfg.paper_state_db_path).unwrap());
    let wallets = (1..=state.ranked_wallets)
        .map(wallet)
        .filter(|wallet| Some(*wallet) != state.newcomer)
        .collect::<Vec<_>>();
    for w in &wallets {
        if scope_drops {
            crate::support::install_verified_empty_anchor(&paper, *w, now - 10);
        } else {
            let age = if waiting_admission && *w == wallet(2) {
                i64::try_from(pe_service::trade_poller::ANCHOR_REFRESH_SECS).unwrap() + 1
            } else {
                10
            };
            crate::support::install_full_history_anchor(&paper, *w, now - age);
        }
        paper
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet: *w,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: now - 10,
            })
            .unwrap();
    }
    let start = QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10000)).unwrap(),
        paper_prefix: TailBinding::from(&Scanner::verify(&cfg.event_log_path).unwrap()),
        source_prefix: TailBinding::from(&Scanner::verify(&cfg.source_event_log_path).unwrap()),
        live_prefix: TailBinding::from(&Scanner::verify(root.join("live_journal.log")).unwrap()),
        artifact_blake3: "a".repeat(64),
        static_config_hash: "b".repeat(64),
        hot_config_hash: runtime.canonical_hash(),
        generation: "rollout".to_owned(),
        activation_id: "rollout".to_owned(),
        ranking_batch_id: 1,
        membership: wallets.clone(),
        membership_proofs_hash: pe_service::qualification::scenario_membership_proofs_hash(
            &paper, &wallets,
        )
        .unwrap(),
        schema_version: 2,
        parser_version: 1,
        financial_semantic_version: pe_service::paper_recovery::FINANCIAL_SEMANTIC_VERSION,
    };
    let start_receipt = Writer::open(&cfg.event_log_path)
        .unwrap()
        .append_synced(envelope(
            serde_json::to_vec(&PaperLogRecord::QualificationStarted(std::sync::Arc::new(
                start,
            )))
            .unwrap(),
            "pe-service.paper",
            now - 5,
            2,
            1,
        ))
        .unwrap();
    paper
        .reset_financial_era(
            start_receipt,
            CollateralAmount::from_decimal_exact(dec!(10000)).unwrap(),
            None,
        )
        .unwrap();
    *state.start.lock().unwrap() = Some(start_receipt);
    if waiting_admission {
        run_waiting_admission(&cfg, &paper, &state, &wallets).await;
        server.abort();
        return;
    }
    if boot_waves {
        let c = rusqlite::Connection::open(&cfg.paper_state_db_path).unwrap();
        // Only the fifth wallet has a seed to promote. Every wallet must take a fresh bracket.
        for w in &wallets[..4] {
            c.execute(
                "DELETE FROM wallet_history_status_v2 WHERE wallet_hex=?1",
                [w.to_string()],
            )
            .unwrap();
        }
        c.execute("UPDATE wallet_history_status_v2 SET complete=0", [])
            .unwrap();
        c.execute(
            "UPDATE position_anchors SET anchored_at_unix=?1",
            [now - 4000],
        )
        .unwrap();
        drop(c);
        let config_path = root.join("service.toml");
        std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
        let child = Child::start(&config_path);
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                if client
                    .get(format!("http://{bind}/health/ready"))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("boot waves must reach successful readiness");
        until(|| {
            let requests = state.full_history_requests.lock().unwrap();
            wallets[..4].iter().all(|w| {
                requests
                    .iter()
                    .filter(|requested| *requested == &w.to_string())
                    .count()
                    == 4
            })
        })
        .await;
        let requests = state.full_history_requests.lock().unwrap().clone();
        for w in &wallets {
            assert_eq!(
                requests
                    .iter()
                    .filter(|requested| *requested == &w.to_string())
                    .count(),
                if w == &wallet(5) { 3 } else { 4 },
                "{requests:?}"
            );
            assert_eq!(
                paper.position_anchors(w).unwrap().len(),
                2,
                "each boot bracket was accepted"
            );
        }
        for w in &wallets[..4] {
            assert!(!paper.wallet_history_complete(w).unwrap());
        }
        assert!(paper.wallet_history_complete(&wallet(5)).unwrap());
        until(|| {
            std::fs::read(&cfg.status_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|status| {
                    status["watchlist_size"] == 1
                        // Readiness can respond before startup reaches its signal loop.
                        // Observe running status ticks before requesting graceful shutdown.
                        && status["uptime_secs"].as_u64().is_some_and(|uptime| uptime >= 2)
                })
        })
        .await;
        state.slow.add_permits(4);
        child.stop().await;
        server.abort();
        return;
    }
    if scope_drops {
        let config_path = root.join("service.toml");
        std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
        let child = Child::start(&config_path);
        let client = reqwest::Client::new();
        until(|| paper.scope_drop_batch_imported(1).unwrap()).await;
        until(|| {
            std::fs::read(&cfg.status_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|status| {
                    status["uptime_secs"]
                        .as_u64()
                        .is_some_and(|uptime| uptime >= 2)
                })
        })
        .await;
        assert!(
            client
                .get(format!("http://{bind}/health/ready"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        // PASS: wallet 3 stays admitted with one local unrepresentable_activity_row drop and no
        // executable anomaly output; its later healthy BUY fills. The anomaly page is recorded;
        // restart preserves captured state, and sealed CLI replay reproduces five decisions/fills.
        // FAIL: wallet deferral/fencing, an anomaly decision/fill, missing healthy copy, state
        // drift after restart, or an inexact/unsuccessful CLI replay.
        let anomaly_epoch = OffsetDateTime::now_utc().unix_timestamp();
        let mut unrepresentable = row(wallet(3), 7, anomaly_epoch, 3007);
        unrepresentable["price"] = "3.1968021978".into();
        state
            .activity
            .lock()
            .unwrap()
            .insert(wallet(3).to_string(), vec![unrepresentable.clone()]);
        until(|| {
            !paper
                .wallet_scope_drops(&wallet(3), anomaly_epoch)
                .unwrap()
                .is_empty()
        })
        .await;
        let local = paper.wallet_scope_drops(&wallet(3), anomaly_epoch).unwrap();
        assert_eq!(local.len(), 1);
        // The v2 fake serves the row under v2 field names in a single terminal cursor page.
        let served_row = data_api_v2::activity_row(&unrepresentable);
        let served_page = json!({"data": [served_row.clone()],
            "pagination": {"limit": state.activity_page_size_cap.unwrap_or(1000),
                "offset": 0, "has_more": false, "next_cursor": null}});
        let raw_row_hash = blake3::hash(&serde_json::to_vec(&served_row).unwrap())
            .to_hex()
            .to_string();
        let unrepresentable_id = pe_core_types::SourceTradeId(format!(
            "{}{raw_row_hash}",
            pe_position_ledger::ACTIVITY_ROW_ANOMALY_NAMESPACE
        ));
        assert_eq!(local[0].scope_kind, "market");
        assert_eq!(local[0].scope_id, condition(7));
        assert_eq!(local[0].cause, "unrepresentable_activity_row");
        assert_eq!(local[0].dropped_at_epoch, anomaly_epoch);
        assert_eq!(local[0].source_trade_id.as_ref(), Some(&unrepresentable_id));
        assert_eq!(
            serde_json::from_str::<Value>(&local[0].proof_json).unwrap(),
            json!({
                "page_raw_hash": blake3::hash(&serde_json::to_vec(&served_page).unwrap()).to_hex().to_string(),
                "page_row_index": 0,
                "raw_row_hash": raw_row_hash,
                "kind": "price_out_of_range"
            })
        );
        assert!(paper.position_validation(&wallet(3)).unwrap().is_some());
        assert!(paper.wallet_history_complete(&wallet(3)).unwrap());
        assert!(!paper.is_wallet_fenced(&wallet(3)).unwrap());
        assert!(
            state
                .authority
                .lock()
                .unwrap()
                .projection
                .iter()
                .any(|row| { row["wallet_hex"] == wallet(3).to_string() })
        );
        assert!(
            paper
                .activity_groups_after(&wallet(3), 0)
                .unwrap()
                .is_empty()
        );
        assert!(paper.decision_pending_history().unwrap().is_empty());
        assert!(paper.list_fills().unwrap().is_empty());
        let epoch = OffsetDateTime::now_utc().unix_timestamp();
        *state.scope_drops.lock().unwrap() = vec![json!({
            "batch_id":2,"wallet_hex":wallet(1),"scope_kind":"market","scope_id":condition(1),
            "cause":"underflow","dropped_at_unix":epoch - 1
        })];
        *state.group_spans.lock().unwrap() =
            vec![json!({"group_id":condition(90),"from_batch_id":2,"to_batch_id":null})];
        state.ranking_batch.store(2, Ordering::SeqCst);
        until(|| paper.scope_drop_batch_imported(2).unwrap()).await;
        assert_eq!(
            paper.ranker_group_snapshot().unwrap(),
            (Some(2), std::collections::BTreeSet::from([condition(90)]))
        );
        let dropped = row(wallet(1), 1, epoch, 1001);
        let id = |row: &Value| {
            pe_source_polymarket_public::parse_activity_trade_observation(
                &serde_json::to_vec(row).unwrap(),
            )
            .unwrap()
            .group_id
            .key()
            .clone()
        };
        let dropped_id = id(&dropped);
        state
            .activity
            .lock()
            .unwrap()
            .insert(wallet(1).to_string(), vec![dropped]);
        until(|| paper.activity_group_state(&dropped_id).unwrap().is_some()).await;
        assert_eq!(
            paper
                .activity_group_state(&dropped_id)
                .unwrap()
                .unwrap()
                .disposition,
            "scope_dropped"
        );
        until(|| OffsetDateTime::now_utc().unix_timestamp() > epoch).await;
        let epoch = OffsetDateTime::now_utc().unix_timestamp();
        let healthy = row(wallet(1), 2, epoch, 1002);
        state
            .activity
            .lock()
            .unwrap()
            .get_mut(&wallet(1).to_string())
            .unwrap()
            .push(healthy);
        eprintln!("scope drops: waiting for first healthy buy");
        until(|| paper.list_fills().unwrap().len() == 1).await;
        let mut anomaly = row(wallet(2), 3, epoch, 2003);
        anomaly["type"] = "MERGE".into();
        anomaly["asset"] = "".into();
        anomaly["side"] = "".into();
        anomaly["outcomeIndex"] = 999.into();
        anomaly["outcome"] = "".into();
        anomaly["price"] = "1".into();
        anomaly["usdcSize"] = "0".into();
        let anomaly_read = crate::support::producer_shaped_read_v2(
            wallet(2),
            &serde_json::to_vec(&vec![anomaly.clone()]).unwrap(),
            epoch,
            epoch,
            crate::support::scenario_receipt(1),
        );
        let anomaly_id = anomaly_read.aggregates[0].group_id.key().clone();
        state
            .activity
            .lock()
            .unwrap()
            .insert(wallet(2).to_string(), vec![anomaly]);
        eprintln!("scope drops: waiting for local merge underflow");
        until(|| {
            !paper
                .wallet_scope_drops(&wallet(2), epoch)
                .unwrap()
                .is_empty()
        })
        .await;
        let local = paper.wallet_scope_drops(&wallet(2), epoch).unwrap();
        assert_eq!(local.len(), 1);
        assert_eq!(
            (
                &*local[0].scope_id,
                &*local[0].cause,
                local[0].source_trade_id.as_ref()
            ),
            (
                condition(3).as_str(),
                "position_underflow",
                Some(&anomaly_id)
            )
        );
        assert!(!paper.is_wallet_fenced(&wallet(2)).unwrap());
        until(|| OffsetDateTime::now_utc().unix_timestamp() > epoch).await;
        let epoch = OffsetDateTime::now_utc().unix_timestamp();
        let excluded = row(wallet(2), 3, epoch, 2013);
        let excluded_id = id(&excluded);
        state
            .activity
            .lock()
            .unwrap()
            .get_mut(&wallet(2).to_string())
            .unwrap()
            .extend([excluded, row(wallet(2), 4, epoch, 2004)]);
        eprintln!("scope drops: waiting for local healthy buy");
        until(|| paper.list_fills().unwrap().len() == 2).await;
        assert_eq!(
            paper
                .activity_group_state(&excluded_id)
                .unwrap()
                .unwrap()
                .disposition,
            "scope_dropped"
        );
        until(|| OffsetDateTime::now_utc().unix_timestamp() > epoch).await;
        let epoch = OffsetDateTime::now_utc().unix_timestamp();
        let original = row(wallet(1), 5, epoch, 1005);
        let original_id = id(&original);
        state
            .activity
            .lock()
            .unwrap()
            .get_mut(&wallet(1).to_string())
            .unwrap()
            .push(original.clone());
        eprintln!("scope drops: waiting for original copied buy");
        until(|| paper.list_fills().unwrap().len() == 3).await;
        until(|| OffsetDateTime::now_utc().unix_timestamp() > epoch).await;
        let later = OffsetDateTime::now_utc().unix_timestamp();
        let mut revised = original;
        revised["size"] = "101".into();
        revised["usdcSize"] = "50.5".into();
        let revised_read = crate::support::producer_shaped_read_v2(
            wallet(1),
            &serde_json::to_vec(&vec![revised.clone()]).unwrap(),
            epoch,
            later,
            crate::support::scenario_receipt(1),
        );
        let revision = revised_read.aggregates[0]
            .semantic_revision
            .as_str()
            .to_owned();
        {
            let mut activity = state.activity.lock().unwrap();
            let rows = activity.get_mut(&wallet(1).to_string()).unwrap();
            *rows.iter_mut().find(|row| id(row) == original_id).unwrap() = revised;
            rows.push(row(wallet(1), 6, later, 1006));
        }
        eprintln!("scope drops: waiting for revision and later buy");
        until(|| paper.list_fills().unwrap().len() == 4).await;
        assert!(
            paper
                .activity_revision_retained_without_fence(&original_id, &revision)
                .unwrap()
        );
        until(|| OffsetDateTime::now_utc().unix_timestamp() > later).await;
        let healthy_epoch = OffsetDateTime::now_utc().unix_timestamp();
        let healthy = row(wallet(3), 8, healthy_epoch, 3008);
        let healthy_id = id(&healthy);
        state
            .activity
            .lock()
            .unwrap()
            .get_mut(&wallet(3).to_string())
            .unwrap()
            .push(healthy);
        eprintln!("scope drops: waiting for unrepresentable-row wallet's healthy buy");
        until(|| paper.list_fills().unwrap().len() == 5).await;
        assert!(paper.list_fills().unwrap().iter().any(|fill| {
            fill.market_id == MarketId(VenueMarketId(condition(8))) && fill.side == Side::Buy
        }));
        let logs = child.stop().await;
        assert!(logs.contains("ranking scope drops imported"), "{logs}");
        assert!(
            pe_event_log::Reader::replay(&cfg.source_event_log_path)
                .unwrap()
                .any(|frame| {
                    let (_, envelope) = frame.unwrap();
                    envelope.source_id.0 == pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID
                        && envelope.schema_version
                            == pe_service::trade_poller::ACTIVITY_POLL_V2_PAGE_SCHEMA_VERSION
                        && serde_json::from_slice::<Value>(&envelope.payload).unwrap()["data"]
                            .as_array()
                            .is_some_and(|rows| {
                                rows.contains(&data_api_v2::activity_row(&unrepresentable))
                            })
                })
        );
        let financial = paper.financial_snapshot(now + 1000).unwrap();
        let history = paper.gate_history().unwrap();
        let decisions = paper.decision_pending_history().unwrap();
        assert_v2_committed_reads(&paper, &cfg.source_event_log_path);
        let fills = paper.list_fills().unwrap();
        assert!(
            decisions
                .iter()
                .any(|row| row.source_trade_id == healthy_id)
        );
        assert!(
            decisions
                .iter()
                .all(|row| row.source_trade_id != unrepresentable_id)
        );
        let groups = wallets
            .iter()
            .map(|w| paper.activity_groups_after(w, 0).unwrap())
            .collect::<Vec<_>>();
        let drops = wallets
            .iter()
            .map(|w| paper.wallet_scope_drops(w, later).unwrap())
            .collect::<Vec<_>>();
        let coverage = wallets
            .iter()
            .map(|w| paper.wallet_coverage(w).unwrap())
            .collect::<Vec<_>>();
        let imports = state.import_reads.lock().unwrap().clone();
        assert!(
            decisions
                .iter()
                .all(|row| row.state == pe_paper_state::DecisionPendingState::Terminal)
        );
        for w in &wallets {
            assert!(!paper.is_wallet_fenced(w).unwrap());
            assert!(!paper.wallet_coverage(w).unwrap().reanchor_required);
        }
        *state.positions.lock().unwrap() = paper.leader_positions().unwrap().into_iter().map(|position| {
            let market = usize::from_str_radix(position.market_id.0.0.trim_start_matches("0x"), 16).unwrap();
            json!({"proxyWallet":position.wallet,"conditionId":position.market_id.0.0,
                "asset":(market*2+101+usize::from(position.outcome_id.0)).to_string(),
                "outcomeIndex":position.outcome_id.0,"size":position.long_contracts.to_decimal().to_string(),
                "negativeRisk":false,"redeemable":false})
        }).collect();
        std::fs::remove_file(&cfg.status_path).unwrap();
        let child = Child::start(&config_path);
        until(|| {
            std::fs::read(&cfg.status_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|status| {
                    status["uptime_secs"]
                        .as_u64()
                        .is_some_and(|uptime| uptime >= 2)
                })
        })
        .await;
        assert!(
            client
                .get(format!("http://{bind}/health/ready"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let restart_logs = child.stop().await;
        assert!(
            restart_logs.contains("source checkpoint verification completed"),
            "{restart_logs}"
        );
        assert!(
            restart_logs.contains("open decision continuations validated"),
            "{restart_logs}"
        );
        assert_eq!(paper.financial_snapshot(now + 1000).unwrap(), financial);
        assert_eq!(paper.gate_history().unwrap(), history);
        assert_eq!(paper.decision_pending_history().unwrap(), decisions);
        assert_v2_committed_reads(&paper, &cfg.source_event_log_path);
        assert_eq!(paper.list_fills().unwrap(), fills);
        assert_eq!(*state.import_reads.lock().unwrap(), imports);
        assert!(paper.scope_drop_batch_imported(1).unwrap());
        assert!(paper.scope_drop_batch_imported(2).unwrap());
        assert_eq!(
            paper.ranker_group_snapshot().unwrap(),
            (Some(2), std::collections::BTreeSet::from([condition(90)]))
        );
        assert_eq!(
            state.authority.lock().unwrap().projection.len(),
            wallets.len()
        );
        for (i, w) in wallets.iter().enumerate() {
            assert_eq!(paper.activity_groups_after(w, 0).unwrap(), groups[i]);
            assert_eq!(paper.wallet_scope_drops(w, later).unwrap(), drops[i]);
            assert_eq!(paper.wallet_coverage(w).unwrap(), coverage[i]);
            assert!(!paper.is_wallet_fenced(w).unwrap());
        }
        seal_and_replay(&cfg, &paper, now, 5).await;
        server.abort();
        return;
    }
    let c = rusqlite::Connection::open(&cfg.paper_state_db_path).unwrap();
    for (n, cause) in [
        (2, "position_underflow"),
        (3, "conversion_requires_reanchor"),
    ] {
        let payload = serde_json::to_vec(&vec![row(wallet(n), n, now - 20, n * 100)]).unwrap();
        let receipt = Writer::open(&cfg.source_event_log_path)
            .unwrap()
            .append_synced(envelope(
                payload.clone(),
                pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                now - 1,
                2,
                1,
            ))
            .unwrap();
        let read =
            crate::support::producer_shaped_read_v2(wallet(n), &payload, now - 1, now - 1, receipt);
        let original = &read.aggregates[0];
        let effect = if n == 2 {
            pe_position_ledger::LedgerEffect::Trade {
                market_id: MarketId(VenueMarketId(condition(n))),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                amount: ShareAmount::from_whole(100).unwrap(),
                price: Price::new(dec!(0.5)).unwrap(),
            }
        } else {
            pe_position_ledger::LedgerEffect::Conversion
        };
        paper
            .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
                scope_drops: Vec::new(),
                observation_retirements: Vec::new(),
                unfenced_revisions: Vec::new(),
                scope_drop_boundary_unix: None,
                wallet: wallet(n),
                source_epoch: now - 20,
                dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                    source_trade_id: original.group_id.key().clone(),
                    transaction_hash: original.group_id.components().transaction_hash.clone(),
                    wallet: wallet(n),
                    source_epoch: now - 20,
                    semantic_revision: original.semantic_revision.as_str().to_owned(),
                    activity_type: "TRADE".to_owned(),
                    disposition: "wallet_fenced".to_owned(),
                    proof_json: effect.to_document().unwrap(),
                    no_copy: None,
                }],
                leader_positions: vec![],
                gate_results: vec![],
                history_effects: vec![],
                history_status: None,
                pending: vec![],
                fence: Some(pe_paper_state::WalletFenceRecord {
                    wallet: wallet(n),
                    source_trade_id: original.group_id.key().clone(),
                    cause: cause.to_owned(),
                    proof_json: json!({"bucket_epoch":now-20}).to_string(),
                    fenced_at_unix: now - 1,
                }),
                reanchor: None,
                advance_cursor: false,
            })
            .unwrap();
        c.execute(
            "UPDATE wallet_history_status_v2 SET complete=0 WHERE wallet_hex=?1",
            [wallet(n).to_string()],
        )
        .unwrap();
    }
    c.execute(
        "UPDATE poll_cursors SET reanchor_required=1 WHERE wallet_hex=?1",
        rusqlite::params![wallet(4).to_string()],
    )
    .unwrap();
    // Wallet 2's pinned ranking timestamp turns stale: only its recorded activity re-admits it.
    assert_eq!(
        c.execute(
            "UPDATE poll_cursors SET last_activity_unix=?2 WHERE wallet_hex=?1",
            rusqlite::params![wallet(2).to_string(), now - 20],
        )
        .unwrap(),
        1
    );
    state.stale_ranked_wallet_2.store(true, Ordering::SeqCst);
    drop(c);
    let config_path = root.join("service.toml");
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
    let child = Child::start(&config_path);
    tokio::time::timeout(Duration::from_secs(30), state.slow_started.notified())
        .await
        .unwrap();
    let epoch = OffsetDateTime::now_utc().unix_timestamp();
    state
        .activity
        .lock()
        .unwrap()
        .insert(wallet(1).to_string(), vec![row(wallet(1), 1, epoch, 1001)]);
    until(|| paper.list_fills().unwrap().len() == 1).await;
    until(|| state.gamma_failures.load(Ordering::SeqCst) > 0).await;
    assert!(
        !state
            .authority
            .lock()
            .unwrap()
            .projection
            .iter()
            .any(|row| row["wallet_hex"] == wallet(4).to_string())
    );
    assert!(paper.is_wallet_fenced(&wallet(2)).unwrap());
    assert!(paper.is_wallet_fenced(&wallet(3)).unwrap());
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .arg("--prepare-source-checkpoint")
        .arg("--paper-state")
        .arg(&cfg.paper_state_db_path);
    let prepared = crate::support::bounded_command_output(command).await;
    assert!(prepared.status.success(), "{prepared:?}");
    let prepared_tail = Scanner::inspect(&cfg.source_event_log_path)
        .unwrap()
        .verified_tail
        .physical_tail;
    state.release_slow.store(true, Ordering::SeqCst);
    state.slow.add_permits(4);
    until(|| !paper.is_wallet_fenced(&wallet(2)).unwrap()).await;
    until(|| {
        let a = state.authority.lock().unwrap();
        a.projection.len() == 2
            && !a
                .projection
                .iter()
                .any(|row| row["wallet_hex"] == wallet(4).to_string())
    })
    .await;
    assert!(paper.wallet_history_complete(&wallet(2)).unwrap());
    assert!(
        paper.gate_history().unwrap()[&wallet(2)].contains(&MarketId(VenueMarketId(condition(2))))
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let epoch = OffsetDateTime::now_utc().unix_timestamp();
    state
        .activity
        .lock()
        .unwrap()
        .get_mut(&wallet(2).to_string())
        .unwrap()
        .push(row(wallet(2), 2, epoch, 2002));
    until(|| {
        !paper.decision_pending_history().unwrap().is_empty()
            && paper
                .cursor(&wallet(2))
                .unwrap()
                .is_some_and(|v| v >= epoch)
    })
    .await;
    assert_eq!(paper.list_fills().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let epoch = OffsetDateTime::now_utc().unix_timestamp();
    state
        .activity
        .lock()
        .unwrap()
        .get_mut(&wallet(2).to_string())
        .unwrap()
        .push(row(wallet(2), 5, epoch, 2005));
    until(|| paper.list_fills().unwrap().len() == 2).await;
    let before = paper.financial_snapshot(now + 1000).unwrap();
    let qty = before
        .positions
        .iter()
        .find(|p| p.market_id.0.0 == condition(5))
        .unwrap()
        .long;
    state.resolved.store(true, Ordering::SeqCst);
    until(|| {
        paper
            .financial_snapshot(now + 1000)
            .unwrap()
            .settlements_7d
            .len()
            == 1
    })
    .await;
    let settled = paper
        .financial_snapshot(OffsetDateTime::now_utc().unix_timestamp())
        .unwrap();
    assert_eq!(settled.cash, before.cash + qty.to_decimal());
    assert!(
        settled
            .positions
            .iter()
            .all(|p| p.market_id.0.0 != condition(5))
    );
    assert_eq!(state.authority.lock().unwrap().settlements.len(), 1);
    let first_logs = child.stop().await;
    assert!(first_logs.contains("source checkpoint"));
    let financial = paper.financial_snapshot(now + 1000).unwrap();
    let history = paper.gate_history().unwrap();
    let ledger = pe_service::paper_recovery::build_leader_ledger(&paper)
        .unwrap()
        .snapshots()
        .clone();
    let decisions = paper.decision_pending_history().unwrap();
    assert!(!decisions.is_empty());
    assert_v2_committed_reads(&paper, &cfg.source_event_log_path);
    // Re-prepare against completed state, then force one wallet's boot walk to fail locally.
    SourceLogBoot::prepare_checkpoint(&cfg.paper_state_db_path).unwrap();
    Writer::open(&cfg.source_event_log_path)
        .unwrap()
        .append_synced(envelope(b"{}".to_vec(), "rollout.suffix", now, 1, 1))
        .unwrap();
    assert!(
        Scanner::verify(&cfg.source_event_log_path)
            .unwrap()
            .physical_tail
            > prepared_tail
    );
    let mut corrupt = row(wallet(1), 1, now - 20, 101);
    corrupt["size"] = json!("-1");
    state
        .activity
        .lock()
        .unwrap()
        .get_mut(&wallet(1).to_string())
        .unwrap()
        .push(corrupt);
    rusqlite::Connection::open(&cfg.paper_state_db_path)
        .unwrap()
        .execute(
            "UPDATE poll_cursors SET reanchor_required=1 WHERE wallet_hex IN (?1, ?2)",
            rusqlite::params![wallet(1).to_string(), wallet(2).to_string()],
        )
        .unwrap();
    // Both wallets require reanchoring, so boot validates both wallets. The healthy venue mirror
    // matches its recorded balance; only the negative-size wallet is left unvalidated.
    *state.positions.lock().unwrap() = paper
        .leader_positions()
        .unwrap()
        .into_iter()
        .filter(|position| position.wallet == wallet(2))
        .map(|position| {
            let market =
                usize::from_str_radix(position.market_id.0.0.trim_start_matches("0x"), 16).unwrap();
            assert_eq!(position.short_contracts, ShareAmount::ZERO);
            json!({
                "proxyWallet": position.wallet,
                "conditionId": position.market_id.0.0,
                "asset": (market * 2 + 101 + usize::from(position.outcome_id.0)).to_string(),
                "outcomeIndex": position.outcome_id.0,
                "size": position.long_contracts.to_decimal().to_string(),
                "negativeRisk": false,
                "redeemable": market == 5,
            })
        })
        .collect();
    state.gamma_fails.store(false, Ordering::SeqCst);
    let child = Child::start(&config_path);
    until(|| {
        paper
            .wallet_coverage(&wallet(4))
            .unwrap()
            .anchored_at_unix
            .is_some_and(|at| at >= now)
    })
    .await;
    until(|| {
        let a = state.authority.lock().unwrap();
        a.projection.len() == 2
            && !a.projection.iter().any(|row| {
                row["wallet_hex"] == wallet(1).to_string()
                    || row["wallet_hex"] == wallet(3).to_string()
            })
    })
    .await;
    assert!(!paper.is_wallet_fenced(&wallet(1)).unwrap());
    assert!(paper.is_wallet_fenced(&wallet(3)).unwrap());
    assert!(!paper.is_wallet_fenced(&wallet(2)).unwrap());
    assert_eq!(paper.financial_snapshot(now + 1000).unwrap(), financial);
    for w in [wallet(1), wallet(2), wallet(3)] {
        assert_eq!(paper.gate_history().unwrap().get(&w), history.get(&w));
    }
    assert_eq!(paper.decision_pending_history().unwrap(), decisions);
    assert_v2_committed_reads(&paper, &cfg.source_event_log_path);
    assert_eq!(state.authority.lock().unwrap().settlements.len(), 1);
    for w in [wallet(1), wallet(2), wallet(3)] {
        assert_eq!(
            pe_service::paper_recovery::build_leader_ledger(&paper)
                .unwrap()
                .position(&w),
            ledger.get(&w)
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let epoch = OffsetDateTime::now_utc().unix_timestamp();
    let copying = row(wallet(2), 6, epoch, 2006);
    let copying_id = pe_source_polymarket_public::parse_activity_trade_observation(
        &serde_json::to_vec(&copying).unwrap(),
    )
    .unwrap()
    .group_id
    .key()
    .clone();
    state
        .activity
        .lock()
        .unwrap()
        .get_mut(&wallet(2).to_string())
        .unwrap()
        .push(copying);
    until(|| paper.list_fills().unwrap().len() == 3).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let second_logs = child.stop().await;
    assert!(
        second_logs.contains("open decision continuations validated"),
        "{second_logs}"
    );
    assert!(
        second_logs.contains("\"checkpoint_used\":true"),
        "{second_logs}"
    );
    assert!(
        second_logs.contains("boot bracket: wallet left unvalidated for runtime admission"),
        "{second_logs}"
    );
    assert!(
        second_logs.contains("invalid share amount"),
        "{second_logs}"
    );
    let fills = paper.list_fills().unwrap();
    assert_eq!(fills.len(), 3);
    let copied = fills
        .iter()
        .filter(|fill| fill.market_id.0.0 == condition(6))
        .collect::<Vec<_>>();
    assert_eq!(
        copied.len(),
        1,
        "the healthy wallet copies once across repeated polls"
    );
    assert!(copied[0].quantity > ShareAmount::ZERO);
    let era = paper_era(scan_paper_log(&cfg.event_log_path).unwrap());
    let prepared = era.frames.iter().filter(|frame| matches!(
        &frame.frame,
        pe_service::paper_recovery::PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            payload: FinancialPayload::Fill { operation, .. }, ..
        }) if operation.leader_wallet == wallet(2) && operation.source_trade_id == copying_id
    )).collect::<Vec<_>>();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].receipt.sequence, copied[0].prepared_seq);
    assert_eq!(era.frames.iter().filter(|frame| matches!(
        &frame.frame,
        pe_service::paper_recovery::PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: pe_service::paper_recovery::FinancialResult::Fill { canonical },
        }) if *prepared_receipt == prepared[0].receipt && canonical.outcome == "applied" && canonical.quantity == copied[0].quantity
    )).count(), 1, "the unique fill has a matching durable financial Final");
    let entries = crate::golden::BracketFinancialHarness::entries(&wallets);
    let initial = pe_trader_index::Watchlist {
        active_count: entries.len(),
        incubator_count: 0,
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
    };
    let restored = replay_membership(&era, initial, &cfg.source_event_log_path)
        .unwrap()
        .unwrap();
    assert_eq!(
        restored
            .watchlist
            .entries
            .iter()
            .map(|e| e.wallet)
            .collect::<std::collections::HashSet<_>>(),
        wallets.into_iter().collect()
    );
    assert_eq!(
        restored
            .watchlist
            .entries
            .iter()
            .filter(|e| paper.wallet_history_complete(&e.wallet).unwrap()
                && !paper.is_wallet_fenced(&e.wallet).unwrap())
            .count(),
        3
    );
    assert_eq!(
        era.frames
            .iter()
            .filter(|f| matches!(
                &f.frame,
                pe_service::paper_recovery::PaperLogFrame::Record(
                    PaperLogRecord::FinancialPrepared {
                        payload: FinancialPayload::Resolution { .. },
                        ..
                    }
                )
            ))
            .count(),
        1
    );
    assert!(
        MigrationMetadata::read(&cfg.paper_state_db_path)
            .unwrap()
            .is_some()
    );
    server.abort();
}

async fn run_waiting_admission(
    cfg: &ServiceConfig,
    paper: &PaperStateDb,
    state: &Arc<HttpState>,
    initial_wallets: &[WalletAddress],
) {
    use std::sync::atomic::Ordering;
    let root = cfg.event_log_path.parent().unwrap();
    let newcomer = state.newcomer.unwrap();
    let refreshing = wallet(2);
    let original = paper.wallet_coverage(&refreshing).unwrap();
    assert!(!original.reanchor_required);
    assert!(pe_service::watchlist_admission::anchor_refresh_due(
        &original,
        state.now,
        pe_service::trade_poller::ANCHOR_REFRESH_SECS,
    ));
    assert!(paper.position_anchors(&newcomer).unwrap().is_empty());
    let config_path = root.join("service.toml");
    std::fs::write(&config_path, toml::to_string(cfg).unwrap()).unwrap();
    let child = Child::start(&config_path);
    tokio::time::timeout(Duration::from_secs(5), state.slow_started.notified())
        .await
        .expect("the age-due routine refresh must start immediately after boot");
    assert_eq!(paper.wallet_coverage(&refreshing).unwrap(), original);
    assert_eq!(
        *state.full_history_requests.lock().unwrap(),
        vec![refreshing.to_string()],
        "boot reuses its anchors; the held request is the routine refresh baseline"
    );
    until(|| state.authority.lock().unwrap().projection.len() == initial_wallets.len()).await;
    assert!(
        state
            .authority
            .lock()
            .unwrap()
            .projection
            .iter()
            .all(|row| {
                initial_wallets
                    .iter()
                    .any(|wallet| row["wallet_hex"] == wallet.to_string())
            })
    );
    state.ranking_batch.store(2, Ordering::SeqCst);
    // This bound is shorter than the held HTTP request's timeout: a failed refresh cannot
    // release the lock in place of the poller's explicit admission yield.
    tokio::time::timeout(Duration::from_secs(8), async {
        until(|| service_logs(root).contains("routine refresh yielded to a waiting admission"))
            .await;
        until(|| {
            state
                .authority
                .lock()
                .unwrap()
                .projection
                .iter()
                .any(|row| row["wallet_hex"] == newcomer.to_string())
        })
        .await;
        assert!(paper.wallet_history_complete(&newcomer).unwrap());
        assert!(!paper.is_wallet_fenced(&newcomer).unwrap());
        let anchors = paper.position_anchors(&newcomer).unwrap();
        assert_eq!(anchors.len(), 1);
        let proof: Value = serde_json::from_str(&anchors[0].proof_json).unwrap();
        assert!(pe_service::position_seeder::anchor_proves_full_history(
            &anchors[0].proof_json
        ));
        let end = proof["baseline_walk"]["fixed_end"].as_i64().unwrap();
        let walks = proof["activity_walks"].as_array().unwrap();
        assert_eq!(walks.len(), 3);
        let requests = state
            .activity_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|query| query["user"] == newcomer.to_string())
            .cloned()
            .collect::<Vec<_>>();
        assert!(requests.len() >= 4, "{requests:?}");
        assert_eq!(requests[0]["start"], "1");
        assert_eq!(requests[0]["end"], end.to_string());
        for (query, walk) in requests[1..4].iter().zip(walks) {
            let start = end - pe_service::position_seeder::REENTRY_HISTORY_OVERLAP_SECS;
            assert_eq!(query["start"], (start + 1).to_string());
            assert_eq!(
                query["end"],
                walk["fixed_end"].as_i64().unwrap().to_string()
            );
            assert_eq!(walk["pages"].as_array().unwrap().len(), 1);
            assert_eq!(walk["pages"][0]["bounds"]["start"], start);
        }
        assert_eq!(
            state
                .full_history_requests
                .lock()
                .unwrap()
                .iter()
                .filter(|wallet| **wallet == newcomer.to_string())
                .count(),
            1
        );
        until(|| OffsetDateTime::now_utc().unix_timestamp() > anchors[0].activity_cutoff_unix)
            .await;
        let epoch = OffsetDateTime::now_utc().unix_timestamp();
        state
            .activity
            .lock()
            .unwrap()
            .insert(newcomer.to_string(), vec![row(newcomer, 3, epoch, 3003)]);
        until(|| {
            paper.list_fills().unwrap().len() == 1
                && state.authority.lock().unwrap().fills.len() == 1
        })
        .await;
    })
    .await
    .expect("the newcomer must be admitted and copy while the routine refresh remains held");
    assert!(!state.release_slow.load(Ordering::SeqCst));
    assert_eq!(state.slow.available_permits(), 0);
    assert_eq!(paper.wallet_coverage(&refreshing).unwrap(), original);
    let fills = paper.list_fills().unwrap();
    assert_eq!(fills[0].market_id.0.0, condition(3));
    assert_eq!(fills[0].side, Side::Buy);
    assert!(fills[0].quantity > ShareAmount::ZERO);
    assert_eq!(
        state
            .authority
            .lock()
            .unwrap()
            .fills
            .values()
            .next()
            .unwrap()["leader_wallet"],
        newcomer.to_string()
    );
    state.release_slow.store(true, Ordering::SeqCst);
    state.slow.add_permits(16);
    until(|| paper.wallet_coverage(&refreshing).unwrap().anchor_seq > original.anchor_seq).await;
    let refreshed = paper.position_anchors(&refreshing).unwrap();
    assert_eq!(
        refreshed.len(),
        2,
        "only the retried refresh installs an anchor"
    );
    assert!(
        state
            .full_history_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|wallet| **wallet == refreshing.to_string())
            .count()
            >= 2,
        "the routine refresh retries its cancelled baseline"
    );
    let proof: Value = serde_json::from_str(&refreshed[1].proof_json).unwrap();
    assert!(proof["baseline_walk"]["fixed_end"].as_i64().unwrap() >= state.now);
    assert_eq!(proof["activity_walks"].as_array().unwrap().len(), 3);
    assert!(
        !paper
            .wallet_coverage(&refreshing)
            .unwrap()
            .reanchor_required
    );
    until(|| {
        paper
            .decision_pending_history()
            .unwrap()
            .iter()
            .all(|row| row.state == pe_paper_state::DecisionPendingState::Terminal)
    })
    .await;
    let logs = child.stop().await;
    assert!(
        logs.contains("routine refresh yielded to a waiting admission"),
        "{logs}"
    );
    let census = service_logs(root)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|line| line["message"] == "boot anchor selection census")
        .unwrap();
    assert_eq!(census["reused"], initial_wallets.len());
    assert_eq!(census["walked"], 0);
    let financial = paper.financial_snapshot(state.now + 1000).unwrap();
    let fills = paper.list_fills().unwrap();
    let history = paper.gate_history().unwrap();
    let decisions = paper.decision_pending_history().unwrap();
    let positions = paper.leader_positions().unwrap();
    let projection = state.authority.lock().unwrap().projection.clone();
    assert_eq!(projection.len(), initial_wallets.len() + 1);
    let restored_membership = || {
        let era = paper_era(scan_paper_log(&cfg.event_log_path).unwrap());
        let entries = crate::golden::BracketFinancialHarness::entries(initial_wallets);
        replay_membership(
            &era,
            pe_trader_index::Watchlist {
                active_count: entries.len(),
                incubator_count: 0,
                entries,
                snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            },
            &cfg.source_event_log_path,
        )
        .unwrap()
        .unwrap()
        .watchlist
        .entries
    };
    let membership = restored_membership();
    assert!(membership.iter().any(|entry| entry.wallet == newcomer));
    assert_eq!(membership.len(), initial_wallets.len() + 1);
    *state.positions.lock().unwrap() = positions.iter().map(|position| json!({
        "proxyWallet":position.wallet,"conditionId":position.market_id.0.0,"asset":"107",
        "outcomeIndex":position.outcome_id.0,"size":position.long_contracts.to_decimal().to_string(),
        "negativeRisk":false,"redeemable":false,
    })).collect();
    std::fs::remove_file(&cfg.status_path).unwrap();
    let child = Child::start(&config_path);
    until(|| {
        std::fs::read(&cfg.status_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .is_some_and(|status| {
                status["watchlist_size"] == membership.len()
                    && status["uptime_secs"]
                        .as_u64()
                        .is_some_and(|uptime| uptime >= 2)
            })
    })
    .await;
    let logs = child.stop().await;
    assert!(
        logs.contains("open decision continuations validated"),
        "{logs}"
    );
    assert_eq!(
        paper.financial_snapshot(state.now + 1000).unwrap(),
        financial
    );
    assert_eq!(paper.list_fills().unwrap(), fills);
    assert_eq!(paper.leader_positions().unwrap(), positions);
    assert_eq!(paper.gate_history().unwrap(), history);
    assert_eq!(paper.decision_pending_history().unwrap(), decisions);
    assert_eq!(
        serde_json::to_value(restored_membership()).unwrap(),
        serde_json::to_value(&membership).unwrap()
    );
    assert_eq!(state.authority.lock().unwrap().projection, projection);
    assert_eq!(state.authority.lock().unwrap().fills.len(), 1);
    let report = seal_and_replay(cfg, paper, state.now, 1).await;
    assert_eq!(report.replay.membership_changes, 1);
    assert_eq!(report.replay.final_membership_count, membership.len());
}

fn service_logs(root: &Path) -> String {
    let mut entries = std::fs::read_dir(root)
        .unwrap()
        .flatten()
        .collect::<Vec<_>>();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    entries
        .into_iter()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            (name.starts_with("service.") && name.ends_with(".jsonl") && !name.contains("error"))
                .then(|| std::fs::read_to_string(entry.path()).unwrap_or_default())
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn seal_and_replay(
    cfg: &ServiceConfig,
    paper: &PaperStateDb,
    now: i64,
    expected_fills: usize,
) -> pe_service::qualification::QualificationReport {
    let root = cfg.event_log_path.parent().unwrap();
    let start_receipt = paper_era(scan_paper_log(&cfg.event_log_path).unwrap())
        .start
        .unwrap()
        .0;
    let financial = paper.financial_snapshot(now + 1000).unwrap();
    let decisions = paper.decision_pending_history().unwrap();
    assert_eq!(decisions.len(), expected_fills);
    let cutoff = last_second_of_day(now) + 1;
    let mut source = Writer::open(&cfg.source_event_log_path).unwrap();
    let boundary_receipt = source
        .append_synced(envelope(
            serde_json::to_vec(&json!({"kind":"daily_boundary","cutoff_unix":cutoff})).unwrap(),
            "pe-service.boundary",
            cutoff,
            1,
            1,
        ))
        .unwrap();
    let price = Price::new(dec!(0.50)).unwrap();
    let prices = financial
        .positions
        .iter()
        .map(|position| {
            let receipt = source
                .append_synced(envelope(
                    serde_json::to_vec(&json!({"history":[{"t":cutoff,"p":"0.50"}]})).unwrap(),
                    "pe-service.clob-prices-history",
                    cutoff,
                    1,
                    1,
                ))
                .unwrap();
            pe_service::paper_recovery::PaperMarkPrice {
                market_id: position.market_id.0.0.clone(),
                outcome_id: position.outcome_id.0,
                price: Some(price),
                sample_unix: Some(cutoff),
                receipt: Some(receipt),
                closure_receipt: None,
                invalid: None,
            }
        })
        .collect();
    drop(source);
    let valued = financial
        .positions
        .iter()
        .map(|position| (position.long.checked_sub(position.short).unwrap(), price))
        .collect::<Vec<_>>();
    let mark = pe_service::paper_recovery::PortfolioMark {
        boundary_receipt,
        cutoff_unix: cutoff,
        source_tail: TailBinding::from(&Scanner::verify(&cfg.source_event_log_path).unwrap()),
        financial_prefix_seq: financial.last_prepared_seq,
        prices,
        cash: financial.cash,
        equity: pe_risk_engine::current_equity(&pe_risk_engine::EquityInputs {
            cash: financial.cash,
            positions: &valued,
        })
        .unwrap(),
        invalid: None,
    };
    Writer::open(&cfg.event_log_path)
        .unwrap()
        .append_synced(envelope(
            serde_json::to_vec(&PaperLogRecord::PortfolioMark(Box::new(mark))).unwrap(),
            "pe-service.paper",
            cutoff,
            2,
            1,
        ))
        .unwrap();
    let source_prefix = TailBinding::from(&Scanner::verify(&cfg.source_event_log_path).unwrap());
    let keys = decisions
        .iter()
        .map(|row| (row.source_trade_id.clone(), row.semantic_revision.clone()))
        .collect::<Vec<_>>();
    let digest = paper
        .seal_decision_evidence_for_source_prefix(&keys, &keys, source_prefix.last_sequence)
        .unwrap();
    let seal = pe_service::paper_recovery::QualificationSealed {
        start_receipt,
        source_prefix,
        financial_prefix: TailBinding::from(&Scanner::verify(&cfg.event_log_path).unwrap()),
        live_prefix: TailBinding::from(
            &pe_execution_core::LiveJournal::verified_tail(root.join("live_journal.log")).unwrap(),
        ),
        decision_evidence_digest: blake3::hash(&digest).to_hex().to_string(),
        sealed_cutoff_unix: cutoff,
        reason: pe_service::paper_recovery::SealReason::Complete,
    };
    let receipt = Writer::open(&cfg.event_log_path)
        .unwrap()
        .append_synced(envelope(
            serde_json::to_vec(&PaperLogRecord::QualificationSealed(Box::new(seal))).unwrap(),
            "pe-service.paper",
            cutoff,
            2,
            1,
        ))
        .unwrap();
    let output_path = root.join("qualification.json");
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .args(["--qualify", "--paper-log"])
        .arg(&cfg.event_log_path)
        .arg("--source-log")
        .arg(&cfg.source_event_log_path)
        .arg("--live-journal")
        .arg(root.join("live_journal.log"))
        .arg("--paper-state")
        .arg(&cfg.paper_state_db_path)
        .arg("--seal-hash")
        .arg(receipt.this_hash.to_hex().as_str())
        .arg("--output")
        .arg(&output_path);
    let output = crate::support::bounded_command_output(command).await;
    assert!(output.status.success(), "{output:?}");
    let report: pe_service::qualification::QualificationReport =
        serde_json::from_slice(&std::fs::read(output_path).unwrap()).unwrap();
    assert!(report.replay.exact, "{:?}", report.reasons);
    assert_eq!(report.replay.fills, expected_fills);
    assert_eq!(report.replay.decisions, decisions.len());
    assert_eq!(
        report.verdict,
        pe_service::qualification::QualificationVerdict::Fail,
        "{:?}",
        report.reasons
    );
    // Only the short run's thresholds and its intentionally open positions may fail; any other
    // financial gate failure (drawdown, a measured copy delay over budget) must surface here.
    let intended = [
        "complete days ",
        "closed copies ",
        "LCB_5pct is not positive",
        "paper p95 copy delay is absent or above budget",
        "paper positions remain open at the sealed financial prefix",
    ];
    assert_eq!(report.reasons.len(), intended.len(), "{:?}", report.reasons);
    for (reason, prefix) in report.reasons.iter().zip(intended) {
        assert!(reason.starts_with(prefix), "{:?}", report.reasons);
    }
    report
}

fn assert_v2_committed_reads(paper: &PaperStateDb, source_path: &Path) {
    use pe_service::bucket_commit::DecisionContinuationV3;
    use pe_service::trade_poller::{ACTIVITY_POLL_SOURCE_ID, ACTIVITY_POLL_V2_PAGE_SCHEMA_VERSION};
    use pe_source_polymarket_public::{
        ACTIVITY_V2_PAGE_SCHEMA_VERSION, ReconciliationPageEvidence,
    };

    let frames = pe_event_log::Reader::replay(source_path)
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .collect::<Vec<_>>();
    let mut multipage = false;
    let decisions = paper.decision_pending_history().unwrap();
    assert!(!decisions.is_empty());
    for decision in decisions {
        let continuation = DecisionContinuationV3::from_durable(&decision).unwrap();
        let pages: Vec<ReconciliationPageEvidence> =
            serde_json::from_value(continuation.facts.decision_inputs["pages"].clone()).unwrap();
        assert!(!pages.is_empty());
        assert_eq!(pages.len(), continuation.page_occurrences.len());
        multipage |= pages.len() > 1;
        for (index, occurrence) in continuation.page_occurrences.iter().enumerate() {
            let frame = frames
                .iter()
                .find(|frame| frame.seq == occurrence.receipt.sequence)
                .unwrap();
            assert_eq!(frame.this_hash, occurrence.receipt.this_hash);
            assert_eq!(frame.source_id.0, ACTIVITY_POLL_SOURCE_ID);
            assert_eq!(frame.schema_version, ACTIVITY_POLL_V2_PAGE_SCHEMA_VERSION);
            let page = pages
                .iter()
                .find(|page| {
                    page.raw_page_hash == occurrence.raw_hash
                        && page.request_url == occurrence.request_url
                })
                .unwrap();
            assert_eq!(page.schema_version, ACTIVITY_V2_PAGE_SCHEMA_VERSION);
            let url = reqwest::Url::parse(&occurrence.request_url).unwrap();
            assert_eq!(url.path(), "/v2/activity");
            let query = url.query_pairs().into_owned().collect::<HashMap<_, _>>();
            assert_eq!(
                query.get("limit").map(String::as_str),
                (index == 0).then_some("1000")
            );
            assert_eq!(query.contains_key("cursor"), index != 0);
            if index != 0 {
                let previous = &continuation.page_occurrences[index - 1];
                let previous = pages
                    .iter()
                    .find(|page| page.request_url == previous.request_url)
                    .unwrap();
                assert_eq!(query.get("cursor"), previous.next_cursor.as_ref());
            }
            let raw: Value = serde_json::from_slice(&frame.payload).unwrap();
            let pagination = &raw["pagination"];
            assert!(pagination.get("has_more").is_some());
            assert!(pagination.get("next_cursor").is_some());
            assert_eq!(page.has_more, pagination["has_more"].as_bool());
            assert_eq!(
                page.next_cursor.as_deref(),
                pagination["next_cursor"].as_str()
            );
            assert_eq!(page.has_more, Some(index + 1 < pages.len()));
        }
    }
    assert!(multipage, "the capped fake forces a committed cursor read");
}

/// Minimal real-process checkpoint fixture reusing the rollout's local financial authority.
pub struct CheckpointFixture {
    pub dir: tempfile::TempDir,
    pub cfg: ServiceConfig,
    pub config_path: std::path::PathBuf,
    pub paper: Arc<PaperStateDb>,
    state: Arc<HttpState>,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for CheckpointFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl CheckpointFixture {
    pub async fn new() -> Self {
        use std::sync::atomic::AtomicBool;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let now = fixture_now().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cfg = ServiceConfig {
            bind: "127.0.0.1:0".to_owned(),
            event_log_path: root.join("paper.log"),
            source_event_log_path: root.join("source.log"),
            paper_state_db_path: root.join("paper.db"),
            legacy_wallet_history_path: root.join("legacy.json"),
            jsonl_log_path: root.join("service.jsonl"),
            status_path: root.join("status.json"),
            polymarket_base_url: base.clone(),
            gamma_base_url: base.clone(),
            polymarket_clob_base_url: base.clone(),
            polygon_receipt_rpc_url: base.clone(),
            supabase_url: base,
            supabase_secret_key: "fixture".to_owned(),
            supabase_authoritative: true,
            bankroll_usd: "10000".to_owned(),
            maintenance_interval_secs: 3600,
            trade_poll_interval_secs: 1,
            gamma_resolution_poll_interval_secs: 3600,
            supabase_refresh_interval_secs: 3600,
            status_interval_secs: 1,
            ..ServiceConfig::default()
        };
        let mut runtime = RuntimeConfig::from_service_config(&cfg);
        runtime.sizing_mode = pe_strategy_winner_follow::SizingMode::Dollar { usd: dec!(5) };
        runtime.sizing_dollar_usd = dec!(5);
        let state = Arc::new(HttpState {
            boot_waves: false,
            ranking_batch: std::sync::atomic::AtomicI64::new(1),
            scope_drops: Mutex::new(Vec::new()),
            group_spans: Mutex::new(Vec::new()),
            import_reads: Mutex::new(Vec::new()),
            rows: crate::golden::golden_config_rows(&runtime),
            now,
            ranked_wallets: 1,
            newcomer: None,
            activity_requests: Mutex::new(Vec::new()),
            full_history_requests: Mutex::new(Vec::new()),
            cut_full_history_replies: std::sync::atomic::AtomicUsize::new(0),
            start: Mutex::new(None),
            authority: Mutex::new(Authority {
                cash: dec!(10000),
                ..Default::default()
            }),
            activity: Mutex::new(HashMap::new()),
            positions: Mutex::new(Vec::new()),
            data_api_v2: Mutex::new(Default::default()),
            activity_page_size_cap: None,
            slow_started: Notify::new(),
            slow: Semaphore::new(0),
            release_slow: AtomicBool::new(true),
            gamma_fails: AtomicBool::new(false),
            stale_ranked_wallet_2: AtomicBool::new(false),
            gamma_failures: std::sync::atomic::AtomicUsize::new(0),
            resolved: AtomicBool::new(false),
        });
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new().fallback(serve).with_state(state.clone()),
            )
            .into_future(),
        );
        rusqlite::Connection::open(&cfg.paper_state_db_path)
            .unwrap()
            .execute_batch(include_str!(
                "../../../paper-state/tests/fixtures/paper_state_v1.sql"
            ))
            .unwrap();
        drop(Writer::open(&cfg.event_log_path).unwrap());
        drop(Writer::open(&cfg.source_event_log_path).unwrap());
        drop(pe_execution_core::LiveJournal::open(root.join("live_journal.log")).unwrap());
        std::fs::write(&cfg.legacy_wallet_history_path, br#"{"wallets":[]}"#).unwrap();
        let paths = PaperMigrationPaths {
            fixed_main: cfg.paper_state_db_path.clone(),
            source_log: cfg.source_event_log_path.clone(),
            paper_log: cfg.event_log_path.clone(),
            live_journal: root.join("live_journal.log"),
            legacy_history: cfg.legacy_wallet_history_path.clone(),
            binary_identity: pe_service::build_info::embedded()
                .source_revision
                .to_owned(),
        };
        let boot = PaperMigrationBoot::prepare(paths.clone(), now).unwrap();
        let side = PaperStateDb::open(&boot.active_main).unwrap();
        side.record_migration_activation_facts(
            &json!({"fixture":"complete"}),
            &paths.binary_identity,
        )
        .unwrap();
        drop(side);
        boot.session.unwrap().finish().unwrap();
        let paper = Arc::new(PaperStateDb::open(&cfg.paper_state_db_path).unwrap());
        crate::support::install_full_history_anchor(&paper, wallet(1), now - 10);
        paper
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet: wallet(1),
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: now - 10,
            })
            .unwrap();
        let members = vec![wallet(1)];
        let start = QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10000)).unwrap(),
            paper_prefix: TailBinding::from(&Scanner::verify(&cfg.event_log_path).unwrap()),
            source_prefix: TailBinding::from(&Scanner::verify(&cfg.source_event_log_path).unwrap()),
            live_prefix: TailBinding::from(&Scanner::verify(&paths.live_journal).unwrap()),
            artifact_blake3: "a".repeat(64),
            static_config_hash: "b".repeat(64),
            hot_config_hash: runtime.canonical_hash(),
            generation: "checkpoint".to_owned(),
            activation_id: "checkpoint".to_owned(),
            ranking_batch_id: 1,
            membership: members.clone(),
            membership_proofs_hash: pe_service::qualification::scenario_membership_proofs_hash(
                &paper, &members,
            )
            .unwrap(),
            schema_version: 2,
            parser_version: 1,
            financial_semantic_version: pe_service::paper_recovery::FINANCIAL_SEMANTIC_VERSION,
        };
        let receipt = Writer::open(&cfg.event_log_path)
            .unwrap()
            .append_synced(envelope(
                serde_json::to_vec(&PaperLogRecord::QualificationStarted(Arc::new(start))).unwrap(),
                "pe-service.paper",
                now - 5,
                2,
                1,
            ))
            .unwrap();
        paper
            .reset_financial_era(
                receipt,
                CollateralAmount::from_decimal_exact(dec!(10000)).unwrap(),
                None,
            )
            .unwrap();
        *state.start.lock().unwrap() = Some(receipt);
        let config_path = root.join("service.toml");
        std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
        Self {
            dir,
            cfg,
            config_path,
            paper,
            state,
            server,
        }
    }
    /// Ordinary per-wallet records, including interrupted prefixes, are old-reader inputs.
    pub fn append_streamed_membership(
        &mut self,
        knockout: bool,
        newcomers: usize,
    ) -> Vec<WalletAddress> {
        use pe_service::paper_recovery::{MembershipChange, MembershipReason};
        let started = std::time::Instant::now();
        let paper_before = std::fs::metadata(&self.cfg.event_log_path).unwrap().len();
        let source_before = std::fs::metadata(&self.cfg.source_event_log_path)
            .unwrap()
            .len();
        let source_seq = Scanner::verify(&self.cfg.source_event_log_path)
            .unwrap()
            .last_sequence
            .map_or(0, |seq| seq.0);
        let members = (2..newcomers + 2).map(wallet).collect::<Vec<_>>();
        let mut entries = Vec::new();
        for (index, member) in members.iter().enumerate() {
            crate::support::install_full_history_anchor(&self.paper, *member, self.state.now - 10);
            self.paper
                .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                    wallet: *member,
                    complete: true,
                    proof_json: "{}".to_owned(),
                    updated_at_unix: self.state.now - 10,
                })
                .unwrap();
            let mut entry = crate::golden::BracketFinancialHarness::entries(&[*member]);
            entries.append(&mut entry);
            let ranking = Writer::open(&self.cfg.source_event_log_path)
                .unwrap()
                .append_synced(envelope(
                    serde_json::to_vec(&json!({"batch_id":2,"entries":entries})).unwrap(),
                    "pe-service.watchlist-ranking",
                    self.state.now,
                    1,
                    1,
                ))
                .unwrap();
            let binding =
                pe_service::qualification::scenario_membership_proofs_hash(&self.paper, &[*member])
                    .unwrap();
            let binding: Value = serde_json::from_str(&binding).unwrap();
            let admission = Writer::open(&self.cfg.source_event_log_path)
                .unwrap()
                .append_synced(envelope(
                    serde_json::to_vec(&json!({"wallet":member,"proof":binding["manifest"]}))
                        .unwrap(),
                    "pe-service.watchlist-admission",
                    self.state.now,
                    1,
                    1,
                ))
                .unwrap();
            let mut evictions = Vec::new();
            if knockout && index == 0 {
                let causal = Writer::open(&self.cfg.source_event_log_path)
                    .unwrap()
                    .append_synced(envelope(
                        serde_json::to_vec(&json!({
                            "wallet":self.wallet(), "evaluated_at_unix":self.state.now,
                            "last_trade_unix":self.state.now - 700_000,
                            "inactivity_threshold_secs":self.cfg.inactivity_threshold_secs,
                            "inactivity_hard_cap_secs":self.cfg.inactivity_hard_cap_secs,
                            "demotion_min_trades":self.cfg.demotion_min_trades,
                            "demotion_cb_alpha":self.cfg.demotion_cb_alpha,
                            "demotion_pnl_window_secs":self.cfg.demotion_pnl_window_secs,
                            "fills":[], "settlements":[],
                        }))
                        .unwrap(),
                        "pe-service.watchlist-knockout",
                        self.state.now,
                        1,
                        1,
                    ))
                    .unwrap();
                evictions.push(json!({"wallet":self.wallet(),"reason":"knockout_inactivity","causal_receipt":causal}));
            }
            let receipts = json!([{"wallet":member,"receipt":admission}]);
            let evidence = if knockout {
                json!({"kind":"knockout_backfill","evictions":evictions,"ranking_receipt":ranking,"admission_receipts":receipts})
            } else {
                json!({"kind":"full_rerank","ranking_receipt":ranking,"admission_receipts":receipts})
            };
            let record = MembershipChange {
                reason: if knockout {
                    MembershipReason::KnockoutInactivity
                } else {
                    MembershipReason::FullRerank
                },
                removed: if index == 0 {
                    vec![self.wallet()]
                } else {
                    Vec::new()
                },
                added: vec![*member],
                capacity: usize::MAX,
                ranking_batch_id: Some(2),
                evidence,
            }
            .into_record();
            Writer::open(&self.cfg.event_log_path)
                .unwrap()
                .append_synced(envelope(
                    serde_json::to_vec(&record).unwrap(),
                    "pe-service.paper",
                    self.state.now,
                    2,
                    1,
                ))
                .unwrap();
        }
        let mut initial = crate::golden::BracketFinancialHarness::entries(&[self.wallet()]);
        let initial = pe_trader_index::Watchlist {
            active_count: initial.len(),
            incubator_count: 0,
            entries: std::mem::take(&mut initial),
            snapshot_at: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(self.state.now).unwrap(),
            ),
        };
        let replayed = replay_membership(
            &paper_era(scan_paper_log(&self.cfg.event_log_path).unwrap()),
            initial,
            &self.cfg.source_event_log_path,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            replayed
                .watchlist
                .entries
                .iter()
                .map(|entry| entry.wallet)
                .collect::<std::collections::HashSet<_>>(),
            members.iter().copied().collect()
        );
        let source_appends = Scanner::verify(&self.cfg.source_event_log_path)
            .unwrap()
            .last_sequence
            .map_or(0, |seq| seq.0)
            - source_seq;
        assert_eq!(
            source_appends,
            u64::try_from(2 * newcomers + usize::from(knockout)).unwrap()
        );
        eprintln!(
            "streamed record wave: knockout={knockout} newcomers={newcomers} paper_bytes={} source_bytes={} paper_appends={newcomers} source_appends={} elapsed_ms={}",
            std::fs::metadata(&self.cfg.event_log_path).unwrap().len() - paper_before,
            std::fs::metadata(&self.cfg.source_event_log_path)
                .unwrap()
                .len()
                - source_before,
            Scanner::verify(&self.cfg.source_event_log_path)
                .unwrap()
                .last_sequence
                .map_or(0, |seq| seq.0)
                - source_seq,
            started.elapsed().as_millis()
        );
        self.cfg.maintenance_interval_secs = 0;
        self.cfg.supabase_refresh_interval_secs = 0;
        std::fs::write(&self.config_path, toml::to_string(&self.cfg).unwrap()).unwrap();
        members
    }

    pub fn copy_wallet(&self, member: WalletAddress) {
        self.state.activity.lock().unwrap().insert(
            member.to_string(),
            vec![row(
                member,
                1,
                OffsetDateTime::now_utc().unix_timestamp(),
                1001,
            )],
        );
    }

    pub fn assert_members(&self, expected: &[WalletAddress]) {
        let status: Value =
            serde_json::from_slice(&std::fs::read(&self.cfg.status_path).unwrap()).unwrap();
        let actual = status["live_wallets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|wallet| wallet.as_str().unwrap().to_owned())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(actual, expected.iter().map(ToString::to_string).collect());
    }

    pub fn copy(&self) {
        self.state.activity.lock().unwrap().insert(
            wallet(1).to_string(),
            vec![row(
                wallet(1),
                1,
                OffsetDateTime::now_utc().unix_timestamp(),
                1001,
            )],
        );
    }
    pub fn logs(&self) -> String {
        service_logs(self.dir.path())
    }

    pub async fn wait_log(&self, message: &str) {
        until(|| self.logs().contains(message)).await;
    }
    pub async fn wait_copy(&self) {
        until(|| self.paper.list_fills().unwrap().len() == 1).await;
    }
    pub fn clear_logs(&self) {
        for entry in std::fs::read_dir(self.dir.path()).unwrap().flatten() {
            if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
    }
    pub fn stale_anchors(&self) {
        rusqlite::Connection::open(&self.cfg.paper_state_db_path)
            .unwrap()
            .execute(
                "UPDATE position_anchors SET anchored_at_unix=?1",
                [self.state.now - 4000],
            )
            .unwrap();
    }
    /// Cut the next full-history page-zero replies inside a string, leaving ordinary polls intact.
    pub fn cut_full_history_replies(&self, count: usize) {
        self.state
            .cut_full_history_replies
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn mirror_positions(&self) {
        *self.state.positions.lock().unwrap() = self.paper.leader_positions().unwrap().into_iter().map(|position| json!({"proxyWallet": position.wallet, "conditionId": position.market_id.0.0, "asset":"103", "outcomeIndex":position.outcome_id.0, "size":position.long_contracts.to_decimal().to_string(), "negativeRisk":false, "redeemable":false})).collect();
    }
    pub fn wallet(&self) -> WalletAddress {
        wallet(1)
    }
    pub fn assert_live_membership(&self) {
        let status: Value =
            serde_json::from_slice(&std::fs::read(&self.cfg.status_path).unwrap()).unwrap();
        assert_eq!(status["watchlist_size"], 1, "{status}");
        assert!(
            self.state
                .authority
                .lock()
                .unwrap()
                .projection
                .iter()
                .any(|row| row["wallet_hex"] == wallet(1).to_string())
        );
        assert!(!self.paper.is_wallet_fenced(&wallet(1)).unwrap());
    }
}

/// Run every streamed prefix through the retained predecessor, then repeat its one copy.
pub async fn streamed_membership_rollback_rows(previous: &Path, current: &Path) {
    for knockout in [false, true] {
        for count in [1, 2] {
            let mut fixture = CheckpointFixture::new().await;
            let members = fixture.append_streamed_membership(knockout, count);
            for (direction, binary) in [("previous", previous), ("current", current)] {
                fixture.clear_logs();
                let child = Child::start_checkpoint(&fixture.config_path, binary, &[]);
                fixture.wait_log("pe-service listening").await;
                if direction == "previous" {
                    fixture.copy_wallet(members[0]);
                }
                fixture.wait_copy().await;
                until(|| {
                    std::fs::read(&fixture.cfg.status_path)
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                        .is_some_and(|status| {
                            status["uptime_secs"]
                                .as_u64()
                                .is_some_and(|uptime| uptime >= 2)
                        })
                })
                .await;
                fixture.assert_members(&members);
                let output = child.finish_checkpoint(Some("-INT")).await;
                assert!(
                    output.status.success(),
                    "{direction} knockout={knockout} prefix={count}: {output:?}"
                );
                assert_eq!(
                    fixture.paper.list_fills().unwrap().len(),
                    1,
                    "copy must remain idempotent"
                );
                fixture.mirror_positions();
                eprintln!(
                    "streamed rollback row: {direction} knockout={knockout} prefix={count} membership and idempotent copy verified"
                );
            }
        }
    }
}

#[cfg(test)]
mod data_api_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

    fn state() -> Arc<HttpState> {
        Arc::new(HttpState {
            boot_waves: false,
            ranking_batch: AtomicI64::new(1),
            scope_drops: Mutex::new(Vec::new()),
            group_spans: Mutex::new(Vec::new()),
            import_reads: Mutex::new(Vec::new()),
            rows: Vec::new(),
            now: 0,
            ranked_wallets: 0,
            newcomer: None,
            activity_requests: Mutex::new(Vec::new()),
            full_history_requests: Mutex::new(Vec::new()),
            cut_full_history_replies: AtomicUsize::new(0),
            start: Mutex::new(None),
            authority: Mutex::new(Authority::default()),
            activity: Mutex::new(HashMap::from([(
                "wallet".to_owned(),
                vec![
                    json!({"proxyWallet":"wallet", "timestamp":20, "type":"REWARD", "asset":"7"}),
                    json!({"proxyWallet":"wallet", "timestamp":10, "type":"TRADE", "asset":"8"}),
                    json!({"proxyWallet":"wallet", "timestamp":9, "type":"TRADE", "asset":"9"}),
                ],
            )])),
            positions: Mutex::new(vec![
                json!({"proxyWallet":"wallet", "asset":"7", "size":"5", "redeemable":false}),
                json!({"proxyWallet":"wallet", "asset":"8", "size":"1", "redeemable":true}),
                json!({"proxyWallet":"other", "asset":"9", "size":"2", "redeemable":false}),
            ]),
            data_api_v2: Mutex::new(Default::default()),
            activity_page_size_cap: None,
            slow_started: Notify::new(),
            slow: Semaphore::new(0),
            release_slow: AtomicBool::new(true),
            gamma_fails: AtomicBool::new(false),
            stale_ranked_wallet_2: AtomicBool::new(false),
            gamma_failures: AtomicUsize::new(0),
            resolved: AtomicBool::new(false),
        })
    }

    async fn reply(state: Arc<HttpState>, uri: &str) -> (StatusCode, Bytes) {
        let response = serve(
            State(state),
            OriginalUri(uri.parse().unwrap()),
            Bytes::new(),
        )
        .await;
        let status = response.status();
        (
            status,
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn v1_reply_bytes_stay_fixed_and_v2_full_history_cut_is_a_real_prefix() {
        let state = state();
        let (status, body) = reply(
            state.clone(),
            "/activity?user=wallet&start=10&end=20&type=TRADE&sortDirection=DESC&offset=0&limit=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_ref(), br#"[{"asset":"7","proxyWallet":"wallet","timestamp":20,"type":"REWARD"},{"asset":"8","proxyWallet":"wallet","timestamp":10,"type":"TRADE"}]"#);
        for (redeemable, expected) in [
            (
                "false",
                br#"[{"asset":"7","proxyWallet":"wallet","redeemable":false,"size":"5"}]"#
                    .as_slice(),
            ),
            (
                "true",
                br#"[{"asset":"8","proxyWallet":"wallet","redeemable":true,"size":"1"}]"#
                    .as_slice(),
            ),
        ] {
            let (status, body) = reply(
                state.clone(),
                &format!("/positions?user=wallet&redeemable={redeemable}&offset=0&limit=1"),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body.as_ref(), expected);
        }
        let uri = "/v2/activity?user=wallet&start=1&end=20&type=TRADE,SPLIT,MERGE,REDEEM,CONVERSION&sort_direction=DESC&limit=1000";
        let (status, complete) = reply(state.clone(), uri).await;
        assert_eq!(status, StatusCode::OK);
        state.cut_full_history_replies.store(1, Ordering::SeqCst);
        let (status, cut) = reply(state.clone(), uri).await;
        assert_eq!(status, StatusCode::OK);
        assert!(complete.starts_with(&cut));
        assert!(cut.len() < complete.len());
        assert!(serde_json::from_slice::<Value>(&cut).is_err());
        let complete = std::str::from_utf8(&complete).unwrap();
        assert_eq!(cut.len(), complete.find("\":\"").unwrap() + 4);
        let (_, retried) = reply(state.clone(), uri).await;
        assert_eq!(retried.as_ref(), complete.as_bytes());
        assert_eq!(
            *state.full_history_requests.lock().unwrap(),
            vec!["wallet"; 3]
        );
        assert_eq!(
            reply(state.clone(), &format!("{uri}&offset=0")).await.0,
            StatusCode::BAD_REQUEST
        );
        let (status, body) = reply(state, "/v2/positions?user=wallet&status=OPEN&filter_type=TOKENS&filter_amount=0&include_archived=true&sort_by=TOKENS&sort_direction=ASC&limit=1000").await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
        assert_eq!(body["data"][0]["current_size"], "1");
        assert_eq!(body["data"][1]["current_size"], "5");
    }

    #[tokio::test]
    async fn v2_activity_keeps_the_wallet_hold_and_full_history_accounting() {
        let state = state();
        state.release_slow.store(false, Ordering::SeqCst);
        let held = state.clone();
        let task = tokio::spawn(async move {
            reply(held, &format!("/v2/activity?user={}&start=1&end=20&type=TRADE,SPLIT,MERGE,REDEEM,CONVERSION&sort_direction=DESC&limit=1000", wallet(2))).await
        });
        tokio::time::timeout(Duration::from_secs(5), state.slow_started.notified())
            .await
            .unwrap();
        assert!(!task.is_finished());
        assert_eq!(
            *state.full_history_requests.lock().unwrap(),
            vec![wallet(2).to_string()]
        );
        state.slow.add_permits(1);
        assert_eq!(task.await.unwrap().0, StatusCode::OK);
    }
}
