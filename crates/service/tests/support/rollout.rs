//! The real service process, local HTTP authority, and unchanged production admission gates.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
    rows: Vec<ConfigRow>,
    now: i64,
    ranked_wallets: usize,
    full_history_requests: Mutex<Vec<String>>,
    start: Mutex<Option<pe_event_log::AppendReceipt>>,
    authority: Mutex<Authority>,
    activity: Mutex<HashMap<String, Vec<Value>>>,
    positions: Mutex<Vec<Value>>,
    slow_started: Notify,
    slow: Semaphore,
    release_slow: std::sync::atomic::AtomicBool,
    gamma_fails: std::sync::atomic::AtomicBool,
    stale_ranked_wallet_2: std::sync::atomic::AtomicBool,
    gamma_failures: std::sync::atomic::AtomicUsize,
    resolved: std::sync::atomic::AtomicBool,
}
async fn serve(
    State(state): State<Arc<HttpState>>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
    let q = url.query_pairs().into_owned().collect::<HashMap<_, _>>();
    let path = url.path();
    if path == "/activity" {
        let w = q.get("user").unwrap();
        if q.get("start").is_some_and(|start| start == "1") {
            state.full_history_requests.lock().unwrap().push(w.clone());
        }
        if w == &wallet(2).to_string() && !state.release_slow.load(Ordering::SeqCst) {
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
        return (StatusCode::OK, Json(json!(data)));
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
        return (StatusCode::OK, Json(json!(data)));
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
            );
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
        v[0]["endDate"] = OffsetDateTime::from_unix_timestamp(state.now + 7200)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
            .into();
        return (StatusCode::OK, Json(v));
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
        v["end_date_iso"] = OffsetDateTime::from_unix_timestamp(state.now + 7200)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
            .into();
        v["tokens"][0]["token_id"] = (market * 2 + 101).to_string().into();
        v["tokens"][1]["token_id"] = (market * 2 + 102).to_string().into();
        return (StatusCode::OK, Json(v));
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
        return (StatusCode::OK, Json(v));
    }
    if path == "/book" {
        let token = q["token_id"].parse::<usize>().unwrap();
        let market = (token - 101) / 2;
        return (
            StatusCode::OK,
            Json(
                json!({"market":condition(market),"asset_id":token.to_string(),"timestamp":OffsetDateTime::now_utc().unix_timestamp_nanos().checked_div(1_000_000).unwrap().to_string(),"min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{"price":"0.50","size":"100000"}],"asks":[{"price":"0.50","size":"100000"}]}),
            ),
        );
    }
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    let mut a = state.authority.lock().unwrap();
    let answer=match path {
        "/rest/v1/accounts"|"/rest/v1/account_credentials"=>json!([]),
        "/rest/v1/service_config"=>json!(state.rows),
        "/rest/v1/ranking_batches"=>json!([{"batch_id":1}]),
        "/rest/v1/ranking_entries"|"/rest/v1/latest_ranking"=>json!((1..=state.ranked_wallets).map(|n|json!({"batch_id":1,"rank":n,"wallet_hex":wallet(n).to_string(),"ls_tstat":"3","hit_rate":"0.65","n_trades":100,"last_trade_unix":if n == 2 && state.stale_ranked_wallet_2.load(Ordering::SeqCst) { state.now - 300_000 } else { state.now - 20 },"survives":true})).collect::<Vec<_>>()),
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
    (StatusCode::OK, Json(answer))
}
struct Child {
    process: std::process::Child,
    root: std::path::PathBuf,
    stdout: Option<tokio::task::JoinHandle<Vec<u8>>>,
    stderr: Option<tokio::task::JoinHandle<Vec<u8>>>,
}
impl Child {
    fn start(config: &Path) -> Self {
        let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"))
            .env_clear()
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
/// repairs first-entry history, settles once, and restores checkpoint+suffix and exact decisions.
pub async fn run() {
    run_case(false).await;
}

pub async fn run_boot_waves() {
    run_case(true).await;
}

async fn run_case(boot_waves: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let now = OffsetDateTime::now_utc().unix_timestamp();
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
        maintenance_interval_secs: if boot_waves { 3600 } else { 1 },
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
        rows: crate::golden::golden_config_rows(&runtime),
        now,
        ranked_wallets: if boot_waves { 5 } else { 4 },
        full_history_requests: Mutex::new(Vec::new()),
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
        slow_started: Notify::new(),
        slow: Semaphore::new(0),
        release_slow: AtomicBool::new(boot_waves),
        gamma_fails: AtomicBool::new(!boot_waves),
        stale_ranked_wallet_2: AtomicBool::new(false),
        gamma_failures: std::sync::atomic::AtomicUsize::new(0),
        resolved: AtomicBool::new(false),
    });
    if boot_waves {
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
    let paper = PaperStateDb::open(&cfg.paper_state_db_path).unwrap();
    let wallets = (1..=state.ranked_wallets).map(wallet).collect::<Vec<_>>();
    for w in &wallets {
        crate::support::install_full_history_anchor(&paper, *w, now - 10);
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
        )
        .unwrap();
    *state.start.lock().unwrap() = Some(start_receipt);
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
        let requests = state.full_history_requests.lock().unwrap().clone();
        for w in &wallets {
            assert_eq!(
                requests
                    .iter()
                    .filter(|requested| *requested == &w.to_string())
                    .count(),
                3,
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
        child.stop().await;
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
        "UPDATE position_anchors SET anchored_at_unix=?2 WHERE wallet_hex=?1",
        rusqlite::params![wallet(4).to_string(), now - 4000],
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
            "UPDATE position_anchors SET anchored_at_unix=?3 WHERE wallet_hex IN (?1, ?2)",
            rusqlite::params![wallet(1).to_string(), wallet(2).to_string(), now - 4000],
        )
        .unwrap();
    // No eligible anchor is reused, so boot validates both wallets. The healthy venue mirror
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
