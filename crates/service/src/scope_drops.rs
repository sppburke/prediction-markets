use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use pe_core_types::{
    RawHttpAttempt, RawHttpResponse, ReceivedAt, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
use pe_paper_state::{PaperStateDb, WalletScopeDrop};
use pe_trader_index::Watchlist;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::activity_ingest::SourceLogHandle;
use crate::orchestrator_control::OrchestratorControl;
use crate::source_event_sink::SourceEventSink;
use crate::supabase_reader::{self, GroupSpan, RANKING_PAGE_SIZE, ScopeDropRow, SupabaseError};

const DROPS_SOURCE_ID: &str = "supabase.ranking_scope_drops";
const DROPS_SCHEMA_VERSION: u32 = 1;
const DROPS_PARSER_VERSION: u32 = 1;
const SPANS_SOURCE_ID: &str = "supabase.ranking_neg_risk_group_spans";
const SPANS_SCHEMA_VERSION: u32 = 1;
const SPANS_PARSER_VERSION: u32 = 1;
const BATCH_SOURCE_ID: &str = "supabase.ranking_batches.scope_drops";
const BATCH_SCHEMA_VERSION: u32 = 1;
const BATCH_PARSER_VERSION: u32 = 1;

/// Imports the recorded evidence before returning a ranking batch to its consumer.
#[derive(Clone)]
pub struct ScopeDropLoader {
    paper_state: Arc<PaperStateDb>,
    application: Application,
}

#[derive(Clone)]
enum Application {
    Boot(Arc<Mutex<SourceEventSink>>),
    Runtime(SourceLogHandle, mpsc::Sender<OrchestratorControl>),
}

fn import_error(error: impl std::fmt::Display) -> SupabaseError {
    SupabaseError::ScopeImport(error.to_string())
}

impl ScopeDropLoader {
    pub fn boot(paper_state: Arc<PaperStateDb>, source_log: Arc<Mutex<SourceEventSink>>) -> Self {
        Self {
            paper_state,
            application: Application::Boot(source_log),
        }
    }

    pub fn runtime(
        paper_state: Arc<PaperStateDb>,
        source_log: SourceLogHandle,
        control: mpsc::Sender<OrchestratorControl>,
    ) -> Self {
        Self {
            paper_state,
            application: Application::Runtime(source_log, control),
        }
    }

    pub async fn fetch_batch(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        anon_key: &str,
        secret_key: &str,
        batch_id: i64,
    ) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
        let batch =
            supabase_reader::fetch_batch(client, base_url, anon_key, secret_key, batch_id).await?;
        self.import_batch(client, base_url, anon_key, secret_key, batch_id)
            .await?;
        Ok(batch)
    }

    pub async fn fetch(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        anon_key: &str,
        secret_key: &str,
    ) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
        match supabase_reader::fetch_latest_batch_id(client, base_url, anon_key, secret_key).await?
        {
            Some(batch) => {
                self.fetch_batch(client, base_url, anon_key, secret_key, batch)
                    .await
            }
            None => Ok((
                Watchlist {
                    entries: Vec::new(),
                    snapshot_at: SourceTimestamp(time::OffsetDateTime::now_utc()),
                    active_count: 0,
                    incubator_count: 0,
                },
                HashMap::new(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fetch_candidates(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        anon_key: &str,
        secret_key: &str,
        batch_id: i64,
        exclude: &[WalletAddress],
    ) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
        let batch = supabase_reader::fetch_candidates(
            client, base_url, anon_key, secret_key, batch_id, exclude,
        )
        .await?;
        self.import_batch(client, base_url, anon_key, secret_key, batch_id)
            .await?;
        Ok(batch)
    }

    pub async fn import_batch(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        anon_key: &str,
        secret_key: &str,
        batch_id: i64,
    ) -> Result<(), SupabaseError> {
        if self
            .paper_state
            .scope_drop_batch_imported(batch_id)
            .map_err(import_error)?
        {
            return Ok(());
        }
        let token = supabase_reader::auth_token(anon_key, secret_key);
        let (group_batch, mut group_ids) = self
            .paper_state
            .ranker_group_snapshot()
            .map_err(import_error)?;
        let mut drops = Vec::new();
        let mut offset = 0;
        loop {
            let (response, receipt) = self
                .observe(
                    supabase_reader::fetch_scope_drops(client, base_url, token, batch_id, offset)
                        .await,
                    DROPS_SOURCE_ID,
                    DROPS_SCHEMA_VERSION,
                    DROPS_PARSER_VERSION,
                )
                .await?;
            let page: Vec<ScopeDropRow> =
                serde_json::from_slice(&response.body).map_err(SupabaseError::Json)?;
            let complete = page.len() < RANKING_PAGE_SIZE;
            offset += page.len();
            for row in page {
                if row.batch_id != batch_id {
                    return Err(SupabaseError::BatchMismatch {
                        expected: batch_id,
                        found: Some(row.batch_id),
                    });
                }
                drops.push((
                    WalletScopeDrop {
                        wallet: row.wallet_hex,
                        scope_kind: row.scope_kind,
                        scope_id: row.scope_id,
                        cause: row.cause,
                        source_trade_id: None,
                        dropped_at_epoch: row.dropped_at_unix,
                        proof_json: String::new(),
                        recorded_at_unix: response.received_at.unix_timestamp(),
                    },
                    receipt,
                ));
            }
            if complete {
                break;
            }
        }
        if group_batch.is_none_or(|current| batch_id > current) {
            let mut spans = Vec::new();
            loop {
                let (response, _) = self
                    .observe(
                        supabase_reader::fetch_group_spans(
                            client,
                            base_url,
                            token,
                            group_batch,
                            batch_id,
                            spans.len(),
                        )
                        .await,
                        SPANS_SOURCE_ID,
                        SPANS_SCHEMA_VERSION,
                        SPANS_PARSER_VERSION,
                    )
                    .await?;
                let page: Vec<GroupSpan> =
                    serde_json::from_slice(&response.body).map_err(SupabaseError::Json)?;
                let complete = page.len() < RANKING_PAGE_SIZE;
                spans.extend(page);
                if complete {
                    break;
                }
            }
            apply_spans(&mut group_ids, &spans, batch_id)?;
        }
        let (response, _) = self
            .observe(
                supabase_reader::fetch_batch_existence(client, base_url, token, batch_id).await,
                BATCH_SOURCE_ID,
                BATCH_SCHEMA_VERSION,
                BATCH_PARSER_VERSION,
            )
            .await?;
        #[derive(serde::Deserialize)]
        struct Batch {
            batch_id: i64,
        }
        let rows: Vec<Batch> =
            serde_json::from_slice(&response.body).map_err(SupabaseError::Json)?;
        if !matches!(rows.as_slice(), [row] if row.batch_id == batch_id) {
            return Err(import_error(format!(
                "ranking batch {batch_id} no longer exists"
            )));
        }
        let count = drops.len();
        match &self.application {
            Application::Boot(_) => {
                self.paper_state
                    .import_scope_drops(batch_id, &drops, &group_ids)
                    .map_err(import_error)?;
                tracing::info!(batch_id, drops = count, "ranking scope drops imported");
            }
            Application::Runtime(_, control) => {
                let (acknowledged, acknowledgement) = oneshot::channel();
                control
                    .send(OrchestratorControl::ImportScopeDrops {
                        batch_id,
                        drops,
                        group_ids,
                        acknowledged,
                    })
                    .await
                    .map_err(import_error)?;
                acknowledgement
                    .await
                    .map_err(import_error)?
                    .map_err(import_error)?;
            }
        }
        Ok(())
    }

    async fn observe(
        &self,
        response: Result<RawHttpResponse, SupabaseError>,
        source_id: &str,
        schema_version: u32,
        parser_version: u32,
    ) -> Result<(RawHttpResponse, AppendReceipt), SupabaseError> {
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if let SupabaseError::CanaryObserved { attempts, .. } = &error {
                    for attempt in attempts {
                        if let RawHttpAttempt::Response(response) = attempt {
                            self.record(response, source_id, schema_version, parser_version)
                                .await?;
                        }
                    }
                }
                return Err(error);
            }
        };
        let receipt = self
            .record(&response, source_id, schema_version, parser_version)
            .await?;
        Ok((response, receipt))
    }

    async fn record(
        &self,
        response: &RawHttpResponse,
        source_id: &str,
        schema_version: u32,
        parser_version: u32,
    ) -> Result<AppendReceipt, SupabaseError> {
        let mut response = response.clone();
        response.source_id = source_id.to_owned();
        let envelope = EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version,
            parser_version,
            observed_at: SourceTimestamp(response.observed_at),
            received_at: ReceivedAt(response.received_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&response).map_err(SupabaseError::Json)?,
        };
        match &self.application {
            Application::Boot(sink) => sink
                .lock()
                .await
                .append_durable(envelope)
                .map_err(import_error),
            Application::Runtime(source_log, _) => {
                source_log.append(envelope).await.map_err(import_error)
            }
        }
    }
}

fn apply_spans(
    groups: &mut BTreeSet<String>,
    spans: &[GroupSpan],
    batch_id: i64,
) -> Result<(), SupabaseError> {
    let mut active = BTreeSet::new();
    for span in spans {
        if span.from_batch_id <= 0
            || span
                .to_batch_id
                .is_some_and(|end| end <= span.from_batch_id)
        {
            return Err(import_error(format!(
                "invalid group span for {}",
                span.group_id
            )));
        }
        groups.remove(&span.group_id);
        if span.from_batch_id <= batch_id && span.to_batch_id.is_none_or(|end| batch_id < end) {
            active.insert(span.group_id.clone());
        }
    }
    groups.extend(active);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use axum::{
        Json, Router,
        extract::{Query, State},
        http::StatusCode,
        response::IntoResponse as _,
        routing::get,
    };
    use serde_json::{Value, json};

    use super::*;

    const NOW: i64 = 1_700_000_000;

    #[derive(Default)]
    struct Fixture {
        drops: Vec<Value>,
        spans: Vec<Value>,
        queries: Vec<(String, HashMap<String, String>)>,
        pruned: bool,
        prune_during_read: bool,
        fail: bool,
    }

    async fn serve(fixture: Arc<StdMutex<Fixture>>) -> (String, tokio::task::JoinHandle<()>) {
        async fn drops(
            State(fixture): State<Arc<StdMutex<Fixture>>>,
            Query(query): Query<HashMap<String, String>>,
        ) -> axum::response::Response {
            let mut fixture = fixture.lock().unwrap();
            fixture.queries.push(("drops".to_owned(), query.clone()));
            if fixture.fail {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error":"unavailable"})),
                )
                    .into_response();
            }
            let offset = query["offset"].parse::<usize>().unwrap();
            if fixture.prune_during_read && offset > 0 {
                fixture.pruned = true;
            }
            let rows = if fixture.pruned {
                Vec::new()
            } else {
                fixture
                    .drops
                    .iter()
                    .skip(offset)
                    .take(RANKING_PAGE_SIZE)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            Json(rows).into_response()
        }
        async fn spans(
            State(fixture): State<Arc<StdMutex<Fixture>>>,
            Query(query): Query<HashMap<String, String>>,
        ) -> Json<Vec<Value>> {
            let mut fixture = fixture.lock().unwrap();
            fixture.queries.push(("spans".to_owned(), query.clone()));
            let selected = fixture
                .spans
                .iter()
                .filter(|row| {
                    let Some(filter) = query.get("or") else {
                        return true;
                    };
                    // The fixture implements the exact two endpoint intervals requested by the reader.
                    let values = filter
                        .split([',', '(', ')'])
                        .filter_map(|part| {
                            part.split_once(".gt.").or_else(|| part.split_once(".lte."))
                        })
                        .collect::<Vec<_>>();
                    let from = values[0].1.parse::<i64>().unwrap();
                    let to = values[1].1.parse::<i64>().unwrap();
                    [row["from_batch_id"].as_i64(), row["to_batch_id"].as_i64()]
                        .into_iter()
                        .flatten()
                        .any(|value| from < value && value <= to)
                })
                .cloned()
                .collect::<Vec<_>>();
            let offset = query["offset"].parse::<usize>().unwrap();
            Json(
                selected
                    .into_iter()
                    .skip(offset)
                    .take(RANKING_PAGE_SIZE)
                    .collect(),
            )
        }
        async fn batches(
            State(fixture): State<Arc<StdMutex<Fixture>>>,
            Query(query): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            let mut fixture = fixture.lock().unwrap();
            fixture.queries.push(("batch".to_owned(), query.clone()));
            let batch_id = query["batch_id"]
                .strip_prefix("eq.")
                .unwrap()
                .parse::<i64>()
                .unwrap();
            Json(if fixture.pruned {
                json!([])
            } else {
                json!([{"batch_id":batch_id}])
            })
        }
        let app = Router::new()
            .route("/rest/v1/ranking_scope_drops", get(drops))
            .route("/rest/v1/ranking_neg_risk_group_spans", get(spans))
            .route("/rest/v1/ranking_batches", get(batches))
            .route(
                "/rest/v1/ranking_entries",
                get(|| async { Json(json!([])) }),
            )
            .with_state(fixture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, task)
    }

    fn row(batch_id: i64, id: usize) -> Value {
        json!({"batch_id":batch_id,"wallet_hex":WalletAddress([1;20]).to_string(),"scope_kind":"market","scope_id":format!("market-{id}"),"cause":"underflow","dropped_at_unix":NOW})
    }

    fn boot_loader(temp: &tempfile::TempDir) -> (ScopeDropLoader, Arc<PaperStateDb>) {
        let state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let loader = ScopeDropLoader::boot(
            state.clone(),
            Arc::new(Mutex::new(
                SourceEventSink::open(temp.path().join("source.log")).unwrap(),
            )),
        );
        (loader, state)
    }

    #[tokio::test]
    async fn multipage_drops_and_first_group_import_are_complete_and_receipted() {
        let fixture = Arc::new(StdMutex::new(Fixture {
            drops: (0..1_001).map(|id| row(10,id)).collect(),
            spans: (0..1_001).map(|id| json!({"group_id":format!("group-{id}"),"from_batch_id":1,"to_batch_id":null})).collect(),
            ..Fixture::default()
        }));
        let (base, server) = serve(fixture.clone()).await;
        let temp = tempfile::tempdir().unwrap();
        let (loader, state) = boot_loader(&temp);
        loader
            .import_batch(&reqwest::Client::new(), &base, "anon", "", 10)
            .await
            .unwrap();
        assert!(state.scope_drop_batch_imported(10).unwrap());
        assert_eq!(state.ranker_group_snapshot().unwrap().0, Some(10));
        assert_eq!(state.ranker_group_snapshot().unwrap().1.len(), 1_001);
        let drops = state
            .wallet_scope_drops(&WalletAddress([1; 20]), NOW)
            .unwrap();
        assert_eq!(drops.len(), 1_001);
        let frames = pe_event_log::Reader::replay(temp.path().join("source.log"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 5);
        let pages = frames
            .iter()
            .filter(|(_, frame)| frame.source_id.0 == DROPS_SOURCE_ID)
            .collect::<Vec<_>>();
        let page_rows = pages
            .iter()
            .map(|(_, frame)| {
                let response: RawHttpResponse = serde_json::from_slice(&frame.payload).unwrap();
                assert_eq!(response.source_id, DROPS_SOURCE_ID);
                serde_json::from_slice::<Vec<ScopeDropRow>>(&response.body).unwrap()
            })
            .collect::<Vec<_>>();
        for drop in drops {
            let proof: Value = serde_json::from_str(&drop.proof_json).unwrap();
            assert_eq!(proof["batch_id"], 10);
            let receipt: AppendReceipt = serde_json::from_value(proof["receipt"].clone()).unwrap();
            let page = pages
                .iter()
                .position(|(receipt_in_log, frame)| {
                    *receipt_in_log == receipt.sequence && frame.this_hash == receipt.this_hash
                })
                .unwrap();
            assert!(
                page_rows[page]
                    .iter()
                    .any(|row| row.scope_id == drop.scope_id)
            );
            assert!(drop.source_trade_id.is_none());
        }
        assert_eq!(
            fixture
                .lock()
                .unwrap()
                .queries
                .iter()
                .filter(|(kind, _)| kind == "drops")
                .map(|(_, q)| q["offset"].clone())
                .collect::<Vec<_>>(),
            ["0", "1000"]
        );
        server.abort();
        // An imported batch must succeed without any further network access.
        loader
            .import_batch(&reqwest::Client::new(), "invalid URL", "anon", "", 10)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn foreign_batch_and_pruning_never_mark_an_import_or_change_prior_state() {
        for failure in [
            "foreign",
            "pruned_before",
            "pruned_during",
            "invalid_span",
            "http",
        ] {
            let fixture = Arc::new(StdMutex::new(Fixture {
                drops: if failure == "pruned_during" {
                    (0..1_001).map(|id| row(10, id)).collect()
                } else {
                    vec![row(if failure == "foreign" { 11 } else { 10 }, 0)]
                },
                pruned: failure == "pruned_before",
                prune_during_read: failure == "pruned_during",
                fail: failure == "http",
                spans: if failure == "invalid_span" {
                    vec![json!({"group_id":"bad","from_batch_id":8,"to_batch_id":8})]
                } else {
                    Vec::new()
                },
                ..Fixture::default()
            }));
            let (base, server) = serve(fixture).await;
            let temp = tempfile::tempdir().unwrap();
            let (loader, state) = boot_loader(&temp);
            state
                .import_scope_drops(1, &[], &BTreeSet::from(["prior".to_owned()]))
                .unwrap();
            let error = loader
                .import_batch(&reqwest::Client::new(), &base, "anon", "", 10)
                .await
                .unwrap_err();
            if failure == "foreign" {
                assert!(matches!(
                    error,
                    SupabaseError::BatchMismatch {
                        expected: 10,
                        found: Some(11)
                    }
                ));
            }
            assert!(!state.scope_drop_batch_imported(10).unwrap());
            assert!(
                state
                    .wallet_scope_drops(&WalletAddress([1; 20]), NOW)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                state.ranker_group_snapshot().unwrap(),
                (Some(1), BTreeSet::from(["prior".to_owned()]))
            );
            if failure == "http" {
                let frames = pe_event_log::Reader::replay(temp.path().join("source.log"))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(frames.len(), 1);
                assert_eq!(
                    serde_json::from_slice::<RawHttpResponse>(&frames[0].1.payload)
                        .unwrap()
                        .status,
                    503
                );
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn later_import_reads_only_changes_and_older_import_preserves_the_newest_set() {
        let fixture = Arc::new(StdMutex::new(Fixture {
            spans: vec![
                json!({"group_id":"a","from_batch_id":1,"to_batch_id":11}),
                json!({"group_id":"b","from_batch_id":1,"to_batch_id":null}),
                json!({"group_id":"c","from_batch_id":11,"to_batch_id":null}),
                json!({"group_id":"d","from_batch_id":12,"to_batch_id":null}),
                json!({"group_id":"reopened","from_batch_id":1,"to_batch_id":5}),
                json!({"group_id":"reopened","from_batch_id":7,"to_batch_id":null}),
            ],
            ..Fixture::default()
        }));
        let (base, server) = serve(fixture.clone()).await;
        let temp = tempfile::tempdir().unwrap();
        let (loader, state) = boot_loader(&temp);
        let client = reqwest::Client::new();
        loader
            .import_batch(&client, &base, "anon", "", 10)
            .await
            .unwrap();
        assert_eq!(
            state.ranker_group_snapshot().unwrap().1,
            BTreeSet::from(["a", "b", "reopened"].map(str::to_owned))
        );
        loader
            .import_batch(&client, &base, "anon", "", 11)
            .await
            .unwrap();
        let expected = (
            Some(11),
            BTreeSet::from(["b", "c", "reopened"].map(str::to_owned)),
        );
        assert_eq!(state.ranker_group_snapshot().unwrap(), expected);
        loader
            .import_batch(&client, &base, "anon", "", 9)
            .await
            .unwrap();
        assert!(state.scope_drop_batch_imported(9).unwrap());
        assert_eq!(state.ranker_group_snapshot().unwrap(), expected);
        let span_queries = fixture
            .lock()
            .unwrap()
            .queries
            .iter()
            .filter(|(kind, _)| kind == "spans")
            .map(|(_, q)| q.clone())
            .collect::<Vec<_>>();
        assert_eq!(span_queries.len(), 2);
        assert!(!span_queries[0].contains_key("or"));
        assert_eq!(
            span_queries[1]["or"],
            "(and(from_batch_id.gt.10,from_batch_id.lte.11),and(to_batch_id.gt.10,to_batch_id.lte.11))"
        );
        drop(loader);
        drop(state);
        assert_eq!(
            PaperStateDb::open(&temp.path().join("paper.db"))
                .unwrap()
                .ranker_group_snapshot()
                .unwrap(),
            expected
        );
        server.abort();
    }

    #[tokio::test]
    async fn runtime_loader_waits_for_owner_acknowledgement_and_propagates_application_failure() {
        let fixture = Arc::new(StdMutex::new(Fixture {
            drops: vec![row(10, 0)],
            ..Fixture::default()
        }));
        let (base, server) = serve(fixture).await;
        let temp = tempfile::tempdir().unwrap();
        let state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let (source, mut source_rx) = SourceLogHandle::channel(2);
        let mut writer = pe_event_log::Writer::open(temp.path().join("source.log")).unwrap();
        let recorder = tokio::spawn(async move {
            while let Some((envelope, ack)) = source_rx.recv_for_test().await {
                ack.send(writer.append_synced(envelope).unwrap()).unwrap();
            }
        });
        let (tx, mut rx) = mpsc::channel(2);
        let loader = ScopeDropLoader::runtime(state.clone(), source, tx);
        let client = reqwest::Client::new();
        for succeed in [false, true] {
            let request = loader.fetch_batch(&client, &base, "anon", "", 10);
            tokio::pin!(request);
            let message = tokio::select! {
                result = &mut request => panic!("batch returned before import acknowledgement: {result:?}"),
                message = rx.recv() => message.unwrap(),
            };
            assert!(!state.scope_drop_batch_imported(10).unwrap());
            let OrchestratorControl::ImportScopeDrops {
                batch_id,
                drops,
                group_ids,
                acknowledged,
            } = message
            else {
                unreachable!()
            };
            assert_eq!(batch_id, 10);
            assert_eq!(drops.len(), 1);
            if succeed {
                state
                    .import_scope_drops(batch_id, &drops, &group_ids)
                    .unwrap();
                acknowledged.send(Ok(())).unwrap();
            } else {
                acknowledged
                    .send(Err("application failed".to_owned()))
                    .unwrap();
            }
            assert_eq!(request.await.is_ok(), succeed);
            assert_eq!(state.scope_drop_batch_imported(10).unwrap(), succeed);
        }
        drop(loader);
        recorder.await.unwrap();
        server.abort();
    }
}
