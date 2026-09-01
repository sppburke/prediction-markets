//! Fail-closed public-input composition for automatic Winner-Follow canary candidates.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, LeaderSignal, SignalConfig, classify_trade};
use pe_core_types::{
    MarketId, Probability, RawEvidence, RawHttpResponse, TraderId, VenueId, WalletAddress,
};
use pe_position_ledger::PositionLedger;
use pe_source_polymarket_public::{PolymarketEndpoint, ReqwestFetcher};
use pe_trader_index::{Watchlist, WatchlistEntry};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::position_seeder::parse_positions_strict;
use crate::supabase_reader::{SupabaseError, fetch_canary_observed};
use crate::trade_parser::parse_trades_strict;

const DATA_HOST: &str = "https://data-api.polymarket.com";
const WATCHLIST_LIMIT: usize = 100;
const PAGE_SIZE: usize = 500;
const POSITION_MAX_PAGES: u32 = 20;
const ACTIVITY_MAX_PAGES: u32 = 11;
const MAX_COPY_LATENCY_MILLIS: i128 = 3_000;

#[derive(Debug)]
pub struct OrganicObservation {
    pub identity: String,
    pub trade: IncomingTrade,
    pub signal: Option<LeaderSignal>,
    pub probability: Option<Probability>,
    pub skip_reason: Option<String>,
    pub evidence: Vec<RawEvidence>,
}

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct OrganicSourceError {
    pub reason: String,
    pub evidence: Vec<RawEvidence>,
}

impl OrganicSourceError {
    fn plain(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            evidence: Vec::new(),
        }
    }

    fn observed(reason: impl Into<String>, evidence: Vec<RawEvidence>) -> Self {
        Self {
            reason: reason.into(),
            evidence,
        }
    }
}

struct FetchedPage {
    response: RawHttpResponse,
    evidence: Vec<RawEvidence>,
}

pub struct OrganicCandidateSource {
    client: reqwest::Client,
    public: Arc<ReqwestFetcher>,
    supabase_url: String,
    supabase_anon_key: String,
    data_host: String,
    watchlist: Option<Watchlist>,
    entries: HashMap<WalletAddress, WatchlistEntry>,
    entry_gate: CopyEntryGate,
    seeded_wallets: HashSet<WalletAddress>,
    ledger: PositionLedger,
    cursors: HashMap<WalletAddress, i64>,
    pending_evidence: Vec<RawEvidence>,
    queued: Option<(IncomingTrade, Vec<RawEvidence>)>,
    in_flight: Option<(IncomingTrade, Vec<RawEvidence>)>,
    seen_trade_ids: HashSet<String>,
}

impl OrganicCandidateSource {
    #[must_use]
    pub fn new(
        public: Arc<ReqwestFetcher>,
        supabase_url: String,
        supabase_anon_key: String,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            public,
            supabase_url,
            supabase_anon_key,
            data_host: DATA_HOST.to_owned(),
            watchlist: None,
            entries: HashMap::new(),
            entry_gate: CopyEntryGate::new(CopyEntryGateConfig, HashMap::new()),
            seeded_wallets: HashSet::new(),
            ledger: PositionLedger::new(),
            cursors: HashMap::new(),
            pending_evidence: Vec::new(),
            queued: None,
            in_flight: None,
            seen_trade_ids: HashSet::new(),
        }
    }

    pub async fn next(&mut self) -> Result<Option<OrganicObservation>, OrganicSourceError> {
        if let Some((trade, evidence)) = self.in_flight.clone() {
            return self.classify(trade, evidence).map(Some);
        }
        if let Err(mut error) = self.refresh_watchlist().await {
            self.pending_evidence.append(&mut error.evidence);
            error.evidence = std::mem::take(&mut self.pending_evidence);
            return Err(error);
        }
        if let Err(mut error) = self.poll_round().await {
            self.pending_evidence.append(&mut error.evidence);
            error.evidence = std::mem::take(&mut self.pending_evidence);
            return Err(error);
        }
        self.in_flight = self.queued.take();
        self.in_flight
            .clone()
            .map(|(trade, evidence)| self.classify(trade, evidence))
            .transpose()
    }

    pub fn acknowledge(&mut self, observation: &OrganicObservation) {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|(trade, _)| trade.source_trade_id == observation.trade.source_trade_id)
        {
            self.in_flight = None;
        }
        if let Err(error) = self.ledger.ingest(&observation.trade) {
            tracing::error!(%error, "organic canary ledger rejected acknowledged trade");
            return;
        }
        self.seen_trade_ids
            .insert(observation.trade.source_trade_id.0.clone());
        self.cursors
            .entry(observation.trade.wallet)
            .and_modify(|cursor| {
                *cursor = (*cursor).max(observation.trade.observed_at.unix_timestamp())
            })
            .or_insert_with(|| observation.trade.observed_at.unix_timestamp());
        if observation.signal.is_some() {
            self.entry_gate
                .record_entry(observation.trade.wallet, &observation.trade.market_id);
        }
    }

    /// Drain observations from a completed poll that produced no candidate.
    /// The daemon journals these as a pre-reservation skip before polling again.
    pub fn take_idle_evidence(&mut self) -> Vec<RawEvidence> {
        std::mem::take(&mut self.pending_evidence)
    }

    async fn refresh_watchlist(&mut self) -> Result<(), OrganicSourceError> {
        let (watchlist, cursors, observations) = fetch_canary_observed(
            &self.client,
            &self.supabase_url,
            &self.supabase_anon_key,
            WATCHLIST_LIMIT,
        )
        .await
        .map_err(organic_supabase_error)?;
        self.pending_evidence
            .extend(observations.into_iter().map(RawEvidence::HttpResponse));
        if watchlist.entries.is_empty() {
            return Err(OrganicSourceError::plain("canonical watchlist is empty"));
        }
        for entry in &watchlist.entries {
            if !self.seeded_wallets.contains(&entry.wallet) {
                let (history, position, evidence) = self.seed_wallet(entry.wallet).await?;
                self.pending_evidence.extend(evidence);
                self.entry_gate
                    .merge_history(HashMap::from([(entry.wallet, history)]));
                self.seeded_wallets.insert(entry.wallet);
                // #544 Lane D integration: the separately stopped organic canary still
                // needs the causal activity/positions bracket before it may be enabled.
                // This isolated pre-publication seed is not used by the service ledger.
                let mut snapshots = self.ledger.snapshots().clone();
                snapshots.insert(entry.wallet, position);
                self.ledger = PositionLedger::from_snapshots(snapshots);
            }
            if let Some(cursor) = cursors.get(&entry.wallet) {
                self.cursors.entry(entry.wallet).or_insert(*cursor);
            }
        }
        self.entries = watchlist
            .entries
            .iter()
            .cloned()
            .map(|entry| (entry.wallet, entry))
            .collect();
        self.watchlist = Some(watchlist);
        Ok(())
    }

    async fn poll_round(&mut self) -> Result<(), OrganicSourceError> {
        let entries = self
            .watchlist
            .as_ref()
            .ok_or_else(|| OrganicSourceError::plain("watchlist unavailable"))?
            .entries
            .clone();
        for entry in &entries {
            let start = self
                .cursors
                .get(&entry.wallet)
                .map(|cursor| cursor.saturating_sub(1));
            let (mut trades, evidence) = self
                .fetch_activity_window(
                    entry.wallet,
                    start,
                    OffsetDateTime::now_utc().unix_timestamp(),
                )
                .await?;
            self.pending_evidence.extend(evidence);
            trades.sort_by_key(|trade| trade.observed_at);
            let mut queued_ids = HashSet::new();
            for trade in trades {
                if self.seen_trade_ids.contains(&trade.source_trade_id.0)
                    || !queued_ids.insert(trade.source_trade_id.0.clone())
                {
                    continue;
                }
                if self
                    .cursors
                    .get(&entry.wallet)
                    .is_some_and(|cursor| trade.observed_at.unix_timestamp() < *cursor)
                {
                    continue;
                }
                self.queued = Some((trade, std::mem::take(&mut self.pending_evidence)));
                return Ok(());
            }
        }
        Ok(())
    }

    async fn fetch_activity_window(
        &self,
        wallet: WalletAddress,
        start: Option<i64>,
        end: i64,
    ) -> Result<(Vec<IncomingTrade>, Vec<RawEvidence>), OrganicSourceError> {
        let mut trades = Vec::new();
        let mut evidence = Vec::new();
        let mut ids = HashSet::new();
        for page in 0..ACTIVITY_MAX_PAGES {
            let url = PolymarketEndpoint::UserTradeActivityPage {
                user: wallet.to_string(),
                end,
                start,
                offset: page.saturating_mul(PAGE_SIZE as u32),
            }
            .url(&self.data_host);
            let fetched = fetch_raw(&self.public, &url).await.map_err(|mut error| {
                evidence.append(&mut error.evidence);
                OrganicSourceError::observed(error.reason, evidence.clone())
            })?;
            evidence.extend(fetched.evidence);
            let page_trades =
                parse_trades_strict(&fetched.response.body, wallet).map_err(|error| {
                    OrganicSourceError::observed(error.to_string(), evidence.clone())
                })?;
            if page_trades.len() > PAGE_SIZE
                || page_trades
                    .iter()
                    .any(|trade| !ids.insert(trade.source_trade_id.0.clone()))
            {
                return Err(OrganicSourceError::observed(
                    "activity pagination repeated or exceeded a page",
                    evidence,
                ));
            }
            let count = page_trades.len();
            trades.extend(page_trades);
            if count < PAGE_SIZE {
                return Ok((trades, evidence));
            }
        }
        Err(OrganicSourceError::observed(
            "activity pagination reached the official offset cap",
            evidence,
        ))
    }

    fn classify(
        &self,
        trade: IncomingTrade,
        activity_evidence: Vec<RawEvidence>,
    ) -> Result<OrganicObservation, OrganicSourceError> {
        let watchlist = self
            .watchlist
            .as_ref()
            .ok_or_else(|| OrganicSourceError::plain("watchlist unavailable"))?;
        let entry = self
            .entries
            .get(&trade.wallet)
            .ok_or_else(|| OrganicSourceError::plain("leader left watchlist"))?;
        let signal = classify_trade(
            &trade,
            self.ledger.position(&trade.wallet),
            watchlist,
            entry.reconstruction_quality,
            VenueId::polymarket(),
            &SignalConfig::default(),
        );
        let mut skip_reason = None;
        let signal = signal.and_then(|signal| {
            if let Some(reason) = self.entry_gate.admit(&signal) {
                skip_reason = Some(reason.to_string());
                None
            } else if (time::OffsetDateTime::now_utc() - trade.observed_at).whole_milliseconds()
                > MAX_COPY_LATENCY_MILLIS
            {
                skip_reason = Some("copy latency exceeded 3000 ms".to_owned());
                None
            } else {
                Some(signal)
            }
        });
        if signal.is_none() && skip_reason.is_none() {
            skip_reason =
                Some("trade did not classify as an eligible Winner-Follow entry".to_owned());
        }
        let probability = signal
            .as_ref()
            .map(|_| Probability(Decimal::from(entry.win_rate_bps.0) / Decimal::from(10_000u32)));
        let identity = signal.as_ref().map_or_else(
            || format!("organic-observation:{}", trade.source_trade_id.0),
            |signal| {
                format!(
                    "wf|{}|{}|{}|{}|buy|{}",
                    TraderId(trade.wallet),
                    trade.source_trade_id.0,
                    trade.market_id.0.0,
                    trade.outcome_id.0,
                    signal.observed_at.unix_timestamp()
                )
            },
        );
        Ok(OrganicObservation {
            identity,
            trade,
            signal,
            probability,
            skip_reason,
            evidence: activity_evidence,
        })
    }

    async fn seed_wallet(
        &self,
        wallet: WalletAddress,
    ) -> Result<
        (
            HashSet<MarketId>,
            pe_copy_signal_engine::PositionSnapshot,
            Vec<RawEvidence>,
        ),
        OrganicSourceError,
    > {
        let (trades, mut evidence) = self
            .fetch_activity_window(wallet, None, OffsetDateTime::now_utc().unix_timestamp())
            .await?;
        let history = trades.into_iter().map(|trade| trade.market_id).collect();
        let mut positions = HashMap::new();
        for page in 0..POSITION_MAX_PAGES {
            let offset = page.saturating_mul(PAGE_SIZE as u32);
            let url = PolymarketEndpoint::CurrentPositions {
                user: wallet.to_string(),
                limit: Some(PAGE_SIZE as u32),
                offset: Some(offset),
                redeemable: Some(false),
                size_threshold: Some(0),
            }
            .url(&self.data_host);
            let fetched = fetch_raw(&self.public, &url).await.map_err(|mut error| {
                evidence.append(&mut error.evidence);
                OrganicSourceError::observed(error.reason, evidence.clone())
            })?;
            evidence.extend(fetched.evidence);
            let raw_positions: Vec<serde_json::Value> =
                serde_json::from_slice(&fetched.response.body).map_err(|error| {
                    OrganicSourceError::observed(error.to_string(), evidence.clone())
                })?;
            for position in &raw_positions {
                let size = position
                    .get("size")
                    .map(|value| {
                        value
                            .as_str()
                            .map_or_else(|| value.to_string(), str::to_owned)
                    })
                    .ok_or_else(|| {
                        OrganicSourceError::observed("position omitted size", evidence.clone())
                    })?
                    .parse::<Decimal>()
                    .map_err(|_| {
                        OrganicSourceError::observed(
                            "position size is not exact decimal",
                            evidence.clone(),
                        )
                    })?;
                if size > Decimal::ZERO && size < Decimal::ONE {
                    return Err(OrganicSourceError::observed(
                        "positive fractional position cannot be represented safely",
                        evidence,
                    ));
                }
            }
            let snapshot =
                parse_positions_strict(&fetched.response.body, wallet).map_err(|error| {
                    OrganicSourceError::observed(error.to_string(), evidence.clone())
                })?;
            let count = raw_positions.len();
            positions.extend(snapshot.positions);
            if count < PAGE_SIZE {
                break;
            }
            if page + 1 == POSITION_MAX_PAGES {
                return Err(OrganicSourceError::observed(
                    "position seed hit the completeness cap",
                    evidence,
                ));
            }
        }
        Ok((
            history,
            pe_copy_signal_engine::PositionSnapshot { wallet, positions },
            evidence,
        ))
    }
}

fn organic_supabase_error(error: SupabaseError) -> OrganicSourceError {
    match error {
        SupabaseError::CanaryObserved { reason, attempts } => {
            OrganicSourceError::observed(reason, attempts.into_iter().map(Into::into).collect())
        }
        error => OrganicSourceError::plain(error.to_string()),
    }
}

async fn fetch_raw(fetcher: &ReqwestFetcher, url: &str) -> Result<FetchedPage, OrganicSourceError> {
    let mut attempts = Vec::new();
    let result = fetcher
        .fetch_page_observed(
            url,
            pe_source_polymarket_public::HttpRequestContext {
                source_id: "polymarket-data-api",
                endpoint_kind: if url.contains("/activity") {
                    "data-activity"
                } else {
                    "data-positions"
                },
            },
            |attempt| {
                attempts.push(attempt);
                Ok(())
            },
        )
        .await;
    let evidence = attempts
        .into_iter()
        .map(RawEvidence::from)
        .collect::<Vec<_>>();
    if let Err(error) = result {
        return Err(OrganicSourceError::observed(error.to_string(), evidence));
    }
    let response = evidence
        .iter()
        .rev()
        .find_map(|observation| match observation {
            RawEvidence::HttpResponse(response) => Some(response.clone()),
            RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
        })
        .ok_or_else(|| OrganicSourceError::observed("missing HTTP response", evidence.clone()))?;
    Ok(FetchedPage { response, evidence })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Response, StatusCode, Uri};
    use axum::response::IntoResponse as _;
    use axum::routing::get;
    use pe_core_types::RawArtifactObservation;
    use serde_json::{Value, json};

    use super::*;

    #[derive(Clone)]
    struct ActivityState(Arc<Vec<Value>>);

    async fn paged_activity(State(state): State<ActivityState>, uri: Uri) -> Response<Body> {
        if uri.query().is_some_and(|query| query.contains("offset=0")) {
            return axum::Json(state.0.as_ref().clone()).into_response();
        }
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("{"))
            .expect("test response is valid")
    }

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    fn artifact(kind: &str) -> RawEvidence {
        RawEvidence::Artifact(RawArtifactObservation {
            source_id: "test".to_owned(),
            artifact_kind: kind.to_owned(),
            path: kind.to_owned(),
            body: kind.as_bytes().to_vec(),
            observed_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        })
    }

    #[tokio::test]
    async fn two_page_malformed_activity_retains_both_responses_in_order() {
        let rows = (0..PAGE_SIZE)
            .map(|index| {
                json!({
                    "transactionHash": format!("trade-{index}"),
                    "conditionId": "condition",
                    "side": "BUY",
                    "size": 1,
                    "price": "0.5",
                    "timestamp": 1_704_067_200,
                    "outcomeIndex": 0
                })
            })
            .collect::<Vec<_>>();
        let app = Router::new()
            .route("/activity", get(paged_activity))
            .with_state(ActivityState(Arc::new(rows)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let public = Arc::new(
            ReqwestFetcher::new(reqwest::Client::new())
                .with_max_retries(0)
                .with_min_interval_ms(0),
        );
        let mut source = OrganicCandidateSource::new(public, String::new(), String::new());
        source.data_host = format!("http://{address}");

        let error = source
            .fetch_activity_window(wallet(), None, 1_704_067_300)
            .await
            .unwrap_err();
        let responses = error
            .evidence
            .iter()
            .filter_map(|evidence| match evidence {
                RawEvidence::HttpResponse(response) => Some(response),
                RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(&responses[0].body)
                .unwrap()
                .len(),
            PAGE_SIZE
        );
        assert_eq!(responses[1].body, b"{");
        assert_eq!(responses[0].endpoint_kind, "data-activity");
        assert_eq!(responses[1].endpoint_kind, "data-activity");
    }

    #[test]
    fn pending_stream_preserves_ranking_cross_wallet_seed_and_activity_order() {
        let public = Arc::new(ReqwestFetcher::new(reqwest::Client::new()));
        let mut source = OrganicCandidateSource::new(public, String::new(), String::new());
        source.pending_evidence.extend([
            artifact("ranking"),
            artifact("seed-wallet-a"),
            artifact("seed-wallet-b"),
            artifact("activity-wallet-a"),
        ]);
        let evidence = source.take_idle_evidence();
        let kinds = evidence
            .iter()
            .filter_map(|evidence| match evidence {
                RawEvidence::Artifact(artifact) => Some(artifact.artifact_kind.as_str()),
                RawEvidence::HttpResponse(_) | RawEvidence::HttpTransportFailure(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                "ranking",
                "seed-wallet-a",
                "seed-wallet-b",
                "activity-wallet-a"
            ]
        );
        assert!(source.pending_evidence.is_empty());
    }
}
