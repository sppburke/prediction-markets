use super::release_bench::{cold_restart, complete_snapshot};
use super::resolution_and_cutoffs::retain_source;
use super::*;

use std::collections::VecDeque;
use std::path::Path;

use axum::body::Bytes;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::{IntoResponse, Response};
use pe_venue_polymarket::{
    RELAYER_DEPOSIT_WALLET_NONCE_PATH, RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX,
    RELAYER_SUBMIT_PATH,
};
use serde_json::{Value, json};

const WALLET: &str = "0x1111111111111111111111111111111111111111";
const START: i64 = 2_000_000_000;
const FILL_DEBIT: Decimal = dec!(2.500120);
/// `2^256 - 1`: the pUSD allowance a polymarket.com Deposit Wallet grants each exchange.
const UNLIMITED: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

#[derive(Clone)]
struct Market {
    condition: String,
    tokens: [String; 2],
    winner: u8,
    closed: bool,
    held: bool,
    redeemable: bool,
    /// CLOB answers this market's resolution lookup with 503.
    lookup_down: bool,
}

impl Market {
    fn new(index: u8, winner: u8) -> Self {
        Self {
            condition: format!("0x{}", format!("{:02x}", 0x77 + index).repeat(32)),
            tokens: [
                (123 + u16::from(index) * 2).to_string(),
                (124 + u16::from(index) * 2).to_string(),
            ],
            winner,
            closed: false,
            held: false,
            redeemable: false,
            lookup_down: false,
        }
    }

    fn row(&self) -> Value {
        json!({
            "proxyWallet": WALLET, "asset": self.tokens[0], "conditionId": self.condition,
            "outcomeIndex": 0, "size": "3.125", "redeemable": self.redeemable,
            "curPrice": if self.closed { if self.winner == 0 { "1" } else { "0" } } else { "0.8" },
            "negativeRisk": false,
        })
    }
}

#[derive(Clone, Copy)]
enum Submission {
    Confirm,
    LostAfterExecution,
    KnownPending,
}

struct Script {
    sealed_bundle: String,
    markets: Vec<Market>,
    cash: Decimal,
    next: VecDeque<Submission>,
    submitted: Vec<String>,
    transactions: BTreeMap<String, (String, bool)>,
    requests: BTreeMap<(String, String), usize>,
    pending_first_poll: BTreeSet<String>,
    late_execution: Option<String>,
    custody_bonus: Decimal,
}

impl Script {
    fn execute(&mut self, condition: &str) {
        let market = self
            .markets
            .iter_mut()
            .find(|m| m.condition == condition)
            .unwrap();
        if market.held {
            market.held = false;
            if market.winner == 0 {
                self.cash += dec!(3.125);
            }
            self.cash += self.custody_bonus;
            self.custody_bonus = Decimal::ZERO;
        }
    }
}

async fn response(
    State(script): State<Arc<Mutex<Script>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut script = script.lock().unwrap();
    assert!(headers["host"].to_str().unwrap().starts_with("127.0.0.1:"));
    *script
        .requests
        .entry((method.to_string(), uri.path().to_owned()))
        .or_default() += 1;
    let path = uri.path();
    let payload = match path {
        "/rest/v1/account_credentials" => json!([{
            "account_id": "acct", "bundle_version": 1, "key_id": "key",
            "sealed_bundle": script.sealed_bundle,
        }]),
        "/version" => json!({"version": 2}),
        "/api/geoblock" => json!({"blocked": false, "country": "US"}),
        "/auth/ban-status/closed-only" => json!({"closed_only": false}),
        "/balance-allowance" => json!({
            "balance": CollateralAmount::from_decimal_exact(script.cash).unwrap().atomic().to_string(),
            "allowances": {
                pe_venue_polymarket::CanaryV2Client::standard_spender().unwrap(): UNLIMITED,
                pe_venue_polymarket::CanaryV2Client::negrisk_spender().unwrap(): UNLIMITED,
            },
        }),
        "/positions" => {
            let query: HashMap<String, String> =
                reqwest::Url::parse(&format!("http://127.0.0.1{uri}"))
                    .unwrap()
                    .query_pairs()
                    .into_owned()
                    .collect();
            let redeemable = query.get("redeemable").unwrap() == "true";
            let rows = script
                .markets
                .iter()
                .filter(|m| {
                    (m.held || script.late_execution.as_deref() == Some(m.condition.as_str()))
                        && m.redeemable == redeemable
                })
                .map(Market::row)
                .collect::<Vec<_>>();
            json!(rows)
        }
        RELAYER_DEPOSIT_WALLET_NONCE_PATH => {
            assert_eq!(headers["RELAYER_API_KEY"], "scenario-key");
            let query = reqwest::Url::parse(&format!("http://127.0.0.1{uri}")).unwrap();
            let address = query
                .query_pairs()
                .find(|(key, _)| key == "address")
                .unwrap()
                .1
                .into_owned();
            json!({"address": address, "nonce": script.submitted.len().to_string()})
        }
        RELAYER_SUBMIT_PATH => {
            assert_eq!(method, Method::POST);
            assert_eq!(headers["RELAYER_API_KEY"], "scenario-key");
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                request["depositWalletParams"]["depositWallet"]
                    .as_str()
                    .unwrap()
                    .to_ascii_lowercase(),
                WALLET
            );
            let data = request["depositWalletParams"]["calls"][0]["data"]
                .as_str()
                .unwrap();
            let condition = script
                .markets
                .iter()
                .find(|m| data.contains(m.condition.trim_start_matches("0x")))
                .unwrap()
                .condition
                .clone();
            script.submitted.push(condition.clone());
            let transaction = format!("tx-{}", script.submitted.len());
            match script.next.pop_front().unwrap_or(Submission::Confirm) {
                Submission::LostAfterExecution => {
                    script.execute(&condition);
                    return (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({"error": "reply lost after execution"})),
                    )
                        .into_response();
                }
                Submission::Confirm => {
                    script.execute(&condition);
                    script
                        .transactions
                        .insert(transaction.clone(), (condition, true));
                }
                Submission::KnownPending => {
                    script.pending_first_poll.insert(transaction.clone());
                    script
                        .transactions
                        .insert(transaction.clone(), (condition, false));
                }
            }
            json!({"transactionID": transaction, "state": "STATE_NEW"})
        }
        path if path.starts_with(RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX) => {
            let transaction =
                path.trim_start_matches(RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX);
            // End submit's bounded poll immediately; later passes use the real status reader.
            if script.pending_first_poll.remove(transaction) {
                return StatusCode::BAD_GATEWAY.into_response();
            }
            let (_, confirmed) = script.transactions.get(transaction).unwrap();
            json!({"transaction_id": transaction, "transaction_hash": if *confirmed { Some("0xconfirmed") } else { None }, "state": if *confirmed { "STATE_CONFIRMED" } else { "STATE_MINED" }})
        }
        path if path.starts_with("/markets/") => {
            let condition = path.trim_start_matches("/markets/");
            let market = script
                .markets
                .iter()
                .find(|m| m.condition == condition)
                .unwrap();
            if market.lookup_down {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let mut payload: Value =
                serde_json::from_slice(&economic_source_payloads(condition)[1]).unwrap();
            payload["is_50_50_outcome"] = json!(false);
            payload["closed"] = json!(market.closed);
            payload["active"] = json!(!market.closed);
            payload["tokens"] = json!([
                {"token_id": market.tokens[0], "outcome": "Yes", "price": if market.winner == 0 { "1" } else { "0" }, "winner": market.closed && market.winner == 0},
                {"token_id": market.tokens[1], "outcome": "No", "price": if market.winner == 1 { "1" } else { "0" }, "winner": market.closed && market.winner == 1},
            ]);
            payload
        }
        _ => panic!("unexpected loopback request: {method} {uri}"),
    };
    Json(payload).into_response()
}

fn sealed_credentials(identity: &Identity) -> String {
    use std::io::Write as _;
    let recipient = identity.to_public();
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .unwrap();
    let mut armored =
        age::armor::ArmoredWriter::wrap_output(Vec::new(), age::armor::Format::AsciiArmor).unwrap();
    let mut writer = encryptor.wrap_output(&mut armored).unwrap();
    writer
        .write_all(
            json!({
                "account_id": "acct", "bundle_version": 1, "key_id": "key",
                "private_key": format!("0x{}", "11".repeat(32)), "deposit_wallet": WALLET,
                "api_key": "00000000-0000-0000-0000-000000000000", "api_secret": "c2VjcmV0",
                "api_passphrase": "passphrase", "relayer_api_key": "scenario-key",
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    writer.finish().unwrap();
    String::from_utf8(armored.finish().unwrap()).unwrap()
}

struct LoopbackServer(tokio::task::JoinHandle<()>);

impl Drop for LoopbackServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Harness {
    dir: tempfile::TempDir,
    state: FanoutState,
    script: Arc<Mutex<Script>>,
    server: LoopbackServer,
    clock: Arc<AtomicI64>,
    venue: PolymarketLiveVenue,
    account: AccountContext,
    now: OffsetDateTime,
}

/// Every request must reach the loopback fakes directly: an inherited proxy would route it elsewhere
/// before the fake server could see it.
fn assert_direct_loopback() {
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        assert!(
            std::env::var_os(name).is_none_or(|value| value.is_empty()),
            "unset {name}: the scenario requires direct loopback requests"
        );
    }
}

impl Harness {
    async fn new(kind: Option<&str>, winners: &[u8]) -> Self {
        assert_direct_loopback();
        let identity = Identity::generate();
        let script = Arc::new(Mutex::new(Script {
            sealed_bundle: sealed_credentials(&identity),
            markets: winners
                .iter()
                .enumerate()
                .map(|(i, winner)| Market::new(u8::try_from(i).unwrap(), *winner))
                .collect(),
            cash: dec!(100),
            next: VecDeque::new(),
            submitted: Vec::new(),
            transactions: BTreeMap::new(),
            requests: BTreeMap::new(),
            pending_first_poll: BTreeSet::new(),
            late_execution: None,
            custody_bonus: Decimal::ZERO,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(response).with_state(script.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut row = armed_account_row("acct");
        row.custody_wallet_kind = kind.map(str::to_owned);
        row.custody_wallet_address = Some(WALLET.to_owned());
        let snapshot = LiveAccountsSnapshot::from_rows(
            vec![row],
            &[CredentialMetaRow {
                account_id: "acct".to_owned(),
                bundle_version: 1,
                key_id: "key".to_owned(),
            }],
        );
        let mut state = fanout_state(&dir, db, snapshot, &base, Some(identity));
        state.config.clob_base_url = base.clone();
        state.config.data_base_url = base.clone();
        state.config.relayer_base_url = base;
        let clock = Arc::new(AtomicI64::new(START));
        let read_clock = clock.clone();
        state.clock = Arc::new(move || {
            OffsetDateTime::from_unix_timestamp(read_clock.load(Ordering::SeqCst)).unwrap()
        });
        let target = DispatchTargetRow {
            dispatch_id: "fixture".to_owned(),
            account_id: "acct".to_owned(),
            exec_rank: 0,
            credential_bundle_version: 1,
            credential_key_id: "key".to_owned(),
            state: "pending".to_owned(),
            terminal_reason: None,
            updated_at_unix: START,
        };
        let CredentialLoad::Ready(credentials) = credentials_for_target(&state, &target).await
        else {
            panic!("loopback credentials unavailable")
        };
        let venue =
            PolymarketLiveVenue::from_credentials(&credentials, &state.config.clob_base_url)
                .await
                .unwrap();
        let account = state.config.live_accounts.snapshot().accounts[0].clone();
        let mut harness = Self {
            dir,
            state,
            script,
            server: LoopbackServer(server),
            clock,
            venue,
            account,
            now: OffsetDateTime::from_unix_timestamp(START).unwrap(),
        };
        harness.gate().await;
        harness
    }

    fn at(&mut self, unix: i64) {
        self.now = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        self.clock.store(unix, Ordering::SeqCst);
    }

    async fn pass(&mut self) {
        drive_redemptions(&mut self.state, self.now).await;
        self.assert_oracle();
    }

    fn cash(&self) -> CollateralAmount {
        CollateralAmount::from_decimal_exact(self.script.lock().unwrap().cash).unwrap()
    }

    async fn gate(&mut self) {
        let account_state = self.venue.read_balance_and_allowance(false).await.unwrap();
        let outcome = ensure_live_portfolio_marks_at(
            &mut self.state,
            &self.account,
            &account_state,
            self.venue.account_binding(),
            WALLET,
            self.now,
        )
        .await;
        assert!(
            matches!(outcome, CheckOutcome::Pass),
            "mark gate: {outcome:?}"
        );
        assert!(matches!(
            live_financial_posture(&self.state, &self.account, self.cash(), false),
            CheckOutcome::Pass
        ));
        self.assert_oracle();
    }

    fn summary(&self) -> ProjectionDerivation {
        live_summary(&self.state, &self.account.account_id).unwrap()
    }

    fn records(&self) -> Vec<LiveJournalEvent> {
        replay_live_account(&self.state, &self.account.account_id).unwrap()
    }

    fn assert_oracle(&self) {
        let events = self.records();
        let mut oracle =
            derive_projection_rows_for_state(&self.state, &self.account.account_id, &events)
                .unwrap();
        let full = sync_live(&self.state)
            .unwrap()
            .derivation(&self.account.account_id)
            .unwrap();
        assert_eq!(projection_snapshot(&full), projection_snapshot(&oracle));
        oracle.fills.clear();
        oracle.validated_prepared_sequences.clear();
        assert_summary(&self.summary(), &oracle);
        let live = sync_live(&self.state).unwrap();
        let view = live.view(&self.account.account_id).unwrap().unwrap();
        assert_eq!(view.applied.len(), events.len());
        assert!(view.applied.values().all(|count| *count == 1));
        assert!(
            view.financial
                .late_applied
                .values()
                .all(|count| *count == 1)
        );
    }

    fn attempts(&self) -> BTreeMap<String, RedemptionAttemptState> {
        let live = sync_live(&self.state).unwrap();
        live.view(&self.account.account_id)
            .unwrap()
            .unwrap()
            .financial
            .main
            .redemption_attempts
            .as_map()
            .values()
            .map(|a| (a.identity.condition_id.0.clone(), a.state.clone()))
            .collect()
    }

    fn condition(&self, index: usize) -> String {
        self.script.lock().unwrap().markets[index].condition.clone()
    }

    fn submissions(&self) -> Vec<String> {
        self.script.lock().unwrap().submitted.clone()
    }

    fn close(&self) {
        for market in &mut self.script.lock().unwrap().markets {
            market.closed = true;
            market.redeemable = true;
        }
    }

    /// Whether a fresh pending copy target of this account would be dispatched now.
    fn dispatches(&self) -> bool {
        let armed = self.state.config.live_accounts.snapshot().accounts[0].is_armed();
        classify_target(
            "pending",
            true,
            armed,
            self.state.closures.reason("acct").is_some(),
        ) == TargetClass::Dispatch
    }

    /// Admit, prepare and expire an order before POST, as the dispatcher does when its book
    /// ages out: the admission is consumed and nothing reaches the venue.
    fn expire_before_post(&self, prepared: &pe_execution_core::LiveOrderPreparedAudit) {
        append_approved_admission(
            &self.state.config.journal,
            &self.account.account_id,
            prepared,
            self.now,
        );
        for payload in [
            LiveJournalPayload::OrderPrepared(Arc::new(prepared.clone())),
            LiveJournalPayload::OrderPreparationFailed(Box::new(
                pe_execution_core::LiveOrderPreparationFailedAudit {
                    identity: prepared.identity.clone(),
                    failure: pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired,
                    control: None,
                },
            )),
        ] {
            self.state
                .config
                .journal
                .append(self.account.account_id.clone(), self.now, payload)
                .unwrap();
        }
    }

    fn custody_count(&self, condition: &str) -> usize {
        self.records().iter().filter(|e| matches!(&e.payload, LiveJournalPayload::RedemptionCustodyReconciled(a) if a.identity.condition_id.0 == condition)).count()
    }

    fn deadline(&self) -> i64 {
        let RedemptionAttemptState::SubmissionReserved {
            deadline_unix: Some(deadline),
            ..
        } = self.attempts()[&self.condition(0)]
        else {
            panic!("expected reserved submission")
        };
        assert_eq!(
            deadline,
            u64::try_from(self.now.unix_timestamp()).unwrap()
                + DEPOSIT_WALLET_REDEMPTION_DEADLINE_SECS
        );
        i64::try_from(deadline).unwrap()
    }

    async fn candidate(
        &self,
        index: u8,
        dispatch: &str,
    ) -> Box<pe_execution_core::LiveOrderPreparedAudit> {
        let market = Market::new(index, 0);
        let mut prepared = finality_prepared();
        prepared.identity = projection_identity(dispatch);
        prepared.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(dispatch, &self.account.account_id);
        prepared
            .identity
            .fill_projection
            .as_mut()
            .unwrap()
            .market_id = market.condition.clone();
        prepared.economic.market.condition_id = PolymarketConditionId(market.condition.clone());
        prepared.economic.market.market_id = market.condition.clone();
        prepared.economic.market.token_id =
            pe_core_types::PolymarketTokenId(market.tokens[0].clone());
        prepared.economic.admission.market.condition_id =
            prepared.economic.market.condition_id.clone();
        prepared.economic.admission.market.ordered_outcome_token_ids =
            market.tokens.clone().map(pe_core_types::PolymarketTokenId);
        prepared.economic.admission.settlement.condition_id =
            prepared.economic.market.condition_id.clone();
        prepared.prepared.condition_id = prepared.economic.market.condition_id.clone();
        prepared.prepared.token_id = prepared.economic.market.token_id.clone();
        prepared.prepared.order_hash = format!("0x{}", format!("{:02x}", 0x22 + index).repeat(32));
        prepared.economic.balance.cash_before = self.cash();
        prepared.account_state =
            unlimited_account_state(self.cash(), self.now, self.venue.account_binding());
        assert_eq!(
            prepared.account_state.allowance,
            CollateralAmount::from_atomic(u64::MAX)
        );
        bind_empty_live_risk_to_paper_prefix(&self.state, &mut prepared, self.now);
        append_economic_sources(
            &self.state,
            &mut prepared,
            &self.account.account_id,
            self.now,
            None,
        )
        .await;
        // Reuse the recorded market fixture, substituting this condition's token identities.
        let payloads = economic_source_payloads(&market.condition);
        let mut receipts = Vec::new();
        let mut payload_hash = String::new();
        for (source, payload) in [
            GAMMA_MARKETS_SOURCE_ID,
            CLOB_LONG_MARKET_SOURCE_ID,
            CLOB_COMPACT_MARKET_SOURCE_ID,
            CLOB_BOOK_SOURCE_ID,
        ]
        .into_iter()
        .zip(payloads)
        {
            let payload = String::from_utf8(payload)
                .unwrap()
                .replace("123", &market.tokens[0])
                .replace("124", &market.tokens[1])
                .into_bytes();
            if source == CLOB_LONG_MARKET_SOURCE_ID {
                payload_hash = blake3::hash(&payload).to_hex().to_string();
            }
            receipts.push(
                self.state
                    .config
                    .source_log
                    .append(EnvelopeIn {
                        source_id: SourceId(source.to_owned()),
                        schema_version: 1,
                        parser_version: 1,
                        observed_at: SourceTimestamp(self.now),
                        received_at: ReceivedAt(self.now),
                        content_type: ContentType::Json,
                        payload,
                    })
                    .await
                    .unwrap(),
            );
        }
        prepared.economic.admission.receipts = AdmissionReceipts {
            gamma: receipts[0],
            clob_long: receipts[1],
            clob_compact: receipts[2],
        };
        prepared.economic.book_receipt = receipts[3];
        prepared.economic.admission.settlement.raw_evidence_hash = payload_hash;
        let derived = self.summary();
        let mut price_receipts = Vec::new();
        for position in &derived.positions {
            let source = gamma_risk_price_source(
                fixture_receipt(0),
                &position.market_id,
                ["0.8", "0.2"],
                self.now,
            );
            price_receipts.push(retain_source(&self.state, source).await);
        }
        let sources = price_receipts
            .iter()
            .map(|r| {
                self.state
                    .config
                    .source_receipts
                    .source_envelope(*r)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mids = replay_live_risk_prices(
            &derived.positions,
            &price_receipts,
            unix_millis_i64(self.now),
            &sources,
            Some(&self.state.config.source_receipts),
        )
        .unwrap();
        let frames = self.state.config.paper_log.snapshot().unwrap();
        let projection = prepared.identity.fill_projection.as_ref().unwrap();
        prepared.economic.risk = compose_live_risk_audit(
            &derived,
            &paper_era_at_evaluation(&frames, unix_millis_i64(self.now)).unwrap(),
            &mids,
            &price_receipts,
            self.now,
            LiveRiskProposal {
                leader_wallet: Some(projection.leader_wallet.clone()),
                market_id: Some(projection.market_id.clone()),
                proposed_debit: prepared.economic.balance.worst_case_debit,
                per_trade_cap_bps: 10_000,
                drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
            },
        )
        .unwrap();
        prepared
    }

    async fn fill(&mut self, index: u8) {
        let prepared = self
            .candidate(index, &format!("scenario-fill-{index}"))
            .await;
        append_approved_admission(
            &self.state.config.journal,
            &self.account.account_id,
            &prepared,
            self.now,
        );
        let event = self
            .state
            .config
            .journal
            .append(
                self.account.account_id.clone(),
                self.now,
                LiveJournalPayload::OrderPrepared(prepared.clone().into()),
            )
            .unwrap();
        let transaction = format!("0x{}", format!("{:02x}", 0x11 + index).repeat(32));
        let mut matched = finality_matched_event(&self.account.account_id, &prepared, 0, self.now);
        let LiveJournalPayload::OrderReconciled(audit) = &mut matched.payload else {
            unreachable!()
        };
        let LiveJournalOrderOutcome::Matched {
            transaction_hashes, ..
        } = &mut Arc::make_mut(audit).outcome
        else {
            unreachable!()
        };
        *transaction_hashes = vec![transaction.clone()];
        self.state
            .config
            .journal
            .append(self.account.account_id.clone(), self.now, matched.payload)
            .unwrap();
        let mut fill = finalized_fill_event(&self.account.account_id, &prepared, 0, event.seq);
        let LiveJournalPayload::OrderFillFinalized(audit) = &mut fill.payload else {
            unreachable!()
        };
        let audit = Arc::make_mut(audit);
        audit.matched_logs[0].transaction_hash = transaction.clone();
        let RawHttpAttempt::Response(receipt) = &mut audit.receipts[1] else {
            unreachable!()
        };
        receipt.ordered_query[1].1 = json!([transaction.clone()]).to_string();
        let token = 123 + u16::from(index) * 2;
        receipt.body = String::from_utf8(receipt.body.clone())
            .unwrap()
            .replace(&format!("0x{}", "11".repeat(32)), &transaction)
            .replace(
                &format!("0x{}", "22".repeat(32)),
                &prepared.prepared.order_hash,
            )
            .replace(&format!("{:064x}", 123), &format!("{token:064x}"))
            .into_bytes();
        self.state
            .config
            .journal
            .append(self.account.account_id.clone(), self.now, fill.payload)
            .unwrap();
        {
            let mut script = self.script.lock().unwrap();
            script.markets[usize::from(index)].held = true;
            script.cash -= FILL_DEBIT;
        }
        self.assert_oracle();
    }
}

/// The account audit the production classifier derives from a Deposit Wallet's answer: the
/// fixture's evidence carrying the wallet's unlimited allowances.
fn unlimited_account_state(
    cash: CollateralAmount,
    observed_at: OffsetDateTime,
    binding: &LiveAccountBindingAudit,
) -> pe_execution_core::LiveAccountStateAudit {
    let mut evidence = account_state_fixture_for_binding(cash, observed_at, binding).evidence;
    for attempt in &mut evidence {
        if let RawHttpAttempt::Response(response) = attempt
            && response.endpoint_kind == "balance-allowance"
        {
            response.body = serde_json::to_vec(&json!({
                "balance": cash.atomic().to_string(),
                "allowances": {
                    pe_venue_polymarket::CanaryV2Client::standard_spender().unwrap(): UNLIMITED,
                    pe_venue_polymarket::CanaryV2Client::negrisk_spender().unwrap(): UNLIMITED,
                },
            }))
            .unwrap();
        }
    }
    let spender = pe_venue_polymarket::CanaryV2Client::standard_spender().unwrap();
    let request_descriptor_hashes = account_request_descriptor_hashes(&evidence, binding);
    let LiveAccountResponseClassification::State(state) = classify_live_account_responses(
        &evidence,
        &spender,
        &request_descriptor_hashes,
        None,
        binding,
    )
    .unwrap() else {
        panic!("an unlimited allowance must classify as account state")
    };
    state.audit().unwrap()
}

fn assert_summary(actual: &ProjectionDerivation, expected: &ProjectionDerivation) {
    assert_eq!(projection_snapshot(actual), projection_snapshot(expected));
}

/// PASS: NULL and deposit-wallet custody submit; other kinds expose the exact closure without submission.
/// FAIL: supported custody cannot submit or unsupported custody sends a nonce/submit request.
#[tokio::test]
async fn redemption_driver_custody_guard() {
    for kind in [
        None,
        Some("deposit_wallet"),
        Some("proxy"),
        Some("safe"),
        Some("eoa"),
    ] {
        let mut h = Harness::new(kind, &[0]).await;
        h.fill(0).await;
        h.close();
        h.pass().await;
        if let Some(kind @ ("proxy" | "safe" | "eoa")) = kind {
            assert!(h.submissions().is_empty());
            assert_eq!(
                h.state.closures.reason("acct"),
                Some(format!("redemption submission unsupported for {kind} custody").as_str())
            );
            assert!(
                !h.script
                    .lock()
                    .unwrap()
                    .requests
                    .keys()
                    .any(|(_, path)| path == RELAYER_DEPOSIT_WALLET_NONCE_PATH
                        || path == RELAYER_SUBMIT_PATH)
            );
        } else {
            assert_eq!(h.submissions(), [h.condition(0)]);
        }
    }
}

/// PASS: a lost execution stays reserved through its deadline, retries the absent family, and credits custody once.
/// FAIL: an early/other-family submission, duplicate payout, or live/file replay disagreement occurs.
#[tokio::test]
async fn redemption_driver_lost_submission_retries_zero_balance_once() {
    let mut h = Harness::new(None, &[0]).await;
    h.fill(0).await;
    h.close();
    h.script
        .lock()
        .unwrap()
        .next
        .push_back(Submission::LostAfterExecution);
    h.pass().await;
    let deadline = h.deadline();
    assert_eq!(h.state.closures.reason("acct"), Some("redemption pending"));
    for now in [START + REDEMPTION_RETRY_SECS, deadline - 1, deadline] {
        h.at(now);
        h.pass().await;
        assert_eq!(h.submissions(), [h.condition(0)]);
        assert_eq!(h.state.closures.reason("acct"), Some("redemption pending"));
    }
    h.at(deadline + 1);
    h.pass().await;
    h.pass().await;
    assert_eq!(h.submissions(), [h.condition(0), h.condition(0)]);
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    let requests = h
        .records()
        .into_iter()
        .filter_map(|e| {
            if let LiveJournalPayload::RedemptionRequested(a) = e.payload {
                Some(a)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(requests[1].redeemable_balance, CollateralAmount::ZERO);
    assert_eq!(
        h.summary().economic_cash,
        Some(dec!(100) - FILL_DEBIT + dec!(3.125))
    );
    let late_condition = h.condition(0);
    h.script.lock().unwrap().late_execution = Some(late_condition);
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert_eq!(h.submissions(), [h.condition(0), h.condition(0)]);
}

/// PASS: a family whose submission is lost twice is retried after each journaled deadline, at zero
/// balance once the first one executed, until a submission is confirmed; custody completes once.
/// FAIL: a retry before its deadline, a stuck reservation, or a double-counted payout.
#[tokio::test]
async fn redemption_driver_retries_lost_submissions_until_confirmed() {
    let mut h = Harness::new(None, &[0]).await;
    h.fill(0).await;
    h.close();
    h.script.lock().unwrap().next.extend([
        Submission::LostAfterExecution,
        Submission::LostAfterExecution,
    ]);
    h.pass().await;
    for attempt in 2..=3 {
        let deadline = h.deadline();
        h.at(deadline);
        h.pass().await;
        assert_eq!(h.submissions().len(), attempt - 1);
        h.at(deadline + 1);
        h.pass().await;
        assert_eq!(h.submissions().len(), attempt);
    }
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    let balances = h
        .records()
        .into_iter()
        .filter_map(|e| match e.payload {
            LiveJournalPayload::RedemptionRequested(a) => Some(a.redeemable_balance),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        balances[1..],
        [CollateralAmount::ZERO, CollateralAmount::ZERO]
    );
    assert_eq!(
        h.summary().economic_cash,
        Some(dec!(100) - FILL_DEBIT + dec!(3.125))
    );
}

/// PASS: a reserved A blocks B; a known pending A is polled until confirmation before B starts.
/// FAIL: B is submitted while A is unresolved, or polling starts a replacement submission.
#[tokio::test]
async fn redemption_driver_one_family_before_the_next() {
    for submission in [Submission::LostAfterExecution, Submission::KnownPending] {
        let mut h = Harness::new(None, &[0, 0]).await;
        h.fill(0).await;
        h.fill(1).await;
        h.close();
        h.script.lock().unwrap().next.push_back(submission);
        h.pass().await;
        for tick in 1..=2 {
            h.at(START + tick * REDEMPTION_RETRY_SECS);
            h.pass().await;
            assert_eq!(h.submissions(), [h.condition(0)]);
        }
        match submission {
            Submission::LostAfterExecution => {
                let RedemptionAttemptState::SubmissionReserved {
                    deadline_unix: Some(deadline),
                    ..
                } = h.attempts()[&h.condition(0)]
                else {
                    unreachable!()
                };
                h.at(i64::try_from(deadline).unwrap() + 1);
                h.pass().await;
            }
            Submission::KnownPending => {
                let condition = h.condition(0);
                {
                    let mut script = h.script.lock().unwrap();
                    assert!(
                        script.requests[&(
                            "GET".to_owned(),
                            format!("{RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX}tx-1")
                        )] >= 3
                    );
                    script.transactions.get_mut("tx-1").unwrap().1 = true;
                    script.execute(&condition);
                }
                h.pass().await;
            }
            Submission::Confirm => unreachable!(),
        }
        h.pass().await;
        assert_eq!(h.custody_count(&h.condition(0)), 1);
        h.pass().await;
        assert_eq!(h.submissions().last(), Some(&h.condition(1)));
    }
}

/// PASS: a condition whose later order failed before POST still resolves, redeems and completes
/// custody.
/// FAIL: the terminal pre-POST failure keeps the condition out of resolution discovery.
#[tokio::test]
async fn redemption_driver_resolves_after_pre_post_failure() {
    let mut h = Harness::new(None, &[0]).await;
    h.fill(0).await;
    let mut failed = h.candidate(0, "pre-post-failed").await;
    failed.prepared.order_hash = format!("0x{}", "44".repeat(32));
    h.expire_before_post(&failed);
    h.assert_oracle();
    h.close();
    h.pass().await;
    assert_eq!(h.submissions(), [h.condition(0)]);
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
}

/// PASS: while one condition's resolution lookup keeps failing, another condition's confirmed
/// redemption reaches custody and admission stays open; the failing condition resolves once its
/// lookup recovers.
/// FAIL: the failed lookup holds back custody, closes admission, or is never retried.
#[tokio::test]
async fn redemption_driver_proceeds_while_a_resolution_lookup_fails() {
    let mut h = Harness::new(None, &[0, 0]).await;
    h.fill(0).await;
    h.fill(1).await;
    {
        let mut script = h.script.lock().unwrap();
        script.markets[0].closed = true;
        script.markets[0].redeemable = true;
    }
    h.pass().await;
    assert_eq!(h.submissions(), [h.condition(0)]);
    {
        let mut script = h.script.lock().unwrap();
        script.markets[1].closed = true;
        script.markets[1].redeemable = true;
        script.markets[1].lookup_down = true;
    }
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert!(h.dispatches());
    assert!(
        !h.records()
            .iter()
            .any(|e| matches!(&e.payload, LiveJournalPayload::ResolutionFinalized(r) if r.condition_id.0 == h.condition(1)))
    );
    h.script.lock().unwrap().markets[1].lookup_down = false;
    h.pass().await;
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(1)), 1);
    assert!(h.dispatches());
}

/// PASS: an account switched off before its losing position resolves still redeems it and
/// completes custody.
/// FAIL: a zero payout on an unarmed account is skipped, leaving its custody work forever.
#[tokio::test]
async fn redemption_driver_completes_losing_custody_for_an_unarmed_account() {
    let mut h = Harness::new(None, &[1]).await;
    h.fill(0).await;
    let mut off = h.state.config.live_accounts.snapshot().as_ref().clone();
    off.accounts[0].requested_live_mode = "off".to_owned();
    off.accounts[0].effective_live_mode = "off".to_owned();
    h.state.config.live_accounts.store(off);
    h.close();
    h.pass().await;
    assert_eq!(
        h.summary().receivable_by_condition[&h.condition(0)],
        CollateralAmount::ZERO
    );
    assert_eq!(h.submissions(), [h.condition(0)]);
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert!(h.summary().receivable_by_condition.is_empty());
}

/// PASS: redeemable Data API rows preserve financial gates but cannot trigger submission before journal resolution.
/// FAIL: a redeemable flag alone closes the gates or causes a Relayer POST.
#[tokio::test]
async fn redemption_driver_waits_for_journal_resolution() {
    let mut h = Harness::new(None, &[0]).await;
    h.fill(0).await;
    h.script.lock().unwrap().markets[0].redeemable = true;
    h.gate().await;
    h.pass().await;
    assert!(h.submissions().is_empty());
    assert!(
        !h.records()
            .iter()
            .any(|e| matches!(e.payload, LiveJournalPayload::ResolutionFinalized(_)))
    );
    h.close();
    h.pass().await;
    assert_eq!(h.submissions(), [h.condition(0)]);
    assert_eq!(
        h.records()
            .iter()
            .filter(|e| matches!(e.payload, LiveJournalPayload::ResolutionFinalized(_)))
            .count(),
        1
    );
}

/// PASS: journal custody work selects 30 seconds and the last reconciled custody restores 300 seconds.
/// FAIL: the cadence ignores outstanding custody or remains fast after completion.
#[tokio::test]
async fn redemption_driver_cadence_tracks_custody_work() {
    let mut h = Harness::new(None, &[0]).await;
    h.fill(0).await;
    h.close();
    h.pass().await;
    assert_eq!(redemption_cadence_secs(&h.state), 30);
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert_eq!(redemption_cadence_secs(&h.state), 300);
}

/// The end-to-end scenario's name, and the variable that makes a run of it the fresh process that
/// reopens a finished scenario's files.
const END_TO_END: &str =
    "live_fanout::tests::unattended_scenario::redemption_driver_unattended_end_to_end";
const REOPEN_SAVED: &str = "PE_UNATTENDED_SCENARIO_SAVED";

/// PASS: unattended winners/loser, lost reply, midnight, custody surplus, rebate, and a fresh
/// process reopening the files, replay exactly once.
/// FAIL: a due confirmation/custody needs more than two passes, submits early, double counts, or disagrees with replay.
#[tokio::test]
async fn redemption_driver_unattended_end_to_end() {
    if let Some(saved) = std::env::var_os(REOPEN_SAVED) {
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(saved).unwrap()).unwrap();
        cold_restart(
            Path::new(saved["fixture"].as_str().unwrap()),
            &AccountId::new(saved["account"].as_str().unwrap()).unwrap(),
            OffsetDateTime::from_unix_timestamp(saved["now_unix"].as_i64().unwrap()).unwrap(),
            &saved["snapshot"],
            "scenario",
            END_TO_END,
        );
        return;
    }
    let mut h = Harness::new(None, &[0, 0, 1]).await;
    for index in 0..3 {
        h.fill(index).await;
    }
    for market in &mut h.script.lock().unwrap().markets {
        market.redeemable = true;
    }
    h.gate().await;
    h.pass().await;
    assert!(h.submissions().is_empty());
    h.close();
    h.script
        .lock()
        .unwrap()
        .next
        .push_back(Submission::LostAfterExecution);
    h.pass().await;
    assert_eq!(
        h.summary().receivable_by_condition[&h.condition(2)],
        CollateralAmount::ZERO
    );
    let deadline = h.deadline();
    h.at(deadline);
    h.pass().await;
    assert_eq!(h.state.closures.reason("acct"), Some("redemption pending"));
    assert_eq!(h.submissions(), [h.condition(0)]);
    h.at(deadline + 1);
    h.pass().await;
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert_eq!(h.summary().receivable, dec!(3.125));
    // A's custody reopens admission while B's payout is outstanding and not yet submitted.
    assert!(h.dispatches());
    let before = h.summary();
    let candidate = h.candidate(3, "admitted-with-B-outstanding").await;
    assert_eq!(
        candidate.economic.balance.cash_before.to_decimal(),
        before.economic_cash.unwrap() - before.receivable
    );
    h.expire_before_post(&candidate);
    h.assert_oracle();
    let late_condition = h.condition(0);
    h.script.lock().unwrap().late_execution = Some(late_condition);
    h.script
        .lock()
        .unwrap()
        .next
        .push_back(Submission::KnownPending);
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(0)), 1);
    assert_eq!(
        h.submissions(),
        [h.condition(0), h.condition(0), h.condition(1)]
    );
    // B's pending transaction closes admission until its custody.
    assert_eq!(h.state.closures.reason("acct"), Some("redemption pending"));
    assert!(!h.dispatches());
    h.script.lock().unwrap().late_execution = None;
    let midnight = (START.div_euclid(86_400) + 1) * 86_400;
    h.at(midnight + 1);
    h.gate().await;
    assert_eq!(
        h.summary().daily_marks[&midnight].to_decimal(),
        before.economic_cash.unwrap()
    );
    {
        let condition = h.condition(1);
        let mut script = h.script.lock().unwrap();
        script.custody_bonus = dec!(10);
        script.execute(&condition);
        script.transactions.get_mut("tx-3").unwrap().1 = true;
    }
    h.pass().await;
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(1)), 1);
    assert!(h.dispatches());
    assert_eq!(h.summary().receivable, Decimal::ZERO);
    assert_eq!(
        redemption_cadence_secs(&h.state),
        30,
        "zero-payout custody remains work"
    );
    assert_eq!(
        h.summary().economic_cash,
        Some(before.economic_cash.unwrap() + dec!(10))
    );
    h.pass().await;
    h.pass().await;
    assert_eq!(h.custody_count(&h.condition(2)), 1);
    assert!(h.dispatches());
    assert!(h.summary().receivable_by_condition.is_empty());
    assert_eq!(redemption_cadence_secs(&h.state), 300);
    h.script.lock().unwrap().cash += dec!(3);
    h.gate().await;
    assert_eq!(
        h.summary().economic_cash,
        Some(before.economic_cash.unwrap() + dec!(13))
    );
    assert_eq!(h.summary().baseline_equity.unwrap().to_decimal(), dec!(100));
    let credits = h
        .records()
        .into_iter()
        .filter_map(|e| {
            if let LiveJournalPayload::CollateralCredited(a) = e.payload {
                Some(a.credited)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        credits,
        [CollateralAmount::from_decimal_exact(dec!(3)).unwrap()]
    );
    // Every recorded account read carries the wallets' unlimited allowance as u64::MAX: the
    // admissions and orders built from it, and the marks, custody and credit from the venue reads.
    let mut account_reads = BTreeMap::<&str, usize>::new();
    for event in h.records() {
        let (kind, allowance) = match &event.payload {
            LiveJournalPayload::AdmissionEvaluated(admission) => (
                "admission",
                admission.account_state.as_ref().unwrap().allowance,
            ),
            LiveJournalPayload::OrderPrepared(prepared) => {
                ("prepared", prepared.account_state.allowance)
            }
            LiveJournalPayload::AccountPortfolioMarked(mark) => {
                ("mark", mark.account_state.allowance)
            }
            LiveJournalPayload::RedemptionCustodyReconciled(custody) => {
                ("custody", custody.account_state.allowance)
            }
            LiveJournalPayload::CollateralCredited(credit) => {
                ("credit", credit.account_state.allowance)
            }
            _ => continue,
        };
        assert_eq!(allowance, CollateralAmount::from_atomic(u64::MAX), "{kind}");
        *account_reads.entry(kind).or_default() += 1;
    }
    assert_eq!(
        account_reads.keys().copied().collect::<Vec<_>>(),
        ["admission", "credit", "custody", "mark", "prepared"]
    );
    let saved = serde_json::json!({
        "fixture": h.dir.path(),
        "account": h.account.account_id.as_str(),
        "now_unix": h.now.unix_timestamp(),
        "snapshot": complete_snapshot(&h.state, &h.account.account_id),
    });
    let Harness {
        dir,
        state,
        server: _server,
        ..
    } = h;
    // The fresh process must acquire its own writers after this one's owners have stopped.
    drop(state);
    wait_for_source_writer(&dir.path().join("live-source.log")).await;
    let saved_path = dir.path().join("expected.json");
    std::fs::write(&saved_path, serde_json::to_vec(&saved).unwrap()).unwrap();
    let reopened = std::process::Command::new(std::env::current_exe().unwrap())
        .args([END_TO_END, "--exact", "--nocapture"])
        .env(REOPEN_SAVED, &saved_path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&reopened.stdout);
    assert!(
        reopened.status.success()
            && stdout.contains("RESTART process=scenario")
            && stdout.contains("1 passed"),
        "{stdout}\n{}",
        String::from_utf8_lossy(&reopened.stderr)
    );
}
