#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use pe_core_types::{LeaderAction, Probability, WalletAddress};
use rust_decimal_macros::dec;

const ORACLE_BASE_REVISION: &str = "dfa00e1c118f3ca21d5bf8a3fd6d6cd36fc19d2d";
const ORACLE_REGENERATOR_COMMAND: &str = "cargo nextest run -p pe-service regenerate_qualification_selection_oracle --run-ignored ignored-only";

fn activity_observation(sequence: u64, payload: &[u8]) -> SourceObservation {
    let at = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
    SourceObservation {
        receipt: AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(payload),
        },
        observed_at: SourceTimestamp(at),
        received_at: ReceivedAt(at),
        received_unix_ms: 1_700_000_100_000,
        source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
        schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        content_type: ContentType::Json,
        payload: payload.to_vec(),
    }
}

fn activity_row(
    condition: &str,
    asset: &str,
    size: &str,
    usdc_size: &str,
    transaction_hash: &str,
    timestamp: i64,
) -> serde_json::Value {
    serde_json::json!({
        "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "type": "TRADE",
        "conditionId": condition,
        "asset": asset,
        "side": "BUY",
        "size": size,
        "usdcSize": usdc_size,
        "price": "0.5",
        "timestamp": timestamp.to_string(),
        "transactionHash": transaction_hash,
        "outcomeIndex": "0"
    })
}

fn activity_payload(rows: Vec<serde_json::Value>) -> Vec<u8> {
    serde_json::to_vec(&rows).unwrap()
}

fn page_evidence(
    url: &str,
    payload: &[u8],
    start: Option<i64>,
    end: i64,
    offset: u32,
) -> ReconciliationPageEvidence {
    let row_count = serde_json::from_slice::<Vec<serde_json::Value>>(payload)
        .unwrap()
        .len();
    ReconciliationPageEvidence {
        request_url: url.to_owned(),
        bounds: Some(pe_source_polymarket_public::ActivityRequestBounds { start, end }),
        partition: None,
        offset,
        row_count: u32::try_from(row_count).unwrap(),
        canonical_page_hash: pe_source_polymarket_public::canonical_page_hash(payload).unwrap(),
        raw_page_hash: blake3::hash(payload).to_hex().to_string(),
        received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
        schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
    }
}

fn activity_request_url(start: Option<i64>, end: i64, offset: u32) -> String {
    pe_source_polymarket_public::PolymarketEndpoint::UserPositionActivityPage {
        user: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        end,
        start: start.map(|start| start.saturating_add(1)),
        offset,
    }
    .url("https://data-api.polymarket.com")
}

fn page_occurrence(observation: &SourceObservation, url: &str, payload: &[u8]) -> PageOccurrence {
    PageOccurrence {
        request_url: url.to_owned(),
        raw_hash: blake3::hash(payload).to_hex().to_string(),
        receipt: observation.receipt,
    }
}

fn activity_page_pair(
    observation: &SourceObservation,
    payload: &[u8],
    start: Option<i64>,
    end: i64,
    offset: u32,
) -> (PageOccurrence, ReconciliationPageEvidence) {
    let url = activity_request_url(start, end, offset);
    (
        page_occurrence(observation, &url, payload),
        page_evidence(&url, payload, start, end, offset),
    )
}

fn parsed_aggregates(payloads: &[&[u8]]) -> Vec<pe_source_polymarket_public::ActivityAggregate> {
    let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let mut rows = Vec::new();
    for payload in payloads {
        let observation = activity_observation(0, payload);
        rows.extend(
            parse_activity_response(
                payload,
                wallet,
                &ActivityParseContext {
                    source_id: SourceId(observation.source_id),
                    observed_at: observation.observed_at,
                    received_at: observation.received_at,
                    transport: ActivityTransport::Replay,
                },
            )
            .unwrap()
            .rows,
        );
    }
    aggregate_activity_rows(&rows).unwrap()
}

fn classification_fixture() -> (DecisionContinuationV3, LedgerMutation) {
    let wallet = WalletAddress::from_hex(&format!("0x{}", "a".repeat(40))).unwrap();
    let source_trade_id = SourceTradeId("g2:classification-fixture".to_owned());
    let market_id = MarketId(VenueMarketId("classification-market".to_owned()));
    let source_epoch = 1_700_000_000;
    let configuration = RuntimeConfig::from_service_config(&ServiceConfig::default());
    let continuation = DecisionContinuationV3::new(
        DecisionContinuationFacts {
            source_trade_id: source_trade_id.clone(),
            semantic_revision: "semantic-v3".to_owned(),
            transaction_hash: "0xclassification".to_owned(),
            wallet,
            source_epoch,
            market_id: market_id.clone(),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            share_amount: ShareAmount::from_whole(1).unwrap(),
            provenance: pe_copy_signal_engine::TradeProvenance::RestPoll,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: configuration.canonical_hash(),
            applied_configuration: configuration,
            frozen_basis: crate::bucket_commit::FrozenDecisionBasis {
                win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                bankroll: dec!(100),
            },
            decision_inputs: serde_json::json!({"fixture": "classification"}),
        },
        None,
        Vec::new(),
        None,
    );
    let mutation = LedgerMutation {
        source_trade_id,
        transaction_hash: "0xclassification".to_owned(),
        wallet,
        source_time: SourceTimestamp(OffsetDateTime::from_unix_timestamp(source_epoch).unwrap()),
        effect: pe_position_ledger::LedgerEffect::Trade {
            market_id,
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            amount: ShareAmount::from_whole(1).unwrap(),
            price: Price::new(dec!(0.5)).unwrap(),
        },
    };
    (continuation, mutation)
}

fn store_read_decision(
    state: &PaperStateDb,
    continuation: &DecisionContinuationV3,
    disposition: &str,
    pending: bool,
) {
    let frozen = &continuation.facts;
    let proof_json = AppliedEffect {
        effect: pe_position_ledger::LedgerEffect::Trade {
            market_id: frozen.market_id.clone(),
            outcome_id: frozen.outcome_id,
            side: frozen.side,
            amount: frozen.share_amount,
            price: frozen.price,
        },
        clamped_residual: None,
    }
    .to_document()
    .unwrap();
    state
        .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
            wallet: frozen.wallet,
            source_epoch: frozen.source_epoch,
            dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                source_trade_id: frozen.source_trade_id.clone(),
                transaction_hash: frozen.transaction_hash.clone(),
                wallet: frozen.wallet,
                source_epoch: frozen.source_epoch,
                semantic_revision: frozen.semantic_revision.clone(),
                activity_type: "trade".to_owned(),
                disposition: disposition.to_owned(),
                proof_json,
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: if pending {
                vec![pe_paper_state::EntryGateResultRecord {
                    source_trade_id: frozen.source_trade_id.clone(),
                    wallet: frozen.wallet,
                    market_id: frozen.market_id.clone(),
                    source_epoch: frozen.source_epoch,
                    result: "admitted".to_owned(),
                    history_consumed: true,
                }]
            } else {
                Vec::new()
            },
            history_effects: Vec::new(),
            history_status: None,
            pending: if pending {
                vec![pe_paper_state::DecisionPendingRecord {
                    source_trade_id: frozen.source_trade_id.clone(),
                    semantic_revision: frozen.semantic_revision.clone(),
                    wallet: frozen.wallet,
                    source_epoch: frozen.source_epoch,
                    frozen_inputs_json: serde_json::to_string(continuation).unwrap(),
                    updated_at_unix: frozen.source_epoch,
                }]
            } else {
                Vec::new()
            },
            fence: None,
            reanchor: None,
            advance_cursor: false,
        })
        .unwrap();
    if pending {
        state
            .close_decision_pending(
                &frozen.source_trade_id,
                "{}",
                "no_fill",
                frozen.source_epoch,
            )
            .unwrap();
    }
}

fn commit_read_fixture(
    continuation: &mut DecisionContinuationV3,
    observations: &mut BTreeMap<u64, SourceObservation>,
) {
    let fixed_end = continuation.facts.decision_inputs["fixed_end"]
        .as_i64()
        .unwrap();
    let pages = serde_json::from_value::<Vec<ReconciliationPageEvidence>>(
        continuation.facts.decision_inputs["pages"].clone(),
    )
    .unwrap();
    let payload = crate::bucket_commit::activity_read_commitment_payload(
        continuation.facts.wallet,
        fixed_end,
        continuation.page_occurrences(),
        &pages,
    )
    .unwrap();
    let sequence = continuation.complete_bound().unwrap().sequence.0 + 1;
    let mut commitment = activity_observation(sequence, &payload);
    commitment.source_id = crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned();
    commitment.schema_version = crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION;
    commitment.parser_version = crate::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION;
    for page in continuation.page_occurrences() {
        observations
            .get_mut(&page.receipt.sequence.0)
            .unwrap()
            .schema_version = crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION;
    }
    *continuation = DecisionContinuationV3::new(
        continuation.facts.clone(),
        continuation.observed_source_receipt,
        continuation.page_occurrences.clone(),
        Some(commitment.receipt),
    );
    observations.insert(sequence, commitment);
}

fn single_read_fixture(
    sequence: u64,
    condition: &str,
    side: &str,
    committed: bool,
) -> (DecisionContinuationV3, BTreeMap<u64, SourceObservation>) {
    let mut row = activity_row(condition, condition, "1", "0.5", condition, 100);
    row["side"] = serde_json::json!(side);
    let payload = activity_payload(vec![row]);
    let observation = activity_observation(sequence, &payload);
    let aggregate = parsed_aggregates(&[&payload]).remove(0);
    let components = aggregate.group_id.components();
    let mut frozen = classification_fixture().0.facts;
    frozen.source_trade_id = aggregate.group_id.key().clone();
    frozen.semantic_revision = aggregate.semantic_revision.as_str().to_owned();
    frozen.transaction_hash = components.transaction_hash.clone();
    frozen.wallet = components.wallet;
    frozen.source_epoch = 100;
    frozen.market_id = MarketId(VenueMarketId(condition.to_owned()));
    frozen.outcome_id = components.outcome.unwrap();
    frozen.side = components.side.unwrap();
    frozen.share_amount = aggregate.share_sum;
    frozen.price = aggregate.volume_weighted_price().unwrap();
    let (page, evidence) = activity_page_pair(&observation, &payload, None, 100, 0);
    frozen.decision_inputs = serde_json::json!({"fixed_end":100,"pages":[evidence]});
    let mut continuation = DecisionContinuationV3::new(frozen, None, vec![page], None);
    let mut observations = BTreeMap::from([(sequence, observation)]);
    if committed {
        commit_read_fixture(&mut continuation, &mut observations);
    }
    (continuation, observations)
}

pub(super) struct SelectionOracleFixture {
    _temp: tempfile::TempDir,
    pub(super) state: PaperStateDb,
    pub(super) source_path: PathBuf,
    pub(super) start: TailBinding,
    pub(super) sealed: TailBinding,
    candidate_last_hash: Option<blake3::Hash>,
}

impl SelectionOracleFixture {
    pub(super) fn candidate(&self) -> LogTailBinding {
        let mut candidate = Scanner::verify(&self.source_path).unwrap();
        if let Some(last_hash) = self.candidate_last_hash {
            candidate.last_hash = last_hash;
        }
        candidate
    }
}

pub(super) fn selection_oracle_cases() -> &'static [&'static str] {
    &[
        "empty_history",
        "start_only",
        "post_start_v3",
        "post_start_v4",
        "pre_start_websocket_v3",
        "pre_start_websocket_v4",
        "post_start_sell",
        "pending_without_decision",
        "open_decision",
        "semantic_revision_drift",
        "complete_read_drift",
        "missing_read_commitment",
        "receipt_hash_mismatch",
        "downgraded_v4",
        "commitment_wrong_source",
        "missing_activity_group",
        "sealed_prefix_not_reached",
        "malformed_activity_page_json",
        "activity_page_row_parse_failure",
        "activity_page_wrong_schema",
        "activity_page_wrong_parser",
        "activity_page_wrong_content_type",
        "malformed_websocket_payload",
        "websocket_wrong_contract",
        "websocket_different_trade_key",
        "additional_decision_row",
        "repeated_decision_row",
        "repeated_source_identity_disjoint_reads",
        "missing_decision_row",
        "disagreeing_complete_reads",
        "repeated_complete_read",
        "overlapping_complete_reads",
        "complete_read_reconstruction_failure",
        "conflicting_semantic_revisions",
        "trade_absent_from_complete_read",
        "history_before_universe_error_precedence",
    ]
}

fn selection_oracle_websocket(continuation: &DecisionContinuationV3) -> SourceObservation {
    let payload = serde_json::to_vec(&serde_json::json!({
        "proxyWallet": continuation.facts.wallet.to_string(),
        "conditionId": "0xoracle",
        "asset": "0xoracle",
        "side": "BUY",
        "size": 1,
        "price": 0.5,
        "timestamp": 100,
        "transactionHash": "0xoracle",
        "outcomeIndex": 0
    }))
    .unwrap();
    let mut websocket = activity_observation(1, &payload);
    websocket.source_id = crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned();
    websocket
}

fn materialize_selection_source(
    path: &Path,
    continuations: &mut [&mut DecisionContinuationV3],
    observations: BTreeMap<u64, SourceObservation>,
) -> (TailBinding, BTreeMap<u64, TailBinding>) {
    let mut writer = Writer::open(path).unwrap();
    let empty = TailBinding::from(&Scanner::verify(path).unwrap());
    let mut boundaries = BTreeMap::new();
    for (logical_sequence, mut observation) in observations {
        if observation.source_id == crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
            && let Some(continuation) = continuations
                .iter()
                .find(|continuation| continuation.read_commitment == Some(observation.receipt))
        {
            let fixed_end = continuation.facts.decision_inputs["fixed_end"]
                .as_i64()
                .unwrap();
            let pages = serde_json::from_value::<Vec<ReconciliationPageEvidence>>(
                continuation.facts.decision_inputs["pages"].clone(),
            )
            .unwrap();
            observation.payload = crate::bucket_commit::activity_read_commitment_payload(
                continuation.facts.wallet,
                fixed_end,
                continuation.page_occurrences(),
                &pages,
            )
            .unwrap();
        }
        let old_receipt = observation.receipt;
        let actual_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(observation.source_id),
                schema_version: observation.schema_version,
                parser_version: observation.parser_version,
                observed_at: observation.observed_at,
                received_at: observation.received_at,
                content_type: observation.content_type,
                payload: observation.payload,
            })
            .unwrap();
        for continuation in continuations.iter_mut() {
            for page in &mut continuation.page_occurrences {
                if page.receipt == old_receipt {
                    page.receipt = actual_receipt;
                }
            }
            if continuation.observed_source_receipt == Some(old_receipt) {
                continuation.observed_source_receipt = Some(actual_receipt);
            }
            if continuation.read_commitment == Some(old_receipt) {
                continuation.read_commitment = Some(actual_receipt);
            }
        }
        boundaries.insert(
            logical_sequence,
            TailBinding::from(&Scanner::verify(path).unwrap()),
        );
    }
    drop(writer);
    let sealed = boundaries
        .last_key_value()
        .map_or(empty.clone(), |(_, binding)| binding.clone());
    (sealed, boundaries)
}

fn standard_selection_oracle_fixture(case: &str) -> SelectionOracleFixture {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    if case == "empty_history" {
        let mut no_continuations = Vec::new();
        let (sealed, _) = materialize_selection_source(
            &source_path,
            no_continuations.as_mut_slice(),
            BTreeMap::new(),
        );
        return SelectionOracleFixture {
            _temp: temp,
            state,
            source_path,
            start: sealed.clone(),
            sealed,
            candidate_last_hash: None,
        };
    }

    let committed = matches!(
        case,
        "post_start_v4"
            | "pre_start_websocket_v4"
            | "missing_read_commitment"
            | "receipt_hash_mismatch"
            | "downgraded_v4"
            | "commitment_wrong_source"
    );
    let (mut continuation, mut observations) = single_read_fixture(2, "0xoracle", "BUY", committed);
    if matches!(case, "pre_start_websocket_v3" | "pre_start_websocket_v4") {
        let websocket = selection_oracle_websocket(&continuation);
        continuation.observed_source_receipt = Some(websocket.receipt);
        continuation.facts.provenance = pe_copy_signal_engine::TradeProvenance::ActivityWs;
        observations.insert(1, websocket);
    }
    if case == "commitment_wrong_source" {
        let sequence = continuation.read_commitment.unwrap().sequence.0;
        observations.get_mut(&sequence).unwrap().source_id =
            crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned();
    }
    let (sealed, boundaries) =
        materialize_selection_source(&source_path, &mut [&mut continuation], observations);
    let start = if case.starts_with("pre_start_websocket") {
        boundaries.get(&1).unwrap().clone()
    } else if case == "start_only" {
        sealed.clone()
    } else {
        TailBinding::from(&LogTailBinding {
            path: std::fs::canonicalize(&source_path).unwrap(),
            physical_tail: 5,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]),
        })
    };

    match case {
        "start_only" | "missing_activity_group" => {}
        "post_start_sell" => {
            store_read_decision(&state, &continuation, "not_buy", false);
        }
        "pending_without_decision" => {
            store_read_decision(&state, &continuation, "decision_pending", false);
        }
        _ => store_read_decision(&state, &continuation, "decision_pending", true),
    }

    let connection = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
    match case {
        "open_decision" => {
            connection
                .execute(
                    "UPDATE decision_pending SET state = 'open', terminal_disposition = NULL",
                    [],
                )
                .unwrap();
        }
        "semantic_revision_drift" => {
            connection
                .execute(
                    "UPDATE activity_groups SET semantic_revision = 'different'",
                    [],
                )
                .unwrap();
        }
        "complete_read_drift" => {
            let mut changed = continuation.clone();
            changed.facts.decision_inputs["fixed_end"] = serde_json::json!(101);
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1",
                    [serde_json::to_string(&changed).unwrap()],
                )
                .unwrap();
        }
        "missing_read_commitment" => {
            let mut changed = serde_json::to_value(&continuation).unwrap();
            changed.as_object_mut().unwrap().remove("read_commitment");
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1",
                    [changed.to_string()],
                )
                .unwrap();
        }
        "receipt_hash_mismatch" => {
            let mut changed = serde_json::to_value(&continuation).unwrap();
            changed["read_commitment"]["this_hash"] =
                serde_json::json!(blake3::hash(b"wrong").to_hex().to_string());
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1",
                    [changed.to_string()],
                )
                .unwrap();
        }
        "downgraded_v4" => {
            let mut changed = serde_json::to_value(&continuation).unwrap();
            changed["version"] = serde_json::json!(3);
            changed.as_object_mut().unwrap().remove("read_commitment");
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1",
                    [changed.to_string()],
                )
                .unwrap();
        }
        _ => {}
    }
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start,
        sealed,
        candidate_last_hash: None,
    }
}

fn raw_activity_selection_fixture(
    payload: &[u8],
    schema_version: u32,
    parser_version: u32,
    content_type: ContentType,
) -> SelectionOracleFixture {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let mut writer = Writer::open(&source_path).unwrap();
    let start = TailBinding::from(&Scanner::verify(&source_path).unwrap());
    let at = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            schema_version,
            parser_version,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type,
            payload: payload.to_vec(),
        })
        .unwrap();
    drop(writer);
    let sealed = TailBinding::from(&Scanner::verify(&source_path).unwrap());
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start,
        sealed,
        candidate_last_hash: None,
    }
}

fn websocket_selection_fixture(
    payload: &[u8],
    source_id: &str,
    schema_version: u32,
    parser_version: u32,
) -> SelectionOracleFixture {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let (mut continuation, mut observations) = single_read_fixture(2, "0xoracle", "BUY", false);
    let mut websocket = activity_observation(1, payload);
    websocket.source_id = source_id.to_owned();
    websocket.schema_version = schema_version;
    websocket.parser_version = parser_version;
    continuation.observed_source_receipt = Some(websocket.receipt);
    continuation.facts.provenance = pe_copy_signal_engine::TradeProvenance::ActivityWs;
    observations.insert(1, websocket);
    let (sealed, _) =
        materialize_selection_source(&source_path, &mut [&mut continuation], observations);
    store_read_decision(&state, &continuation, "decision_pending", true);
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start: TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        sealed,
        candidate_last_hash: None,
    }
}

fn continuation_for_activity_page(
    observation: &SourceObservation,
    payload: &[u8],
    condition: &str,
) -> DecisionContinuationV3 {
    let aggregate = parsed_aggregates(&[payload])
        .into_iter()
        .find(|aggregate| {
            aggregate
                .group_id
                .components()
                .condition_id
                .as_ref()
                .is_some_and(|candidate| candidate.0 == condition)
        })
        .unwrap();
    let components = aggregate.group_id.components();
    let mut frozen = classification_fixture().0.facts;
    frozen.source_trade_id = aggregate.group_id.key().clone();
    frozen.semantic_revision = aggregate.semantic_revision.as_str().to_owned();
    frozen.transaction_hash = components.transaction_hash.clone();
    frozen.wallet = components.wallet;
    frozen.source_epoch = 100;
    frozen.market_id = MarketId(VenueMarketId(condition.to_owned()));
    frozen.outcome_id = components.outcome.unwrap();
    frozen.side = components.side.unwrap();
    frozen.share_amount = aggregate.share_sum;
    frozen.price = aggregate.volume_weighted_price().unwrap();
    let (page, evidence) = activity_page_pair(observation, payload, None, 100, 0);
    frozen.decision_inputs = serde_json::json!({"fixed_end": 100, "pages": [evidence]});
    DecisionContinuationV3::new(frozen, None, vec![page], None)
}

fn two_decision_read_fixture(kind: &str) -> SelectionOracleFixture {
    let payload = activity_payload(vec![
        activity_row("0xread-a", "read-a", "1", "0.5", "0xread-a", 100),
        activity_row("0xread-b", "read-b", "1", "0.5", "0xread-b", 100),
    ]);
    let observation = activity_observation(2, &payload);
    let mut first = continuation_for_activity_page(&observation, &payload, "0xread-a");
    let mut second = continuation_for_activity_page(&observation, &payload, "0xread-b");
    first.facts.source_epoch = 99;
    match kind {
        "disagree" => {
            second.facts.decision_inputs["agreement_marker"] = serde_json::json!(false);
        }
        "overlap" => {
            let request_url = activity_request_url(None, 101, 0);
            second.page_occurrences[0].request_url = request_url.clone();
            second.facts.decision_inputs["fixed_end"] = serde_json::json!(101);
            second.facts.decision_inputs["pages"][0]["request_url"] =
                serde_json::json!(request_url);
            second.facts.decision_inputs["pages"][0]["bounds"]["end"] = serde_json::json!(101);
        }
        "repeat" => {}
        _ => unreachable!(),
    }
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let (sealed, _) = materialize_selection_source(
        &source_path,
        &mut [&mut first, &mut second],
        BTreeMap::from([(2, observation)]),
    );
    store_read_decision(&state, &first, "decision_pending", true);
    if kind == "repeat" {
        store_read_decision(&state, &second, "not_buy", false);
        let connection = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE decision_pending_unconstrained AS SELECT * FROM decision_pending;
                 INSERT INTO decision_pending_unconstrained SELECT * FROM decision_pending;
                 DROP TABLE decision_pending;
                 ALTER TABLE decision_pending_unconstrained RENAME TO decision_pending;",
            )
            .unwrap();
    } else {
        store_read_decision(&state, &second, "decision_pending", true);
    }
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start: TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        sealed,
        candidate_last_hash: None,
    }
}

fn conflicting_revision_fixture() -> SelectionOracleFixture {
    let first_payload = activity_payload(vec![
        activity_row(
            "0xrevision-a",
            "revision-a",
            "1",
            "0.5",
            "0xrevision-a",
            100,
        ),
        activity_row(
            "0xrevision-b",
            "revision-b",
            "1",
            "0.5",
            "0xrevision-b",
            100,
        ),
    ]);
    let second_payload = activity_payload(vec![
        activity_row("0xrevision-a", "revision-a", "2", "1", "0xrevision-a", 100),
        activity_row("0xrevision-b", "revision-b", "2", "1", "0xrevision-b", 100),
    ]);
    let first_observation = activity_observation(1, &first_payload);
    let second_observation = activity_observation(2, &second_payload);
    let mut first =
        continuation_for_activity_page(&first_observation, &first_payload, "0xrevision-a");
    let mut second =
        continuation_for_activity_page(&second_observation, &second_payload, "0xrevision-b");
    first.facts.source_epoch = 99;
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let (sealed, _) = materialize_selection_source(
        &source_path,
        &mut [&mut first, &mut second],
        BTreeMap::from([(1, first_observation), (2, second_observation)]),
    );
    store_read_decision(&state, &first, "decision_pending", true);
    store_read_decision(&state, &second, "decision_pending", true);
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start: TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        sealed,
        candidate_last_hash: None,
    }
}

fn absent_complete_read_trade_fixture() -> SelectionOracleFixture {
    let (mut page_trade, mut observations) = single_read_fixture(2, "0xpage-trade", "BUY", false);
    let (mut decision, _) = single_read_fixture(2, "0xdecision-trade", "BUY", false);
    decision.page_occurrences = page_trade.page_occurrences.clone();
    decision.facts.decision_inputs = page_trade.facts.decision_inputs.clone();
    let websocket_payload = serde_json::to_vec(&serde_json::json!({
        "proxyWallet": decision.facts.wallet.to_string(),
        "conditionId": "0xdecision-trade",
        "asset": "0xdecision-trade",
        "side": "BUY",
        "size": 1,
        "price": 0.5,
        "timestamp": 100,
        "transactionHash": "0xdecision-trade",
        "outcomeIndex": 0
    }))
    .unwrap();
    let mut websocket = activity_observation(1, &websocket_payload);
    websocket.source_id = crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned();
    decision.observed_source_receipt = Some(websocket.receipt);
    decision.facts.provenance = pe_copy_signal_engine::TradeProvenance::ActivityWs;
    observations.insert(1, websocket);
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let (sealed, _) = materialize_selection_source(
        &source_path,
        &mut [&mut page_trade, &mut decision],
        observations,
    );
    store_read_decision(&state, &page_trade, "not_buy", false);
    store_read_decision(&state, &decision, "decision_pending", true);
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start: TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        sealed,
        candidate_last_hash: None,
    }
}

fn additional_decision_fixture() -> SelectionOracleFixture {
    let fixture = standard_selection_oracle_fixture("post_start_v3");
    let connection = rusqlite::Connection::open(fixture._temp.path().join("paper.db")).unwrap();
    let frozen: String = connection
        .query_row(
            "SELECT frozen_inputs_json FROM decision_pending",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut changed = serde_json::from_str::<serde_json::Value>(&frozen).unwrap();
    changed["semantic_revision"] = serde_json::json!("additional");
    connection
        .execute("UPDATE activity_groups SET disposition = 'not_buy'", [])
        .unwrap();
    connection
        .execute(
            "UPDATE decision_pending SET semantic_revision = 'additional', frozen_inputs_json = ?1",
            [changed.to_string()],
        )
        .unwrap();
    fixture
}

fn repeated_decision_fixture(disjoint_payloads: bool) -> SelectionOracleFixture {
    let first_payload = activity_payload(vec![activity_row(
        "0xrepeated-row",
        "repeated-row",
        "1",
        "0.5",
        "0xrepeated-row",
        100,
    )]);
    let mut second_payload = first_payload.clone();
    if disjoint_payloads {
        second_payload.push(b' ');
    }
    let first_observation = activity_observation(1, &first_payload);
    let second_observation = activity_observation(2, &second_payload);
    let mut first =
        continuation_for_activity_page(&first_observation, &first_payload, "0xrepeated-row");
    let mut second =
        continuation_for_activity_page(&second_observation, &second_payload, "0xrepeated-row");
    assert_eq!(first.facts.source_trade_id, second.facts.source_trade_id);
    assert_eq!(
        first.facts.semantic_revision,
        second.facts.semantic_revision
    );
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.log");
    let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
    let (sealed, _) = materialize_selection_source(
        &source_path,
        &mut [&mut first, &mut second],
        BTreeMap::from([(1, first_observation), (2, second_observation)]),
    );
    assert_ne!(
        first.page_occurrences[0].receipt,
        second.page_occurrences[0].receipt
    );
    if disjoint_payloads {
        assert_ne!(
            first.page_occurrences[0].raw_hash,
            second.page_occurrences[0].raw_hash
        );
    }
    store_read_decision(&state, &first, "decision_pending", true);
    let connection = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE decision_pending_unconstrained AS SELECT * FROM decision_pending;
             INSERT INTO decision_pending_unconstrained SELECT * FROM decision_pending;
             DROP TABLE decision_pending;
             ALTER TABLE decision_pending_unconstrained RENAME TO decision_pending;",
        )
        .unwrap();
    connection
        .execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1
             WHERE rowid = (SELECT MAX(rowid) FROM decision_pending)",
            [serde_json::to_string(&second).unwrap()],
        )
        .unwrap();
    SelectionOracleFixture {
        _temp: temp,
        state,
        source_path,
        start: TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        sealed,
        candidate_last_hash: None,
    }
}

fn sealed_prefix_not_reached_fixture() -> SelectionOracleFixture {
    let mut fixture = standard_selection_oracle_fixture("post_start_v3");
    let wrong_hash = blake3::hash(b"wrong-sealed-prefix");
    fixture.sealed.last_hash = wrong_hash.to_hex().to_string();
    fixture.candidate_last_hash = Some(wrong_hash);
    fixture
}

fn history_before_universe_error_fixture() -> SelectionOracleFixture {
    let fixture = raw_activity_selection_fixture(
        b"{",
        pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        ContentType::Json,
    );
    rusqlite::Connection::open(fixture._temp.path().join("paper.db"))
        .unwrap()
        .execute("DROP TABLE decision_pending", [])
        .unwrap();
    fixture
}

pub(super) fn build_selection_oracle_fixture(case: &str) -> SelectionOracleFixture {
    match case {
        "sealed_prefix_not_reached" => sealed_prefix_not_reached_fixture(),
        "malformed_activity_page_json" => raw_activity_selection_fixture(
            b"{",
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            ContentType::Json,
        ),
        "activity_page_row_parse_failure" => raw_activity_selection_fixture(
            br#"[{"type":"TRADE"}]"#,
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            ContentType::Json,
        ),
        "activity_page_wrong_schema" => raw_activity_selection_fixture(
            b"[]",
            99,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            ContentType::Json,
        ),
        "activity_page_wrong_parser" => raw_activity_selection_fixture(
            b"[]",
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            99,
            ContentType::Json,
        ),
        "activity_page_wrong_content_type" => raw_activity_selection_fixture(
            b"[]",
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            ContentType::Raw,
        ),
        "malformed_websocket_payload" => websocket_selection_fixture(
            b"{",
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        ),
        "websocket_wrong_contract" => {
            let correct =
                selection_oracle_websocket(&single_read_fixture(2, "0xoracle", "BUY", false).0);
            websocket_selection_fixture(
                &correct.payload,
                "wrong.websocket.source",
                pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            )
        }
        "websocket_different_trade_key" => {
            let different = serde_json::to_vec(&serde_json::json!({
                "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "conditionId": "0xdifferent",
                "asset": "0xdifferent",
                "side": "BUY",
                "size": 1,
                "price": 0.5,
                "timestamp": 100,
                "transactionHash": "0xdifferent",
                "outcomeIndex": 0
            }))
            .unwrap();
            websocket_selection_fixture(
                &different,
                crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            )
        }
        "additional_decision_row" => additional_decision_fixture(),
        "repeated_decision_row" => repeated_decision_fixture(false),
        "repeated_source_identity_disjoint_reads" => repeated_decision_fixture(true),
        "missing_decision_row" => standard_selection_oracle_fixture("pending_without_decision"),
        "disagreeing_complete_reads" => two_decision_read_fixture("disagree"),
        "repeated_complete_read" => two_decision_read_fixture("repeat"),
        "overlapping_complete_reads" => two_decision_read_fixture("overlap"),
        "complete_read_reconstruction_failure" => {
            standard_selection_oracle_fixture("complete_read_drift")
        }
        "conflicting_semantic_revisions" => conflicting_revision_fixture(),
        "trade_absent_from_complete_read" => absent_complete_read_trade_fixture(),
        "history_before_universe_error_precedence" => history_before_universe_error_fixture(),
        _ => standard_selection_oracle_fixture(case),
    }
}

pub(super) fn qualification_error_variant(error: &QualificationError) -> &'static str {
    match error {
        QualificationError::PaperLog(_) => "PaperLog",
        QualificationError::EventLog(_) => "EventLog",
        QualificationError::PaperState(_) => "PaperState",
        QualificationError::Json(_) => "Json",
        QualificationError::Io(_) => "Io",
        QualificationError::InsufficientEvidence(_) => "InsufficientEvidence",
    }
}

pub(super) fn render_selection_oracle_result(
    result: Result<&SelectedDecisionRows, &QualificationError>,
) -> String {
    match result {
        Ok(selected) => format!(
            "Ok\nrows: {:?}\nin_prefix: {:?}",
            selected.rows, selected.in_prefix
        ),
        Err(error) => format!(
            "Err\nvariant: {}\ntext: {}",
            qualification_error_variant(error),
            error
        ),
    }
}

fn selection_oracle_header() -> String {
    format!(
        "# Recorded from pre-#574 base revision {ORACLE_BASE_REVISION}.\n\
         # Command: {ORACLE_REGENERATOR_COMMAND}\n\
         # Run the regenerator on that exact base revision to refresh this file.\n\n"
    )
}

pub(super) fn selection_oracle_output() -> String {
    let mut output = selection_oracle_header();
    for case in selection_oracle_cases() {
        let fixture = build_selection_oracle_fixture(case);
        let result = decision_rows_for_source_prefix(
            &fixture.state,
            &fixture.source_path,
            &fixture.start,
            &fixture.sealed,
        );
        let rendered = render_selection_oracle_result(result.as_ref());
        output.push_str(&format!("=== {case} ===\n{rendered}\n"));
    }
    output
}

pub(super) fn expected_selection_oracle_result<'a>(oracle: &'a str, case: &str) -> &'a str {
    let marker = format!("=== {case} ===\n");
    let start = oracle.find(&marker).unwrap() + marker.len();
    let remainder = &oracle[start..];
    let end = remainder.find("\n=== ").unwrap_or(remainder.len());
    remainder[..end].trim_end_matches('\n')
}

/// Regenerate the pre-refactor selection characterization data for GitHub issue #574.
/// Run this ignored test only from the base revision named in the fixture header.
#[test]
#[ignore = "deliberately regenerates checked-in qualification selection characterization"]
fn regenerate_qualification_selection_oracle() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/qualification_selection_oracle.txt");
    fs::write(path, selection_oracle_output()).unwrap();
}
