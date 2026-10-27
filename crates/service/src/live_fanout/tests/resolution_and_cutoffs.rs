use super::*;

fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).unwrap()
}

fn push_event(
    events: &mut Vec<LiveJournalEvent>,
    account: &AccountId,
    stamp: OffsetDateTime,
    payload: LiveJournalPayload,
) -> u64 {
    let seq = events.last().map_or(1, |event| event.seq + 1);
    events.push(LiveJournalEvent {
        account_id: account.clone(),
        seq,
        timestamp: stamp,
        payload,
    });
    seq
}

fn other_condition_prepared() -> Box<pe_execution_core::LiveOrderPreparedAudit> {
    condition_prepared("88", 125, "second-condition")
}

pub(super) fn condition_prepared(
    condition_byte: &str,
    token_number: u64,
    dispatch: &str,
) -> Box<pe_execution_core::LiveOrderPreparedAudit> {
    let mut prepared = finality_prepared();
    let condition = PolymarketConditionId(format!("0x{}", condition_byte.repeat(32)));
    let token = pe_core_types::PolymarketTokenId(token_number.to_string());
    prepared.identity = projection_identity(dispatch);
    prepared
        .identity
        .fill_projection
        .as_mut()
        .unwrap()
        .market_id = condition.0.clone();
    prepared.economic.market.condition_id = condition.clone();
    prepared.economic.market.market_id = condition.0.clone();
    prepared.economic.market.token_id = token.clone();
    prepared.economic.admission.market.condition_id = condition.clone();
    prepared.economic.admission.market.ordered_outcome_token_ids = [
        token.clone(),
        pe_core_types::PolymarketTokenId((token_number + 1).to_string()),
    ];
    prepared.economic.admission.settlement.condition_id = condition.clone();
    prepared.prepared.condition_id = condition;
    prepared.prepared.token_id = token;
    prepared
}

pub(super) fn push_fill(
    events: &mut Vec<LiveJournalEvent>,
    account: &AccountId,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    stamp: OffsetDateTime,
) {
    let admission = approved_admission_event(account, prepared, 0, stamp);
    push_event(events, account, stamp, admission.payload);
    let prepared_seq = push_event(
        events,
        account,
        stamp,
        LiveJournalPayload::OrderPrepared(Arc::new(prepared.clone())),
    );
    let mut matched = finality_matched_event(account, prepared, 0, stamp);
    let transaction_hash = activity_transaction_hash(&prepared.identity);
    if let LiveJournalPayload::OrderReconciled(reconciled) = &mut matched.payload
        && let LiveJournalOrderOutcome::Matched {
            transaction_hashes, ..
        } = &mut Arc::make_mut(reconciled).outcome
    {
        *transaction_hashes = vec![transaction_hash.clone()];
    }
    push_event(events, account, stamp, matched.payload);
    let mut fill = finalized_fill_event(account, prepared, 0, prepared_seq);
    if let LiveJournalPayload::OrderFillFinalized(fill) = &mut fill.payload {
        let fill = Arc::make_mut(fill);
        fill.matched_logs[0].transaction_hash = transaction_hash.clone();
        if let RawHttpAttempt::Response(response) = &mut fill.receipts[1] {
            // Vary the recorded fixture's asset word for the second condition.
            let body = String::from_utf8(receipt_with_word(
                1,
                prepared.prepared.token_id.0.parse().unwrap(),
            ))
            .unwrap();
            response.body = body
                .replace(&format!("0x{}", "11".repeat(32)), &transaction_hash)
                .into_bytes();
            response.ordered_query[1].1 = serde_json::json!([transaction_hash]).to_string();
        }
    }
    push_event(events, account, stamp, fill.payload);
}

fn filled_stream(account: &AccountId) -> Vec<LiveJournalEvent> {
    let mut events = vec![baseline_event(account, 1)];
    push_fill(&mut events, account, &finality_prepared(), at(10));
    events
}

fn push_resolution(
    events: &mut Vec<LiveJournalEvent>,
    sources: &mut Vec<EventEnvelope>,
    account: &AccountId,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    winner: u8,
    stamp: OffsetDateTime,
    receipt_seq: u64,
) {
    let mut event = resolution_event(account, 0);
    if let LiveJournalPayload::ResolutionFinalized(resolution) = &mut event.payload {
        let resolution = Arc::make_mut(resolution);
        resolution.condition_id = prepared.prepared.condition_id.clone();
        resolution.payout_by_outcome_index_json = BinaryPayoutVector::winner(usize::from(winner))
            .unwrap()
            .canonical_json();
        resolution.source_append_receipt = fixture_receipt(receipt_seq);
    }
    let mut source = resolution_source_envelope(&event);
    let mut body: serde_json::Value = serde_json::from_slice(&source.payload).unwrap();
    for (token, expected) in body["tokens"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(&prepared.economic.admission.market.ordered_outcome_token_ids)
    {
        token["token_id"] = serde_json::json!(expected.0);
    }
    source.payload = serde_json::to_vec(&body).unwrap();
    source.raw_payload_hash = blake3::hash(&source.payload);
    source.received_at = ReceivedAt(stamp);
    source.observed_at = SourceTimestamp(stamp);
    sources.push(source);
    push_event(events, account, stamp, event.payload);
}

fn position_sources(
    binding: &LiveAccountBindingAudit,
    positions: &[CanonicalPositionAudit],
    receipt_seq: u64,
    stamp: OffsetDateTime,
) -> (LivePositionEvidenceAudit, Vec<EventEnvelope>) {
    let page = |redeemable| {
        serde_json::to_vec(
            &positions
                .iter()
                .filter(|position| position.redeemable == redeemable)
                .map(|position| {
                    serde_json::json!({
                        "proxyWallet": binding.custody_wallet,
                        "asset": position.token_id.0,
                        "conditionId": position.condition_id.0,
                        "outcomeIndex": position.outcome_index,
                        "size": position.size.to_decimal().to_string(),
                        "negativeRisk": false,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
    };
    position_evidence(receipt_seq, stamp, binding, [page(false), page(true)])
}

pub(super) fn push_redemption_attempt(
    events: &mut Vec<LiveJournalEvent>,
    account: &AccountId,
    condition: &PolymarketConditionId,
    stamp: OffsetDateTime,
) -> RedemptionAttemptIdentity {
    let wallet = finality_prepared().prepared.funder;
    let call = build_redemption_call(condition.clone(), false).unwrap();
    let identity = RedemptionAttemptIdentity {
        account_id: account.clone(),
        condition_id: condition.clone(),
        adapter: call.to.clone(),
        custody_wallet: wallet.clone(),
    };
    push_event(
        events,
        account,
        stamp,
        LiveJournalPayload::RedemptionRequested(Box::new(
            pe_execution_core::RedemptionRequestedAudit {
                identity: identity.clone(),
                attempt_count: 1,
                redeemable_balance: CollateralAmount::from_atomic(3_125_000),
                request: pe_execution_core::RedemptionRequestAudit {
                    call_to: call.to,
                    calldata: call.calldata,
                    condition_id: condition.clone(),
                    neg_risk: false,
                    custody: pe_execution_core::RedemptionCustodyAudit::DepositWallet,
                    signer_address: wallet.clone(),
                    custody_wallet: wallet,
                    deadline_unix: Some(90_000),
                    metadata_hash: "metadata".to_owned(),
                    request_hash: "request".to_owned(),
                    schema_version: 1,
                    parser_version: 1,
                    adapter_version: "fixture".to_owned(),
                },
            },
        )),
    );
    push_event(
        events,
        account,
        stamp,
        LiveJournalPayload::RedemptionTransactionIdentified(Box::new(
            pe_execution_core::RedemptionTransactionAudit {
                identity: identity.clone(),
                attempt_count: 1,
                transaction_id: "tx-1".to_owned(),
                submit_body_hash: "body".to_owned(),
                evidence: Vec::new(),
                evidence_hashes: Vec::new(),
            },
        )),
    );
    push_event(
        events,
        account,
        stamp,
        LiveJournalPayload::RedemptionReceiptTransition(Box::new(
            pe_execution_core::RedemptionReceiptAudit {
                identity: identity.clone(),
                attempt_count: 1,
                transaction_id: "tx-1".to_owned(),
                transaction_hash: Some(format!("0x{}", "99".repeat(32))),
                status: pe_execution_core::RedemptionReceiptStatusAudit::Confirmed,
                evidence: Vec::new(),
                evidence_hashes: Vec::new(),
            },
        )),
    );
    identity
}

pub(super) fn push_custody(
    events: &mut Vec<LiveJournalEvent>,
    sources: &mut Vec<EventEnvelope>,
    identity: RedemptionAttemptIdentity,
    cash: Decimal,
    remaining: &[CanonicalPositionAudit],
    stamp: OffsetDateTime,
    receipt_seq: u64,
) {
    let binding = account_binding_fixture(&identity.account_id, &identity.custody_wallet);
    let (evidence, pages) = position_sources(&binding, remaining, receipt_seq, stamp);
    sources.extend(pages);
    let account = identity.account_id.clone();
    push_event(
        events,
        &account,
        stamp,
        LiveJournalPayload::RedemptionCustodyReconciled(Arc::new(
            pe_execution_core::RedemptionCustodyReconciledAudit {
                identity,
                account_state: account_state_fixture_for_binding(
                    CollateralAmount::from_decimal_exact(cash).unwrap(),
                    stamp,
                    &binding,
                ),
                venue_positions: remaining.to_vec(),
                venue_position_receipts: evidence.pages.iter().map(|page| page.receipt).collect(),
            },
        )),
    );
}

/// PASS: a condition holding a finalized fill becomes resolvable once every order prepared for it
/// is terminal, whichever transition ended it (fill, failure before POST, finality conflict, kill
/// or reject); one non-terminal order holds it back; conflicting token mappings fail.
/// FAIL: discovery skips a condition whose orders all ended, or resolves one that can still fill.
#[test]
fn resolvable_conditions_follow_the_fold_terminal_flags() {
    let filled = finality_prepared();
    let condition = filled.prepared.condition_id.0.clone();
    let tokens = filled
        .economic
        .admission
        .market
        .ordered_outcome_token_ids
        .clone();
    let mut other = finality_prepared();
    other.identity = projection_identity("other-order");
    let mut reducer = ProjectionReducer::new(AccountId::new("account").unwrap());
    reducer
        .prepared_orders
        .insert("filled".to_owned(), (1, Arc::new(*filled), true));
    reducer
        .prepared_orders
        .insert("other".to_owned(), (2, Arc::new(*other.clone()), false));
    reducer.unresolved_fill_conditions.insert(condition.clone());
    assert!(reducer.resolvable_conditions().unwrap().is_empty());
    reducer.prepared_orders.get_mut("other").unwrap().2 = true;
    assert_eq!(
        reducer.resolvable_conditions().unwrap(),
        BTreeMap::from([(condition, tokens)])
    );
    other.economic.admission.market.ordered_outcome_token_ids[0] =
        pe_core_types::PolymarketTokenId("999".to_owned());
    reducer
        .prepared_orders
        .insert("conflicting".to_owned(), (3, Arc::new(*other), true));
    assert_eq!(
        reducer.resolvable_conditions(),
        Err("condition admission mapping conflicts")
    );
}

/// PASS: a losing token remains a zero receivable and redeemable custody holding until custody.
/// FAIL: resolution drops the zero entry, or zero-payout custody changes economic cash.
#[test]
fn losing_resolution_retains_zero_receivable_until_custody() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    let mut events = filled_stream(&account);
    let mut sources = Vec::new();
    let before = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
    push_resolution(
        &mut events,
        &mut sources,
        &account,
        &prepared,
        1,
        at(20),
        20,
    );
    let losing = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
    assert_eq!(losing.receivable, before.receivable);
    assert_eq!(losing.economic_cash, before.economic_cash);
    assert_eq!(
        losing
            .receivable_by_condition
            .get(&prepared.prepared.condition_id.0),
        Some(&CollateralAmount::ZERO)
    );
    assert_eq!(losing.custody_positions.len(), 1);
    assert_eq!(
        losing.custody_positions[0].token_id,
        prepared.prepared.token_id
    );
    assert!(losing.custody_positions[0].redeemable);
    let identity = push_redemption_attempt(
        &mut events,
        &account,
        &prepared.prepared.condition_id,
        at(25),
    );
    push_custody(
        &mut events,
        &mut sources,
        identity,
        dec!(7.499880),
        &[],
        at(30),
        110,
    );
    let settled = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
    assert!(settled.receivable_by_condition.is_empty());
    assert!(settled.custody_positions.is_empty());
    assert_eq!(settled.receivable, Decimal::ZERO);
    assert_eq!(settled.economic_cash, losing.economic_cash);
}

/// PASS: winner and loser resolution/custody commute across two independent conditions.
/// FAIL: either order loses zero-valued custody work or changes the final economics.
#[test]
fn winner_and_loser_custody_converge_in_both_orders() {
    let account = AccountId::new("account").unwrap();
    let winner = finality_prepared();
    let loser = other_condition_prepared();
    for resolution_order in [[&winner, &loser], [&loser, &winner]] {
        for custody_order in [[&winner, &loser], [&loser, &winner]] {
            let mut events = filled_stream(&account);
            push_fill(&mut events, &account, &loser, at(12));
            let mut sources = Vec::new();
            for (index, prepared) in resolution_order.into_iter().enumerate() {
                let before = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
                let is_loser = prepared.prepared.condition_id == loser.prepared.condition_id;
                push_resolution(
                    &mut events,
                    &mut sources,
                    &account,
                    prepared,
                    u8::from(is_loser),
                    at(20),
                    20 + u64::try_from(index).unwrap(),
                );
                let after = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
                if is_loser {
                    assert_eq!(after.receivable, before.receivable);
                    assert_eq!(
                        after.receivable_by_condition[&prepared.prepared.condition_id.0],
                        CollateralAmount::ZERO
                    );
                }
            }
            let resolved = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
            assert_eq!(resolved.economic_cash, Some(dec!(8.124760)));
            assert_eq!(resolved.receivable, dec!(3.125));
            assert_eq!(resolved.receivable_by_condition.len(), 2);
            assert!(
                resolved
                    .custody_positions
                    .iter()
                    .all(|position| position.redeemable)
            );
            for (index, prepared) in custody_order.into_iter().enumerate() {
                let before = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
                let remaining = before
                    .custody_positions
                    .iter()
                    .filter(|position| position.condition_id != prepared.prepared.condition_id)
                    .cloned()
                    .collect::<Vec<_>>();
                let outstanding = if remaining
                    .iter()
                    .any(|position| position.condition_id == winner.prepared.condition_id)
                {
                    dec!(3.125)
                } else {
                    Decimal::ZERO
                };
                let identity = push_redemption_attempt(
                    &mut events,
                    &account,
                    &prepared.prepared.condition_id,
                    at(25),
                );
                push_custody(
                    &mut events,
                    &mut sources,
                    identity,
                    dec!(8.124760) - outstanding,
                    &remaining,
                    at(30),
                    110 + 2 * u64::try_from(index).unwrap(),
                );
                let after = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
                assert_eq!(after.economic_cash, resolved.economic_cash);
                assert!(
                    !after
                        .receivable_by_condition
                        .contains_key(&prepared.prepared.condition_id.0)
                );
            }
            let settled = derive_with_baseline_evidence(&account, &events, &sources).unwrap();
            assert!(settled.custody_positions.is_empty());
            assert!(settled.receivable_by_condition.is_empty());
            assert_eq!(settled.receivable, Decimal::ZERO);
        }
    }
}

/// PASS: the runtime gate tolerates an early redeemable flag but rejects one extra share atomic.
/// FAIL: a flag-only difference blocks the gate, or the size difference passes it.
#[tokio::test]
async fn runtime_inventory_masks_redeemable_but_preserves_exact_size() {
    async fn positions(
        Query(query): Query<HashMap<String, String>>,
        State(size): State<&'static str>,
    ) -> Json<Vec<serde_json::Value>> {
        if query.get("redeemable").map(String::as_str) == Some("true") {
            Json(vec![serde_json::json!({
                "proxyWallet": "0x1111111111111111111111111111111111111111",
                "asset": "123", "conditionId": finality_prepared().prepared.condition_id.0,
                "outcomeIndex": 0, "size": size, "negativeRisk": false,
            })])
        } else {
            Json(Vec::new())
        }
    }
    for (size, expected) in [
        ("3.125", CheckOutcome::Pass),
        (
            "3.125001",
            CheckOutcome::PersistentFail("venue inventory drift"),
        ),
    ] {
        let app = Router::new()
            .route("/positions", get(positions))
            .with_state(size);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let snapshot = armed_snapshot("acct");
        let account = snapshot.accounts[0].clone();
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        let now = at(1_700_000_000);
        state.clock = Arc::new(move || now);
        state.config.data_base_url = format!("http://{address}");
        let baseline = append_recovery_baseline(
            &state,
            &account.account_id,
            now,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        append_recovery_filled_position(&state, &account.account_id, &baseline, now).await;
        let before = live_summary(&state, &account.account_id).unwrap();
        assert!(!before.custody_positions[0].redeemable);
        let binding = before.account_binding.unwrap();
        let audit = account_state_fixture_for_binding(
            CollateralAmount::from_atomic(7_499_880),
            now,
            &binding,
        );
        assert_eq!(
            ensure_live_portfolio_marks_at(
                &mut state,
                &account,
                &venue_account_state_from_audit(&audit),
                &binding,
                &binding.custody_wallet,
                now
            )
            .await,
            expected,
            "size={size}"
        );
        server.abort();
    }
}

fn crossing_event(account: &AccountId, seq: u64, stamp: OffsetDateTime) -> LiveJournalEvent {
    let binding = finality_prepared().frozen_binding;
    LiveJournalEvent {
        account_id: account.clone(),
        seq,
        timestamp: stamp,
        payload: LiveJournalPayload::CredentialBindingMismatch {
            frozen: binding.clone(),
            current: binding,
        },
    }
}

/// Every field a gate summary carries; batch rebuilds also carry the historical fill rows and
/// prepared sequences that summaries leave out.
fn cutoff_snapshot(derived: &ProjectionDerivation) -> serde_json::Value {
    let mut snapshot = projection_snapshot(derived);
    snapshot["fills"] = serde_json::Value::Null;
    snapshot["validated_prepared_sequences"] = serde_json::Value::Null;
    snapshot
}

fn filtered_rebuild(
    account: &AccountId,
    events: &[LiveJournalEvent],
    sources: &[EventEnvelope],
    cutoff: i64,
) -> Result<ProjectionDerivation, ProjectionReducerError> {
    let events = events
        .iter()
        .filter(|event| event.timestamp < at(cutoff))
        .cloned()
        .collect::<Vec<_>>();
    let sources = sources
        .iter()
        .filter(|source| source.received_at.0 < at(cutoff))
        .cloned()
        .collect::<Vec<_>>();
    derive_projection_rows_with_sources(account, &events, &sources)
}

fn fold_stream(
    account: &AccountId,
    events: &[LiveJournalEvent],
    sources: &[EventEnvelope],
) -> ProjectionFold {
    let mut fold = ProjectionFold::new(account.clone());
    for event in events {
        fold.apply(event, sources).unwrap();
    }
    fold
}

fn daily_from_view(
    account: &AccountId,
    seq: u64,
    cutoff: i64,
    bounded: &ProjectionDerivation,
    current: &ProjectionDerivation,
    sources: &mut Vec<EventEnvelope>,
) -> LiveJournalEvent {
    let stamp = at(172_810);
    let binding = current.account_binding.as_ref().unwrap();
    let (evidence, pages) = position_sources(binding, &current.custody_positions, 150, stamp);
    sources.extend(pages);
    let mut equity = bounded.economic_cash.unwrap();
    let prices = bounded
        .custody_positions
        .iter()
        .filter(|position| !position.redeemable)
        .enumerate()
        .map(|(index, position)| {
            let receipt = fixture_receipt(160 + u64::try_from(index).unwrap());
            sources.push(source_envelope(
                receipt,
                "pe-service.clob-prices-history",
                serde_json::to_vec(&serde_json::json!({"history": [{"t": cutoff, "p": "0.5"}]}))
                    .unwrap(),
                stamp,
            ));
            equity += position.size.to_decimal() * dec!(0.5);
            pe_execution_core::MarkPrice {
                condition_id: position.condition_id.clone(),
                outcome_index: position.outcome_index,
                price: Price(dec!(0.5)),
                receipt,
                observed_unix: cutoff,
            }
        })
        .collect();
    LiveJournalEvent {
        account_id: account.clone(),
        seq,
        timestamp: stamp,
        payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
            pe_execution_core::AccountPortfolioMarkedAudit {
                kind: MarkKind::Daily,
                cutoff_unix: cutoff,
                account_binding: binding.clone(),
                account_state: account_state_fixture_for_binding(
                    CollateralAmount::from_decimal_exact(
                        current.economic_cash.unwrap() - current.receivable,
                    )
                    .unwrap(),
                    stamp,
                    binding,
                ),
                venue_positions: current.custody_positions.clone(),
                venue_position_evidence: evidence,
                marked_positions: bounded.custody_positions.clone(),
                prices,
                equity: CollateralAmount::from_decimal_exact(equity).unwrap(),
            },
        )),
    }
}

fn push_credit(
    events: &mut Vec<LiveJournalEvent>,
    sources: &mut Vec<EventEnvelope>,
    account: &AccountId,
    stamp: OffsetDateTime,
) {
    let binding = account_binding_fixture(account, &finality_prepared().prepared.funder);
    let (evidence, pages) = empty_position_evidence(120, stamp, &binding);
    sources.extend(pages);
    push_event(
        events,
        account,
        stamp,
        LiveJournalPayload::CollateralCredited(Box::new(
            pe_execution_core::CollateralCreditedAudit {
                account_state: account_state_fixture_for_binding(
                    CollateralAmount::from_atomic(11_000_000),
                    stamp,
                    &binding,
                ),
                account_binding: binding,
                venue_positions: Vec::new(),
                venue_position_evidence: evidence,
                credited: CollateralAmount::from_atomic(1_000_000),
            },
        )),
    );
}

/// PASS: source-citing records on either side of midnight match filtered rebuilds and valid Dailies.
/// FAIL: a late record is missed, applied twice, or applied to a cutoff at/before its stamp.
#[test]
fn source_citing_cutoff_copies_equal_filtered_rebuilds() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    for kind in ["baseline", "resolution", "custody", "credit"] {
        for stamp in [86_399, 86_400, 86_401] {
            let mut events = if kind == "baseline" {
                Vec::new()
            } else if kind == "credit" {
                vec![baseline_event(&account, 1)]
            } else {
                filled_stream(&account)
            };
            let mut sources = if kind == "baseline" {
                Vec::new()
            } else {
                baseline_sources(&account)
            };
            let identity = if kind == "custody" {
                push_resolution(
                    &mut events,
                    &mut sources,
                    &account,
                    &prepared,
                    0,
                    at(20),
                    20,
                );
                Some(push_redemption_attempt(
                    &mut events,
                    &account,
                    &prepared.prepared.condition_id,
                    at(25),
                ))
            } else {
                None
            };
            let crossing = crossing_event(&account, 0, at(172_800));
            push_event(&mut events, &account, crossing.timestamp, crossing.payload);
            match kind {
                "baseline" => {
                    let mut baseline = baseline_event(&account, 0);
                    baseline.timestamp = at(stamp);
                    let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut baseline.payload
                    else {
                        unreachable!()
                    };
                    mark.cutoff_unix = stamp;
                    let (evidence, pages) =
                        empty_position_evidence(100, at(stamp), &mark.account_binding);
                    mark.venue_position_evidence = evidence;
                    sources.extend(pages);
                    push_event(&mut events, &account, baseline.timestamp, baseline.payload);
                }
                "resolution" => push_resolution(
                    &mut events,
                    &mut sources,
                    &account,
                    &prepared,
                    0,
                    at(stamp),
                    20,
                ),
                "custody" => push_custody(
                    &mut events,
                    &mut sources,
                    identity.unwrap(),
                    dec!(10.624880),
                    &[],
                    at(stamp),
                    110,
                ),
                "credit" => push_credit(&mut events, &mut sources, &account, at(stamp)),
                _ => unreachable!(),
            }
            let target_seq = events.last().unwrap().seq;
            let mut fold = fold_stream(&account, &events, &sources);
            for cutoff in [86_400, 172_800] {
                let bounded = fold.cutoff_summary(cutoff).unwrap();
                let rebuilt = filtered_rebuild(&account, &events, &sources, cutoff).unwrap();
                assert_eq!(
                    cutoff_snapshot(&bounded),
                    cutoff_snapshot(&rebuilt),
                    "kind={kind} stamp={stamp} cutoff={cutoff}"
                );
                if kind != "baseline" {
                    assert_eq!(
                        fold.late_applied
                            .get(&(cutoff, target_seq))
                            .copied()
                            .unwrap_or_default(),
                        usize::from(stamp < cutoff),
                        "kind={kind} stamp={stamp} cutoff={cutoff}"
                    );
                }
                // A midnight before the first Baseline is outside the financial era.
                if bounded.baseline_sequence.is_some() {
                    let daily = daily_from_view(
                        &account,
                        target_seq + u64::try_from(cutoff / 86_400).unwrap(),
                        cutoff,
                        &bounded,
                        &fold.summary().unwrap(),
                        &mut sources,
                    );
                    fold.apply(&daily, &sources).unwrap();
                    assert!(fold.summary().unwrap().daily_marks.contains_key(&cutoff));
                }
            }
            assert!(fold.late_applied.iter().all(|((cutoff, seq), count)| {
                *count == 1
                    && events
                        .iter()
                        .find(|event| event.seq == *seq)
                        .is_some_and(|event| event.timestamp < at(*cutoff))
            }));
        }
    }
}

/// PASS: a redemption confirmation journaled before the Baseline joins only the cutoffs after
/// its stamp: stamped after midnight, that day's view and Daily mark fail as the filtered rebuild
/// does; stamped before, the view equals the rebuild and the mark is accepted.
/// FAIL: a cutoff view includes a pre-Baseline record stamped at or after its cutoff.
#[test]
fn pre_baseline_records_join_only_cutoffs_after_their_stamp() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    for attempt_stamp in [86_399, 86_401] {
        let mut events = Vec::new();
        let identity = push_redemption_attempt(
            &mut events,
            &account,
            &prepared.prepared.condition_id,
            at(attempt_stamp),
        );
        let baseline = baseline_event(&account, 0);
        push_event(&mut events, &account, baseline.timestamp, baseline.payload);
        push_fill(&mut events, &account, &prepared, at(10));
        let mut sources = baseline_sources(&account);
        push_resolution(
            &mut events,
            &mut sources,
            &account,
            &prepared,
            0,
            at(20),
            20,
        );
        let resolved = derive_projection_rows_with_sources(&account, &events, &sources).unwrap();
        push_custody(
            &mut events,
            &mut sources,
            identity,
            resolved.economic_cash.unwrap(),
            &[],
            at(30),
            110,
        );
        let fold = fold_stream(&account, &events, &sources);
        let current = fold.summary().unwrap();
        let rebuilt = filtered_rebuild(&account, &events, &sources, 86_400);
        let seq = events.last().unwrap().seq + 1;
        let mark = daily_from_view(&account, seq, 86_400, &current, &current, &mut sources);
        let mut marked = events.clone();
        marked.push(mark);
        let marked = derive_projection_rows_with_sources(&account, &marked, &sources);
        if attempt_stamp < 86_400 {
            assert_eq!(
                cutoff_snapshot(&fold.cutoff_summary(86_400).unwrap()),
                cutoff_snapshot(&rebuilt.unwrap())
            );
            assert!(marked.is_ok(), "{:?}", marked.err());
        } else {
            for result in [rebuilt, fold.cutoff_summary(86_400), marked] {
                assert!(matches!(
                    result,
                    Err(ProjectionReducerError::CustodyWithoutConfirmedRedemption)
                ));
            }
        }
    }
}

/// PASS: cutoff copies preserve the filtered rebuild's missing-dependency errors.
/// FAIL: an excluded admission, resolution, or confirmation leaks into the cutoff copy.
#[test]
fn cutoff_dependencies_excluded_by_stamp_match_filtered_errors() {
    let account = AccountId::new("account").unwrap();
    for dependency in [
        "admission",
        "resolution",
        "requested",
        "identified",
        "confirmed",
        "attempt",
    ] {
        let mut prepared = finality_prepared();
        if dependency == "admission" {
            prepared.economic.admission.market.observed_at_unix = 86_400;
            prepared.economic.admission.settlement.observed_at_unix = 86_400;
        }
        let mut events = vec![baseline_event(&account, 1)];
        push_fill(&mut events, &account, &prepared, at(10));
        let mut sources = baseline_sources(&account);
        push_resolution(
            &mut events,
            &mut sources,
            &account,
            &prepared,
            0,
            at(20),
            20,
        );
        let identity = push_redemption_attempt(
            &mut events,
            &account,
            &prepared.prepared.condition_id,
            at(25),
        );
        push_custody(
            &mut events,
            &mut sources,
            identity,
            dec!(10.624880),
            &[],
            at(30),
            110,
        );
        for event in &mut events {
            let excluded = matches!(
                (dependency, &event.payload),
                ("admission", LiveJournalPayload::AdmissionEvaluated(_))
                    | ("resolution", LiveJournalPayload::ResolutionFinalized(_))
                    | (
                        "requested" | "attempt",
                        LiveJournalPayload::RedemptionRequested(_)
                    )
                    | (
                        "identified" | "attempt",
                        LiveJournalPayload::RedemptionTransactionIdentified(_)
                    )
                    | (
                        "confirmed" | "attempt",
                        LiveJournalPayload::RedemptionReceiptTransition(_)
                    )
            );
            if excluded {
                event.timestamp = at(86_400);
            }
        }
        let mut fold = fold_stream(&account, &events, &sources);
        let bounded = fold.cutoff_summary(86_400);
        let rebuilt = filtered_rebuild(&account, &events, &sources, 86_400);
        if let Err(error) = &bounded {
            let current = fold.summary().unwrap();
            let daily = daily_from_view(
                &account,
                events.last().unwrap().seq + 1,
                86_400,
                &current,
                &current,
                &mut sources,
            );
            assert_eq!(
                format!("{:?}", fold.apply(&daily, &sources).err().unwrap()),
                format!("{error:?}"),
                "dependency={dependency}"
            );
        }
        match dependency {
            "admission" => {
                assert!(matches!(
                    bounded,
                    Err(ProjectionReducerError::MissingApprovedAdmission)
                ));
                assert!(matches!(
                    rebuilt,
                    Err(ProjectionReducerError::MissingApprovedAdmission)
                ));
            }
            "resolution" => {
                assert!(matches!(
                    bounded,
                    Err(ProjectionReducerError::IdentityConflict)
                ));
                assert!(matches!(
                    rebuilt,
                    Err(ProjectionReducerError::IdentityConflict)
                ));
            }
            "confirmed" | "attempt" => {
                assert!(matches!(
                    bounded,
                    Err(ProjectionReducerError::CustodyWithoutConfirmedRedemption)
                ));
                assert!(matches!(
                    rebuilt,
                    Err(ProjectionReducerError::CustodyWithoutConfirmedRedemption)
                ));
            }
            _ => assert_eq!(
                cutoff_snapshot(&bounded.unwrap()),
                cutoff_snapshot(&rebuilt.unwrap())
            ),
        }
    }
}

/// PASS: Baseline, resolution, custody, Daily, and credit require evidence received by their stamp.
/// FAIL: a record one nanosecond before a cited receipt passes, or equality/later stamps fail.
#[test]
fn source_citing_records_require_receipts_no_later_than_stamp() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    for kind in ["baseline", "resolution", "custody", "daily", "credit"] {
        let mut events = if matches!(kind, "baseline" | "credit") {
            Vec::new()
        } else {
            filled_stream(&account)
        };
        let mut sources = baseline_sources(&account);
        match kind {
            "baseline" => {
                events.push(baseline_event(&account, 1));
            }
            "resolution" => push_resolution(
                &mut events,
                &mut sources,
                &account,
                &prepared,
                0,
                at(20),
                20,
            ),
            "custody" => {
                push_resolution(
                    &mut events,
                    &mut sources,
                    &account,
                    &prepared,
                    0,
                    at(20),
                    20,
                );
                let identity = push_redemption_attempt(
                    &mut events,
                    &account,
                    &prepared.prepared.condition_id,
                    at(25),
                );
                push_custody(
                    &mut events,
                    &mut sources,
                    identity,
                    dec!(10.624880),
                    &[],
                    at(30),
                    110,
                );
            }
            "daily" => {
                let fold = fold_stream(&account, &events, &sources);
                let current = fold.summary().unwrap();
                let daily = daily_from_view(
                    &account,
                    events.last().unwrap().seq + 1,
                    86_400,
                    &current,
                    &current,
                    &mut sources,
                );
                events.push(daily);
            }
            "credit" => {
                events.push(baseline_event(&account, 1));
                push_credit(&mut events, &mut sources, &account, at(30));
            }
            _ => unreachable!(),
        }
        let target = events.last().unwrap();
        let receipt = target.timestamp + time::Duration::nanoseconds(1);
        let receipts = match &target.payload {
            LiveJournalPayload::AccountPortfolioMarked(mark) => mark
                .venue_position_evidence
                .pages
                .iter()
                .map(|page| page.receipt)
                .chain(mark.prices.iter().map(|price| price.receipt))
                .collect::<Vec<_>>(),
            LiveJournalPayload::ResolutionFinalized(resolution) => {
                vec![resolution.source_append_receipt]
            }
            LiveJournalPayload::RedemptionCustodyReconciled(custody) => {
                custody.venue_position_receipts.clone()
            }
            LiveJournalPayload::CollateralCredited(credit) => credit
                .venue_position_evidence
                .pages
                .iter()
                .map(|page| page.receipt)
                .collect(),
            _ => unreachable!(),
        };
        assert!(!receipts.is_empty());
        for delayed in receipts {
            let mut delayed_sources = sources.clone();
            for source in &mut delayed_sources {
                if delayed.sequence == source.seq && delayed.this_hash == source.this_hash {
                    source.received_at = ReceivedAt(receipt);
                }
            }
            let mut delayed_events = events.clone();
            assert!(
                matches!(
                    derive_projection_rows_with_sources(
                        &account,
                        &delayed_events,
                        &delayed_sources
                    ),
                    Err(ProjectionReducerError::EvidenceAfterStamp)
                ),
                "kind={kind} receipt={delayed:?}"
            );
            for stamp in [receipt, receipt + time::Duration::nanoseconds(1)] {
                delayed_events.last_mut().unwrap().timestamp = stamp;
                derive_projection_rows_with_sources(&account, &delayed_events, &delayed_sources)
                    .unwrap_or_else(|error| {
                        panic!("kind={kind} stamp={stamp} receipt={delayed:?}: {error:?}")
                    });
            }
        }
    }
}

/// PASS: source-citing audit history before Baseline is exempt from the receipt/stamp rule.
/// FAIL: stale pre-era records prevent an otherwise valid Baseline or affect its economics.
#[test]
fn evidence_after_stamp_is_ignored_before_baseline() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    let mut events = Vec::new();
    let mut sources = Vec::new();
    push_resolution(
        &mut events,
        &mut sources,
        &account,
        &prepared,
        1,
        at(20),
        20,
    );
    let identity = push_redemption_attempt(
        &mut events,
        &account,
        &prepared.prepared.condition_id,
        at(25),
    );
    push_custody(
        &mut events,
        &mut sources,
        identity,
        dec!(10),
        &[],
        at(30),
        110,
    );
    push_credit(&mut events, &mut sources, &account, at(40));
    let mut daily = baseline_event(&account, 0);
    let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut daily.payload else {
        unreachable!()
    };
    mark.kind = MarkKind::Daily;
    mark.cutoff_unix = 86_400;
    let (evidence, pages) = empty_position_evidence(150, at(200_000), &mark.account_binding);
    mark.venue_position_evidence = evidence;
    sources.extend(pages);
    push_event(&mut events, &account, at(50), daily.payload);
    for source in &mut sources {
        source.received_at = ReceivedAt(at(200_000));
    }
    sources.extend(baseline_sources(&account));
    let baseline = baseline_event(&account, 0);
    push_event(&mut events, &account, at(100), baseline.payload);
    let derived = derive_projection_rows_with_sources(&account, &events, &sources).unwrap();
    assert_eq!(derived.economic_cash, Some(dec!(10)));
    assert_eq!(derived.receivable, Decimal::ZERO);
    assert!(derived.custody_positions.is_empty());
    assert!(derived.daily_marks.is_empty());
    assert_eq!(derived.baseline_sequence, Some(events.last().unwrap().seq));
}

pub(super) async fn retain_source(state: &FanoutState, source: EventEnvelope) -> AppendReceipt {
    state
        .config
        .source_log
        .append(EnvelopeIn {
            source_id: source.source_id,
            schema_version: source.schema_version,
            parser_version: source.parser_version,
            observed_at: source.observed_at,
            received_at: source.received_at,
            content_type: source.content_type,
            payload: source.payload,
        })
        .await
        .unwrap()
}

/// PASS: stamping uses the later of now and every cited receipt, independent of receipt order.
/// FAIL: a newer receipt is missed, an older one moves the stamp back, or empty evidence changes now.
#[tokio::test]
async fn runtime_stamp_uses_now_or_newest_receipt() {
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    let oldest = retain_source(
        &state,
        source_envelope(fixture_receipt(1), "stamp-fixture", b"{}".to_vec(), at(10)),
    )
    .await;
    let newest_at = at(30) + time::Duration::nanoseconds(123);
    let newest = retain_source(
        &state,
        source_envelope(
            fixture_receipt(2),
            "stamp-fixture",
            b"{}".to_vec(),
            newest_at,
        ),
    )
    .await;
    assert_eq!(
        stamp_after_evidence(&state, at(20), std::iter::empty()).unwrap(),
        at(20)
    );
    for receipts in [[oldest, newest], [newest, oldest]] {
        assert_eq!(
            stamp_after_evidence(&state, at(20), &receipts).unwrap(),
            newest_at
        );
        assert_eq!(
            stamp_after_evidence(&state, newest_at, &receipts).unwrap(),
            newest_at
        );
        assert_eq!(
            stamp_after_evidence(&state, at(40), &receipts).unwrap(),
            at(40)
        );
    }
}

pub(super) async fn append_resolution_to_state(
    state: &FanoutState,
    account: &AccountId,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) {
    let mut events = Vec::new();
    let mut sources = Vec::new();
    push_resolution(&mut events, &mut sources, account, prepared, 0, now, 20);
    let receipt = retain_source(state, sources.remove(0)).await;
    let mut event = events.remove(0);
    let LiveJournalPayload::ResolutionFinalized(resolution) = &mut event.payload else {
        unreachable!()
    };
    let resolution = Arc::make_mut(resolution);
    resolution.source_append_receipt = receipt;
    state
        .config
        .journal
        .append(account.clone(), now, event.payload)
        .unwrap();
}

async fn bind_current_economics(
    state: &FanoutState,
    account: &AccountId,
    prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
    derived: &ProjectionDerivation,
    now: OffsetDateTime,
) {
    let cash =
        CollateralAmount::from_decimal_exact(derived.economic_cash.unwrap() - derived.receivable)
            .unwrap();
    prepared.economic.balance.cash_before = cash;
    prepared.account_state =
        account_state_fixture_for_binding(cash, now, derived.account_binding.as_ref().unwrap());
    prepared.identity.idempotency_key =
        LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, account);
    prepared.economic.admission.scheduled_end_unix = Some(now.unix_timestamp() + 300);
    bind_empty_live_risk_to_paper_prefix(state, prepared, now);
    let frames = state.config.paper_log.snapshot().unwrap();
    let projection = prepared.identity.fill_projection.as_ref().unwrap();
    prepared.economic.risk = compose_live_risk_audit(
        derived,
        &paper_era_at_evaluation(&frames, unix_millis_i64(now)).unwrap(),
        &BTreeMap::new(),
        &[],
        now,
        LiveRiskProposal {
            leader_wallet: Some(projection.leader_wallet.clone()),
            market_id: Some(prepared.economic.market.market_id.clone()),
            proposed_debit: prepared.economic.balance.worst_case_debit,
            per_trade_cap_bps: 10_000,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
        },
    )
    .unwrap();
    append_economic_sources(state, prepared, account, now, None).await;
    if prepared.prepared.token_id.0 != "123" {
        // The shared fixture emits 123/124. Retain the corresponding recorded responses for
        // this condition's distinct token pair, keeping its observation/continuation fixture.
        let tokens = &prepared.economic.admission.market.ordered_outcome_token_ids;
        let mut receipts = Vec::new();
        for mut source in economic_source_envelopes(prepared, now).into_iter().take(4) {
            let body = String::from_utf8(source.payload)
                .unwrap()
                .replace("123", &tokens[0].0)
                .replace("124", &tokens[1].0);
            source.payload = body.into_bytes();
            if source.source_id.0 == CLOB_LONG_MARKET_SOURCE_ID {
                prepared.economic.admission.settlement.raw_evidence_hash =
                    blake3::hash(&source.payload).to_hex().to_string();
            }
            receipts.push(retain_source(state, source).await);
        }
        prepared.economic.admission.receipts = AdmissionReceipts {
            gamma: receipts[0],
            clob_long: receipts[1],
            clob_compact: receipts[2],
        };
        prepared.economic.book_receipt = receipts[3];
    }
}

/// PASS: with B outstanding after A custody, incremental and batch strict replay accept E minus R_B.
/// FAIL: spendable cash is rejected or cash including B's outstanding payout is accepted.
#[tokio::test]
async fn outstanding_receivable_is_excluded_from_strict_admission_cash() {
    for use_economic_cash in [false, true] {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let snapshot = armed_snapshot("acct");
        let account = snapshot.accounts[0].account_id.clone();
        let state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        let now = at(1_700_000_000);
        let baseline = append_recovery_baseline(
            &state,
            &account,
            now,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        append_recovery_filled_position(&state, &account, &baseline, now).await;
        let first = finality_prepared();
        append_resolution_to_state(&state, &account, &first, now).await;
        let before_second = live_summary(&state, &account).unwrap();
        let mut second = other_condition_prepared();
        bind_current_economics(&state, &account, &mut second, &before_second, now).await;
        assert_eq!(
            second.economic.balance.cash_before,
            CollateralAmount::from_atomic(7_499_880)
        );
        // Reuse the fill stream constructor with the journal's actual prepared sequence.
        let mut second_events = Vec::new();
        push_fill(&mut second_events, &account, &second, now);
        let mut prepared_seq = None;
        for mut event in second_events {
            if let LiveJournalPayload::OrderFillFinalized(fill) = &mut event.payload {
                Arc::make_mut(fill).prepared_journal_seq = prepared_seq.unwrap();
            }
            let is_prepared = matches!(event.payload, LiveJournalPayload::OrderPrepared(_));
            let appended = state
                .config
                .journal
                .append(account.clone(), now, event.payload)
                .unwrap();
            if is_prepared {
                prepared_seq = Some(appended.seq);
            }
        }
        append_resolution_to_state(&state, &account, &second, now).await;
        let resolved = live_summary(&state, &account).unwrap();
        assert_eq!(resolved.receivable_by_condition.len(), 2);
        assert_eq!(resolved.receivable, dec!(6.25));
        let remaining = resolved
            .custody_positions
            .iter()
            .filter(|position| position.condition_id == second.prepared.condition_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut custody_events = Vec::new();
        let mut sources = Vec::new();
        let identity = push_redemption_attempt(
            &mut custody_events,
            &account,
            &first.prepared.condition_id,
            now,
        );
        push_custody(
            &mut custody_events,
            &mut sources,
            identity,
            dec!(8.124760),
            &remaining,
            now,
            110,
        );
        let mut receipts = Vec::new();
        for source in sources {
            receipts.push(retain_source(&state, source).await);
        }
        for mut event in custody_events {
            if let LiveJournalPayload::RedemptionCustodyReconciled(custody) = &mut event.payload {
                let custody = Arc::make_mut(custody);
                custody.venue_position_receipts.clone_from(&receipts);
            }
            state
                .config
                .journal
                .append(account.clone(), now, event.payload)
                .unwrap();
        }
        let before = live_summary(&state, &account).unwrap();
        assert_eq!(before.economic_cash, Some(dec!(11.249760)));
        assert_eq!(before.receivable, dec!(3.125));
        assert_eq!(before.receivable_by_condition.len(), 1);
        assert!(
            before
                .receivable_by_condition
                .contains_key(&second.prepared.condition_id.0)
        );
        let prefix = replay_live_account(&state, &account).unwrap();
        let oracle = derive_projection_rows_for_state(&state, &account, &prefix).unwrap();
        assert_eq!(cutoff_snapshot(&before), cutoff_snapshot(&oracle));
        let mut next = condition_prepared("99", 127, "after-one-custody");
        bind_current_economics(&state, &account, &mut next, &before, now).await;
        assert_eq!(
            next.economic.balance.cash_before,
            CollateralAmount::from_atomic(8_124_760)
        );
        if use_economic_cash {
            next.economic.balance.cash_before = CollateralAmount::from_atomic(11_249_760);
        }
        append_approved_admission(state.config.journal.as_ref(), &account, &next, now);
        let events = replay_live_account(&state, &account).unwrap();
        let batch = derive_projection_rows_for_state(&state, &account, &events);
        let incremental = live_summary(&state, &account);
        if use_economic_cash {
            assert!(matches!(
                batch,
                Err(ProjectionReducerError::InvalidRiskEvidence)
            ));
            assert!(matches!(incremental, Err(LiveStateError::Rejected(_))));
            assert!(
                state.live.lock().unwrap().accounts[&account]
                    .rejected
                    .is_some()
            );
        } else {
            assert_eq!(
                cutoff_snapshot(&incremental.unwrap()),
                cutoff_snapshot(&batch.unwrap())
            );
            let live = sync_live(&state).unwrap();
            assert!(
                live.view(&account)
                    .unwrap()
                    .unwrap()
                    .applied
                    .values()
                    .all(|count| *count == 1)
            );
        }
    }
}

/// PASS: late admissions, resolutions, and redemption attempts update every still-pending copy once.
/// FAIL: a consumed Daily's copy receives a late record, or the next midnight misses it.
#[test]
fn late_dependencies_before_and_after_daily_only_update_pending_copies() {
    let account = AccountId::new("account").unwrap();
    let prepared = finality_prepared();
    for kind in ["admission", "resolution", "attempt"] {
        for after_daily in [false, true] {
            let mut events = filled_stream(&account);
            let mut sources = baseline_sources(&account);
            if kind == "attempt" {
                push_resolution(
                    &mut events,
                    &mut sources,
                    &account,
                    &prepared,
                    0,
                    at(20),
                    20,
                );
            }
            let crossing = crossing_event(&account, 0, at(172_800));
            push_event(&mut events, &account, crossing.timestamp, crossing.payload);
            let mut fold = fold_stream(&account, &events, &sources);
            let mut late = events.clone();
            let prefix_len = late.len();
            match kind {
                "admission" => {
                    let mut next = finality_prepared();
                    next.identity = projection_identity("late-admission");
                    next.economic.admission.market.observed_at_unix = 86_399;
                    next.economic.admission.settlement.observed_at_unix = 86_399;
                    next.economic.balance.cash_before = CollateralAmount::from_atomic(7_499_880);
                    next.account_state =
                        account_state_fixture(CollateralAmount::from_atomic(7_499_880), at(86_399));
                    next.prepared.order_hash = format!("0x{}", "55".repeat(32));
                    push_event(
                        &mut late,
                        &account,
                        at(86_399),
                        approved_admission_event(&account, &next, 0, at(86_399)).payload,
                    );
                    // The prepared order consumes the admission, so a view without it fails.
                    push_event(
                        &mut late,
                        &account,
                        at(86_399),
                        LiveJournalPayload::OrderPrepared(Arc::new(*next)),
                    );
                }
                "resolution" => push_resolution(
                    &mut late,
                    &mut sources,
                    &account,
                    &prepared,
                    0,
                    at(86_399),
                    20,
                ),
                "attempt" => {
                    push_redemption_attempt(
                        &mut late,
                        &account,
                        &prepared.prepared.condition_id,
                        at(86_399),
                    );
                }
                _ => unreachable!(),
            }
            let late = late.split_off(prefix_len);
            if !after_daily {
                for event in &late {
                    fold.apply(event, &sources).unwrap();
                    events.push(event.clone());
                }
            }
            let bounded = fold.cutoff_summary(86_400).unwrap();
            assert_eq!(
                cutoff_snapshot(&bounded),
                cutoff_snapshot(&filtered_rebuild(&account, &events, &sources, 86_400).unwrap())
            );
            let daily = daily_from_view(
                &account,
                events.last().unwrap().seq + 1,
                86_400,
                &bounded,
                &fold.summary().unwrap(),
                &mut sources,
            );
            fold.apply(&daily, &sources).unwrap();
            events.push(daily);
            let mut late_sequences = Vec::new();
            for mut event in late {
                if after_daily {
                    event.seq = events.last().unwrap().seq + 1;
                    fold.apply(&event, &sources).unwrap();
                    events.push(event.clone());
                }
                late_sequences.push(event.seq);
            }
            if kind == "attempt" {
                let identity = RedemptionAttemptIdentity {
                    account_id: account.clone(),
                    condition_id: prepared.prepared.condition_id.clone(),
                    adapter: build_redemption_call(prepared.prepared.condition_id.clone(), false)
                        .unwrap()
                        .to,
                    custody_wallet: prepared.prepared.funder.clone(),
                };
                push_custody(
                    &mut events,
                    &mut sources,
                    identity,
                    dec!(10.624880),
                    &[],
                    at(86_399),
                    110,
                );
                fold.apply(events.last().unwrap(), &sources).unwrap();
                late_sequences.push(events.last().unwrap().seq);
            }
            assert_eq!(
                cutoff_snapshot(&fold.cutoff_summary(172_800).unwrap()),
                cutoff_snapshot(&filtered_rebuild(&account, &events, &sources, 172_800).unwrap()),
                "kind={kind} after_daily={after_daily}"
            );
            for seq in late_sequences {
                assert_eq!(fold.late_applied.get(&(172_800, seq)), Some(&1));
                if after_daily {
                    assert!(!fold.late_applied.contains_key(&(86_400, seq)));
                }
            }
            assert!(!fold.cutoffs.contains_key(&86_400));
        }
    }
}
