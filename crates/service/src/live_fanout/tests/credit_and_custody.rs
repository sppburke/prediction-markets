use super::resolution_and_cutoffs::retain_source;
use super::*;

fn amount(value: Decimal) -> CollateralAmount {
    CollateralAmount::from_decimal_exact(value).unwrap()
}

fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).unwrap()
}

fn filled_stream(account_id: &AccountId) -> Vec<LiveJournalEvent> {
    let prepared = finality_prepared();
    vec![
        baseline_event(account_id, 1),
        approved_admission_event(account_id, &prepared, 2, at(5)),
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 3,
            timestamp: at(5),
            payload: LiveJournalPayload::OrderPrepared(prepared.clone().into()),
        },
        finality_matched_event(account_id, &prepared, 4, at(6)),
        finalized_fill_event(account_id, &prepared, 5, 3),
    ]
}

fn credit_event(
    account_id: &AccountId,
    seq: u64,
    stamp: OffsetDateTime,
    collateral: Decimal,
    credited: Decimal,
    positions: &[CanonicalPositionAudit],
) -> (LiveJournalEvent, Vec<EventEnvelope>) {
    let binding = account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
    let bodies = [false, true].map(|redeemable| {
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
    });
    let (venue_position_evidence, sources) = position_evidence(120, stamp, &binding, bodies);
    (
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: stamp,
            payload: LiveJournalPayload::CollateralCredited(Box::new(
                pe_execution_core::CollateralCreditedAudit {
                    account_binding: binding.clone(),
                    account_state: account_state_fixture_for_binding(
                        amount(collateral),
                        stamp,
                        &binding,
                    ),
                    venue_positions: positions.to_vec(),
                    venue_position_evidence,
                    credited: amount(credited),
                },
            )),
        },
        sources,
    )
}

/// PASS: an exact authenticated surplus raises cash and free collateral without rebasing equity.
/// FAIL: the credit is rejected, counted incorrectly, or changes the Baseline equity.
#[test]
fn reducer_accepts_exact_credit_without_rebasing_equity() {
    let account_id = AccountId::new("account").unwrap();
    let baseline = baseline_event(&account_id, 1);
    let before =
        derive_with_baseline_evidence(&account_id, std::slice::from_ref(&baseline), &[]).unwrap();
    let (credit, sources) = credit_event(&account_id, 2, at(30), dec!(11.25), dec!(1.25), &[]);
    let after = derive_with_baseline_evidence(&account_id, &[baseline, credit], &sources).unwrap();

    assert_eq!(
        after.economic_cash,
        Some(before.economic_cash.unwrap() + dec!(1.25))
    );
    assert_eq!(after.latest_free_collateral, Some(dec!(11.25)));
    assert_eq!(after.baseline_equity, before.baseline_equity);
    assert_eq!(after.baseline_equity, Some(amount(dec!(10))));
    assert_eq!(after.reserved, Decimal::ZERO);
    assert_eq!(after.receivable, Decimal::ZERO);
}

/// PASS: either one-atomic error, a nonpositive surplus, and a repeated credit fail closed.
/// FAIL: any inexact, nonexistent, or already-consumed surplus is accepted as a credit.
#[test]
fn reducer_rejects_inexact_nonpositive_and_duplicate_credits() {
    let account_id = AccountId::new("account").unwrap();
    for (collateral, credited) in [
        (dec!(11.25), dec!(1.249999)),
        (dec!(11.25), dec!(1.250001)),
        (dec!(10), Decimal::ZERO),
        (dec!(9.999999), dec!(0.000001)),
    ] {
        let (credit, sources) = credit_event(&account_id, 2, at(30), collateral, credited, &[]);
        let result = derive_with_baseline_evidence(
            &account_id,
            &[baseline_event(&account_id, 1), credit],
            &sources,
        );
        assert!(
            matches!(result, Err(ProjectionReducerError::InvalidCollateralCredit)),
            "collateral={collateral}, credited={credited}: {:?}",
            result.err()
        );
    }

    let (credit, sources) = credit_event(&account_id, 2, at(30), dec!(11.25), dec!(1.25), &[]);
    let mut events = vec![baseline_event(&account_id, 1), credit.clone()];
    derive_with_baseline_evidence(&account_id, &events, &sources).unwrap();
    let mut duplicate = credit;
    duplicate.seq = 3;
    events.push(duplicate);
    assert!(matches!(
        derive_with_baseline_evidence(&account_id, &events, &sources),
        Err(ProjectionReducerError::InvalidCollateralCredit)
    ));
}

/// PASS: both a nonterminal reservation and an outstanding winning receivable prevent credits.
/// FAIL: a surplus becomes spendable while either reservation or receivable remains outstanding.
#[test]
fn reducer_rejects_credit_with_reserved_order_or_receivable() {
    let account_id = AccountId::new("account").unwrap();
    for receivable in [false, true] {
        let mut events = filled_stream(&account_id);
        let mut sources = Vec::new();
        if receivable {
            let resolution = resolution_event(&account_id, 6);
            sources.push(resolution_source_envelope(&resolution));
            events.push(resolution);
        } else {
            events.truncate(3);
        }
        let before = derive_with_baseline_evidence(&account_id, &events, &sources).unwrap();
        if receivable {
            assert_eq!(before.receivable, dec!(3.125));
            assert_eq!(before.reserved, Decimal::ZERO);
        } else {
            assert_eq!(before.reserved, dec!(2.500120));
            assert_eq!(before.receivable, Decimal::ZERO);
        }
        let (credit, credit_sources) = credit_event(
            &account_id,
            7,
            at(30),
            before.economic_cash.unwrap() + dec!(1.25),
            dec!(1.25),
            &before.custody_positions,
        );
        sources.extend(credit_sources);
        events.push(credit);
        let result = derive_with_baseline_evidence(&account_id, &events, &sources);
        assert!(
            matches!(result, Err(ProjectionReducerError::InvalidCollateralCredit)),
            "receivable={receivable}: {:?}",
            result.err()
        );
    }
}

/// PASS: journal-versus-venue comparison masks redeemable while preserving exact held quantity.
/// FAIL: a redeemable-only change blocks credit, or missing/different held quantity is accepted.
#[test]
fn reducer_credit_masks_redeemable_but_rejects_inventory_drift() {
    let account_id = AccountId::new("account").unwrap();
    let events = filled_stream(&account_id);
    let before = derive_with_baseline_evidence(&account_id, &events, &[]).unwrap();
    let mut redeemable = before.custody_positions.clone();
    assert_eq!(redeemable.len(), 1);
    assert!(!redeemable[0].redeemable);
    redeemable[0].redeemable = true;
    let (credit, sources) = credit_event(
        &account_id,
        6,
        at(30),
        before.economic_cash.unwrap() + dec!(1.25),
        dec!(1.25),
        &redeemable,
    );
    let mut credited_events = events.clone();
    credited_events.push(credit);
    let after = derive_with_baseline_evidence(&account_id, &credited_events, &sources).unwrap();
    assert_eq!(after.economic_cash, Some(dec!(8.749880)));
    assert_eq!(after.latest_free_collateral, Some(dec!(8.749880)));
    assert_eq!(after.baseline_equity, before.baseline_equity);
    assert_eq!(after.custody_positions, before.custody_positions);

    let mut changed_size = redeemable;
    changed_size[0].size = ShareAmount::from_atomic(3_125_001);
    for positions in [Vec::new(), changed_size] {
        let (credit, sources) = credit_event(
            &account_id,
            6,
            at(30),
            dec!(8.749880),
            dec!(1.25),
            &positions,
        );
        let mut changed_events = events.clone();
        changed_events.push(credit);
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &changed_events, &sources),
            Err(ProjectionReducerError::CustodyInventoryMismatch)
        ));
    }
}

/// PASS: even one cited position page received after the credit stamp prevents replay.
/// FAIL: future position evidence can authorize a previously stamped collateral credit.
#[test]
fn reducer_credit_rejects_position_evidence_after_stamp() {
    let account_id = AccountId::new("account").unwrap();
    let (credit, mut sources) = credit_event(&account_id, 2, at(30), dec!(11.25), dec!(1.25), &[]);
    let events = [baseline_event(&account_id, 1), credit];
    derive_with_baseline_evidence(&account_id, &events, &sources).unwrap();
    sources[1].received_at = ReceivedAt(at(30) + time::Duration::nanoseconds(1));
    assert!(matches!(
        derive_with_baseline_evidence(&account_id, &events, &sources),
        Err(ProjectionReducerError::EvidenceAfterStamp)
    ));
}

/// PASS: a post-midnight credit changes current cash while the accepted Daily mark uses old cash.
/// FAIL: the credit leaks into cutoff equity, rejects the Daily mark, or disappears afterward.
#[test]
fn credit_after_midnight_is_excluded_from_daily_cutoff() {
    let account_id = AccountId::new("account").unwrap();
    let baseline = baseline_event(&account_id, 1);
    let (credit, sources) = credit_event(&account_id, 2, at(86_401), dec!(11.25), dec!(1.25), &[]);
    let mut all_sources = baseline_sources(&account_id);
    all_sources.extend(sources);
    let mut fold = ProjectionFold::new(account_id.clone());
    fold.apply(&baseline, &all_sources).unwrap();
    fold.apply(&credit, &all_sources).unwrap();
    assert_eq!(fold.summary().unwrap().economic_cash, Some(dec!(11.25)));
    let cutoff = fold.cutoff_summary(86_400).unwrap();
    assert_eq!(cutoff.economic_cash, Some(dec!(10)));
    let LiveJournalPayload::CollateralCredited(credit_audit) = &credit.payload else {
        unreachable!();
    };
    let daily = LiveJournalEvent {
        account_id: account_id.clone(),
        seq: 3,
        timestamp: at(86_402),
        payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
            pe_execution_core::AccountPortfolioMarkedAudit {
                kind: MarkKind::Daily,
                cutoff_unix: 86_400,
                account_binding: credit_audit.account_binding.clone(),
                account_state: credit_audit.account_state.clone(),
                venue_positions: Vec::new(),
                venue_position_evidence: credit_audit.venue_position_evidence.clone(),
                marked_positions: cutoff.custody_positions,
                prices: Vec::new(),
                equity: amount(cutoff.economic_cash.unwrap()),
            },
        )),
    };
    fold.apply(&daily, &all_sources).unwrap();
    let summary = fold.summary().unwrap();
    assert_eq!(summary.daily_marks.get(&86_400), Some(&amount(dec!(10))));
    assert_eq!(summary.economic_cash, Some(dec!(11.25)));
    assert_eq!(summary.baseline_equity, Some(amount(dec!(10))));
    let replayed =
        derive_with_baseline_evidence(&account_id, &[baseline, credit, daily], &all_sources[2..])
            .unwrap();
    assert_eq!(
        projection_snapshot(&summary),
        projection_snapshot(&replayed)
    );
}

/// PASS: strict admission replay reconstructs credited spendable cash and rejects pre-credit cash.
/// FAIL: credited cash cannot replay exactly, or an admission can still size from the old balance.
#[test]
fn strict_replay_reconstructs_spendable_cash_after_credit() {
    let (_dir, account_id, mut events, mut sources, paper_frames, mut admission) =
        reconstructed_live_risk_fixture();
    verify_replayed_live_risk(&account_id, &events, &admission, &sources, &paper_frames).unwrap();
    let before = derive_projection_rows_with_sources(&account_id, &events, &sources).unwrap();
    let old_cash = admission.economic.balance.cash_before;
    let new_cash = before.economic_cash.unwrap() + dec!(1.25);
    let now = at(20);
    let (credit, credit_sources) = credit_event(
        &account_id,
        5,
        now,
        new_cash,
        dec!(1.25),
        &before.custody_positions,
    );
    events.push(credit);
    sources.extend(credit_sources);
    let derived = derive_projection_rows_with_sources(&account_id, &events, &sources).unwrap();
    assert_eq!(derived.economic_cash, Some(dec!(8.749880)));
    assert_eq!(derived.receivable, Decimal::ZERO);
    admission.economic.balance.cash_before = amount(new_cash);
    admission.account_state = Some(account_state_fixture_for_binding(
        amount(new_cash),
        now,
        derived.account_binding.as_ref().unwrap(),
    ));
    let price_receipts = admission.economic.risk.price_receipts.clone();
    let mids = replay_live_risk_prices(
        &derived.positions,
        &price_receipts,
        unix_millis_i64(now),
        &sources,
        None,
    )
    .unwrap();
    let projection = admission.identity.fill_projection.as_ref().unwrap();
    admission.economic.risk = compose_live_risk_audit(
        &derived,
        &paper_era_at_evaluation(&paper_frames, unix_millis_i64(now)).unwrap(),
        &mids,
        &price_receipts,
        now,
        LiveRiskProposal {
            leader_wallet: Some(projection.leader_wallet.clone()),
            market_id: Some(projection.market_id.clone()),
            proposed_debit: admission.economic.balance.worst_case_debit,
            per_trade_cap_bps: 10_000,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
        },
    )
    .unwrap();
    verify_replayed_live_risk(&account_id, &events, &admission, &sources, &paper_frames).unwrap();
    assert_eq!(
        admission.economic.balance.cash_before,
        amount(dec!(8.749880))
    );
    admission.economic.balance.cash_before = old_cash;
    assert!(matches!(
        verify_replayed_live_risk(&account_id, &events, &admission, &sources, &paper_frames),
        Err(ProjectionReducerError::InvalidRiskEvidence)
    ));
}

/// PASS: payout alone preserves equity, a concurrent credit adds its surplus, and a deficit fails.
/// FAIL: custody double-counts payout, misses surplus, retains the receivable, or accepts a deficit.
#[test]
fn custody_preserves_payout_recognizes_surplus_and_rejects_atomic_deficit() {
    let account_id = AccountId::new("account").unwrap();
    let mut events = filled_stream(&account_id);
    let resolution = resolution_event(&account_id, 6);
    let mut sources = vec![resolution_source_envelope(&resolution)];
    events.push(resolution);
    let before = derive_with_baseline_evidence(&account_id, &events, &sources).unwrap();
    assert_eq!(before.economic_cash, Some(dec!(10.624880)));
    assert_eq!(before.receivable, dec!(3.125));
    let binding = before.account_binding.as_ref().unwrap();
    let identity = RedemptionAttemptIdentity {
        account_id: account_id.clone(),
        condition_id: finality_prepared().prepared.condition_id.clone(),
        adapter: "adapter".to_owned(),
        custody_wallet: binding.custody_wallet.clone(),
    };
    events.extend([
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 7,
            timestamp: at(28),
            payload: LiveJournalPayload::RedemptionTransactionIdentified(Box::new(
                pe_execution_core::RedemptionTransactionAudit {
                    identity: identity.clone(),
                    attempt_count: 1,
                    transaction_id: "tx-1".to_owned(),
                    submit_body_hash: "body".to_owned(),
                    evidence: Vec::new(),
                    evidence_hashes: Vec::new(),
                },
            )),
        },
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 8,
            timestamp: at(29),
            payload: LiveJournalPayload::RedemptionReceiptTransition(Box::new(
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
        },
    ]);
    let (position_evidence, position_sources) = empty_position_evidence(110, at(30), binding);
    sources.extend(position_sources);
    for surplus in [Decimal::ZERO, dec!(2), dec!(-0.000001)] {
        let collateral =
            (before.economic_cash.unwrap() - before.receivable) + before.receivable + surplus;
        let custody = LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 9,
            timestamp: at(30),
            payload: LiveJournalPayload::RedemptionCustodyReconciled(Arc::new(
                pe_execution_core::RedemptionCustodyReconciledAudit {
                    identity: identity.clone(),
                    account_state: account_state_fixture_for_binding(
                        amount(collateral),
                        at(30),
                        binding,
                    ),
                    venue_positions: Vec::new(),
                    venue_position_receipts: position_evidence
                        .pages
                        .iter()
                        .map(|page| page.receipt)
                        .collect(),
                },
            )),
        };
        let mut custody_events = events.clone();
        custody_events.push(custody.clone());
        let result = derive_with_baseline_evidence(&account_id, &custody_events, &sources);
        if surplus < Decimal::ZERO {
            assert!(matches!(result, Err(ProjectionReducerError::CashMismatch)));
        } else {
            let after = result.unwrap();
            assert_eq!(
                after.economic_cash,
                Some(before.economic_cash.unwrap() + surplus)
            );
            assert_eq!(after.latest_free_collateral, Some(collateral));
            assert_eq!(after.receivable, Decimal::ZERO);
            assert!(after.receivable_by_condition.is_empty());
            assert!(after.custody_positions.is_empty());
            assert_eq!(after.baseline_equity, before.baseline_equity);
            // A duplicate custody fact cannot credit the same surplus twice.
            let mut duplicate = custody;
            duplicate.seq = 10;
            custody_events.push(duplicate);
            let repeated =
                derive_with_baseline_evidence(&account_id, &custody_events, &sources).unwrap();
            assert_eq!(repeated.economic_cash, after.economic_cash);
            // A second confirmed family for the reconciled condition has no receivable left to
            // consume, so it cannot recognize a surplus the runtime would never journal.
            let second = RedemptionAttemptIdentity {
                adapter: "adapter-2".to_owned(),
                ..identity.clone()
            };
            let mut second_events = custody_events.clone();
            for (offset, mut event) in custody_events
                [custody_events.len() - 4..custody_events.len() - 2]
                .iter()
                .cloned()
                .enumerate()
            {
                event.seq = 11 + u64::try_from(offset).unwrap();
                match &mut event.payload {
                    LiveJournalPayload::RedemptionTransactionIdentified(audit) => {
                        audit.identity = second.clone();
                    }
                    LiveJournalPayload::RedemptionReceiptTransition(audit) => {
                        audit.identity = second.clone();
                    }
                    _ => unreachable!(),
                }
                second_events.push(event);
            }
            let mut second_custody = second_events[second_events.len() - 4].clone();
            second_custody.seq = 13;
            let LiveJournalPayload::RedemptionCustodyReconciled(audit) =
                &mut second_custody.payload
            else {
                unreachable!()
            };
            let audit = Arc::make_mut(audit);
            audit.identity = second;
            audit.account_state = account_state_fixture_for_binding(
                amount(collateral + Decimal::ONE),
                at(30),
                binding,
            );
            second_events.push(second_custody);
            assert!(matches!(
                derive_with_baseline_evidence(&account_id, &second_events, &sources),
                Err(ProjectionReducerError::IdentityConflict)
            ));
        }
    }
}

async fn append_reserved_order(
    state: &FanoutState,
    account_id: &AccountId,
    account_state: pe_execution_core::LiveAccountStateAudit,
    now: OffsetDateTime,
) {
    let mut prepared = finality_prepared();
    prepared.identity.idempotency_key =
        LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, account_id);
    prepared.account_state = account_state;
    bind_empty_live_risk_to_paper_prefix(state, &mut prepared, now);
    append_economic_sources(state, &mut prepared, account_id, now, None).await;
    append_approved_admission(state.config.journal.as_ref(), account_id, &prepared, now);
    state
        .config
        .journal
        .append(
            account_id.clone(),
            now,
            LiveJournalPayload::OrderPrepared(prepared.into()),
        )
        .unwrap();
}

/// PASS: the runtime mark gate journals one exact credit, passes again unchanged, and opens posture.
/// FAIL: it rejects the surplus, writes a second credit, or leaves strict financial posture closed.
#[tokio::test]
async fn runtime_mark_gate_credits_surplus_once_and_passes_posture() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let snapshot = armed_snapshot("account");
    let account = snapshot.accounts[0].clone();
    let now = at(30);
    let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    state.config.data_base_url = format!("http://{address}");
    let account_id = &account.account_id;
    append_recovery_baseline(&state, account_id, at(0), amount(dec!(10))).await;
    let binding = account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
    let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
        amount(dec!(11.25)),
        now,
        &binding,
    ));
    let before = state.config.journal.records_from(0).unwrap().len();
    for _ in 0..2 {
        assert_eq!(
            ensure_live_portfolio_marks_at(
                &mut state,
                &account,
                &authenticated,
                &binding,
                &binding.custody_wallet,
                now
            )
            .await,
            CheckOutcome::Pass
        );
        let records = state.config.journal.records_from(0).unwrap();
        assert_eq!(records.len(), before + 1);
        let credits = records
            .iter()
            .filter_map(|event| match &event.payload {
                LiveJournalPayload::CollateralCredited(credit) => Some(credit),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].credited, amount(dec!(1.25)));
        assert_eq!(
            live_financial_posture(&state, &account, authenticated.collateral_balance, true),
            CheckOutcome::Pass
        );
    }
    let summary = live_summary(&state, account_id).unwrap();
    assert_eq!(summary.economic_cash, Some(dec!(11.25)));
    assert_eq!(summary.baseline_equity, Some(amount(dec!(10))));
    server.abort();
}

/// PASS: a reserved-order surplus and a one-atomic deficit both fail without journal growth.
/// FAIL: the runtime credits reserved cash, tolerates the deficit, or appends any financial record.
#[tokio::test]
async fn runtime_mark_gate_rejects_reserved_surplus_and_atomic_deficit() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for reserved in [false, true] {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let snapshot = armed_snapshot("account");
        let account = snapshot.accounts[0].clone();
        let now = at(30);
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        state.clock = Arc::new(move || now);
        state.config.data_base_url = format!("http://{address}");
        let account_id = &account.account_id;
        let baseline = append_recovery_baseline(&state, account_id, at(0), amount(dec!(10))).await;
        if reserved {
            append_reserved_order(&state, account_id, baseline, at(0)).await;
            assert_eq!(
                live_summary(&state, account_id).unwrap().reserved,
                dec!(2.500120)
            );
        }
        let binding =
            account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
        let collateral = if reserved {
            dec!(11.25)
        } else {
            dec!(9.999999)
        };
        let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
            amount(collateral),
            now,
            &binding,
        ));
        let before = state.config.journal.records_from(0).unwrap().len();
        assert_eq!(
            ensure_live_portfolio_marks_at(
                &mut state,
                &account,
                &authenticated,
                &binding,
                &binding.custody_wallet,
                now
            )
            .await,
            CheckOutcome::PersistentFail("authenticated cash drift")
        );
        let records = state.config.journal.records_from(0).unwrap();
        assert_eq!(records.len(), before);
        assert!(
            !records
                .iter()
                .any(|event| matches!(event.payload, LiveJournalPayload::CollateralCredited(_)))
        );
    }
    server.abort();
}

fn kill_only_credit_risk(
    state: &FanoutState,
    account: &AccountContext,
    now: OffsetDateTime,
    proposed_debit: CollateralAmount,
) -> RiskAudit {
    compose_live_risk_audit(
        &live_summary(state, &account.account_id).unwrap(),
        &paper_era_at_evaluation(
            &state.config.paper_log.snapshot().unwrap(),
            unix_millis_i64(now),
        )
        .unwrap(),
        &BTreeMap::new(),
        &[],
        now,
        LiveRiskProposal {
            leader_wallet: None,
            market_id: None,
            proposed_debit,
            per_trade_cap_bps: 10_000,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::KillOnly,
        },
    )
    .unwrap()
}

async fn engage_absolute_loss_for_credit(
    state: &FanoutState,
    account: &AccountContext,
    risk: &RiskAudit,
    receiver: &mut tokio::sync::mpsc::Receiver<OrchestratorControl>,
    now: OffsetDateTime,
) {
    let owner = async {
        let OrchestratorControl::RiskHaltChange {
            owner,
            cause,
            state: transition,
            evidence,
            acknowledged,
        } = receiver.recv().await.unwrap()
        else {
            panic!("expected halt control")
        };
        assert_eq!(
            owner,
            RiskHaltOwner::LiveAccount(account.account_id.clone())
        );
        assert_eq!(cause, pe_risk_engine::RiskHaltCause::AbsoluteLoss);
        assert_eq!(transition, HaltState::Engaged);
        let receipt = state
            .config
            .paper_log
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                    owner,
                    cause,
                    state: transition,
                    evidence,
                })
                .unwrap(),
            })
            .unwrap();
        acknowledged.send(Ok(receipt)).unwrap();
    };
    let (result, ()) = tokio::join!(sync_live_risk_halts(state, account, risk), owner);
    assert!(result.unwrap());
}

/// PASS: a runtime credit raises economic cash while the latched AbsoluteLoss halt stays engaged
/// and keeps the risk snapshot at the loss floor.
/// FAIL: a credit rebases equity, releases or re-sends the halt, or lifts the latched snapshot.
#[tokio::test]
async fn runtime_credit_keeps_absolute_loss_latched() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let snapshot = armed_snapshot("account");
    let account = snapshot.accounts[0].clone();
    let now = at(30);
    let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    state.config.data_base_url = format!("http://{address}");
    append_recovery_baseline(&state, &account.account_id, at(0), amount(dec!(10))).await;
    let baseline = live_summary(&state, &account.account_id)
        .unwrap()
        .baseline_equity;
    let mut prepared = finality_prepared();
    bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
    let (control, mut receiver) = tokio::sync::mpsc::channel(4);
    state.config.orchestrator_control = control.downgrade();
    let mut loss = kill_only_credit_risk(&state, &account, now, CollateralAmount::ZERO);
    loss.snapshot.absolute_pnl_bps = BasisPoints(pe_risk_engine::KILL_ONLY_DRAWDOWN_BPS);
    engage_absolute_loss_for_credit(&state, &account, &loss, &mut receiver, now).await;
    let paper_count = state.config.paper_log.snapshot().unwrap().len();
    let binding = account_binding_fixture(
        &account.account_id,
        "0x1111111111111111111111111111111111111111",
    );
    let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
        amount(dec!(11.25)),
        now,
        &binding,
    ));
    assert_eq!(
        ensure_live_portfolio_marks_at(
            &mut state,
            &account,
            &authenticated,
            &binding,
            &binding.custody_wallet,
            now,
        )
        .await,
        CheckOutcome::Pass
    );
    let summary = live_summary(&state, &account.account_id).unwrap();
    assert_eq!(summary.economic_cash, Some(dec!(11.25)));
    assert_eq!(summary.baseline_equity, baseline);
    assert_eq!(baseline, Some(amount(dec!(10))));
    let risk = kill_only_credit_risk(&state, &account, now, CollateralAmount::ZERO);
    assert!(sync_live_risk_halts(&state, &account, &risk).await.unwrap());
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(global_risk_halt_active(&state, pe_risk_engine::DrawdownPolicy::KillOnly).unwrap());
    assert_eq!(
        state.config.paper_log.snapshot().unwrap().len(),
        paper_count
    );
    assert_eq!(
        risk.snapshot.absolute_pnl_bps,
        BasisPoints(pe_risk_engine::KILL_ONLY_DRAWDOWN_BPS)
    );
    server.abort();
}

/// PASS: crossing the fixed 70% floor changes measured loss and cash room without rebasing or unlatching.
/// FAIL: the floor moves with credit, unlatched PnL stays stale, or an AbsoluteLoss latch releases.
#[tokio::test]
async fn runtime_credit_crosses_fixed_absolute_loss_floor_with_and_without_latch() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let held_app = Router::new().route("/positions", get(recovery_open_position));
    let held_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let held_address = held_listener.local_addr().unwrap();
    let held_server =
        tokio::spawn(async move { axum::serve(held_listener, held_app).await.unwrap() });
    for latched in [false, true] {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let snapshot = armed_snapshot("account");
        let account = snapshot.accounts[0].clone();
        let now = at(30);
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        state.clock = Arc::new(move || now);
        state.config.data_base_url = format!("http://{address}");
        let binding = account_binding_fixture(
            &account.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        let baseline_state = account_state_fixture_for_binding(amount(dec!(3)), now, &binding);
        assert_eq!(
            ensure_live_portfolio_marks_at(
                &mut state,
                &account,
                &venue_account_state_from_audit(&baseline_state),
                &binding,
                &binding.custody_wallet,
                now,
            )
            .await,
            CheckOutcome::Pass
        );
        // Reuse the recorded fill at a smaller, valid starting bankroll so a loss crosses the floor.
        let mut prepared = finality_prepared();
        prepared.identity.idempotency_key = LiveOrderIdentity::idempotency_key_for(
            &prepared.identity.dispatch_id,
            &account.account_id,
        );
        prepared.account_state = baseline_state;
        prepared.economic.balance.cash_before = amount(dec!(3));
        bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
        prepared.economic.risk = compose_live_risk_audit(
            &live_summary(&state, &account.account_id).unwrap(),
            &paper_era_at_evaluation(
                &state.config.paper_log.snapshot().unwrap(),
                unix_millis_i64(now),
            )
            .unwrap(),
            &BTreeMap::new(),
            &[],
            now,
            LiveRiskProposal {
                leader_wallet: prepared
                    .identity
                    .fill_projection
                    .as_ref()
                    .map(|p| p.leader_wallet.clone()),
                market_id: Some(prepared.economic.market.market_id.clone()),
                proposed_debit: prepared.economic.balance.worst_case_debit,
                per_trade_cap_bps: 10_000,
                drawdown_policy: pe_risk_engine::DrawdownPolicy::KillOnly,
            },
        )
        .unwrap();
        assert_eq!(prepared.economic.risk.decision, RiskDecisionAudit::Approved);
        append_economic_sources(&state, &mut prepared, &account.account_id, now, None).await;
        append_approved_admission(&state.config.journal, &account.account_id, &prepared, now);
        let prepared_event = state
            .config
            .journal
            .append(
                account.account_id.clone(),
                now,
                LiveJournalPayload::OrderPrepared(prepared.clone().into()),
            )
            .unwrap();
        for event in [
            finality_matched_event(&account.account_id, &prepared, 0, now),
            finalized_fill_event(&account.account_id, &prepared, 0, prepared_event.seq),
        ] {
            state
                .config
                .journal
                .append(account.account_id.clone(), now, event.payload)
                .unwrap();
        }
        let mut resolution = resolution_event(&account.account_id, 0);
        let LiveJournalPayload::ResolutionFinalized(audit) = &resolution.payload else {
            unreachable!()
        };
        let mut audit = audit.as_ref().clone();
        audit.payout_by_outcome_index_json =
            BinaryPayoutVector::winner(1).unwrap().canonical_json();
        resolution.payload = LiveJournalPayload::ResolutionFinalized(audit.into());
        let source = resolution_source_envelope(&resolution);
        let receipt = retain_source(&state, source).await;
        let LiveJournalPayload::ResolutionFinalized(audit) = &resolution.payload else {
            unreachable!()
        };
        let mut audit = audit.as_ref().clone();
        audit.source_append_receipt = receipt;
        state
            .config
            .journal
            .append(
                account.account_id.clone(),
                now,
                LiveJournalPayload::ResolutionFinalized(audit.into()),
            )
            .unwrap();
        assert_eq!(
            live_summary(&state, &account.account_id)
                .unwrap()
                .economic_cash,
            Some(dec!(0.499880))
        );
        state.config.data_base_url = format!("http://{held_address}");

        let (control, mut receiver) = tokio::sync::mpsc::channel(4);
        state.config.orchestrator_control = control.downgrade();
        let mut below = None;
        for collateral in [dec!(2.099999), dec!(2.25)] {
            let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
                amount(collateral),
                now,
                &binding,
            ));
            assert_eq!(
                ensure_live_portfolio_marks_at(
                    &mut state,
                    &account,
                    &authenticated,
                    &binding,
                    &binding.custody_wallet,
                    now
                )
                .await,
                CheckOutcome::Pass
            );
            let summary = live_summary(&state, &account.account_id).unwrap();
            assert_eq!(summary.baseline_equity, Some(amount(dec!(3))));
            assert_eq!(summary.economic_cash, Some(collateral));
            assert_eq!(summary.receivable, Decimal::ZERO);
            let risk = kill_only_credit_risk(&state, &account, now, CollateralAmount::ZERO);
            match &below {
                None => {
                    assert_eq!(risk.snapshot.absolute_pnl_bps, BasisPoints(-3001));
                    assert_eq!(
                        pe_strategy_winner_follow::cash_fraction_budget(
                            dec!(0.5),
                            collateral,
                            summary.baseline_equity.unwrap().to_decimal()
                        ),
                        Some(Decimal::ZERO)
                    );
                    assert!(
                        !global_risk_halt_active(&state, pe_risk_engine::DrawdownPolicy::KillOnly)
                            .unwrap()
                    );
                    if latched {
                        engage_absolute_loss_for_credit(
                            &state,
                            &account,
                            &risk,
                            &mut receiver,
                            now,
                        )
                        .await;
                    }
                    below = Some(summary);
                }
                Some(before) => {
                    assert_eq!(summary.baseline_equity, before.baseline_equity);
                    assert_eq!(summary.realized_closes, before.realized_closes);
                    assert_eq!(summary.daily_marks, before.daily_marks);
                    assert_eq!(
                        pe_strategy_winner_follow::cash_fraction_budget(
                            dec!(0.5),
                            collateral,
                            summary.baseline_equity.unwrap().to_decimal()
                        ),
                        Some(dec!(0.15))
                    );
                    if !latched {
                        assert_eq!(risk.snapshot.absolute_pnl_bps, BasisPoints(-2500));
                        assert_eq!(risk.decision, RiskDecisionAudit::Approved);
                    }
                    assert_eq!(
                        sync_live_risk_halts(&state, &account, &risk).await.unwrap(),
                        latched
                    );
                    assert_eq!(
                        global_risk_halt_active(&state, pe_risk_engine::DrawdownPolicy::KillOnly)
                            .unwrap(),
                        latched
                    );
                    assert!(matches!(
                        receiver.try_recv(),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    ));
                }
            }
        }
    }
    server.abort();
    held_server.abort();
}

/// PASS: credit arithmetic accepts its largest exact amount and refuses Decimal or atomic overflow.
/// FAIL: subtraction overflows, a surplus wraps, or an unrepresentable credit is returned.
#[test]
fn collateral_credit_checked_overflow_fails_closed() {
    let maximum = CollateralAmount::from_atomic(u64::MAX);
    assert_eq!(
        collateral_credit(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, maximum),
        Some(maximum)
    );
    assert_eq!(
        collateral_credit(Decimal::ZERO, Decimal::ZERO, Decimal::MIN, maximum),
        None
    );
    assert_eq!(
        collateral_credit(Decimal::ZERO, Decimal::ZERO, dec!(-0.000001), maximum),
        None
    );
}

/// PASS: a credit landing after this tick's mark check makes the tick's posture read of the same
/// balance refuse without journaling; the next tick's pre-risk mark check journals it once and the
/// order proceeds from the credited cash.
/// FAIL: posture admits the unexplained balance, the refusal journals anything, the next tick
/// rejects the credit, counts it twice, or reserves the order from old cash.
#[tokio::test]
async fn runtime_credit_between_posture_and_mark_reads_allows_next_order() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let snapshot = armed_snapshot("account");
    let account = snapshot.accounts[0].clone();
    let now = at(30);
    let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    state.config.data_base_url = format!("http://{address}");
    append_recovery_baseline(&state, &account.account_id, at(0), amount(dec!(10))).await;
    let before = state.config.journal.records_from(0).unwrap().len();
    assert_eq!(
        live_financial_posture(&state, &account, amount(dec!(11.25)), false),
        CheckOutcome::PersistentFail("authenticated cash drift")
    );
    assert_eq!(state.config.journal.records_from(0).unwrap().len(), before);
    let binding = account_binding_fixture(
        &account.account_id,
        "0x1111111111111111111111111111111111111111",
    );
    let credited_state = account_state_fixture_for_binding(amount(dec!(11.25)), now, &binding);
    assert_eq!(
        ensure_live_portfolio_marks_at(
            &mut state,
            &account,
            &venue_account_state_from_audit(&credited_state),
            &binding,
            &binding.custody_wallet,
            now
        )
        .await,
        CheckOutcome::Pass
    );
    assert_eq!(
        live_financial_posture(&state, &account, amount(dec!(11.25)), false),
        CheckOutcome::Pass
    );
    let mut prepared = finality_prepared();
    prepared.account_state = credited_state;
    prepared.economic.balance.cash_before = amount(dec!(11.25));
    prepared.identity.idempotency_key =
        LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, &account.account_id);
    bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
    prepared.economic.risk = kill_only_credit_risk(
        &state,
        &account,
        now,
        prepared.economic.balance.worst_case_debit,
    );
    assert_eq!(prepared.economic.risk.decision, RiskDecisionAudit::Approved);
    append_economic_sources(&state, &mut prepared, &account.account_id, now, None).await;
    append_approved_admission(&state.config.journal, &account.account_id, &prepared, now);
    state
        .config
        .journal
        .append(
            account.account_id.clone(),
            now,
            LiveJournalPayload::OrderPrepared(prepared.into()),
        )
        .unwrap();
    let summary = live_summary(&state, &account.account_id).unwrap();
    assert_eq!(summary.economic_cash, Some(dec!(11.25)));
    assert_eq!(summary.reserved, dec!(2.500120));
    let records = state.config.journal.records_from(0).unwrap();
    assert_eq!(records.len(), before + 3);
    assert_eq!(
        records
            .iter()
            .filter(|event| matches!(event.payload, LiveJournalPayload::CollateralCredited(_)))
            .count(),
        1
    );
    server.abort();
}

/// PASS: an outstanding winning receivable defers a runtime surplus without appending a credit.
/// FAIL: a credit spends the outstanding payout or changes the journal while custody is pending.
#[tokio::test]
async fn runtime_mark_gate_defers_surplus_with_receivable() {
    let app = Router::new().route("/positions", get(recovery_open_position));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let snapshot = armed_snapshot("account");
    let account = snapshot.accounts[0].clone();
    let now = at(30);
    let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    state.config.data_base_url = format!("http://{address}");
    let baseline =
        append_recovery_baseline(&state, &account.account_id, at(0), amount(dec!(10))).await;
    append_recovery_filled_position(&state, &account.account_id, &baseline, at(0)).await;
    let resolution = resolution_event(&account.account_id, 0);
    let source = resolution_source_envelope(&resolution);
    let receipt = retain_source(&state, source).await;
    let LiveJournalPayload::ResolutionFinalized(audit) = &resolution.payload else {
        unreachable!()
    };
    let mut audit = audit.as_ref().clone();
    audit.source_append_receipt = receipt;
    state
        .config
        .journal
        .append(
            account.account_id.clone(),
            now,
            LiveJournalPayload::ResolutionFinalized(audit.into()),
        )
        .unwrap();
    let summary = live_summary(&state, &account.account_id).unwrap();
    assert_eq!(summary.receivable, dec!(3.125));
    assert_eq!(summary.reserved, Decimal::ZERO);
    let binding = summary.account_binding.as_ref().unwrap();
    let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
        amount(summary.economic_cash.unwrap() - summary.receivable + dec!(1)),
        now,
        binding,
    ));
    let before = state.config.journal.records_from(0).unwrap().len();
    assert_eq!(
        ensure_live_portfolio_marks_at(
            &mut state,
            &account,
            &authenticated,
            binding,
            &binding.custody_wallet,
            now
        )
        .await,
        CheckOutcome::PersistentFail("authenticated cash drift")
    );
    assert_eq!(state.config.journal.records_from(0).unwrap().len(), before);
    assert_eq!(
        live_summary(&state, &account.account_id)
            .unwrap()
            .economic_cash,
        summary.economic_cash
    );
    server.abort();
}

/// PASS: a credit durable before re-derive survives a fresh state and the next gate writes no second credit.
/// FAIL: reopening loses the credit, reapplies it, or requires the discarded in-memory projection.
#[tokio::test]
async fn runtime_credit_append_before_rederive_reopens_without_duplicate() {
    let app = Router::new().route("/positions", get(empty_positions));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let snapshot = armed_snapshot("account");
    let account = snapshot.accounts[0].clone();
    let now = at(30);
    let mut state = fanout_state(
        &dir,
        db.clone(),
        snapshot.clone(),
        "http://127.0.0.1:9",
        None,
    );
    state.clock = Arc::new(move || now);
    append_recovery_baseline(&state, &account.account_id, at(0), amount(dec!(10))).await;
    assert_eq!(
        live_summary(&state, &account.account_id)
            .unwrap()
            .economic_cash,
        Some(dec!(10))
    );
    let (credit, sources) = credit_event(&account.account_id, 2, now, dec!(11.25), dec!(1.25), &[]);
    let LiveJournalPayload::CollateralCredited(audit) = credit.payload else {
        unreachable!()
    };
    let mut audit = *audit;
    for (page, source) in audit.venue_position_evidence.pages.iter_mut().zip(sources) {
        page.receipt = retain_source(&state, source).await;
    }
    let appended = state
        .config
        .journal
        .append(
            account.account_id.clone(),
            now,
            LiveJournalPayload::CollateralCredited(Box::new(audit)),
        )
        .unwrap();
    // Read the old view directly: syncing here would miss the append-before-re-derive crash seam.
    assert_eq!(
        state
            .live
            .lock()
            .unwrap()
            .view(&account.account_id)
            .unwrap()
            .unwrap()
            .applied
            .get(&appended.seq),
        None
    );
    drop(state);
    wait_for_source_writer(&dir.path().join("live-source.log")).await;
    let mut fresh = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    fresh.clock = Arc::new(move || now);
    fresh.config.data_base_url = format!("http://{address}");
    let binding = account_binding_fixture(
        &account.account_id,
        "0x1111111111111111111111111111111111111111",
    );
    let authenticated = venue_account_state_from_audit(&account_state_fixture_for_binding(
        amount(dec!(11.25)),
        now,
        &binding,
    ));
    for _ in 0..2 {
        assert_eq!(
            ensure_live_portfolio_marks_at(
                &mut fresh,
                &account,
                &authenticated,
                &binding,
                &binding.custody_wallet,
                now
            )
            .await,
            CheckOutcome::Pass
        );
        assert_eq!(
            live_financial_posture(&fresh, &account, amount(dec!(11.25)), true),
            CheckOutcome::Pass
        );
        let summary = live_summary(&fresh, &account.account_id).unwrap();
        assert_eq!(summary.economic_cash, Some(dec!(11.25)));
        assert_eq!(summary.baseline_equity, Some(amount(dec!(10))));
        let records = fresh.config.journal.records_from(0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records
                .iter()
                .filter(|event| matches!(event.payload, LiveJournalPayload::CollateralCredited(_)))
                .count(),
            1
        );
        assert_eq!(
            sync_live(&fresh)
                .unwrap()
                .view(&account.account_id)
                .unwrap()
                .unwrap()
                .applied
                .get(&appended.seq),
            Some(&1)
        );
    }
    server.abort();
}
