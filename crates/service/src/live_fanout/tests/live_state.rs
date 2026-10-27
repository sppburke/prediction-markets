use super::*;

fn gate_fields(derived: &ProjectionDerivation) -> serde_json::Value {
    serde_json::json!({
        "positions": derived.positions,
        "custody_positions": derived.custody_positions,
        "reserved": derived.reserved,
        "economic_cash": derived.economic_cash,
        "receivable": derived.receivable,
        "baseline_equity": derived.baseline_equity,
        "baseline_cutoff_unix": derived.baseline_cutoff_unix,
        "baseline_sequence": derived.baseline_sequence,
        "account_binding": derived.account_binding,
        "daily_marks": derived.daily_marks,
        "realized_closes": derived.realized_closes,
        "latest_free_collateral": derived.latest_free_collateral,
        "latest_reconciled_at": derived.latest_reconciled_at,
        "receivable_by_condition": derived.receivable_by_condition,
        "pending_approved_admission_keys": derived.pending_approved_admission_keys,
        "open_exposures": derived.open_exposures.iter().map(|exposure| (
            &exposure.leader_wallet, &exposure.market_id, exposure.debit,
        )).collect::<Vec<_>>(),
    })
}

fn assert_matches_file_oracles(state: &FanoutState, account: &AccountId) {
    let events = replay_live_account(state, account).unwrap();
    let batch = derive_projection_rows_for_state(state, account, &events).unwrap();
    assert_eq!(
        gate_fields(&live_summary(state, account).unwrap()),
        gate_fields(&batch)
    );
    // Historical fill rows and prepared sequences belong to the full derivation, not summary.
    let full = sync_live(state).unwrap().derivation(account).unwrap();
    assert_eq!(
        serde_json::to_value(&full.fills).unwrap(),
        serde_json::to_value(&batch.fills).unwrap()
    );
    assert_eq!(
        full.validated_prepared_sequences,
        batch.validated_prepared_sequences
    );
    let mut replayed = pe_execution_core::live_journal::recovery_inventory(
        &state.config.journal_path,
        state.config.era_live_prefix.as_ref(),
    )
    .unwrap();
    let baseline_seq = batch.baseline_sequence.unwrap();
    replayed
        .approved_admissions
        .retain(|entry| entry.admission_journal_seq >= baseline_seq);
    replayed
        .terminal_admissions
        .retain(|entry| entry.journal_seq >= baseline_seq);
    replayed
        .open_orders
        .retain(|entry| entry.inventory.prepared_journal_seq >= baseline_seq);
    assert_eq!(verified_recovery_inventory(state).unwrap(), replayed);
}

fn assert_applied_once(state: &FanoutState, account: &AccountId, sequences: &[u64]) {
    let live = state.live.lock().unwrap();
    let view = live.accounts.get(account).unwrap();
    assert_eq!(
        view.applied,
        sequences.iter().copied().map(|seq| (seq, 1)).collect()
    );
    assert!(
        view.financial
            .late_applied
            .values()
            .all(|count| *count == 1)
    );
}

fn source_input(envelope: &EventEnvelope) -> EnvelopeIn {
    EnvelopeIn {
        source_id: envelope.source_id.clone(),
        schema_version: envelope.schema_version,
        parser_version: envelope.parser_version,
        observed_at: envelope.observed_at.clone(),
        received_at: envelope.received_at.clone(),
        content_type: envelope.content_type.clone(),
        payload: envelope.payload.clone(),
    }
}

fn append_terminal_preparation(
    state: &FanoutState,
    pending: &pe_execution_core::live_journal::ApprovedAdmissionRecoveryEntry,
    now: OffsetDateTime,
) -> LiveJournalEvent {
    state
        .config
        .journal
        .append(
            pending.account_id.clone(),
            now,
            LiveJournalPayload::OrderPreparationFailed(Box::new(
                pe_execution_core::LiveOrderPreparationFailedAudit {
                    identity: pending.admission.identity.clone(),
                    failure: pe_execution_core::LiveOrderPreparationFailure::Venue,
                    control: None,
                },
            )),
        )
        .unwrap()
}

async fn exercise_live_reads(state: &mut FanoutState, account: &AccountId, now: OffsetDateTime) {
    for _ in 0..5 {
        let summary = live_summary(state, account).unwrap();
        let context = state
            .config
            .live_accounts
            .snapshot()
            .accounts
            .iter()
            .find(|entry| &entry.account_id == account)
            .unwrap()
            .clone();
        assert_eq!(
            live_financial_posture(
                state,
                &context,
                CollateralAmount::from_whole(10).unwrap(),
                false
            ),
            CheckOutcome::Pass
        );
        let _ = recovery_is_pending(state).unwrap();
        let _ = verified_recovery_inventory(state).unwrap();
        // This pass has only Approved or terminal admissions, so it needs no venue I/O.
        run_recovery_pass(state, || now, false).await.unwrap();
        reconcile_projections(state, now).await;
        assert_eq!(
            gate_fields(&live_summary(state, account).unwrap()),
            gate_fields(&summary)
        );
    }
}

async fn append_resolution_fixture(
    state: &FanoutState,
    account: &AccountId,
    at: OffsetDateTime,
    winner: usize,
) -> LiveJournalEvent {
    let mut event = resolution_event(account, 0);
    let LiveJournalPayload::ResolutionFinalized(audit) = &mut event.payload else {
        unreachable!()
    };
    let audit = Arc::make_mut(audit);
    audit.payout_by_outcome_index_json =
        BinaryPayoutVector::winner(winner).unwrap().canonical_json();
    let mut source = resolution_source_envelope(&event);
    source.observed_at = SourceTimestamp(at);
    source.received_at = ReceivedAt(at);
    let receipt = state
        .config
        .source_log
        .append(source_input(&source))
        .await
        .unwrap();
    let LiveJournalPayload::ResolutionFinalized(audit) = &mut event.payload else {
        unreachable!()
    };
    let audit = Arc::make_mut(audit);
    audit.source_append_receipt = receipt;
    state
        .config
        .journal
        .append(account.clone(), at, event.payload)
        .unwrap()
}

/// PASS: a valid staged admission, prepared order, fill and resolution have the same gate
/// fields and recovery inventory as strict file replay at every transition.
/// FAIL: any incremental financial field or post-Baseline recovery entry differs.
#[tokio::test]
async fn live_state_matches_strict_financial_and_recovery_file_oracles() {
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    let (pending, wire) = stage_recoverable_approved_with_copy_policy(
        &mut state,
        now,
        None,
        false,
        Some(crate::bucket_commit::PaperFreshnessPolicy {
            activity_ws_enabled: true,
            copy_latency_budget_secs: 120,
        }),
    )
    .await;
    let account = pending.account_id.clone();
    assert_matches_file_oracles(&state, &account);
    assert_eq!(
        verified_recovery_inventory(&state)
            .unwrap()
            .approved_admissions
            .len(),
        1
    );

    let mut prepared = finality_prepared();
    prepared.identity = pending.admission.identity.clone();
    prepared.frozen_binding = pending.admission.frozen_binding.clone();
    prepared.account_state = pending.admission.account_state.as_ref().unwrap().clone();
    prepared.economic = pending.admission.economic.clone();
    prepared.prepared = wire;
    let event = state
        .config
        .journal
        .append(
            account.clone(),
            now,
            LiveJournalPayload::OrderPrepared(prepared.clone().into()),
        )
        .unwrap();
    assert_matches_file_oracles(&state, &account);
    assert!(live_summary(&state, &account).unwrap().reserved > Decimal::ZERO);
    assert_eq!(
        verified_recovery_inventory(&state)
            .unwrap()
            .open_orders
            .len(),
        1
    );

    let matched = finality_matched_event(&account, &prepared, 0, now);
    state
        .config
        .journal
        .append(account.clone(), now, matched.payload)
        .unwrap();
    let fill = finalized_fill_event(&account, &prepared, 0, event.seq);
    state
        .config
        .journal
        .append(account.clone(), now, fill.payload)
        .unwrap();
    assert_matches_file_oracles(&state, &account);
    assert_eq!(live_summary(&state, &account).unwrap().positions.len(), 1);

    append_resolution_fixture(&state, &account, now + time::Duration::seconds(1), 0).await;
    assert_matches_file_oracles(&state, &account);
    let resolved = live_summary(&state, &account).unwrap();
    assert!(resolved.positions.is_empty());
    assert_eq!(resolved.custody_positions.len(), 1);
    assert_eq!(resolved.realized_closes.len(), 1);
    assert!(resolved.receivable > Decimal::ZERO);
}

/// PASS: all gate, recovery and projection reads apply each published record exactly once,
/// including a newly appended terminal preparation fact.
/// FAIL: repeated reads replay a prefix, omit the new record, or apply any record twice.
#[tokio::test]
async fn live_state_repeated_read_paths_apply_each_record_once() {
    let hits = Arc::new(Mutex::new(Vec::<String>::new()));
    let app = Router::new()
        .fallback(counting_fallback)
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut state = fanout_state(&dir, db, armed_snapshot("acct"), &url, None);
    state.clock = Arc::new(move || now);
    let (pending, _) = stage_recoverable_approved(&mut state, now, None).await;
    let account = &pending.account_id;
    let initial = live_records(&state, account)
        .unwrap()
        .iter()
        .map(|event| event.seq)
        .collect::<Vec<_>>();
    exercise_live_reads(&mut state, account, now).await;
    assert_applied_once(&state, account, &initial);

    let terminal = append_terminal_preparation(&state, &pending, now);
    exercise_live_reads(&mut state, account, now).await;
    let mut expected = initial;
    expected.push(terminal.seq);
    assert_applied_once(&state, account, &expected);
    assert!(!recovery_is_pending(&state).unwrap());
    assert!(
        hits.lock()
            .unwrap()
            .iter()
            .any(|path| path == "/rest/v1/live_account_state")
    );
    server.abort();
}

/// PASS: after the journal path is renamed, its open writer publishes a new record and
/// gate, recovery and projection reads consume it without reopening or scanning that path.
/// FAIL: a reader needs the missing file or continues to expose only the old prefix.
#[tokio::test]
async fn live_state_read_paths_survive_renamed_journal_and_see_new_append() {
    let hits = Arc::new(Mutex::new(Vec::<String>::new()));
    let app = Router::new()
        .fallback(counting_fallback)
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut state = fanout_state(&dir, db, armed_snapshot("acct"), &url, None);
    state.clock = Arc::new(move || now);
    let (pending, _) = stage_recoverable_approved(&mut state, now, None).await;
    let account = &pending.account_id;
    let initial = live_records(&state, account).unwrap();
    let moved = dir.path().join("renamed.journal");
    std::fs::rename(&state.config.journal_path, &moved).unwrap();
    let terminal = append_terminal_preparation(&state, &pending, now);
    exercise_live_reads(&mut state, account, now).await;
    let records = live_records(&state, account).unwrap();
    assert_eq!(records.len(), initial.len() + 1);
    assert_eq!(records.last().unwrap().as_ref(), &terminal);
    assert!(
        verified_recovery_inventory(&state)
            .unwrap()
            .approved_admissions
            .is_empty()
    );
    assert!(!recovery_is_pending(&state).unwrap());
    assert!(!state.config.journal_path.exists());
    assert!(!hits.lock().unwrap().is_empty());
    assert_applied_once(
        &state,
        account,
        &records.iter().map(|record| record.seq).collect::<Vec<_>>(),
    );
    server.abort();
}

/// PASS: an unprepared finalized fill closes only its account, never returns its old summary,
/// applies the rejected record once, and skips every later record for that account.
/// FAIL: stale/partial state escapes, the other account closes, or rejection is re-applied.
#[tokio::test]
async fn live_state_rejected_record_closes_only_its_account_and_stops_application() {
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    let account = AccountId::new("acct").unwrap();
    let other = AccountId::new("other").unwrap();
    append_recovery_baseline(
        &state,
        &account,
        now,
        CollateralAmount::from_whole(10).unwrap(),
    )
    .await;
    append_recovery_baseline(
        &state,
        &other,
        now,
        CollateralAmount::from_whole(10).unwrap(),
    )
    .await;
    let before = live_summary(&state, &account).unwrap();
    let other_before = gate_fields(&live_summary(&state, &other).unwrap());
    let invalid = finalized_fill_event(&account, &finality_prepared(), 0, 999);
    let rejected = state
        .config
        .journal
        .append(account.clone(), now, invalid.payload)
        .unwrap();
    let later = state
        .config
        .journal
        .append(
            account.clone(),
            now,
            LiveJournalPayload::CredentialBindingMismatch {
                frozen: CredentialBindingIdentity {
                    version: 1,
                    key_id: "key".to_owned(),
                },
                current: CredentialBindingIdentity {
                    version: 2,
                    key_id: "rotated".to_owned(),
                },
            },
        )
        .unwrap();
    for _ in 0..5 {
        assert!(matches!(
            live_summary(&state, &account),
            Err(LiveStateError::Rejected(_))
        ));
        assert!(matches!(
            live_records(&state, &account),
            Err(LiveStateError::Rejected(_))
        ));
        let context = state.config.live_accounts.snapshot().accounts[0].clone();
        assert_eq!(
            live_financial_posture(
                &state,
                &context,
                CollateralAmount::from_whole(10).unwrap(),
                false
            ),
            CheckOutcome::PersistentFail("live financial journal conflicts")
        );
        assert!(matches!(
            recovery_is_pending(&state),
            Err(FanoutError::LiveState(LiveStateError::Rejected(_)))
        ));
        assert!(matches!(
            verified_recovery_inventory(&state),
            Err(FanoutError::LiveState(LiveStateError::Rejected(_)))
        ));
        assert_eq!(
            gate_fields(&live_summary(&state, &other).unwrap()),
            other_before
        );
    }
    let live = state.live.lock().unwrap();
    let view = live.accounts.get(&account).unwrap();
    assert_eq!(view.applied.get(&rejected.seq), Some(&1));
    assert!(!view.applied.contains_key(&later.seq));
    assert_eq!(view.records.len(), 1);
    assert_eq!(
        gate_fields(&view.financial.summary().unwrap()),
        gate_fields(&before)
    );
}

/// PASS: missing economic receipts leave the admission unapplied and the account open;
/// appending the recorded source suffix lets the next read apply that admission exactly once.
/// FAIL: missing evidence permanently rejects the account or retries duplicate prefix effects.
#[tokio::test]
async fn live_state_missing_admission_sources_are_retried_after_source_append() {
    let producer_dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&producer_dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut producer = fanout_state(
        &producer_dir,
        db.clone(),
        armed_snapshot("acct"),
        "http://127.0.0.1:9",
        None,
    );
    let (pending, _) = stage_recoverable_approved_with_copy_policy(
        &mut producer,
        now,
        None,
        false,
        Some(crate::bucket_commit::PaperFreshnessPolicy {
            activity_ws_enabled: true,
            copy_latency_budget_secs: 120,
        }),
    )
    .await;
    let account = &pending.account_id;
    let expected = gate_fields(&live_summary(&producer, account).unwrap());
    let sources = pe_event_log::Reader::replay(producer_dir.path().join("live-source.log"))
        .unwrap()
        .map(|entry| entry.unwrap().1)
        .collect::<Vec<_>>();
    let first_missing = pending
        .admission
        .economic
        .admission
        .receipts
        .gamma
        .sequence
        .0;
    let dir = tempdir().unwrap();
    std::fs::copy(
        &producer.config.journal_path,
        dir.path().join("live.journal"),
    )
    .unwrap();
    std::fs::copy(
        producer_dir.path().join("paper.log"),
        dir.path().join("paper.log"),
    )
    .unwrap();
    {
        let mut writer = pe_event_log::Writer::open(dir.path().join("live-source.log")).unwrap();
        for source in sources.iter().take(usize::try_from(first_missing).unwrap()) {
            let receipt = writer.append_synced(source_input(source)).unwrap();
            assert_eq!(receipt.this_hash, source.this_hash);
        }
    }
    let state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    for _ in 0..3 {
        assert!(matches!(
            live_summary(&state, account),
            Err(LiveStateError::Unavailable(_))
        ));
        let live = state.live.lock().unwrap();
        let view = live.accounts.get(account).unwrap();
        assert!(view.rejected.is_none());
        assert_eq!(view.applied.len(), 1);
        assert!(!view.applied.contains_key(&pending.admission_journal_seq));
        assert_eq!(live.next_seq, pending.admission_journal_seq);
    }
    for source in sources.iter().skip(usize::try_from(first_missing).unwrap()) {
        let receipt = state
            .config
            .source_log
            .append(source_input(source))
            .await
            .unwrap();
        assert_eq!(receipt.sequence, source.seq);
        assert_eq!(receipt.this_hash, source.this_hash);
    }
    for _ in 0..3 {
        assert_eq!(
            gate_fields(&live_summary(&state, account).unwrap()),
            expected
        );
    }
    assert_applied_once(&state, account, &[0, pending.admission_journal_seq]);
}

/// PASS: dropping the state and reopening the same journal/evidence files rebuilds each
/// record once and reproduces the exact pre-restart gate summary.
/// FAIL: restart skips, duplicates, rejects or changes any previously verified state.
#[tokio::test]
async fn live_state_restart_rebuilds_each_record_once_with_identical_summary() {
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut state = fanout_state(
        &dir,
        db.clone(),
        armed_snapshot("acct"),
        "http://127.0.0.1:9",
        None,
    );
    let (price, _) = append_recovery_risk_price(&state, now).await;
    let (pending, _) = stage_recoverable_approved(&mut state, now, Some(price)).await;
    let account = pending.account_id;
    let before = gate_fields(&live_summary(&state, &account).unwrap());
    let sequences = live_records(&state, &account)
        .unwrap()
        .iter()
        .map(|event| event.seq)
        .collect::<Vec<_>>();
    assert_applied_once(&state, &account, &sequences);
    let path = state.config.journal_path.clone();
    let decoded_before = pe_event_log::scan_metrics::decoded_count(&path).unwrap();
    drop(state);
    // Let the fixture's source owner observe its closed channel and release its writer.
    tokio::task::yield_now().await;
    let state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    let decoded_after_open = pe_event_log::scan_metrics::decoded_count(&path).unwrap();
    assert_eq!(
        decoded_after_open - decoded_before,
        u64::try_from(sequences.len()).unwrap()
    );
    assert!(state.live.lock().unwrap().accounts.is_empty());
    assert_eq!(
        gate_fields(&live_summary(&state, &account).unwrap()),
        before
    );
    assert_applied_once(&state, &account, &sequences);
    assert_eq!(
        pe_event_log::scan_metrics::decoded_count(&path).unwrap(),
        decoded_after_open
    );
    assert_matches_file_oracles(&state, &account);
}

async fn assert_unpublished_gates(fail_sync: bool) {
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut snapshot = armed_snapshot("acct");
    snapshot.fetched_at_unix = Some(now.unix_timestamp());
    let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
    let (pending, _) = stage_recoverable_approved(&mut state, now, None).await;
    let account = &pending.account_id;
    let _ = live_summary(&state, account).unwrap();
    let applied_before = state
        .live
        .lock()
        .unwrap()
        .accounts
        .get(account)
        .unwrap()
        .applied
        .clone();
    let mut control = state
        .config
        .live_accounts
        .snapshot()
        .control_observation(None, now.unix_timestamp());
    if !fail_sync {
        control.decided_at_unix += 1;
    }
    let payload = LiveJournalPayload::PendingTargetControl(Box::new(
        pe_execution_core::live_journal::LiveTargetControlAudit {
            dispatch_id: "dispatch-finality".to_owned(),
            account_id: account.clone(),
            exec_rank: 0,
            control,
            outcome: pe_execution_core::live_journal::LiveTargetControlOutcome::NotArmed,
        },
    ));
    let LiveJournalPayload::PendingTargetControl(audit) = &payload else {
        unreachable!()
    };
    assert!(audit.valid());
    #[cfg(feature = "scenario")]
    if fail_sync {
        state.config.journal.scenario_fail_next_sync().unwrap();
    }
    #[cfg(not(feature = "scenario"))]
    assert!(!fail_sync);
    assert!(
        state
            .config
            .journal
            .append(account.clone(), now, payload.clone())
            .is_err()
    );
    assert!(
        state
            .config
            .journal
            .append(account.clone(), now, payload)
            .is_err()
    );
    let context = state.config.live_accounts.snapshot().accounts[0].clone();
    for _ in 0..3 {
        assert!(matches!(
            live_summary(&state, account),
            Err(LiveStateError::Unavailable(_))
        ));
        assert_eq!(
            live_financial_posture(
                &state,
                &context,
                CollateralAmount::from_whole(10).unwrap(),
                false
            ),
            CheckOutcome::Transient("live journal unavailable")
        );
        assert!(matches!(
            recovery_is_pending(&state),
            Err(FanoutError::LiveState(LiveStateError::Unavailable(_)))
        ));
        assert!(verified_recovery_inventory(&state).is_err());
    }
    assert_eq!(
        state
            .live
            .lock()
            .unwrap()
            .accounts
            .get(account)
            .unwrap()
            .applied,
        applied_before
    );
}

/// PASS: a durable malformed control and another append publish nothing to financial gates.
/// FAIL: a gate returns its cached pre-failure summary or applies any unpublished record.
#[tokio::test]
async fn live_state_malformed_append_then_append_closes_all_gate_reads() {
    assert_unpublished_gates(false).await;
}

/// PASS: sync uncertainty and another append publish nothing to financial gates.
/// FAIL: a gate returns its cached pre-failure summary or applies any unpublished record.
#[cfg(feature = "scenario")]
#[tokio::test]
async fn live_state_sync_failure_then_append_closes_all_gate_reads() {
    assert_unpublished_gates(true).await;
}

/// PASS: cadence is 300 without custody work, 30 for any resolved holding (zero payout
/// included), and 300 after its confirmed redemption and custody reconciliation.
/// FAIL: a zero receivable hides token custody work or completed custody keeps fast polling.
#[tokio::test]
async fn live_state_redemption_cadence_tracks_winning_and_zero_payout_custody() {
    for winner in [0, 1] {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let state = fanout_state(
            &dir,
            db,
            armed_snapshot("other"),
            "http://127.0.0.1:9",
            None,
        );
        assert_eq!(redemption_cadence_secs(&state), 300);
        let account = AccountId::new("acct").unwrap();
        let baseline = append_recovery_baseline(
            &state,
            &account,
            now,
            CollateralAmount::from_whole(10).unwrap(),
        )
        .await;
        append_recovery_filled_position(&state, &account, &baseline, now).await;
        assert_eq!(redemption_cadence_secs(&state), 300);
        let at = now + time::Duration::seconds(1);
        append_resolution_fixture(&state, &account, at, winner).await;
        let resolved = live_summary(&state, &account).unwrap();
        assert_eq!(redemption_cadence_secs(&state), 30);
        assert_eq!(resolved.receivable_by_condition.len(), 1);
        if winner == 1 {
            assert_eq!(resolved.receivable, Decimal::ZERO);
        }
        let binding = resolved.account_binding.as_ref().unwrap();
        let identity = RedemptionAttemptIdentity {
            account_id: account.clone(),
            condition_id: PolymarketConditionId(
                resolved
                    .receivable_by_condition
                    .keys()
                    .next()
                    .unwrap()
                    .clone(),
            ),
            adapter: "adapter".to_owned(),
            custody_wallet: binding.custody_wallet.clone(),
        };
        state
            .config
            .journal
            .append(
                account.clone(),
                at,
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
            )
            .unwrap();
        state
            .config
            .journal
            .append(
                account.clone(),
                at,
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
            )
            .unwrap();
        let (_, sources) = empty_position_evidence(0, at, binding);
        let mut receipts = Vec::new();
        for source in sources {
            receipts.push(
                state
                    .config
                    .source_log
                    .append(source_input(&source))
                    .await
                    .unwrap(),
            );
        }
        let cash = CollateralAmount::from_decimal_exact(resolved.economic_cash.unwrap()).unwrap();
        state
            .config
            .journal
            .append(
                account.clone(),
                at,
                LiveJournalPayload::RedemptionCustodyReconciled(Arc::new(
                    pe_execution_core::RedemptionCustodyReconciledAudit {
                        identity,
                        account_state: account_state_fixture_for_binding(cash, at, binding),
                        venue_positions: Vec::new(),
                        venue_position_receipts: receipts,
                    },
                )),
            )
            .unwrap();
        assert!(
            live_summary(&state, &account)
                .unwrap()
                .custody_positions
                .is_empty()
        );
        assert_eq!(redemption_cadence_secs(&state), 300);
    }
}
