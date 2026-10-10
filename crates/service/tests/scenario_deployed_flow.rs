//! #588 F2: runtime admission, real stream/poller copying, expiry, restart, and sealed qualification.
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::duplicate_mod
)]

#[path = "support/golden.rs"]
mod golden;

/// PASS: absent newcomer history passes full-history bracket validation and publication before
/// stream observations traverse the real two-slot poller and orchestrator gates to fresh/corrected
/// paper fills. A delayed portfolio read expires and releases its staged target; completed-state
/// restart and sealed CLI replay preserve receipts, economics, membership and history and return
/// Pass. Fill/expiry terminal clocks are fixed and asserted.
///
/// Scope: the golden corpus retains its recorded policy (min/max resolution horizons disabled,
/// impact cap 300, market end dates in 2030) and recorded admission artifacts supplied through
/// scenario hooks. Bracket preparation completes separately before polling; the poller has no
/// admission preparer. Dispatch proof stops at durable readiness; no fanout consumer runs.
///
/// Related gate coverage lives in `scenario_execution_gates`, whose pre-Start fixtures disable
/// both horizon bounds and whose mandatory-cap case asserts zero book requests; it does not prove
/// unchanged-policy horizon/impact admission. Pure horizon boundaries are tested by
/// `orchestrator::tests::{rejects_too_far_out, rejects_too_soon, bounds_are_inclusive_at_edges}`.
/// Real `LiveAdmissionBuilder` over mocked HTTP is covered by `scenario_paper_prepared_freshness`'s
/// `reverse_completion_admission_executes_and_qualifies_exactly` and `scenario_boot_order`'s
/// `staged_recovery_records_admission_before_observation_producers_start`; consumption of released
/// targets in `live_fanout::tests::ordered_execution_is_primary_first_and_next_waits_for_terminal`.
/// Naturally eligible production opportunities remain operator acceptance under issue #588 and
/// the identifier-bound check in `docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md`, step 6.
/// Membership source envelopes still use `AdmissionPreparer::record_artifact` wall time, so exact
/// replay within this run does not assert cross-run byte equality of receipts or sealed digests.
#[tokio::test]
async fn deployed_flow_replays_exactly_and_qualifies() {
    golden::deployed_flow_replays_exactly_and_qualifies().await;
}

#[path = "support/rollout.rs"]
mod rollout;
mod support;

#[tokio::test]
async fn paper_service_rollout_restarts_settles_and_replays() {
    rollout::run().await;
}

/// PASS: a run that crosses a UTC midnight gets a usable daily-boundary mark from the fixture.
#[tokio::test]
async fn paper_service_rollout_serves_the_daily_boundary_mark() {
    rollout::boundary_mark().await;
}

/// PASS: duplicate delivery is refused by the existing prepared-identity guard and credits no
/// cash twice. This exercises the real settlement control once, without altering golden policy.
#[tokio::test]
async fn paper_service_rollout_duplicate_resolution_candidate_never_credits_twice() {
    use pe_core_types::{
        PolymarketConditionId, ReceivedAt, SourceId, SourceTimestamp, WalletAddress,
    };
    use pe_event_log::{ContentType, EnvelopeIn};
    use pe_service::orchestrator_control::OrchestratorControl;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(pe_paper_state::PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    support::install_full_history_anchor(&paper, wallet, 0);
    paper
        .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: 0,
        })
        .unwrap();
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[wallet]).await;
    h.ordinary_entry(wallet, 100).await;
    assert_eq!(paper.list_fills().unwrap().len(), 1);
    let condition = PolymarketConditionId(format!("0x{:064x}", 2));
    let mut payload: serde_json::Value = serde_json::from_slice(include_bytes!(
        "fixtures/golden_stream_v1/clob_resolution.json"
    ))
    .unwrap();
    payload["condition_id"] = condition.0.clone().into();
    payload["tokens"][0]["token_id"] = "103".into();
    payload["tokens"][1]["token_id"] = "104".into();
    let now = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
    let receipt = h
        .source
        .append(EnvelopeIn {
            source_id: SourceId("polymarket.clob.market".to_owned()),
            schema_version: pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
            observed_at: SourceTimestamp(now),
            received_at: ReceivedAt(now),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&payload).unwrap(),
        })
        .await
        .unwrap();
    let before = paper.financial_snapshot(100).unwrap();
    for duplicate in [false, true] {
        let (acknowledged, ack) = tokio::sync::oneshot::channel();
        h.control
            .send(OrchestratorControl::ResolutionCandidate {
                condition: condition.clone(),
                payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                receipt,
                acknowledged,
            })
            .await
            .unwrap();
        let result = ack.await.unwrap();
        assert_eq!(result.is_err(), duplicate);
        let snapshot = paper.financial_snapshot(100).unwrap();
        assert_eq!(
            snapshot.cash,
            before.cash + before.positions[0].long.to_decimal()
        );
        assert!(snapshot.positions.is_empty());
        assert_eq!(snapshot.settlements_7d.len(), 1);
    }
    assert_eq!(h.mutations(), 2); // one fill and one settlement
    h.shutdown().await;
}

/// PASS: accepted unseeded wallets in the first wave cannot stop boot before the eligible fifth.
#[tokio::test]
async fn paper_service_rollout_boot_continues_past_history_incomplete_wave_to_readiness() {
    rollout::run_boot_waves().await;
}
