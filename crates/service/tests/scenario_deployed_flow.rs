//! #588 F2: runtime admission, real stream/poller copying, expiry, restart, and sealed qualification.
#![cfg(feature = "scenario")]

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
