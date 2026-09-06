//! Requested/effective live-mode machine (#508 Decision 8).
//!
//! The site writes `requested_live_mode`; THIS machine is the sole writer of the
//! effective value (via the `account_set_effective_mode` control RPC, so state + audit
//! event commit atomically). The sole v1 transition is `off → live_tiny`, and arming
//! requires ALL of: a decryptable credential bundle whose embedded binding matches the
//! row; venue account state not `closed_only`; not geoblocked; balance AND allowance
//! for BOTH V2 exchange spenders (standard + NegRisk) covering at least the account's
//! posture; a current seal/config/semantic-bound `Pass` report; and an audited operator
//! review carrying that same seal and the exact report BLAKE3 POST-DATING the Phase-D
//! executor's first boot (the arming fence — D1 provably ships dark) that is not
//! superseded by a later revocation.
//! `requested_live_mode` alone never authorizes live.
//!
//! Armed-account semantics (Decision 8, explicit): persistent invalid conditions —
//! credentials, qualification/report review evidence, `closed_only` — DEMOTE to `off`
//! with an audited reason; transient evidence failures (venue query errors) refuse
//! orders without demoting; a pending/ambiguous/failed redemption closes that account's
//! new-BUY admission WITHOUT demoting (the explicit non-demoting exception).

use std::path::Path;

use serde::Deserialize;
use time::OffsetDateTime;

use crate::qualification::QualificationReport;

/// One arming-condition evaluation. `Transient` failures never demote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Pass,
    /// Persistent invalidity (bad credentials, qualification/review evidence, closed_only).
    PersistentFail(&'static str),
    /// Transient evidence failure (query error/timeout) — refuse orders, keep mode.
    Transient(&'static str),
}

/// Canonical sealed qualification report and its exact loaded-byte identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationFacts {
    report: QualificationReport,
    report_blake3: String,
}

impl QualificationFacts {
    /// Retain the whole report so ordinary-live admission can validate every promotion invariant.
    #[must_use]
    pub fn from_report(report: &QualificationReport, report_blake3: String) -> Self {
        Self {
            report: report.clone(),
            report_blake3,
        }
    }
}

/// Loading a qualification report is boot evidence collection, never an arming fallback.
#[derive(Debug, thiserror::Error)]
pub enum QualificationReportLoadError {
    #[error("read qualification report: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode qualification report: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid qualification report: {0}")]
    Invalid(&'static str),
}

/// Load the canonical report emitted by `pe-service --qualify`.
pub fn load_qualification_facts(
    path: &Path,
) -> Result<QualificationFacts, QualificationReportLoadError> {
    let bytes = std::fs::read(path)?;
    let report = serde_json::from_slice::<QualificationReport>(&bytes)?;
    report
        .validate_for_live_promotion()
        .map_err(QualificationReportLoadError::Invalid)?;
    let report_blake3 = blake3::hash(&bytes).to_hex().to_string();
    Ok(QualificationFacts::from_report(&report, report_blake3))
}

/// Venue-side arming probes, trait-injected so the machine is testable without network
/// and the real implementation can ride the generalized live client.
pub trait ArmingProbe {
    /// Venue account state: `closed_only` is a persistent fail; a query error transient.
    fn account_state(&self) -> CheckOutcome;
    /// Geoblock/jurisdiction: blocked is persistent; a query error transient.
    fn geoblock(&self) -> CheckOutcome;
    /// Balance + allowance for BOTH V2 exchange spenders (standard AND NegRisk —
    /// mirroring Decision 12's both-adapter approval).
    fn balance_and_both_spender_allowances(&self) -> CheckOutcome;
    /// Key-free Polygon finalized-receipt endpoint identity/health. Failure is order-scoped and
    /// prevents arming or new BUYs without demoting an already-armed account.
    fn polygon_finality(&self) -> CheckOutcome;
}

/// The promotion-review facts for one account, read from `account_events`.
#[derive(Debug, Clone, Default)]
pub struct PromotionFacts {
    /// Newest `promotion_reviewed` event time (Unix), if any.
    pub latest_review_unix: Option<i64>,
    /// Seal hash carried by that exact newest review. `None` is a legacy, unbound review.
    pub latest_review_seal_hash: Option<String>,
    /// Exact qualification-report byte digest carried by that same review.
    pub latest_review_report_blake3: Option<String>,
    /// Newest `promotion_review_revoked` event time (Unix), if any.
    pub latest_revocation_unix: Option<i64>,
}

impl PromotionFacts {
    /// A review is valid iff it post-dates the fence, remains unrevoked, and binds the current
    /// qualification seal plus the exact loaded report bytes. Legacy reviews are invalid.
    fn outcome_after_fence(
        &self,
        fence_unix: i64,
        seal_hash: &str,
        report_blake3: &str,
    ) -> CheckOutcome {
        let Some(review) = self
            .latest_review_unix
            .filter(|review| *review > fence_unix)
        else {
            return CheckOutcome::PersistentFail("no valid post-fence promotion review");
        };
        if self
            .latest_revocation_unix
            .is_some_and(|revocation| revocation >= review)
        {
            return CheckOutcome::PersistentFail("promotion review was revoked");
        }
        let Some(review_seal_hash) = self.latest_review_seal_hash.as_deref() else {
            return CheckOutcome::PersistentFail("legacy promotion review has no seal binding");
        };
        if review_seal_hash != seal_hash {
            return CheckOutcome::PersistentFail("promotion review seal binding mismatches report");
        }
        let Some(review_report_blake3) = self.latest_review_report_blake3.as_deref() else {
            return CheckOutcome::PersistentFail(
                "promotion review has no qualification report BLAKE3 binding",
            );
        };
        if review_report_blake3 != report_blake3 {
            return CheckOutcome::PersistentFail(
                "promotion review qualification report BLAKE3 binding mismatches loaded bytes",
            );
        }
        CheckOutcome::Pass
    }
}

/// Inputs to one per-account mode evaluation.
pub struct ModeInputs<'a, P: ArmingProbe> {
    pub requested_live_mode: &'a str,
    pub effective_live_mode: &'a str,
    pub enabled: bool,
    /// Credential bundle decrypt + binding validation result (persistent when invalid).
    pub credentials: CheckOutcome,
    /// Canonical `--qualify` report loaded by the running service. Missing is fail-closed.
    pub qualification: Option<QualificationFacts>,
    /// Latest `QualificationSealed` append hash in the verified current paper era.
    pub current_seal_hash: Option<&'a str>,
    /// Canonical hash of the economic runtime configuration currently applied by the service.
    pub economic_configuration_hash: &'a str,
    /// Financial semantics compiled into the running service.
    pub financial_semantic_version: u32,
    pub promotion: &'a PromotionFacts,
    /// The arming fence; `None` = the executor has never booted (nothing may arm).
    pub fence_unix: Option<i64>,
    /// Accounts ALREADY effective `live_tiny` (excluding this one). Arming is refused at
    /// [`crate::live_accounts::LIVE_ARMED_ACCOUNTS_MAX`] (#508 Decision 4 — the tested v1
    /// bound is enforced, not advisory).
    pub already_armed_count: usize,
    pub probe: &'a P,
}

/// The machine's decision for one account on one evaluation pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModeDecision {
    /// No change required.
    Keep,
    /// Transition the effective mode (commit via the control RPC with the reason).
    SetEffective { mode: &'static str, reason: String },
    /// Keep the mode but refuse orders this pass (transient evidence failure).
    RefuseOrders { reason: String },
}

/// Evaluate one account. Pure given its inputs (probes injected).
pub fn evaluate_mode<P: ArmingProbe>(inputs: &ModeInputs<'_, P>) -> ModeDecision {
    let armed = inputs.effective_live_mode == "live_tiny";
    let wants_armed = inputs.requested_live_mode == "live_tiny" && inputs.enabled;

    // Kill path: a request of `off` (or disable) always wins, armed or not.
    if armed && !wants_armed {
        return ModeDecision::SetEffective {
            mode: "off",
            reason: "operator requested off/disabled".to_owned(),
        };
    }
    if !armed && !wants_armed {
        return ModeDecision::Keep;
    }
    // v1 bound (Decision 4): the service refuses arming beyond live_armed_accounts_max.
    if !armed && inputs.already_armed_count >= crate::live_accounts::LIVE_ARMED_ACCOUNTS_MAX {
        return ModeDecision::RefuseOrders {
            reason: format!(
                "arming refused: {} accounts already armed (live_armed_accounts_max)",
                inputs.already_armed_count
            ),
        };
    }

    let qualification = qualification_outcome(inputs);
    let promotion = match (
        inputs.fence_unix,
        inputs.current_seal_hash,
        inputs.qualification.as_ref(),
    ) {
        (Some(fence), Some(seal_hash), Some(report)) => {
            inputs
                .promotion
                .outcome_after_fence(fence, seal_hash, &report.report_blake3)
        }
        (None, _, _) => CheckOutcome::PersistentFail("live executor arming fence is missing"),
        (_, None, _) => CheckOutcome::PersistentFail("current qualification seal is missing"),
        (_, _, None) => CheckOutcome::PersistentFail("qualification report is missing"),
    };

    // Gather the condition set.
    let checks = [
        (
            "credentials",
            inputs.credentials.clone(),
            /* persistent-capable */ true,
        ),
        (
            "qualification_report",
            qualification,
            /* persistent-capable */ true,
        ),
        ("promotion_review", promotion, true),
        ("account_state", inputs.probe.account_state(), true),
        ("geoblock", inputs.probe.geoblock(), false),
        (
            "balance_allowance",
            inputs.probe.balance_and_both_spender_allowances(),
            false,
        ),
        ("polygon_finality", inputs.probe.polygon_finality(), false),
    ];

    let mut transient: Option<String> = None;
    for (name, outcome, persistent_demotes) in checks {
        match outcome {
            CheckOutcome::Pass => {}
            CheckOutcome::PersistentFail(detail) => {
                return if armed && persistent_demotes {
                    // Armed + persistent invalidity → demote with an audited reason.
                    ModeDecision::SetEffective {
                        mode: "off",
                        reason: format!("demoted: {name} — {detail}"),
                    }
                } else if armed {
                    // Armed but the dimension is treated as order-scoped (geoblock):
                    // refuse orders without demoting.
                    ModeDecision::RefuseOrders {
                        reason: format!("{name} — {detail}"),
                    }
                } else {
                    // Arming attempt fails: keep the prior effective value (off).
                    ModeDecision::RefuseOrders {
                        reason: format!("arming refused: {name} — {detail}"),
                    }
                };
            }
            CheckOutcome::Transient(detail) => {
                transient.get_or_insert_with(|| format!("{name} — {detail}"));
            }
        }
    }
    if let Some(reason) = transient {
        // Transient evidence failures never arm and never demote.
        return ModeDecision::RefuseOrders { reason };
    }

    if armed {
        ModeDecision::Keep
    } else {
        ModeDecision::SetEffective {
            mode: "live_tiny",
            reason: "armed: all admission checks passed".to_owned(),
        }
    }
}

fn qualification_outcome<P: ArmingProbe>(inputs: &ModeInputs<'_, P>) -> CheckOutcome {
    let Some(report) = inputs.qualification.as_ref() else {
        return CheckOutcome::PersistentFail("qualification report is missing");
    };
    if let Err(reason) = report.report.validate_for_live_promotion() {
        return CheckOutcome::PersistentFail(reason);
    }
    let Some(current_seal_hash) = inputs.current_seal_hash else {
        return CheckOutcome::PersistentFail("current qualification seal is missing");
    };
    if report.report.evidence.seal_hash != current_seal_hash {
        return CheckOutcome::PersistentFail("qualification report seal mismatches current seal");
    }
    if report.report.evidence.hot_config_hash.as_deref() != Some(inputs.economic_configuration_hash)
    {
        return CheckOutcome::PersistentFail(
            "qualification report economic configuration mismatches runtime",
        );
    }
    if report.report.evidence.financial_semantic_version != Some(inputs.financial_semantic_version)
    {
        return CheckOutcome::PersistentFail(
            "qualification report financial semantics mismatch runtime",
        );
    }
    CheckOutcome::Pass
}

/// One `account_events` row shape the promotion reader needs (PostgREST select).
#[derive(Debug, Deserialize)]
pub struct PromotionEventRow {
    pub event_kind: String,
    pub created_at: String,
    pub evidence_ref: Option<String>,
}

/// Fold `account_events` rows (any order) into [`PromotionFacts`].
#[must_use]
pub fn promotion_facts_from_rows(rows: &[PromotionEventRow]) -> PromotionFacts {
    let mut facts = PromotionFacts::default();
    for row in rows {
        let Ok(t) = OffsetDateTime::parse(
            &row.created_at,
            &time::format_description::well_known::Rfc3339,
        ) else {
            continue;
        };
        let unix = t.unix_timestamp();
        match row.event_kind.as_str() {
            "promotion_reviewed"
                if facts
                    .latest_review_unix
                    .is_none_or(|current| unix > current) =>
            {
                facts.latest_review_unix = Some(unix);
                facts.latest_review_seal_hash = None;
                facts.latest_review_report_blake3 = None;
                if let Some(evidence_ref) = row.evidence_ref.as_deref() {
                    if let Some((seal_hash, report_blake3)) =
                        promotion_review_bindings(evidence_ref)
                    {
                        facts.latest_review_seal_hash = Some(seal_hash.to_owned());
                        facts.latest_review_report_blake3 = Some(report_blake3.to_owned());
                    } else if valid_blake3(evidence_ref) {
                        // Preserve the old seal-only shape solely so it fails with the specific
                        // missing-report-digest disposition below.
                        facts.latest_review_seal_hash = Some(evidence_ref.to_owned());
                    }
                }
            }
            "promotion_reviewed" => {}
            "promotion_review_revoked" => {
                facts.latest_revocation_unix =
                    Some(facts.latest_revocation_unix.map_or(unix, |c| c.max(unix)));
            }
            _ => {}
        }
    }
    facts
}

/// The durable `evidence_ref` format is `<seal_blake3>:<qualification_report_blake3>`.
fn promotion_review_bindings(value: &str) -> Option<(&str, &str)> {
    let (seal_hash, report_blake3) = value.split_once(':')?;
    (valid_blake3(seal_hash) && valid_blake3(report_blake3)).then_some((seal_hash, report_blake3))
}

fn valid_blake3(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use crate::qualification::QualificationVerdict;

    const SEAL_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_SEAL_HASH: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const ECONOMIC_CONFIGURATION_HASH: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const REPORT_BLAKE3: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const OTHER_REPORT_BLAKE3: &str =
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const FINANCIAL_SEMANTIC_VERSION: u32 = 1;

    struct FixtureProbe {
        account_state: CheckOutcome,
        geoblock: CheckOutcome,
        balance: CheckOutcome,
        polygon: CheckOutcome,
    }

    impl ArmingProbe for FixtureProbe {
        fn account_state(&self) -> CheckOutcome {
            self.account_state.clone()
        }
        fn geoblock(&self) -> CheckOutcome {
            self.geoblock.clone()
        }
        fn balance_and_both_spender_allowances(&self) -> CheckOutcome {
            self.balance.clone()
        }
        fn polygon_finality(&self) -> CheckOutcome {
            self.polygon.clone()
        }
    }

    fn all_pass() -> FixtureProbe {
        FixtureProbe {
            account_state: CheckOutcome::Pass,
            geoblock: CheckOutcome::Pass,
            balance: CheckOutcome::Pass,
            polygon: CheckOutcome::Pass,
        }
    }

    fn reviewed(after_fence: bool) -> PromotionFacts {
        PromotionFacts {
            latest_review_unix: Some(if after_fence { 2_000 } else { 500 }),
            latest_review_seal_hash: Some(SEAL_HASH.to_owned()),
            latest_review_report_blake3: Some(REPORT_BLAKE3.to_owned()),
            latest_revocation_unix: None,
        }
    }

    fn passing_qualification() -> QualificationFacts {
        let mut facts = load_report_bytes(&qualification_report_bytes("pass"));
        facts.report_blake3 = REPORT_BLAKE3.to_owned();
        facts
    }

    fn qualification_report_bytes(verdict: &str) -> Vec<u8> {
        let reasons = if verdict == "pass" {
            vec!["all sealed one-system gates passed; manual review remains required"]
        } else {
            vec!["LCB_5pct is not positive"]
        };
        let report = serde_json::json!({
            "version": 1,
            "verdict": verdict,
            "reasons": reasons,
            "sealed_cutoff_unix": 2_000,
            "first_valid_mark_unix": 1_000,
            "promotion_anchor_mark_unix": 1_000,
            "complete_days": 30,
            "closed_copies": 90,
            "paper_p95_delay_ms": 1_000,
            "delay_samples_ms": [],
            "complete_day_log_equity_growth": [],
            "lcb_5pct_decimal": "0.01",
            "promotion_max_drawdown_fraction": "0.05",
            "start_to_seal_max_drawdown_fraction": "0.05",
            "absolute_profit_loss": "1",
            "demotions": 0,
            "marks": [],
            "thresholds": {
                "minimum_complete_days": 30,
                "minimum_closed_copies": 90,
                "maximum_p95_delay_ms": 2_000,
                "maximum_drawdown_fraction_exclusive": "0.1",
                "lcb_5pct_must_be_positive": true
            },
            "evidence": {
                "start_sequence": 1,
                "start_hash": OTHER_SEAL_HASH,
                "seal_sequence": 2,
                "seal_hash": SEAL_HASH,
                "source_prefix_hash": OTHER_SEAL_HASH,
                "financial_prefix_hash": OTHER_SEAL_HASH,
                "decision_evidence_digest": OTHER_SEAL_HASH,
                "artifact_blake3": OTHER_SEAL_HASH,
                "static_config_hash": OTHER_SEAL_HASH,
                "hot_config_hash": ECONOMIC_CONFIGURATION_HASH,
                "policy_hash": OTHER_SEAL_HASH,
                "financial_semantic_version": FINANCIAL_SEMANTIC_VERSION,
                "economic_core_hashes": []
            },
            "replay": {
                "exact": true,
                "financial_prepared": 90,
                "financial_final": 90,
                "decisions": 90,
                "fills": 90,
                "no_fills": 0,
                "no_copies": 0,
                "membership_changes": 0,
                "final_membership_count": 1
            }
        });
        let mut bytes = serde_json::to_vec(&report).expect("encode qualification report fixture");
        bytes.push(b'\n');
        bytes
    }

    fn try_load_report_bytes(
        bytes: &[u8],
    ) -> Result<QualificationFacts, QualificationReportLoadError> {
        let temp = tempfile::tempdir().expect("create qualification report tempdir");
        let path = temp.path().join("qualification.json");
        std::fs::write(&path, bytes).expect("write qualification report fixture");
        load_qualification_facts(&path)
    }

    fn load_report_bytes(bytes: &[u8]) -> QualificationFacts {
        try_load_report_bytes(bytes).expect("load qualification report fixture")
    }

    fn edit_report_bytes(bytes: &[u8], edit: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
        let mut report =
            serde_json::from_slice(bytes).expect("decode qualification report fixture for edit");
        edit(&mut report);
        let mut edited =
            serde_json::to_vec(&report).expect("encode edited qualification report fixture");
        edited.push(b'\n');
        edited
    }

    fn assert_invalid_report(bytes: &[u8], expected_reason: &str) {
        assert!(
            matches!(
                try_load_report_bytes(bytes),
                Err(QualificationReportLoadError::Invalid(reason))
                    if reason.contains(expected_reason)
            ),
            "report unexpectedly passed validation"
        );
    }

    fn inputs<'a, P: ArmingProbe>(
        requested: &'a str,
        effective: &'a str,
        creds: CheckOutcome,
        promotion: &'a PromotionFacts,
        probe: &'a P,
    ) -> ModeInputs<'a, P> {
        ModeInputs {
            requested_live_mode: requested,
            effective_live_mode: effective,
            enabled: true,
            credentials: creds,
            qualification: Some(passing_qualification()),
            current_seal_hash: Some(SEAL_HASH),
            economic_configuration_hash: ECONOMIC_CONFIGURATION_HASH,
            financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
            promotion,
            fence_unix: Some(1_000),
            already_armed_count: 0,
            probe,
        }
    }

    #[test]
    fn arms_only_when_every_check_passes() {
        let probe = all_pass();
        let promo = reviewed(true);
        let d = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert_eq!(
            d,
            ModeDecision::SetEffective {
                mode: "live_tiny",
                reason: "armed: all admission checks passed".to_owned()
            }
        );
    }

    #[test]
    fn pre_fence_promotion_record_is_never_honored() {
        // The arming fence (round 4): a record predating the executor's first boot must
        // not arm — D1 provably ships dark.
        let probe = all_pass();
        let promo = reviewed(false);
        let d = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(
            matches!(d, ModeDecision::RefuseOrders { ref reason } if reason.contains("promotion_review")),
            "{d:?}"
        );
        // No fence recorded at all (executor never booted) likewise refuses.
        let mut no_fence = inputs("live_tiny", "off", CheckOutcome::Pass, &promo, &probe);
        no_fence.fence_unix = None;
        assert!(matches!(
            evaluate_mode(&no_fence),
            ModeDecision::RefuseOrders { .. }
        ));
    }

    #[test]
    fn revocation_supersedes_and_demotes_an_armed_account() {
        let probe = all_pass();
        let promo = PromotionFacts {
            latest_review_unix: Some(2_000),
            latest_review_seal_hash: Some(SEAL_HASH.to_owned()),
            latest_review_report_blake3: Some(REPORT_BLAKE3.to_owned()),
            latest_revocation_unix: Some(3_000), // later revocation supersedes
        };
        let d = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(
            matches!(d, ModeDecision::SetEffective { mode: "off", ref reason }
                if reason.contains("promotion_review")),
            "{d:?}"
        );
        // A re-review AFTER the revocation is valid again.
        let promo2 = PromotionFacts {
            latest_review_unix: Some(4_000),
            latest_review_seal_hash: Some(SEAL_HASH.to_owned()),
            latest_review_report_blake3: Some(REPORT_BLAKE3.to_owned()),
            latest_revocation_unix: Some(3_000),
        };
        assert_eq!(
            promo2.outcome_after_fence(1_000, SEAL_HASH, REPORT_BLAKE3),
            CheckOutcome::Pass
        );
    }

    #[test]
    fn armed_transient_refuses_without_demoting_and_persistent_demotes() {
        // Transient geoblock query error: refuse orders, keep the mode.
        let probe = FixtureProbe {
            geoblock: CheckOutcome::Transient("query timeout"),
            ..all_pass()
        };
        let promo = reviewed(true);
        let d = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(matches!(d, ModeDecision::RefuseOrders { .. }), "{d:?}");

        // Persistent closed_only: demote with an audited reason.
        let probe2 = FixtureProbe {
            account_state: CheckOutcome::PersistentFail("closed_only"),
            ..all_pass()
        };
        let d2 = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe2,
        ));
        assert!(
            matches!(d2, ModeDecision::SetEffective { mode: "off", .. }),
            "{d2:?}"
        );

        // Persistent invalid credentials likewise demote.
        let probe3 = all_pass();
        let d3 = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::PersistentFail("binding mismatch"),
            &promo,
            &probe3,
        ));
        assert!(
            matches!(d3, ModeDecision::SetEffective { mode: "off", .. }),
            "{d3:?}"
        );
    }

    #[test]
    fn armed_balance_or_allowance_failure_refuses_without_demoting() {
        let probe = FixtureProbe {
            balance: CheckOutcome::PersistentFail("allowance below posture"),
            ..all_pass()
        };
        let promo = reviewed(true);
        let decision = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(
            matches!(decision, ModeDecision::RefuseOrders { ref reason }
                if reason.contains("balance_allowance")),
            "{decision:?}"
        );
    }

    #[test]
    fn polygon_finality_health_is_required_but_never_demotes() {
        let promo = reviewed(true);
        let probe = FixtureProbe {
            polygon: CheckOutcome::PersistentFail("wrong chain identity"),
            ..all_pass()
        };
        let arming = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(
            matches!(arming, ModeDecision::RefuseOrders { ref reason }
                if reason.contains("polygon_finality")),
            "{arming:?}"
        );
        let armed = evaluate_mode(&inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(matches!(armed, ModeDecision::RefuseOrders { .. }));
    }

    #[test]
    fn operator_kill_and_arming_failure_keep_prior_value() {
        let probe = all_pass();
        let promo = reviewed(true);
        // Armed + requested off → demote (the ≤30 s kill path).
        let d = evaluate_mode(&inputs(
            "off",
            "live_tiny",
            CheckOutcome::Pass,
            &promo,
            &probe,
        ));
        assert!(matches!(d, ModeDecision::SetEffective { mode: "off", .. }));
        // Arming attempt with failed credentials keeps `off` (refuse, not arm).
        let d2 = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::PersistentFail("cannot decrypt"),
            &promo,
            &probe,
        ));
        assert!(
            matches!(d2, ModeDecision::RefuseOrders { ref reason } if reason.contains("arming refused")),
            "{d2:?}"
        );
        // Off and not requesting: no decision.
        let d3 = evaluate_mode(&inputs("off", "off", CheckOutcome::Pass, &promo, &probe));
        assert_eq!(d3, ModeDecision::Keep);
    }

    #[test]
    fn arming_a_third_account_is_refused() {
        // Decision 4 (#508): v1 arms at most two accounts; the service refuses a third.
        let probe = all_pass();
        let promo = reviewed(true);
        let mut third = inputs("live_tiny", "off", CheckOutcome::Pass, &promo, &probe);
        third.already_armed_count = 2;
        let d = evaluate_mode(&third);
        assert!(
            matches!(d, ModeDecision::RefuseOrders { ref reason } if reason.contains("live_armed_accounts_max")),
            "{d:?}"
        );
        // An ALREADY-armed account is unaffected by the bound (it only gates arming).
        let mut armed = inputs("live_tiny", "live_tiny", CheckOutcome::Pass, &promo, &probe);
        armed.already_armed_count = 2;
        assert_eq!(evaluate_mode(&armed), ModeDecision::Keep);
    }

    #[test]
    fn promotion_rows_fold_to_latest_facts() {
        let rows = vec![
            PromotionEventRow {
                event_kind: "promotion_reviewed".into(),
                created_at: "2026-08-11T00:00:00Z".into(),
                evidence_ref: Some(format!("{SEAL_HASH}:{REPORT_BLAKE3}")),
            },
            PromotionEventRow {
                event_kind: "promotion_review_revoked".into(),
                created_at: "2026-08-11T01:00:00Z".into(),
                evidence_ref: None,
            },
            PromotionEventRow {
                event_kind: "account_created".into(),
                created_at: "2026-08-10T00:00:00Z".into(),
                evidence_ref: None,
            },
        ];
        let facts = promotion_facts_from_rows(&rows);
        assert!(facts.latest_review_unix.is_some());
        assert_eq!(facts.latest_review_seal_hash.as_deref(), Some(SEAL_HASH));
        assert_eq!(
            facts.latest_review_report_blake3.as_deref(),
            Some(REPORT_BLAKE3)
        );
        assert!(facts.latest_revocation_unix > facts.latest_review_unix);
    }

    #[test]
    fn legacy_seal_only_review_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = promotion_facts_from_rows(&[PromotionEventRow {
            event_kind: "promotion_reviewed".to_owned(),
            created_at: "2026-08-11T00:00:00Z".to_owned(),
            evidence_ref: Some(SEAL_HASH.to_owned()),
        }]);
        assert_eq!(
            promotion.latest_review_seal_hash.as_deref(),
            Some(SEAL_HASH)
        );
        assert!(promotion.latest_review_report_blake3.is_none());
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH, REPORT_BLAKE3),
            CheckOutcome::PersistentFail(reason) if reason.contains("report BLAKE3")
        ));
        let decision = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::Pass,
            &promotion,
            &probe,
        ));
        assert!(
            matches!(decision, ModeDecision::RefuseOrders { ref reason }
                if reason.contains("promotion_review") && reason.contains("report BLAKE3")),
            "{decision:?}"
        );
    }

    #[test]
    fn mismatched_review_seal_is_a_persistent_failure() {
        let probe = all_pass();
        let mut promotion = reviewed(true);
        promotion.latest_review_seal_hash = Some(OTHER_SEAL_HASH.to_owned());
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH, REPORT_BLAKE3),
            CheckOutcome::PersistentFail(_)
        ));
        let decision = evaluate_mode(&inputs(
            "live_tiny",
            "off",
            CheckOutcome::Pass,
            &promotion,
            &probe,
        ));
        assert!(
            matches!(decision, ModeDecision::RefuseOrders { ref reason }
                if reason.contains("promotion_review") && reason.contains("mismatches")),
            "{decision:?}"
        );
    }

    /// PASS: the complete report shape emitted by the golden qualification scenario is accepted.
    #[test]
    fn genuine_golden_qualification_report_shape_passes() {
        let facts = load_report_bytes(&qualification_report_bytes("pass"));
        assert_eq!(facts.report.verdict, QualificationVerdict::Pass);
        assert!(facts.report.replay.exact);
        assert_eq!(facts.report.complete_days, 30);
        assert_eq!(facts.report.closed_copies, 90);
    }

    /// PASS: changing only a failing report's verdict leaves failure reasons and is rejected.
    #[test]
    fn fail_report_edited_to_pass_with_failure_reasons_is_rejected() {
        let edited = edit_report_bytes(&qualification_report_bytes("fail"), |report| {
            report["verdict"] = serde_json::json!("pass");
        });
        assert_invalid_report(&edited, "reasons");
    }

    /// PASS: a report version unknown to this service cannot become promotion evidence.
    #[test]
    fn wrong_qualification_report_version_is_rejected() {
        let edited = edit_report_bytes(&qualification_report_bytes("pass"), |report| {
            report["version"] = serde_json::json!(2);
        });
        assert_invalid_report(&edited, "version");
    }

    /// PASS: a report without exact replay cannot become promotion evidence.
    #[test]
    fn inexact_qualification_replay_is_rejected() {
        let edited = edit_report_bytes(&qualification_report_bytes("pass"), |report| {
            report["replay"]["exact"] = serde_json::json!(false);
        });
        assert_invalid_report(&edited, "not exact");
    }

    /// PASS: a retained report inconsistency is persistent and demotes an armed account.
    #[test]
    fn inconsistent_qualification_report_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = reviewed(true);
        let mut inputs = inputs(
            "live_tiny",
            "live_tiny",
            CheckOutcome::Pass,
            &promotion,
            &probe,
        );
        inputs
            .qualification
            .as_mut()
            .expect("qualification fixture")
            .report
            .replay
            .exact = false;

        assert_eq!(
            qualification_outcome(&inputs),
            CheckOutcome::PersistentFail("qualification report replay is not exact")
        );
        assert!(matches!(
            evaluate_mode(&inputs),
            ModeDecision::SetEffective { mode: "off", ref reason }
                if reason.contains("qualification_report") && reason.contains("not exact")
        ));
    }

    /// PASS: every failing promotion metric contradicts a Pass verdict and is rejected.
    #[test]
    fn pass_report_with_failing_gate_metrics_is_rejected() {
        for (field, value, reason) in [
            ("complete_days", serde_json::json!(29), "complete-days"),
            ("closed_copies", serde_json::json!(89), "closed-copies"),
            ("lcb_5pct_decimal", serde_json::json!("0"), "LCB_5pct"),
            (
                "promotion_max_drawdown_fraction",
                serde_json::json!("0.1"),
                "promotion-drawdown",
            ),
            (
                "paper_p95_delay_ms",
                serde_json::json!(2_001),
                "paper-delay",
            ),
        ] {
            let edited = edit_report_bytes(&qualification_report_bytes("pass"), |report| {
                report[field] = value;
            });
            assert_invalid_report(&edited, reason);
        }
    }

    #[test]
    fn one_byte_report_tamper_is_a_persistent_failure() {
        let original_bytes = qualification_report_bytes("pass");
        let original_blake3 = blake3::hash(&original_bytes).to_hex().to_string();
        let mut tampered_bytes = original_bytes;
        let final_byte = tampered_bytes
            .last_mut()
            .expect("qualification report fixture is nonempty");
        *final_byte = b' ';
        let tampered = load_report_bytes(&tampered_bytes);
        assert_ne!(tampered.report_blake3, original_blake3);

        let promotion = PromotionFacts {
            latest_review_unix: Some(2_000),
            latest_review_seal_hash: Some(SEAL_HASH.to_owned()),
            latest_review_report_blake3: Some(original_blake3),
            latest_revocation_unix: None,
        };
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH, &tampered.report_blake3),
            CheckOutcome::PersistentFail(reason) if reason.contains("BLAKE3")
        ));
    }

    #[test]
    fn forged_pass_report_is_a_persistent_failure() {
        let reviewed_bytes = qualification_report_bytes("fail");
        let reviewed_blake3 = blake3::hash(&reviewed_bytes).to_hex().to_string();
        let forged = load_report_bytes(&qualification_report_bytes("pass"));
        assert_eq!(forged.report.verdict, QualificationVerdict::Pass);
        assert_ne!(forged.report_blake3, reviewed_blake3);

        let promotion = PromotionFacts {
            latest_review_unix: Some(2_000),
            latest_review_seal_hash: Some(SEAL_HASH.to_owned()),
            latest_review_report_blake3: Some(reviewed_blake3),
            latest_revocation_unix: None,
        };
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH, &forged.report_blake3),
            CheckOutcome::PersistentFail(reason) if reason.contains("BLAKE3")
        ));
    }

    #[test]
    fn stale_report_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = reviewed(true);
        let mut stale = inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        stale.current_seal_hash = Some(OTHER_SEAL_HASH);
        assert_eq!(
            qualification_outcome(&stale),
            CheckOutcome::PersistentFail("qualification report seal mismatches current seal")
        );
    }

    #[test]
    fn report_digest_review_mismatch_is_a_persistent_failure() {
        let mut promotion = reviewed(true);
        promotion.latest_review_report_blake3 = Some(OTHER_REPORT_BLAKE3.to_owned());
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH, REPORT_BLAKE3),
            CheckOutcome::PersistentFail(reason) if reason.contains("BLAKE3")
        ));
    }

    #[test]
    fn missing_qualification_report_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = reviewed(true);
        let mut missing = inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        missing.qualification = None;
        assert!(matches!(
            qualification_outcome(&missing),
            CheckOutcome::PersistentFail(_)
        ));
        let decision = evaluate_mode(&missing);
        assert!(
            matches!(decision, ModeDecision::RefuseOrders { ref reason }
                if reason.contains("qualification_report") && reason.contains("missing")),
            "{decision:?}"
        );
    }

    #[test]
    fn non_pass_qualification_report_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = reviewed(true);
        let mut not_pass = inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        let mut report = passing_qualification();
        report.report.verdict = QualificationVerdict::Fail;
        not_pass.qualification = Some(report);
        assert!(matches!(
            qualification_outcome(&not_pass),
            CheckOutcome::PersistentFail(_)
        ));
        assert!(matches!(
            evaluate_mode(&not_pass),
            ModeDecision::RefuseOrders { ref reason }
                if reason.contains("qualification_report") && reason.contains("not Pass")
        ));
    }

    #[test]
    fn mismatched_report_identity_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = reviewed(true);

        let mut seal_mismatch = inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        seal_mismatch.current_seal_hash = Some(OTHER_SEAL_HASH);
        assert!(matches!(
            evaluate_mode(&seal_mismatch),
            ModeDecision::RefuseOrders { ref reason }
                if reason.contains("qualification_report") && reason.contains("seal mismatches")
        ));

        let mut economics_mismatch =
            inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        economics_mismatch.economic_configuration_hash = OTHER_SEAL_HASH;
        assert!(matches!(
            evaluate_mode(&economics_mismatch),
            ModeDecision::RefuseOrders { ref reason }
                if reason.contains("qualification_report") && reason.contains("economic")
        ));

        let mut semantics_mismatch =
            inputs("live_tiny", "off", CheckOutcome::Pass, &promotion, &probe);
        semantics_mismatch.financial_semantic_version = FINANCIAL_SEMANTIC_VERSION + 1;
        assert!(matches!(
            evaluate_mode(&semantics_mismatch),
            ModeDecision::RefuseOrders { ref reason }
                if reason.contains("qualification_report") && reason.contains("semantics")
        ));
    }
}
