//! Requested/effective live-mode machine (#508 Decision 8).
//!
//! The site writes `requested_live_mode`; THIS machine is the sole writer of the
//! effective value (via the `account_set_effective_mode` control RPC, so state + audit
//! event commit atomically). The sole v1 transition is `off → live_tiny`, and arming
//! requires ALL of: a decryptable credential bundle whose embedded binding matches the
//! row; venue account state not `closed_only`; not geoblocked; balance AND allowance
//! for BOTH V2 exchange spenders (standard + NegRisk) covering at least the account's
//! posture; a current seal/config/semantic-bound `Pass` report; and an audited operator
//! review carrying that same seal POST-DATING the Phase-D executor's first boot (the
//! arming fence — D1 provably ships dark) that is not superseded by a later revocation.
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

use crate::qualification::{QualificationReport, QualificationVerdict};

/// One arming-condition evaluation. `Transient` failures never demote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Pass,
    /// Persistent invalidity (bad credentials, qualification/review evidence, closed_only).
    PersistentFail(&'static str),
    /// Transient evidence failure (query error/timeout) — refuse orders, keep mode.
    Transient(&'static str),
}

/// Promotion-relevant fields extracted from the canonical sealed qualification report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationFacts {
    verdict: QualificationVerdict,
    seal_hash: String,
    economic_configuration_hash: Option<String>,
    financial_semantic_version: Option<u32>,
}

impl QualificationFacts {
    /// Retain only the report fields that ordinary-live mode admission compares with runtime truth.
    #[must_use]
    pub fn from_report(report: &QualificationReport) -> Self {
        Self {
            verdict: report.verdict,
            seal_hash: report.evidence.seal_hash.clone(),
            economic_configuration_hash: report.evidence.hot_config_hash.clone(),
            financial_semantic_version: report.evidence.financial_semantic_version,
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
}

/// Load the canonical report emitted by `pe-service --qualify`.
pub fn load_qualification_facts(
    path: &Path,
) -> Result<QualificationFacts, QualificationReportLoadError> {
    let bytes = std::fs::read(path)?;
    let report = serde_json::from_slice::<QualificationReport>(&bytes)?;
    Ok(QualificationFacts::from_report(&report))
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
    /// Newest `promotion_review_revoked` event time (Unix), if any.
    pub latest_revocation_unix: Option<i64>,
}

impl PromotionFacts {
    /// A review is valid iff it post-dates the fence, remains unrevoked, and names the current
    /// qualification seal. A pre-binding legacy review is persistent invalid evidence.
    fn outcome_after_fence(&self, fence_unix: i64, seal_hash: &str) -> CheckOutcome {
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
    let promotion = match (inputs.fence_unix, inputs.current_seal_hash) {
        (Some(fence), Some(seal_hash)) => inputs.promotion.outcome_after_fence(fence, seal_hash),
        (None, _) => CheckOutcome::PersistentFail("live executor arming fence is missing"),
        (_, None) => CheckOutcome::PersistentFail("current qualification seal is missing"),
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
    if report.verdict != QualificationVerdict::Pass {
        return CheckOutcome::PersistentFail("qualification report verdict is not Pass");
    }
    let Some(current_seal_hash) = inputs.current_seal_hash else {
        return CheckOutcome::PersistentFail("current qualification seal is missing");
    };
    if report.seal_hash != current_seal_hash {
        return CheckOutcome::PersistentFail("qualification report seal mismatches current seal");
    }
    if report.economic_configuration_hash.as_deref() != Some(inputs.economic_configuration_hash) {
        return CheckOutcome::PersistentFail(
            "qualification report economic configuration mismatches runtime",
        );
    }
    if report.financial_semantic_version != Some(inputs.financial_semantic_version) {
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
                facts.latest_review_seal_hash = row
                    .evidence_ref
                    .as_deref()
                    .filter(|value| valid_seal_hash(value))
                    .map(str::to_owned);
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

fn valid_seal_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEAL_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_SEAL_HASH: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const ECONOMIC_CONFIGURATION_HASH: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
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
            latest_revocation_unix: None,
        }
    }

    fn passing_qualification() -> QualificationFacts {
        QualificationFacts {
            verdict: QualificationVerdict::Pass,
            seal_hash: SEAL_HASH.to_owned(),
            economic_configuration_hash: Some(ECONOMIC_CONFIGURATION_HASH.to_owned()),
            financial_semantic_version: Some(FINANCIAL_SEMANTIC_VERSION),
        }
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
            latest_revocation_unix: Some(3_000),
        };
        assert_eq!(
            promo2.outcome_after_fence(1_000, SEAL_HASH),
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
                evidence_ref: Some(SEAL_HASH.to_owned()),
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
        assert!(facts.latest_revocation_unix > facts.latest_review_unix);
    }

    #[test]
    fn legacy_unbound_review_is_a_persistent_failure() {
        let probe = all_pass();
        let promotion = promotion_facts_from_rows(&[PromotionEventRow {
            event_kind: "promotion_reviewed".to_owned(),
            created_at: "2026-08-11T00:00:00Z".to_owned(),
            evidence_ref: Some("legacy-report-reference".to_owned()),
        }]);
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH),
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
                if reason.contains("promotion_review") && reason.contains("legacy")),
            "{decision:?}"
        );
    }

    #[test]
    fn mismatched_review_seal_is_a_persistent_failure() {
        let probe = all_pass();
        let mut promotion = reviewed(true);
        promotion.latest_review_seal_hash = Some(OTHER_SEAL_HASH.to_owned());
        assert!(matches!(
            promotion.outcome_after_fence(1_000, SEAL_HASH),
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
        report.verdict = QualificationVerdict::Fail;
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
