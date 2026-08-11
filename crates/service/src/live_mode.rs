//! Requested/effective live-mode machine (#508 Decision 8).
//!
//! The site writes `requested_live_mode`; THIS machine is the sole writer of the
//! effective value (via the `account_set_effective_mode` control RPC, so state + audit
//! event commit atomically). The sole v1 transition is `off → live_tiny`, and arming
//! requires ALL of: a decryptable credential bundle whose embedded binding matches the
//! row; venue account state not `closed_only`; not geoblocked; balance AND allowance
//! for BOTH V2 exchange spenders (standard + NegRisk) covering at least the account's
//! posture; and an audited operator promotion-review record POST-DATING the Phase-D
//! executor's first boot (the arming fence — D1 provably ships dark) that is not
//! superseded by a later revocation. `requested_live_mode` alone never authorizes live.
//!
//! Armed-account semantics (Decision 8, explicit): persistent invalid conditions —
//! credentials, promotion record, `closed_only` — DEMOTE the effective mode to `off`
//! with an audited reason; transient evidence failures (venue query errors) refuse
//! orders without demoting; a pending/ambiguous/failed redemption closes that account's
//! new-BUY admission WITHOUT demoting (the explicit non-demoting exception).

use serde::Deserialize;
use time::OffsetDateTime;

/// One arming-condition evaluation. `Transient` failures never demote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Pass,
    /// Persistent invalidity (bad credentials, revoked promotion, closed_only).
    PersistentFail(&'static str),
    /// Transient evidence failure (query error/timeout) — refuse orders, keep mode.
    Transient(&'static str),
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
}

/// The promotion-review facts for one account, read from `account_events`.
#[derive(Debug, Clone, Default)]
pub struct PromotionFacts {
    /// Newest `promotion_reviewed` event time (Unix), if any.
    pub latest_review_unix: Option<i64>,
    /// Newest `promotion_review_revoked` event time (Unix), if any.
    pub latest_revocation_unix: Option<i64>,
}

impl PromotionFacts {
    /// Decision-8 validity predicate (round 5): a promotion record is valid iff it
    /// post-dates the arming fence and is not superseded by a later revocation. No
    /// time-based expiry in v1.
    #[must_use]
    pub fn valid_after_fence(&self, fence_unix: i64) -> bool {
        match self.latest_review_unix {
            Some(review) if review > fence_unix => {
                self.latest_revocation_unix.is_none_or(|rev| rev < review)
            }
            _ => false,
        }
    }
}

/// Inputs to one per-account mode evaluation.
pub struct ModeInputs<'a, P: ArmingProbe> {
    pub requested_live_mode: &'a str,
    pub effective_live_mode: &'a str,
    pub enabled: bool,
    /// Credential bundle decrypt + binding validation result (persistent when invalid).
    pub credentials: CheckOutcome,
    pub promotion: &'a PromotionFacts,
    /// The arming fence; `None` = the executor has never booted (nothing may arm).
    pub fence_unix: Option<i64>,
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

    // Promotion-record validity (persistent when invalid).
    let promotion_ok = inputs
        .fence_unix
        .is_some_and(|fence| inputs.promotion.valid_after_fence(fence));

    // Gather the condition set.
    let checks = [
        (
            "credentials",
            inputs.credentials.clone(),
            /* persistent-capable */ true,
        ),
        (
            "promotion_record",
            if promotion_ok {
                CheckOutcome::Pass
            } else {
                CheckOutcome::PersistentFail("no valid post-fence promotion record")
            },
            true,
        ),
        ("account_state", inputs.probe.account_state(), true),
        ("geoblock", inputs.probe.geoblock(), false),
        (
            "balance_allowance",
            inputs.probe.balance_and_both_spender_allowances(),
            true,
        ),
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

/// One `account_events` row shape the promotion reader needs (PostgREST select).
#[derive(Debug, Deserialize)]
pub struct PromotionEventRow {
    pub event_kind: String,
    pub created_at: String,
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
            "promotion_reviewed" => {
                facts.latest_review_unix =
                    Some(facts.latest_review_unix.map_or(unix, |c| c.max(unix)));
            }
            "promotion_review_revoked" => {
                facts.latest_revocation_unix =
                    Some(facts.latest_revocation_unix.map_or(unix, |c| c.max(unix)));
            }
            _ => {}
        }
    }
    facts
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureProbe {
        account_state: CheckOutcome,
        geoblock: CheckOutcome,
        balance: CheckOutcome,
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
    }

    fn all_pass() -> FixtureProbe {
        FixtureProbe {
            account_state: CheckOutcome::Pass,
            geoblock: CheckOutcome::Pass,
            balance: CheckOutcome::Pass,
        }
    }

    fn reviewed(after_fence: bool) -> PromotionFacts {
        PromotionFacts {
            latest_review_unix: Some(if after_fence { 2_000 } else { 500 }),
            latest_revocation_unix: None,
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
            promotion,
            fence_unix: Some(1_000),
            probe,
        }
    }

    #[test]
    fn arms_only_when_every_check_passes() {
        let probe = all_pass();
        let promo = reviewed(true);
        let d = evaluate_mode(&inputs("live_tiny", "off", CheckOutcome::Pass, &promo, &probe));
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
        let d = evaluate_mode(&inputs("live_tiny", "off", CheckOutcome::Pass, &promo, &probe));
        assert!(
            matches!(d, ModeDecision::RefuseOrders { ref reason } if reason.contains("promotion_record")),
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
                if reason.contains("promotion_record")),
            "{d:?}"
        );
        // A re-review AFTER the revocation is valid again.
        let promo2 = PromotionFacts {
            latest_review_unix: Some(4_000),
            latest_revocation_unix: Some(3_000),
        };
        assert!(promo2.valid_after_fence(1_000));
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
    fn promotion_rows_fold_to_latest_facts() {
        let rows = vec![
            PromotionEventRow {
                event_kind: "promotion_reviewed".into(),
                created_at: "2026-08-11T00:00:00Z".into(),
            },
            PromotionEventRow {
                event_kind: "promotion_review_revoked".into(),
                created_at: "2026-08-11T01:00:00Z".into(),
            },
            PromotionEventRow {
                event_kind: "account_created".into(),
                created_at: "2026-08-10T00:00:00Z".into(),
            },
        ];
        let facts = promotion_facts_from_rows(&rows);
        assert!(facts.latest_review_unix.is_some());
        assert!(facts.latest_revocation_unix > facts.latest_review_unix);
    }
}
