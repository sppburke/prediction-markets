//! Scoped drops, complete-second proof reuse, and first-entry history (#739).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use pe_copy_signal_engine::{PositionState, SignalConfig};
use pe_core_types::{
    CollateralAmount, LeaderAction, MarketId, MarketOutcomeId, OutcomeId, PolymarketConditionId,
    PolymarketTokenId, Price, ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_position_ledger::{
    DropCause, EntryClassification, LedgerEffect, LedgerError, LedgerMutation, MarketLookup,
    PositionLedger, SameSecondEntryPolicy, Scope, ScopeKind, ScopeLookups, ScopedSecond,
    SecondRecord, classify_scoped_historical_second, classify_scoped_second,
};
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityTransport, ActivityType, NormalizedActivity, aggregate_activity_rows,
};
use rust_decimal_macros::dec;
use time::OffsetDateTime;

const A: &str = "0xaaa";
const B: &str = "0xbbb";
const C: &str = "0xccc";
const GROUP: &str = "0xeee";
const UNKNOWN: &str = "0xddd";

#[derive(Default)]
struct Lookups {
    markets: BTreeMap<String, Option<String>>,
    tokens: BTreeMap<String, String>,
    groups: BTreeSet<String>,
}

impl ScopeLookups for Lookups {
    fn market(&self, id: &str) -> MarketLookup {
        match self.markets.get(id) {
            Some(Some(group)) => MarketLookup::Grouped(group.clone()),
            Some(None) => MarketLookup::Ungrouped,
            None => MarketLookup::Unknown,
        }
    }

    fn token_market(&self, id: &str) -> Option<String> {
        self.tokens.get(id).cloned()
    }

    fn is_group(&self, id: &str) -> bool {
        self.groups.contains(id)
    }
}

fn lookups() -> Lookups {
    Lookups {
        markets: BTreeMap::from([
            (A.to_owned(), Some(GROUP.to_owned())),
            (B.to_owned(), None),
            (C.to_owned(), Some(GROUP.to_owned())),
        ]),
        tokens: BTreeMap::from([
            ("token-a".to_owned(), A.to_owned()),
            ("token-b".to_owned(), B.to_owned()),
        ]),
        groups: BTreeSet::from([GROUP.to_owned()]),
    }
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn market(id: &str) -> MarketId {
    MarketId(VenueMarketId(id.to_owned()))
}

fn scope(kind: ScopeKind, id: &str) -> Scope {
    Scope {
        kind,
        id: id.to_owned(),
    }
}

fn activity(
    kind: ActivityType,
    label: &str,
    condition: Option<&str>,
    token: Option<&str>,
    side: Option<Side>,
    outcome: Option<u16>,
    atomic: u64,
) -> ActivityAggregate {
    let timestamp = SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_000).unwrap());
    let row = NormalizedActivity {
        activity_type: kind,
        source_id: SourceId("fixture".to_owned()),
        wallet: wallet(),
        transaction_hash: format!("0x{label}"),
        condition_id: condition.map(|id| PolymarketConditionId(id.to_owned())),
        asset: token.map(|id| PolymarketTokenId(id.to_owned())),
        outcome: outcome.map(OutcomeId),
        side,
        price: Price(dec!(0.5)),
        share_amount: ShareAmount::from_atomic(atomic),
        source_usdc_amount: CollateralAmount::ZERO,
        is_combo: false,
        source_time: timestamp.clone(),
        observed_at: timestamp.clone(),
        received_at: ReceivedAt(timestamp.0),
        raw_row_hash: "fixture".to_owned(),
        raw_row_json: "{}".to_owned(),
        parser_version: 2,
        schema_version: 2,
        transport: ActivityTransport::Rest,
    };
    aggregate_activity_rows(&[row]).unwrap().remove(0)
}

fn buy(label: &str, condition: &str, outcome: u16, atomic: u64) -> ActivityAggregate {
    activity(
        ActivityType::Trade,
        label,
        Some(condition),
        Some("token"),
        Some(Side::Buy),
        Some(outcome),
        atomic,
    )
}

fn position_activity(
    kind: ActivityType,
    label: &str,
    condition: &str,
    atomic: u64,
) -> ActivityAggregate {
    activity(kind, label, Some(condition), None, None, Some(0), atomic)
}

fn records(aggregates: &[ActivityAggregate]) -> Vec<SecondRecord<'_>> {
    aggregates
        .iter()
        .map(|aggregate| SecondRecord {
            aggregate,
            mutation: LedgerMutation::from_activity(aggregate),
        })
        .collect()
}

fn classify(
    ledger: &PositionLedger,
    aggregates: &[ActivityAggregate],
    dropped: &BTreeSet<Scope>,
    certified: bool,
    consumed: &HashSet<MarketId>,
    lookups: &Lookups,
) -> ScopedSecond {
    classify_scoped_historical_second(
        ledger,
        wallet(),
        &records(aggregates),
        dropped,
        certified,
        ReconstructionQuality::new(100).unwrap(),
        &|id| consumed.contains(id),
        lookups,
    )
    .unwrap()
}

fn fresh(aggregates: &[ActivityAggregate]) -> ScopedSecond {
    classify(
        &PositionLedger::new(),
        aggregates,
        &BTreeSet::new(),
        false,
        &HashSet::new(),
        &lookups(),
    )
}

fn assert_problem(result: &ScopedSecond, kind: ScopeKind, id: &str, cause: DropCause) {
    assert!(result.problem_second);
    assert_eq!(result.problems.len(), 1);
    assert_eq!(result.problems[0].scope, scope(kind, id));
    assert_eq!(result.problems[0].cause, cause);
    assert!(
        result
            .decisions
            .iter()
            .all(|decision| decision.entry != EntryClassification::Admitted)
    );
}

fn state(ledger: &PositionLedger, id: &str, outcome: u16) -> PositionState {
    ledger
        .position(&wallet())
        .and_then(|snapshot| {
            snapshot
                .positions
                .get(&MarketOutcomeId::new(market(id), OutcomeId(outcome)))
                .copied()
        })
        .unwrap_or_default()
}

#[test]
fn scope_and_cause_wire_order_is_lexical() {
    let kinds = [ScopeKind::Event, ScopeKind::Market];
    let causes = [
        DropCause::Conversion,
        DropCause::OrderDependent,
        DropCause::Overflow,
        DropCause::Underflow,
        DropCause::UnknownCondition,
        DropCause::UnknownType,
        DropCause::Unmapped,
    ];
    let kind_names = kinds.map(|value| serde_json::to_string(&value).unwrap());
    let cause_names = causes.map(|value| serde_json::to_string(&value).unwrap());
    assert_eq!(kind_names, ["\"event\"", "\"market\""]);
    assert_eq!(
        cause_names,
        [
            "\"conversion\"",
            "\"order_dependent\"",
            "\"overflow\"",
            "\"underflow\"",
            "\"unknown_condition\"",
            "\"unknown_type\"",
            "\"unmapped\""
        ]
    );
    assert!(kinds.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(causes.windows(2).all(|pair| pair[0] < pair[1]));
    for value in kinds {
        assert_eq!(
            serde_json::from_str::<ScopeKind>(&serde_json::to_string(&value).unwrap()).unwrap(),
            value
        );
    }
    for value in causes {
        assert_eq!(
            serde_json::from_str::<DropCause>(&serde_json::to_string(&value).unwrap()).unwrap(),
            value
        );
    }
}

#[test]
fn record_problem_causes_resolve_to_their_market_or_event() {
    for (kind, amount, condition, token, scope_kind, id, cause) in [
        (
            ActivityType::Conversion,
            1,
            Some(A),
            None,
            ScopeKind::Event,
            GROUP,
            DropCause::Conversion,
        ),
        (
            ActivityType::Unknown("NEW".to_owned()),
            0,
            Some(A),
            None,
            ScopeKind::Event,
            GROUP,
            DropCause::UnknownType,
        ),
        (
            ActivityType::Unknown("NEW".to_owned()),
            0,
            Some(B),
            None,
            ScopeKind::Market,
            B,
            DropCause::UnknownType,
        ),
        (
            ActivityType::Redeem,
            0,
            None,
            Some("token-a"),
            ScopeKind::Market,
            A,
            DropCause::UnknownCondition,
        ),
        (
            ActivityType::Split,
            1,
            None,
            Some("token-a"),
            ScopeKind::Market,
            A,
            DropCause::Unmapped,
        ),
    ] {
        let aggregate = activity(kind, "problem", condition, token, None, Some(0), amount);
        let result = fresh(std::slice::from_ref(&aggregate));
        assert_problem(&result, scope_kind, id, cause);
        assert_eq!(result.problems[0].trigger, *aggregate.group_id.key());
        assert!(result.apply.is_empty());
        assert!(result.consumed.is_empty());
        assert!(result.ignored.is_empty());
    }
}

#[test]
fn resolution_prefers_group_then_known_market_then_token_and_counts_unresolvable() {
    let mut lookup = lookups();
    lookup.markets.insert(GROUP.to_owned(), None);
    for (condition, token, expected) in [
        (
            Some(GROUP),
            Some("token-b"),
            Some(scope(ScopeKind::Event, GROUP)),
        ),
        (
            Some(A),
            Some("token-b"),
            Some(scope(ScopeKind::Event, GROUP)),
        ),
        (Some(B), Some("token-a"), Some(scope(ScopeKind::Market, B))),
        (
            Some(UNKNOWN),
            Some("token-b"),
            Some(scope(ScopeKind::Market, B)),
        ),
        (None, Some("token-a"), Some(scope(ScopeKind::Event, GROUP))),
        (Some(UNKNOWN), None, None),
        (None, None, None),
    ] {
        let aggregate = activity(
            ActivityType::Conversion,
            "resolution",
            condition,
            token,
            None,
            None,
            1,
        );
        let result = classify(
            &PositionLedger::new(),
            std::slice::from_ref(&aggregate),
            &BTreeSet::new(),
            false,
            &HashSet::new(),
            &lookup,
        );
        match expected {
            Some(expected) => {
                assert_eq!(result.problems[0].scope, expected);
                assert!(result.ignored.is_empty());
            }
            None => {
                assert!(!result.problem_second);
                assert!(result.problems.is_empty());
                assert_eq!(
                    result.ignored,
                    vec![(aggregate.group_id.key().clone(), ActivityType::Conversion)]
                );
            }
        }
        assert!(result.apply.is_empty());
        assert!(result.consumed.is_empty());
    }
}

#[test]
fn market_causes_do_not_promote_to_groups_or_use_group_matches() {
    for (kind, condition, token, expected) in [
        (ActivityType::Split, None, "token-a", A),
        (ActivityType::Merge, None, "token-b", B),
        (ActivityType::Split, Some("\\xeee"), "token-b", B),
    ] {
        let aggregate = activity(kind, "token-market", condition, Some(token), None, None, 1);
        assert_problem(
            &fresh(&[aggregate]),
            ScopeKind::Market,
            expected,
            DropCause::Unmapped,
        );
    }
}

#[test]
fn unmapped_pair_activity_is_resolved_by_token_or_ignored_beside_a_valid_buy() {
    for kind in [ActivityType::Split, ActivityType::Merge] {
        for token in [Some("token-a"), Some("missing"), None] {
            let problem = activity(kind.clone(), "missing-market", None, token, None, None, 1);
            let valid = buy("other-buy", B, 0, 10);
            let result = fresh(&[problem.clone(), valid.clone()]);
            assert_eq!(result.apply.len(), 1);
            assert_eq!(result.apply[0].source_trade_id, *valid.group_id.key());
            assert_eq!(result.consumed, vec![market(B)]);
            if token == Some("token-a") {
                assert_problem(&result, ScopeKind::Market, A, DropCause::Unmapped);
                assert!(result.ignored.is_empty());
            } else {
                assert!(!result.problem_second);
                assert_eq!(
                    result.ignored,
                    vec![(problem.group_id.key().clone(), kind.clone())]
                );
                assert_eq!(result.decisions[0].entry, EntryClassification::Admitted);
            }
        }
    }
}

#[test]
fn group_drop_is_inclusive_and_covers_a_market_named_in_a_later_second() {
    let conversion = position_activity(ActivityType::Conversion, "conversion", GROUP, 1);
    let result = fresh(&[
        buy("same-second-a", A, 0, 10),
        conversion,
        buy("same-second-b", B, 0, 10),
    ]);
    assert_problem(&result, ScopeKind::Event, GROUP, DropCause::Conversion);
    assert_eq!(result.apply.len(), 1);
    assert_eq!(
        result.apply[0].effect.effective(),
        LedgerMutation::from_activity(&buy("same-second-b", B, 0, 10))
            .unwrap()
            .effect
            .effective()
    );
    assert_eq!(result.consumed, vec![market(A), market(B)]);
    let dropped = result
        .problems
        .into_iter()
        .map(|problem| problem.scope)
        .collect();
    let later = classify(
        &PositionLedger::new(),
        &[buy("later-c", C, 0, 10)],
        &dropped,
        false,
        &HashSet::new(),
        &lookups(),
    );
    assert!(!later.problem_second);
    assert!(later.problems.is_empty());
    assert!(later.apply.is_empty());
    assert!(later.decisions.is_empty());
    assert!(later.ignored.is_empty());
    assert_eq!(later.consumed, vec![market(C)]);
}

#[test]
fn unknown_type_on_an_ungrouped_market_drops_only_that_market() {
    let unknown = position_activity(ActivityType::Unknown("NEW".to_owned()), "unknown", B, 0);
    let result = fresh(&[unknown, buy("a-buy", A, 0, 10), buy("b-buy", B, 0, 10)]);
    assert_problem(&result, ScopeKind::Market, B, DropCause::UnknownType);
    assert_eq!(result.apply.len(), 1);
    assert_eq!(result.decisions[0].market_id, market(A));
    let dropped = BTreeSet::from([scope(ScopeKind::Market, B)]);
    let later = classify(
        &PositionLedger::new(),
        &[buy("a-later", A, 0, 10)],
        &dropped,
        false,
        &HashSet::new(),
        &lookups(),
    );
    assert_eq!(later.decisions[0].entry, EntryClassification::Admitted);
}

#[test]
fn raw_only_precedence_preserves_zero_conversion_combo_and_rebound_trades() {
    let mut combo = buy("combo", "\\xaaa", 0, 10);
    combo.is_combo = true;
    let zero_conversion =
        position_activity(ActivityType::Conversion, "zero-conversion", "\\xeee", 0);
    let zero_trade = buy("zero-trade", "\\xaaa", 0, 0);
    let reward = position_activity(ActivityType::Reward, "reward", "\\xeee", 10);
    let rebound = buy("unbound-token", "\\xaaa", 1, 10);
    let aggregates = [zero_conversion, zero_trade, combo, reward, rebound];
    let mut records = records(&aggregates);
    records[4].mutation.as_mut().unwrap().effect = LedgerEffect::RawOnly;
    let result = classify_scoped_historical_second(
        &PositionLedger::new(),
        wallet(),
        &records,
        &BTreeSet::new(),
        false,
        ReconstructionQuality::new(100).unwrap(),
        &|_| false,
        &lookups(),
    )
    .unwrap();
    assert!(!result.problem_second);
    assert!(result.problems.is_empty());
    assert!(result.ignored.is_empty());
    assert_eq!(result.apply.len(), 5);
    assert!(
        result
            .apply
            .iter()
            .all(|mutation| mutation.effect.effective() == &LedgerEffect::RawOnly)
    );
    assert!(result.decisions.is_empty());
    assert!(result.consumed.is_empty());
}

#[test]
fn unknown_condition_redeem_with_an_unresolvable_token_is_ignored() {
    let redeem = activity(
        ActivityType::Redeem,
        "zero-redeem",
        None,
        Some("missing"),
        None,
        Some(0),
        0,
    );
    let result = fresh(std::slice::from_ref(&redeem));
    assert!(!result.problem_second);
    assert!(result.problems.is_empty());
    assert!(result.apply.is_empty());
    assert!(result.decisions.is_empty());
    assert!(result.consumed.is_empty());
    assert_eq!(
        result.ignored,
        vec![(redeem.group_id.key().clone(), ActivityType::Redeem)]
    );
}

#[test]
fn known_condition_requires_anchor_is_a_no_op_regardless_of_pre_second_ledger() {
    let redeem = position_activity(ActivityType::Redeem, "zero-redeem", A, 0);
    let same_second = fresh(&[redeem.clone(), buy("same-second-buy", A, 0, 10)]);
    assert!(!same_second.problem_second);
    assert!(same_second.problems.is_empty());
    assert_eq!(same_second.apply.len(), 2);
    assert_eq!(
        same_second.decisions[0].entry,
        EntryClassification::Admitted
    );
    assert_eq!(same_second.consumed, vec![market(A)]);
    for amount in [None, Some(0), Some(10)] {
        let mut ledger = PositionLedger::new();
        if let Some(amount) = amount {
            ledger.replace_wallet_snapshot(
                wallet(),
                HashMap::from([(
                    MarketOutcomeId::new(market(A), OutcomeId(1)),
                    PositionState {
                        long_contracts: ShareAmount::from_atomic(amount),
                        short_contracts: ShareAmount::ZERO,
                    },
                )]),
            );
        }
        let result = classify(
            &ledger,
            std::slice::from_ref(&redeem),
            &BTreeSet::new(),
            false,
            &HashSet::new(),
            &lookups(),
        );
        assert!(!result.problem_second);
        assert!(result.problems.is_empty());
        assert!(result.ignored.is_empty());
        assert!(result.decisions.is_empty());
        assert!(result.consumed.is_empty());
        assert_eq!(result.apply.len(), 1);
        assert_eq!(result.apply[0].effect, LedgerEffect::RequiresAnchor);
        let before = ledger.snapshots().clone();
        ledger.apply_all_or_none(&result.apply).unwrap();
        assert_eq!(ledger.snapshots(), &before);
        let dropped = BTreeSet::from([scope(ScopeKind::Event, GROUP)]);
        let dropped_result = classify(
            &ledger,
            std::slice::from_ref(&redeem),
            &dropped,
            false,
            &HashSet::new(),
            &lookups(),
        );
        assert!(dropped_result.apply.is_empty());
        assert!(dropped_result.problems.is_empty());
        assert!(dropped_result.ignored.is_empty());
    }
}

#[test]
fn single_component_underflow_and_overflow_drop_markets() {
    for (kind, cause) in [
        (ActivityType::Merge, DropCause::Underflow),
        (ActivityType::Redeem, DropCause::Underflow),
        (ActivityType::Split, DropCause::Overflow),
        (ActivityType::Trade, DropCause::Overflow),
    ] {
        let mut ledger = PositionLedger::new();
        if cause == DropCause::Overflow {
            ledger.replace_wallet_snapshot(
                wallet(),
                HashMap::from([(
                    MarketOutcomeId::new(market(A), OutcomeId(0)),
                    PositionState {
                        long_contracts: ShareAmount::from_atomic(u64::MAX),
                        short_contracts: ShareAmount::ZERO,
                    },
                )]),
            );
        }
        let aggregate = if kind == ActivityType::Trade {
            buy("overflow", A, 0, 1)
        } else {
            position_activity(kind, "arithmetic", A, 100)
        };
        let result = classify(
            &ledger,
            &[aggregate],
            &BTreeSet::new(),
            false,
            &HashSet::new(),
            &lookups(),
        );
        assert_problem(&result, ScopeKind::Market, A, cause);
        assert!(result.apply.is_empty());
    }
}

#[test]
fn homogeneous_buy_pieces_count_once_and_every_piece_applies() {
    let aggregates = [
        buy("piece-c", A, 0, 1_000_001),
        buy("piece-a", A, 0, 2_000_003),
        buy("piece-b", A, 0, 3_000_005),
    ];
    let result = fresh(&aggregates);
    assert!(!result.problem_second);
    assert_eq!(result.apply.len(), 3);
    assert_eq!(result.consumed, vec![market(A)]);
    let minimum = aggregates
        .iter()
        .map(|aggregate| &aggregate.group_id.key().0)
        .min()
        .unwrap();
    let admitted = result
        .decisions
        .iter()
        .filter(|decision| decision.entry == EntryClassification::Admitted)
        .collect::<Vec<_>>();
    assert_eq!(admitted.len(), 1);
    assert_eq!(&admitted[0].source_trade_id.0, minimum);
    assert!(!admitted[0].action_order_dependent);
    assert_eq!(
        result
            .decisions
            .iter()
            .filter(|decision| decision.entry == EntryClassification::NotFirstEntry)
            .count(),
        2
    );
    let mut ledger = PositionLedger::new();
    ledger.apply_all_or_none(&result.apply).unwrap();
    assert_eq!(state(&ledger, A, 0).long_contracts.atomic(), 6_000_009);
    let mut reversed = aggregates.clone();
    reversed.reverse();
    assert_eq!(fresh(&reversed), result);
}

#[test]
fn opposite_outcome_buys_in_separate_components_are_ambiguous_per_market() {
    let result = fresh(&[buy("yes", A, 0, 10), buy("no", A, 1, 10)]);
    assert!(!result.problem_second);
    assert_eq!(result.apply.len(), 2);
    assert_eq!(result.consumed, vec![market(A)]);
    assert!(
        result
            .decisions
            .iter()
            .all(|decision| decision.entry == EntryClassification::AmbiguousFirstEntrySameSecond)
    );
    assert!(
        result
            .decisions
            .iter()
            .all(|decision| !decision.action_order_dependent)
    );
}

#[test]
fn one_failed_outcome_drops_valid_components_in_its_market_but_applies_another_market() {
    let redeem = position_activity(ActivityType::Redeem, "underflow", A, 100);
    let aggregates = [
        buy("other-outcome", A, 1, 10),
        redeem.clone(),
        buy("other-market", B, 0, 10),
    ];
    let result = fresh(&aggregates);
    assert_problem(&result, ScopeKind::Market, A, DropCause::Underflow);
    assert_eq!(result.problems[0].trigger, *redeem.group_id.key());
    assert_eq!(result.apply.len(), 1);
    assert_eq!(result.decisions[0].market_id, market(B));
    assert_eq!(result.consumed, vec![market(A), market(B)]);
    let mut ledger = PositionLedger::new();
    ledger.apply_all_or_none(&result.apply).unwrap();
    assert_eq!(state(&ledger, A, 1).long_contracts, ShareAmount::ZERO);
    assert_eq!(state(&ledger, B, 0).long_contracts.atomic(), 10);
    let mut reversed = aggregates;
    reversed.reverse();
    assert_eq!(fresh(&reversed), result);
}

#[test]
fn order_dependent_mix_uses_first_canonical_component_record_as_trigger() {
    let aggregates = [
        buy("piece", A, 0, 100),
        position_activity(ActivityType::Redeem, "redeem", A, 100),
        buy("other", B, 0, 10),
    ];
    let result = fresh(&aggregates);
    assert_problem(&result, ScopeKind::Market, A, DropCause::OrderDependent);
    let minimum = aggregates[..2]
        .iter()
        .map(|aggregate| &aggregate.group_id.key().0)
        .min()
        .unwrap();
    assert_eq!(&result.problems[0].trigger.0, minimum);
    assert_eq!(result.apply.len(), 1);
    assert_eq!(result.consumed, vec![market(A), market(B)]);
}

#[test]
fn multiple_failed_components_and_scopes_are_all_reported_in_scope_order() {
    let aggregates = [
        position_activity(ActivityType::Redeem, "a-underflow", A, 100),
        position_activity(ActivityType::Merge, "b-underflow", B, 100),
        position_activity(ActivityType::Conversion, "event-drop", GROUP, 1),
    ];
    let result = fresh(&aggregates);
    assert_eq!(
        result
            .problems
            .iter()
            .map(|problem| problem.scope.clone())
            .collect::<Vec<_>>(),
        vec![
            scope(ScopeKind::Event, GROUP),
            scope(ScopeKind::Market, A),
            scope(ScopeKind::Market, B)
        ]
    );
    assert!(result.apply.is_empty());
    let mut reversed = aggregates;
    reversed.reverse();
    assert_eq!(fresh(&reversed), result);
}

#[test]
fn two_problems_for_one_scope_choose_the_same_trigger_and_cause_in_every_order() {
    let aggregates = [
        position_activity(ActivityType::Conversion, "conversion", A, 1),
        position_activity(ActivityType::Unknown("NEW".to_owned()), "unknown", A, 1),
    ];
    let expected = if aggregates[0].group_id.key().0 < aggregates[1].group_id.key().0 {
        (&aggregates[0], DropCause::Conversion)
    } else {
        (&aggregates[1], DropCause::UnknownType)
    };
    let result = fresh(&aggregates);
    assert_problem(&result, ScopeKind::Event, GROUP, expected.1);
    assert_eq!(result.problems[0].trigger, *expected.0.group_id.key());
    assert_eq!(fresh(&aggregates), result);
    let mut reversed = aggregates;
    reversed.reverse();
    assert_eq!(fresh(&reversed), result);
}

#[test]
fn existing_drops_suppress_problems_silently_and_buy_consumption_survives() {
    for dropped in [scope(ScopeKind::Market, A), scope(ScopeKind::Event, GROUP)] {
        let aggregates = [
            buy("dropped-buy", A, 0, 10),
            buy("dropped-bad-buy", "\\xaaa", 0, 10),
            position_activity(ActivityType::Redeem, "dropped-redeem", A, 0),
        ];
        let result = classify(
            &PositionLedger::new(),
            &aggregates,
            &BTreeSet::from([dropped]),
            false,
            &HashSet::new(),
            &lookups(),
        );
        assert!(!result.problem_second);
        assert!(result.problems.is_empty());
        assert!(result.ignored.is_empty());
        assert!(result.apply.is_empty());
        assert!(result.decisions.is_empty());
        assert_eq!(result.consumed, vec![market(A)]);
    }
    let conversion = position_activity(ActivityType::Conversion, "already-dropped", GROUP, 1);
    let result = classify(
        &PositionLedger::new(),
        &[conversion],
        &BTreeSet::from([scope(ScopeKind::Event, GROUP)]),
        false,
        &HashSet::new(),
        &lookups(),
    );
    assert!(!result.problem_second);
    assert!(result.problems.is_empty());
    assert!(result.ignored.is_empty());
}

#[test]
fn hex_prefix_normalization_is_only_for_resolution_and_drops_later_canonical_trade() {
    let conversion =
        position_activity(ActivityType::Conversion, "prefixed-conversion", "\\xEEE", 1);
    assert_problem(
        &fresh(&[conversion]),
        ScopeKind::Event,
        GROUP,
        DropCause::Conversion,
    );
    for id in ["\\xaaa", "0xAAA", "0XAAA"] {
        let aggregate = buy("prefixed-trade", id, 0, 10);
        let result = fresh(&[aggregate]);
        assert_problem(&result, ScopeKind::Market, A, DropCause::Unmapped);
        assert!(result.apply.is_empty());
        assert_eq!(result.consumed, vec![market(A)]);
        let later = classify(
            &PositionLedger::new(),
            &[buy("canonical-later", A, 0, 10)],
            &BTreeSet::from([scope(ScopeKind::Market, A)]),
            false,
            &HashSet::new(),
            &lookups(),
        );
        assert!(later.apply.is_empty());
        assert!(later.decisions.is_empty());
        assert_eq!(later.consumed, vec![market(A)]);
    }
}

#[test]
fn noncanonical_position_ids_drop_the_normalized_market_for_every_position_effect() {
    for kind in [
        ActivityType::Trade,
        ActivityType::Redeem,
        ActivityType::Split,
        ActivityType::Merge,
    ] {
        let aggregate = activity(
            kind,
            "prefixed-position",
            Some("\\xaaa"),
            Some("token-a"),
            Some(Side::Buy),
            Some(0),
            100,
        );
        assert_problem(
            &fresh(&[aggregate]),
            ScopeKind::Market,
            A,
            DropCause::Unmapped,
        );
    }
}

#[test]
fn tokenless_buy_before_and_beside_a_bound_buy_is_ignored_without_consumption() {
    for condition in [Some(A), None] {
        let tokenless = activity(
            ActivityType::Trade,
            "tokenless",
            condition,
            None,
            Some(Side::Buy),
            Some(1),
            10,
        );
        let first = fresh(std::slice::from_ref(&tokenless));
        assert!(!first.problem_second);
        assert!(first.apply.is_empty());
        assert!(first.consumed.is_empty());
        assert_eq!(
            first.ignored,
            vec![(tokenless.group_id.key().clone(), ActivityType::Trade)]
        );
        let bound = buy("bound", A, 0, 10);
        let later = fresh(std::slice::from_ref(&bound));
        assert_eq!(later.decisions[0].entry, EntryClassification::Admitted);
        let result = fresh(&[tokenless.clone(), bound.clone()]);
        assert!(!result.problem_second);
        assert_eq!(result.apply.len(), 1);
        assert_eq!(result.decisions[0].source_trade_id, *bound.group_id.key());
        assert_eq!(result.decisions[0].entry, EntryClassification::Admitted);
        assert!(!result.decisions[0].action_order_dependent);
        assert_eq!(result.consumed, vec![market(A)]);
        assert_eq!(result.ignored, first.ignored);
        assert_eq!(fresh(&[bound, tokenless]), result);
    }
}

#[test]
fn certified_problem_second_remains_unscored_after_its_trigger_disappears() {
    let trigger = position_activity(ActivityType::Unknown("NEW".to_owned()), "trigger", A, 1);
    let unaffected = buy("unaffected", B, 0, 10);
    let original = fresh(&[trigger, unaffected.clone()]);
    assert_problem(&original, ScopeKind::Event, GROUP, DropCause::UnknownType);
    let dropped = original
        .problems
        .iter()
        .map(|problem| problem.scope.clone())
        .collect();
    let replacement = classify(
        &PositionLedger::new(),
        std::slice::from_ref(&unaffected),
        &dropped,
        true,
        &HashSet::new(),
        &lookups(),
    );
    assert!(replacement.problem_second);
    assert!(replacement.problems.is_empty());
    assert_eq!(replacement.apply, original.apply);
    assert_eq!(replacement.decisions, original.decisions);
    assert_eq!(replacement.consumed, original.consumed);
    let mut ledger = PositionLedger::new();
    ledger.apply_all_or_none(&replacement.apply).unwrap();
    assert_eq!(state(&ledger, B, 0).long_contracts.atomic(), 10);
    assert_eq!(
        classify(
            &PositionLedger::new(),
            &[unaffected],
            &dropped,
            true,
            &HashSet::new(),
            &lookups()
        ),
        replacement
    );
}

#[test]
fn buys_consume_at_zero_balance_and_reopening_is_not_a_first_entry() {
    let mut ledger = PositionLedger::new();
    let mut history = HashSet::new();
    for (label, side, entry) in [
        ("first", Side::Buy, EntryClassification::Admitted),
        ("close", Side::Sell, EntryClassification::NotBuy),
        ("reopen", Side::Buy, EntryClassification::NotFirstEntry),
    ] {
        let aggregate = activity(
            ActivityType::Trade,
            label,
            Some(A),
            Some("token"),
            Some(side),
            Some(0),
            100,
        );
        let result = classify(
            &ledger,
            &[aggregate],
            &BTreeSet::new(),
            false,
            &history,
            &lookups(),
        );
        assert_eq!(result.decisions[0].entry, entry);
        if label == "reopen" {
            assert_eq!(state(&ledger, A, 0), PositionState::default());
            assert_eq!(result.decisions[0].action, LeaderAction::Entry);
        }
        ledger.apply_all_or_none(&result.apply).unwrap();
        history.extend(result.consumed);
    }
}

#[test]
fn buy_sell_split_merge_and_split_merge_have_equal_balances_but_different_history() {
    let mut outcomes = Vec::new();
    for traded in [false, true] {
        let mut ledger = PositionLedger::new();
        let mut history = HashSet::new();
        let mut aggregates = Vec::new();
        if traded {
            aggregates.push(buy("first", A, 0, 100));
            aggregates.push(activity(
                ActivityType::Trade,
                "close",
                Some(A),
                Some("token"),
                Some(Side::Sell),
                Some(0),
                100,
            ));
        }
        aggregates.extend([
            position_activity(ActivityType::Split, "split", A, 100),
            position_activity(ActivityType::Merge, "merge", A, 100),
        ]);
        for aggregate in aggregates {
            let result = classify(
                &ledger,
                &[aggregate],
                &BTreeSet::new(),
                false,
                &history,
                &lookups(),
            );
            assert!(!result.problem_second);
            ledger.apply_all_or_none(&result.apply).unwrap();
            history.extend(result.consumed);
        }
        assert_eq!(state(&ledger, A, 0), PositionState::default());
        assert_eq!(state(&ledger, A, 1), PositionState::default());
        let next = classify(
            &ledger,
            &[buy("next", A, 0, 100)],
            &BTreeSet::new(),
            false,
            &history,
            &lookups(),
        );
        outcomes.push(next.decisions[0].entry);
    }
    assert_eq!(
        outcomes,
        vec![
            EntryClassification::Admitted,
            EntryClassification::NotFirstEntry
        ]
    );
}

#[test]
fn caller_classification_context_and_entry_policy_are_preserved() {
    let aggregates = [buy("one", A, 0, 10), buy("two", A, 0, 10)];
    for (policy, complete, expected) in [
        (
            SameSecondEntryPolicy::Legacy,
            true,
            EntryClassification::AmbiguousFirstEntrySameSecond,
        ),
        (
            SameSecondEntryPolicy::HomogeneousPieces,
            false,
            EntryClassification::WalletHistoryIncomplete,
        ),
    ] {
        let result = classify_scoped_second(
            policy,
            &PositionLedger::new(),
            wallet(),
            &records(&aggregates),
            &BTreeSet::new(),
            false,
            ReconstructionQuality::new(100).unwrap(),
            &SignalConfig::default(),
            complete,
            &|_| false,
            &lookups(),
        )
        .unwrap();
        assert!(
            result
                .decisions
                .iter()
                .all(|decision| decision.entry == expected)
        );
    }
    let result = classify_scoped_second(
        SameSecondEntryPolicy::HomogeneousPieces,
        &PositionLedger::new(),
        wallet(),
        &records(&aggregates),
        &BTreeSet::new(),
        false,
        ReconstructionQuality::new(0).unwrap(),
        &SignalConfig::default(),
        true,
        &|_| false,
        &lookups(),
    )
    .unwrap();
    assert!(
        result
            .decisions
            .iter()
            .all(|decision| decision.action == LeaderAction::Unknown)
    );
    assert_eq!(result.consumed, vec![market(A)]);
}

#[test]
fn unknown_payout_market_buy_still_applies_and_consumes() {
    let result = fresh(&[buy("unknown-payout", UNKNOWN, 0, 10)]);
    assert!(!result.problem_second);
    assert!(result.ignored.is_empty());
    assert_eq!(result.apply.len(), 1);
    assert_eq!(result.consumed, vec![market(UNKNOWN)]);
}

#[test]
fn unresolvable_failed_component_does_not_erase_another_outcomes_consumption() {
    let result = fresh(&[
        position_activity(ActivityType::Redeem, "unknown-underflow", UNKNOWN, 100),
        buy("known-effect", UNKNOWN, 1, 10),
    ]);
    assert!(!result.problem_second);
    assert_eq!(result.ignored.len(), 1);
    assert_eq!(result.apply.len(), 1);
    assert_eq!(result.consumed, vec![market(UNKNOWN)]);
}

#[test]
fn caller_errors_other_than_mapping_failure_fail_closed() {
    let aggregate = buy("caller-error", A, 0, 10);
    let error = LedgerError::Overflow {
        source_trade_id: aggregate.group_id.key().clone(),
    };
    let record = SecondRecord {
        aggregate: &aggregate,
        mutation: Err(error.clone()),
    };
    assert_eq!(
        classify_scoped_historical_second(
            &PositionLedger::new(),
            wallet(),
            &[record],
            &BTreeSet::new(),
            false,
            ReconstructionQuality::new(100).unwrap(),
            &|_| false,
            &lookups()
        ),
        Err(error)
    );
}

#[test]
fn verified_outcomes_own_homogeneity_and_every_corrected_piece_applies() {
    let aggregates = [buy("stamped-yes", A, 0, 10), buy("stamped-no", A, 1, 20)];
    let mut records = records(&aggregates);
    records[1].mutation = records[1].mutation.clone().map(|mutation| {
        mutation.with_verified_identity(
            MarketOutcomeId::new(market(A), OutcomeId(0)),
            "bound-payout".to_owned(),
        )
    });
    let result = classify_scoped_historical_second(
        &PositionLedger::new(),
        wallet(),
        &records,
        &BTreeSet::new(),
        false,
        ReconstructionQuality::new(100).unwrap(),
        &|_| false,
        &lookups(),
    )
    .unwrap();
    assert_eq!(
        result
            .decisions
            .iter()
            .filter(|decision| decision.entry == EntryClassification::Admitted)
            .count(),
        1
    );
    assert_eq!(result.apply.len(), 2);
    assert_eq!(result.consumed, vec![market(A)]);
    let mut ledger = PositionLedger::new();
    ledger.apply_all_or_none(&result.apply).unwrap();
    assert_eq!(state(&ledger, A, 0).long_contracts.atomic(), 30);
    assert_eq!(state(&ledger, A, 1), PositionState::default());
}

#[test]
fn a_later_drop_keeps_earlier_entries_and_positions() {
    let mut ledger = PositionLedger::new();
    let earlier = fresh(&[buy("earlier", A, 0, 10)]);
    ledger.apply_all_or_none(&earlier.apply).unwrap();
    let history = earlier.consumed.iter().cloned().collect();
    let before = ledger.snapshots().clone();
    let result = classify(
        &ledger,
        &[
            position_activity(ActivityType::Conversion, "later-conversion", GROUP, 1),
            buy("later-b", B, 0, 20),
        ],
        &BTreeSet::new(),
        false,
        &history,
        &lookups(),
    );
    assert_problem(&result, ScopeKind::Event, GROUP, DropCause::Conversion);
    assert_eq!(ledger.snapshots(), &before);
    ledger.apply_all_or_none(&result.apply).unwrap();
    assert_eq!(state(&ledger, A, 0).long_contracts.atomic(), 10);
    assert_eq!(state(&ledger, B, 0).long_contracts.atomic(), 20);
    assert_eq!(earlier.decisions[0].entry, EntryClassification::Admitted);
}

#[test]
fn mixed_problem_seconds_are_identical_for_all_record_permutations() {
    let aggregates = [
        buy("a-buy", A, 0, 100),
        position_activity(ActivityType::Redeem, "a-redeem", A, 100),
        buy("b-buy", B, 0, 10),
        activity(ActivityType::Split, "ignored", None, None, None, None, 10),
    ];
    let expected = fresh(&aggregates);
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if order.into_iter().collect::<BTreeSet<_>>().len() == 4 {
                        assert_eq!(
                            fresh(&order.map(|index| aggregates[index].clone())),
                            expected
                        );
                    }
                }
            }
        }
    }
}
