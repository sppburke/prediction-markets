//! Read-only AC1 sample, adapted from the classifier-five wallet harness.

use super::projection_v3::{
    BoundPayouts, ScopeDrop, classify_loaded_wallet, recorded_inputs, spool_path, verify_spool,
};
use super::*;
use pe_position_ledger::{
    DropCause, MarketLookup, ScopeKind, ScopeLookups, ScopeProblem, SecondRecord,
    classify_scoped_historical_second,
};
use std::io::BufRead as _;

pub(super) fn run(cache_path: &Path) -> Result<Value, BootstrapError> {
    let connection = open_existing_ro(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    let transaction = connection.unchecked_transaction()?;
    verify_history_certificates(&transaction)?;
    let identity = fresh_collection_record(&transaction)?.ok_or(BootstrapError::Internal)?;
    if identity.version != 4 {
        return invalid("classifier sample requires history format three".to_owned());
    }
    let inputs = recorded_inputs(&transaction)?;
    let count: i64 = transaction.query_row(
        "SELECT ranker_projection_count FROM cache_v2_migration_state",
        [],
        |row| row.get(0),
    )?;
    verify_spool(
        cache_path,
        inputs
            .projection_spool
            .as_ref()
            .ok_or(BootstrapError::Internal)?,
        to_u64(count, "projection count")?,
    )?;
    let payout = BoundPayouts::read(&transaction)?;
    let proof = incremental::HistoryProof::load(&transaction, &identity)?;
    let quality = ReconstructionQuality::new(100).map_err(|error| BootstrapError::Invalid {
        message: error.to_string(),
    })?;
    let mut reader = std::io::BufReader::new(File::open(spool_path(cache_path))?).lines();
    let mut next = reader
        .next()
        .transpose()?
        .map(|line| serde_json::from_str::<Value>(&line))
        .transpose()?;
    let started = Instant::now();
    let mut wallets = 0;
    let mut groups = 0;
    let mut selected = 0;
    let mut baseline_count = 0;
    let mut differences = Vec::new();
    let mut by_rule = BTreeMap::<String, u64>::new();
    let mut by_cause = BTreeMap::<String, u64>::new();
    aggregate_scan::scoped(|scan| {
        for wallet in identity
            .wallets
            .iter()
            .filter(|wallet| wallet.ends_with("00"))
        {
            let receipt = incremental::receipt_for_identity(&transaction, &identity, wallet)?;
            if receipt.excluded() {
                continue;
            }
            let certificate = incremental::HistoryCertificate::load(&transaction, wallet)?
                .ok_or(BootstrapError::Internal)?;
            if certificate.generation != identity.generation {
                return invalid(format!(
                    "sample certificate is not at finalized head for {wallet}"
                ));
            }
            let chain =
                incremental::HistoryChain::load(&transaction, wallet, &proof, identity.generation)?;
            let mut check = chain.check();
            let mut aggregates = Vec::new();
            if let Some(error) =
                scan.for_each_history(&transaction, wallet, |aggregate, json| {
                    if let Some(json) = json {
                        check.push(&aggregate, json)?;
                    }
                    aggregates.push(aggregate);
                    Ok(())
                })?
            {
                return Err(error);
            }
            let checked =
                check.finish(identity.generation, certificate.scope_drops_json.clone())?;
            if checked.newest_source_unix != certificate.newest_source_unix
                || checked.newest_trade_unix != certificate.newest_trade_unix
            {
                return invalid(format!("sample certificate recency differs for {wallet}"));
            }
            let by_id: HashMap<_, _> = aggregates
                .iter()
                .map(|aggregate| (aggregate.group_id.key().0.as_str(), aggregate))
                .collect();
            let baseline =
                super::classify_loaded_wallet(wallet, &aggregates, &payout.tokens, quality)?;
            let classified = classify_loaded_wallet(certificate.clone(), &aggregates, &payout)?;
            if classified.certificate.scope_drops_json != certificate.scope_drops_json {
                return invalid(format!("sample certificate drops differ for {wallet}"));
            }
            let ids: BTreeSet<_> = classified
                .rows
                .iter()
                .map(|row| {
                    row["source_trade_id"]
                        .as_str()
                        .ok_or(BootstrapError::Internal)
                })
                .collect::<Result<_, _>>()?;
            let baseline: BTreeSet<_> = baseline.iter().map(String::as_str).collect();
            // Independent fourteen-field adapter: take the source aggregate and frozen payout
            // columns directly, deriving the outcome from tokens rather than classifier decisions.
            for row in &classified.rows {
                let id = row["source_trade_id"]
                    .as_str()
                    .ok_or(BootstrapError::Internal)?;
                let aggregate = by_id.get(id).copied().ok_or(BootstrapError::Internal)?;
                let c = aggregate.group_id.components();
                let market = c.condition_id.as_ref().ok_or(BootstrapError::Internal)?;
                let asset = c.asset.as_ref().ok_or(BootstrapError::Internal)?;
                let (tokens, _, _) = payout
                    .tokens
                    .get(&market.0)
                    .ok_or(BootstrapError::Internal)?;
                let outcome = tokens
                    .iter()
                    .position(|token| token == &asset.0)
                    .ok_or(BootstrapError::Internal)?;
                let (_, end, vector) = payout
                    .markets
                    .get(&market.0)
                    .ok_or(BootstrapError::Internal)?;
                let expected = serde_json::json!({"activity_generation":identity.generation,"asset":asset.0,
                    "classifier_version":6,"condition_id":market.0,"end_date_unix":end,
                    "outcome_id":u16::try_from(outcome).map_err(|_| BootstrapError::Internal)?,"payout_vector_json":vector,
                    "price_weighted_share_amount_str":aggregate.price_weighted_share_sum.0.to_string(),
                    "share_amount_str":aggregate.share_sum.to_decimal().to_string(),"side":"buy",
                    "source_time_unix":aggregate.source_time.0.unix_timestamp(),"source_trade_id":id,
                    "source_usdc_amount_str":aggregate.source_usdc_sum.to_decimal().to_string(),"wallet_hex":wallet});
                if &expected != row {
                    return invalid(format!(
                        "sample fourteen-field adapter mismatch for {wallet}/{id}"
                    ));
                }
            }
            let mut spooled = Vec::new();
            while let Some(row) = &next {
                let row_wallet = row["wallet_hex"].as_str().ok_or(BootstrapError::Internal)?;
                if row_wallet > wallet.as_str() {
                    break;
                }
                if row_wallet == wallet {
                    spooled.push(row.clone());
                }
                next = reader
                    .next()
                    .transpose()?
                    .map(|line| serde_json::from_str::<Value>(&line))
                    .transpose()?;
            }
            if spooled != classified.rows {
                return invalid(format!("sample projection spool mismatch for {wallet}"));
            }
            let drops: Vec<ScopeDrop> = serde_json::from_str(&certificate.scope_drops_json)?;
            let trace = if ids == baseline {
                None
            } else {
                let trace = trace_rules(wallet, &aggregates, &payout, &drops, quality)?;
                if trace
                    .baseline_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>()
                    != baseline
                    || trace
                        .selected_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>()
                        != ids
                {
                    return invalid(format!(
                        "sample diagnostic replay differs from classifier owners for {wallet}"
                    ));
                }
                Some(trace)
            };
            for id in ids.symmetric_difference(&baseline) {
                let aggregate = by_id.get(*id).copied().ok_or(BootstrapError::Internal)?;
                let time = aggregate.source_time.0.unix_timestamp();
                let explanation = difference_rule(
                    &aggregates,
                    aggregate,
                    &drops,
                    trace.as_ref().ok_or(BootstrapError::Internal)?,
                    &by_id,
                    &payout,
                    ids.contains(id),
                    &baseline,
                    quality,
                )?;
                let count = by_rule.entry(explanation.rule.to_owned()).or_default();
                *count = checked_activity_count(*count, 1)?;
                if let Some(cause) = explanation.cause {
                    let key = serde_json::to_value(cause)?
                        .as_str()
                        .ok_or(BootstrapError::Internal)?
                        .to_owned();
                    let count = by_cause.entry(key).or_default();
                    *count = checked_activity_count(*count, 1)?;
                }
                differences.push(serde_json::json!({"wallet_hex":wallet,"source_trade_id":id,
                    "source_time_unix":time,"rule":explanation.rule,"trigger":explanation.trigger,
                    "cause":explanation.cause,"scope":explanation.scope,
                    "selected_v6":ids.contains(id)}));
            }
            wallets = checked_activity_count(wallets, 1)?;
            groups = checked_activity_count(
                groups,
                u64::try_from(aggregates.len()).map_err(|_| BootstrapError::Internal)?,
            )?;
            selected = checked_activity_count(
                selected,
                u64::try_from(ids.len()).map_err(|_| BootstrapError::Internal)?,
            )?;
            baseline_count = checked_activity_count(
                baseline_count,
                u64::try_from(baseline.len()).map_err(|_| BootstrapError::Internal)?,
            )?;
        }
        Ok(())
    })?;
    transaction.rollback()?;
    Ok(
        serde_json::json!({"pid":std::process::id(),"wallets":wallets,"aggregates":groups,
        "classifier_6_entries":selected,"classifier_5_entries":baseline_count,"differences":differences,
        "differences_by_rule":by_rule,"differences_by_cause":by_cause,
        "elapsed_ms":started.elapsed().as_millis()}),
    )
}

#[derive(Clone)]
struct Stop {
    second: i64,
    trigger: String,
    cause: DropCause,
}

struct RuleTrace {
    problems: Vec<(i64, ScopeProblem)>,
    ignored: HashSet<String>,
    pieces: BTreeMap<String, Vec<String>>,
    stop: Option<Stop>,
    baseline_ids: BTreeSet<String>,
    selected_ids: BTreeSet<String>,
}

// The diagnostic replay delegates both verdicts to their existing owners. The
// unchanged classifier-five harness above remains the selected-ID authority.
fn trace_rules(
    wallet_hex: &str,
    aggregates: &[ActivityAggregate],
    payout: &BoundPayouts,
    drops: &[ScopeDrop],
    quality: ReconstructionQuality,
) -> Result<RuleTrace, BootstrapError> {
    let wallet = WalletAddress::from_hex(wallet_hex).map_err(|error| BootstrapError::Invalid {
        message: error.to_string(),
    })?;
    let mut trace = RuleTrace {
        problems: Vec::new(),
        ignored: HashSet::new(),
        pieces: BTreeMap::new(),
        stop: None,
        baseline_ids: BTreeSet::new(),
        selected_ids: BTreeSet::new(),
    };
    let mut positions = HashMap::new();
    let mut baseline_positions = HashMap::new();
    let mut history = HashSet::new();
    let mut baseline_history = HashSet::new();
    for second in aggregates.chunk_by(|a, b| a.source_time == b.source_time) {
        let at = second[0].source_time.0.unix_timestamp();
        let records: Vec<_> = second
            .iter()
            .map(|aggregate| SecondRecord {
                aggregate,
                mutation: verified_mutation(aggregate, &payout.tokens),
            })
            .collect();
        let keys: HashSet<_> = records
            .iter()
            .filter_map(|record| record.mutation.as_ref().ok())
            .flat_map(LedgerMutation::touched_keys)
            .collect();
        let mut ledger = PositionLedger::new();
        ledger.replace_wallet_snapshot(
            wallet,
            keys.iter()
                .filter_map(|key| positions.get(key).map(|state| (key.clone(), *state)))
                .collect(),
        );
        let mut baseline_ledger = PositionLedger::new();
        baseline_ledger.replace_wallet_snapshot(
            wallet,
            keys.iter()
                .filter_map(|key| {
                    baseline_positions
                        .get(key)
                        .map(|state| (key.clone(), *state))
                })
                .collect(),
        );
        let earlier = drops
            .iter()
            .filter(|drop| drop.dropped_at_unix < at)
            .map(ScopeDrop::scope)
            .collect();
        // A drop is inclusive during application. Excluding its starting second
        // here lets the same owner identify its actual trigger for the report.
        let discovered = classify_scoped_historical_second(
            &ledger,
            wallet,
            &records,
            &earlier,
            false,
            quality,
            &|market| history.contains(&market.to_string()),
            payout,
        )
        .map_err(|error| BootstrapError::Invalid {
            message: error.to_string(),
        })?;
        trace
            .problems
            .extend(discovered.problems.into_iter().map(|problem| (at, problem)));
        trace
            .ignored
            .extend(discovered.ignored.into_iter().map(|(id, _)| id.0));
        let inclusive = drops
            .iter()
            .filter(|drop| drop.dropped_at_unix <= at)
            .map(ScopeDrop::scope)
            .collect();
        let classified = classify_scoped_historical_second(
            &ledger,
            wallet,
            &records,
            &inclusive,
            drops.iter().any(|drop| drop.dropped_at_unix == at),
            quality,
            &|market| history.contains(&market.to_string()),
            payout,
        )
        .map_err(|error| BootstrapError::Invalid {
            message: error.to_string(),
        })?;
        for decision in &classified.decisions {
            if projected_decision(decision, second, payout) {
                trace
                    .selected_ids
                    .insert(decision.source_trade_id.0.clone());
            }
            if decision.entry == EntryClassification::Admitted {
                let pieces: Vec<_> = classified
                    .apply
                    .iter()
                    .filter(|mutation| {
                        matches!(mutation.effect.effective(), LedgerEffect::Trade {
                        market_id, outcome_id, side: Side::Buy, .. }
                        if *market_id == decision.market_id
                        && *outcome_id == decision.outcome_id)
                    })
                    .map(|mutation| mutation.source_trade_id.0.clone())
                    .collect();
                if pieces.len() > 1 {
                    trace
                        .pieces
                        .insert(decision.source_trade_id.0.clone(), pieces);
                }
            }
        }
        if trace.stop.is_none() {
            let mutations: Result<Vec<_>, _> = records
                .iter()
                .map(|record| verified_mutation(record.aggregate, &payout.tokens))
                .collect();
            let mut stop = None;
            match mutations {
                Err(error) => stop = Some(error_stop(at, &error)),
                Ok(mutations) => {
                    if let Some(record) =
                        second.iter().zip(&mutations).find(|(aggregate, mutation)| {
                            matches!(mutation.effect.effective(), LedgerEffect::RequiresAnchor)
                                && aggregate.group_id.components().condition_id.is_none()
                        })
                    {
                        stop = Some(Stop {
                            second: at,
                            trigger: record.0.group_id.key().0.clone(),
                            cause: DropCause::UnknownCondition,
                        });
                    } else {
                        match classify_complete_historical_second(
                            &baseline_ledger,
                            wallet,
                            &mutations,
                            quality,
                            &|market| baseline_history.contains(&market.to_string()),
                        ) {
                            Err(error) => stop = Some(error_stop(at, &error)),
                            Ok(SecondVerdict::OrderDependent { trigger }) => {
                                let problem = trace
                                    .problems
                                    .iter()
                                    .find(|(time, problem)| *time == at && problem.trigger == trigger)
                                    .ok_or_else(|| BootstrapError::Invalid {
                                    message: format!(
                                        "unexplained classifier-five stop for {wallet_hex} at {at}"
                                    ),
                                })?;
                                stop = Some(Stop {
                                    second: at,
                                    trigger: problem.1.trigger.0.clone(),
                                    cause: problem.1.cause,
                                });
                            }
                            Ok(SecondVerdict::OrderIndependent { decisions, .. }) => {
                                trace.baseline_ids.extend(
                                    decisions
                                        .iter()
                                        .filter(|decision| {
                                            projected_decision(decision, second, payout)
                                        })
                                        .map(|decision| decision.source_trade_id.0.clone()),
                                );
                                match baseline_ledger.apply_all_or_none(&mutations) {
                                    Err(error) => stop = Some(error_stop(at, &error)),
                                    Ok(_) => {
                                        if let Some(snapshot) = baseline_ledger.position(&wallet) {
                                            baseline_positions.extend(
                                                snapshot
                                                    .positions
                                                    .iter()
                                                    .map(|(key, state)| (key.clone(), *state)),
                                            );
                                        }
                                        baseline_history.extend(
                                            decisions
                                                .iter()
                                                .filter(|decision| decision.side == Side::Buy)
                                                .map(|decision| decision.market_id.to_string()),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            trace.stop = stop;
        }
        ledger
            .apply_all_or_none(&classified.apply)
            .map_err(|error| BootstrapError::Invalid {
                message: format!("sample scoped application failed: {error}"),
            })?;
        if let Some(snapshot) = ledger.position(&wallet) {
            positions.extend(
                snapshot
                    .positions
                    .iter()
                    .map(|(key, state)| (key.clone(), *state)),
            );
        }
        history.extend(
            classified
                .consumed
                .into_iter()
                .map(|market| market.to_string()),
        );
    }
    Ok(trace)
}

fn projected_decision(
    decision: &pe_position_ledger::TradeDecision,
    second: &[ActivityAggregate],
    payout: &BoundPayouts,
) -> bool {
    decision.entry == EntryClassification::Admitted
        && !decision.action_order_dependent
        && decision.amount != ShareAmount::ZERO
        && payout
            .tokens
            .get(&decision.market_id.to_string())
            .is_some_and(|(_, _, eligible)| *eligible)
        && second.iter().any(|aggregate| {
            aggregate.group_id.key() == &decision.source_trade_id
                && aggregate.group_id.components().condition_id.is_some()
                && aggregate.group_id.components().asset.is_some()
                && aggregate.group_id.components().outcome.is_some()
                && aggregate.group_id.components().side.is_some()
        })
}

fn error_stop(second: i64, error: &LedgerError) -> Stop {
    let (trigger, cause) = match error {
        LedgerError::InvalidMapping {
            source_trade_id, ..
        } => (source_trade_id, DropCause::Unmapped),
        LedgerError::Underflow {
            source_trade_id, ..
        } => (source_trade_id, DropCause::Underflow),
        LedgerError::Overflow {
            source_trade_id, ..
        } => (source_trade_id, DropCause::Overflow),
        LedgerError::Conversion { source_trade_id } => (source_trade_id, DropCause::Conversion),
        LedgerError::UnknownEffect { source_trade_id } => (source_trade_id, DropCause::UnknownType),
    };
    Stop {
        second,
        trigger: trigger.0.clone(),
        cause,
    }
}

struct Explanation {
    rule: &'static str,
    trigger: String,
    cause: Option<DropCause>,
    scope: Option<pe_position_ledger::Scope>,
}

fn missing_mapping(aggregate: &ActivityAggregate) -> bool {
    let c = aggregate.group_id.components();
    aggregate.share_sum != ShareAmount::ZERO
        && !aggregate.is_combo
        && matches!(
            c.activity_type,
            ActivityType::Trade | ActivityType::Redeem | ActivityType::Split | ActivityType::Merge
        )
        && (c.condition_id.is_none()
            || (c.activity_type == ActivityType::Trade && c.asset.is_none()))
}

#[allow(clippy::too_many_arguments)]
fn difference_rule(
    aggregates: &[ActivityAggregate],
    selected: &ActivityAggregate,
    drops: &[ScopeDrop],
    trace: &RuleTrace,
    by_id: &HashMap<&str, &ActivityAggregate>,
    payout: &BoundPayouts,
    selected_v6: bool,
    baseline: &BTreeSet<&str>,
    quality: ReconstructionQuality,
) -> Result<Explanation, BootstrapError> {
    let time = selected.source_time.0.unix_timestamp();
    let id = selected.group_id.key().0.as_str();
    let market = selected
        .group_id
        .components()
        .condition_id
        .as_ref()
        .ok_or(BootstrapError::Internal)?
        .0
        .as_str();
    if !selected_v6 {
        let drop = drops
            .iter()
            .find(|drop| {
                drop.dropped_at_unix <= time
                    && match drop.scope_kind {
                        ScopeKind::Market => drop.scope_id == market,
                        ScopeKind::Event => {
                            matches!(payout.market(market), MarketLookup::Grouped(group)
                    if group == drop.scope_id)
                        }
                    }
            })
            .or_else(|| drops.iter().find(|drop| drop.dropped_at_unix == time));
        if let Some(drop) = drop {
            let problem = trace
                .problems
                .iter()
                .find(|(at, problem)| {
                    *at == drop.dropped_at_unix
                        && problem.scope == drop.scope()
                        && problem.cause == drop.cause
                })
                .ok_or_else(|| BootstrapError::Invalid {
                    message: format!(
                        "unexplained certified-drop trigger for {id}: {} at {}",
                        drop.scope_id, drop.dropped_at_unix
                    ),
                })?;
            let trigger = by_id
                .get(problem.1.trigger.0.as_str())
                .copied()
                .ok_or(BootstrapError::Internal)?;
            let noncanonical = drop.cause == DropCause::Unmapped
                && trigger
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .is_some_and(|id| {
                        !id.0.starts_with("0x")
                            || id.0[2..]
                                .chars()
                                .any(|c| !c.is_ascii_digit() && !('a'..='f').contains(&c))
                    });
            return Ok(Explanation {
                rule: if noncanonical {
                    "decision_1_non_canonical_id_unmapped"
                } else {
                    "decision_1_scoped_drop"
                },
                trigger: problem.1.trigger.0.clone(),
                cause: Some(drop.cause),
                scope: Some(drop.scope()),
            });
        }
    }
    if selected_v6
        && let Some(stop) = &trace.stop
        && stop.second <= time
    {
        let trigger = by_id
            .get(stop.trigger.as_str())
            .copied()
            .ok_or(BootstrapError::Internal)?;
        let problem = trace
            .problems
            .iter()
            .find(|(at, problem)| *at == stop.second && problem.trigger.0 == stop.trigger);
        if problem.is_some() || trace.ignored.contains(&stop.trigger) {
            return Ok(Explanation {
                rule: if missing_mapping(trigger) {
                    "acquisition_3_missing_mapping_decision_1_scope_or_ignored"
                } else {
                    "decision_1_problem_second_scoped_continuation"
                },
                trigger: stop.trigger.clone(),
                cause: Some(stop.cause),
                scope: problem.map(|(_, problem)| problem.scope.clone()),
            });
        }
    }
    if selected_v6 && let Some(pieces) = trace.pieces.get(id) {
        return Ok(Explanation {
            rule: "decision_1_homogeneous_pieces",
            trigger: pieces.join(","),
            cause: None,
            scope: None,
        });
    }
    let tokenless: Vec<_> = aggregates
        .iter()
        .filter(|aggregate| {
            let c = aggregate.group_id.components();
            aggregate.source_time <= selected.source_time
                && c.activity_type == ActivityType::Trade
                && c.asset.is_none()
                && c.condition_id.as_ref().is_some_and(|id| id.0 == market)
                && aggregate.share_sum != ShareAmount::ZERO
        })
        .collect();
    if !tokenless.is_empty() {
        let without: Vec<_> = aggregates
            .iter()
            .filter(|aggregate| {
                !tokenless
                    .iter()
                    .any(|raw| raw.group_id.key() == aggregate.group_id.key())
            })
            .cloned()
            .collect();
        let counterfactual = super::classify_loaded_wallet(
            &selected.group_id.components().wallet.to_string(),
            &without,
            &payout.tokens,
            quality,
        )?;
        if counterfactual.iter().any(|candidate| candidate == id) == selected_v6
            && baseline.contains(id) != selected_v6
        {
            return Ok(Explanation {
                rule: "decision_1_tokenless_raw_only_acquisition_3",
                trigger: tokenless
                    .iter()
                    .map(|aggregate| aggregate.group_id.key().0.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                cause: None,
                scope: None,
            });
        }
    }
    invalid(format!("unexplained classifier-five difference at {id}"))
}
