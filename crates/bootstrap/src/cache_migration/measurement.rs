//! Read-only AC1 sample, adapted from the classifier-five wallet harness.

use super::projection_v3::{
    BoundPayouts, ScopeDrop, classify_loaded_wallet, recorded_inputs, spool_path, verify_spool,
};
use super::*;
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
            let mut aggregates = Vec::new();
            if let Some(error) = scan.for_each_history(&transaction, wallet, |aggregate, _| {
                aggregates.push(aggregate);
                Ok(())
            })? {
                return Err(error);
            }
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
                let aggregate = aggregates
                    .iter()
                    .find(|aggregate| aggregate.group_id.key().0 == id)
                    .ok_or(BootstrapError::Internal)?;
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
            for id in ids.symmetric_difference(&baseline) {
                let aggregate = aggregates
                    .iter()
                    .find(|aggregate| aggregate.group_id.key().0 == *id)
                    .ok_or(BootstrapError::Internal)?;
                let time = aggregate.source_time.0.unix_timestamp();
                let (rule, trigger) = difference_rule(&aggregates, aggregate, &drops)?;
                differences.push(serde_json::json!({"wallet_hex":wallet,"source_trade_id":id,
                    "source_time_unix":time,"rule":rule,"trigger":trigger,"selected_v6":ids.contains(id)}));
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
        "elapsed_ms":started.elapsed().as_millis()}),
    )
}

fn difference_rule(
    aggregates: &[ActivityAggregate],
    selected: &ActivityAggregate,
    drops: &[ScopeDrop],
) -> Result<(&'static str, String), BootstrapError> {
    let time = selected.source_time.0.unix_timestamp();
    let market = selected
        .group_id
        .components()
        .condition_id
        .as_ref()
        .map(|id| id.0.as_str());
    for aggregate in aggregates
        .iter()
        .filter(|aggregate| aggregate.source_time.0.unix_timestamp() <= time)
    {
        let c = aggregate.group_id.components();
        if c.activity_type == ActivityType::Trade && aggregate.share_sum != ShareAmount::ZERO {
            if c.condition_id
                .as_ref()
                .is_some_and(|id| !id.0.starts_with("0x"))
            {
                return Ok((
                    "decision_1_non_canonical_id_unmapped",
                    aggregate.group_id.key().0.clone(),
                ));
            }
            if c.asset.is_none() && c.condition_id.as_ref().map(|id| id.0.as_str()) == market {
                return Ok((
                    "decision_1_tokenless_raw_only_acquisition_3",
                    aggregate.group_id.key().0.clone(),
                ));
            }
        }
        if c.condition_id.is_none()
            && matches!(
                c.activity_type,
                ActivityType::Trade
                    | ActivityType::Split
                    | ActivityType::Merge
                    | ActivityType::Redeem
            )
        {
            return Ok((
                "acquisition_3_missing_mapping_decision_1_scope_or_ignored",
                aggregate.group_id.key().0.clone(),
            ));
        }
        if drops
            .iter()
            .any(|drop| drop.dropped_at_unix == aggregate.source_time.0.unix_timestamp())
        {
            return Ok((
                "decision_1_problem_second_scoped_continuation",
                aggregate.group_id.key().0.clone(),
            ));
        }
    }
    let pieces: Vec<_> = aggregates
        .iter()
        .filter(|aggregate| {
            aggregate.source_time == selected.source_time
                && aggregate.group_id.components().condition_id
                    == selected.group_id.components().condition_id
                && aggregate.group_id.components().side == Some(Side::Buy)
        })
        .collect();
    if pieces.len() > 1 {
        let trigger = pieces
            .iter()
            .map(|aggregate| aggregate.group_id.key().0.as_str())
            .min()
            .ok_or(BootstrapError::Internal)?;
        return Ok(("decision_1_homogeneous_pieces", trigger.to_owned()));
    }
    if let Some(drop) = drops.iter().find(|drop| drop.dropped_at_unix <= time) {
        return Ok((
            "decision_1_cumulative_certified_scope",
            format!("{}:{}", drop.scope_id, drop.dropped_at_unix),
        ));
    }
    invalid(format!(
        "unexplained classifier-five difference at {}",
        selected.group_id.key().0
    ))
}
