//! The verified history pass and its durable, flat projection commitment.

use super::*;
use pe_position_ledger::{
    DropCause, MarketLookup, Scope, ScopeKind, ScopeLookups, SecondRecord,
    classify_scoped_historical_second,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SpoolCommitment {
    path_name: String,
    size_bytes: u64,
    lines: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportProjectionSummary {
    pub count: u64,
    pub digest: String,
    pub classifier_version: u32,
    pub oracle_version: u32,
    pub activity_generation: u64,
    pub spool_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportTable {
    count: u64,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportManifest {
    version: u32,
    activity_scope: String,
    tables: BTreeMap<String, ExportTable>,
    projection: ExportProjectionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopeDrop {
    cause: DropCause,
    pub(super) dropped_at_unix: i64,
    pub(super) scope_id: String,
    scope_kind: ScopeKind,
}

impl ScopeDrop {
    fn scope(&self) -> Scope {
        Scope {
            kind: self.scope_kind,
            id: self.scope_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassificationReport {
    pub ignored_by_activity_type: BTreeMap<String, u64>,
    pub drops_by_cause: BTreeMap<String, u64>,
    pub neg_risk_markets_without_group_id: u64,
}

type PayoutMarket = (Option<String>, Option<i64>, Option<String>);

pub(super) struct BoundPayouts {
    pub(super) tokens: PayoutTokens,
    pub(super) markets: BTreeMap<String, PayoutMarket>,
    token_markets: HashMap<String, String>,
    groups: HashSet<String>,
}

impl BoundPayouts {
    pub(super) fn read(connection: &Connection) -> Result<Self, BootstrapError> {
        let mut bound = Self {
            tokens: load_payout_tokens(connection)?,
            markets: BTreeMap::new(),
            token_markets: HashMap::new(),
            groups: HashSet::new(),
        };
        let mut statement = connection.prepare(
            "SELECT market_id, tokens_json, raw_page_sha256, neg_risk_market_id,
                    end_date_unix, payout_vector_json, payout_status
             FROM clob_payout_evidence_v2 ORDER BY market_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let market: String = row.get(0)?;
            let tokens: Vec<ClobToken> = serde_json::from_str(&row.get::<_, String>(1)?)?;
            let tokens: Vec<String> = tokens
                .into_iter()
                .map(|token| token.token_id.unwrap_or_default())
                .collect();
            for token in tokens.iter().filter(|token| !token.is_empty()) {
                bound.token_markets.insert(token.clone(), market.clone());
            }
            let group: Option<String> = row.get(3)?;
            if let Some(group) = &group {
                bound.groups.insert(group.clone());
            }
            let end: Option<i64> = row.get(4)?;
            let payout: Option<String> = row.get(5)?;
            bound.markets.insert(market, (group, end, payout));
        }
        Ok(bound)
    }
}

impl ScopeLookups for BoundPayouts {
    fn market(&self, condition_id: &str) -> MarketLookup {
        match self.markets.get(condition_id) {
            None => MarketLookup::Unknown,
            Some((Some(group), _, _)) => MarketLookup::Grouped(group.clone()),
            Some(_) => MarketLookup::Ungrouped,
        }
    }
    fn token_market(&self, token_id: &str) -> Option<String> {
        self.token_markets.get(token_id).cloned()
    }
    fn is_group(&self, id: &str) -> bool {
        self.groups.contains(id)
    }
}

pub(super) struct ClassifiedWallet {
    pub(super) certificate: incremental::HistoryCertificate,
    pub(super) rows: Vec<Value>,
    ignored: BTreeMap<String, u64>,
    causes: BTreeMap<String, u64>,
}

pub(super) fn classify_loaded_wallet(
    mut certificate: incremental::HistoryCertificate,
    aggregates: &[ActivityAggregate],
    payout: &BoundPayouts,
) -> Result<ClassifiedWallet, BootstrapError> {
    let wallet = WalletAddress::from_hex(&certificate.wallet_hex).map_err(|error| {
        BootstrapError::Invalid {
            message: format!(
                "invalid certified wallet {}: {error}",
                certificate.wallet_hex
            ),
        }
    })?;
    let stored: Vec<ScopeDrop> = serde_json::from_str(&certificate.scope_drops_json)?;
    let mut drops: BTreeMap<Scope, ScopeDrop> = stored
        .into_iter()
        .map(|drop| (drop.scope(), drop))
        .collect();
    let mut positions = HashMap::new();
    let mut history = HashSet::<String>::new();
    let mut rows = Vec::new();
    let mut ignored = BTreeMap::new();
    let quality = ReconstructionQuality::new(100).map_err(|error| BootstrapError::Invalid {
        message: format!("invalid bootstrap reconstruction quality: {error}"),
    })?;
    for aggregates in aggregates.chunk_by(|a, b| a.source_time == b.source_time) {
        let second = aggregates[0].source_time.0.unix_timestamp();
        let dropped = drops
            .iter()
            .filter(|(_, drop)| drop.dropped_at_unix <= second)
            .map(|(scope, _)| scope.clone())
            .collect();
        let certified_problem_second = drops.values().any(|drop| drop.dropped_at_unix == second);
        let records: Vec<_> = aggregates
            .iter()
            .map(|aggregate| SecondRecord {
                aggregate,
                mutation: verified_mutation(aggregate, &payout.tokens),
            })
            .collect();
        let keys = records
            .iter()
            .filter_map(|record| record.mutation.as_ref().ok())
            .flat_map(LedgerMutation::touched_keys)
            .collect::<HashSet<_>>();
        let mut ledger = PositionLedger::new();
        ledger.replace_wallet_snapshot(
            wallet,
            keys.iter()
                .filter_map(|key| positions.get(key).map(|state| (key.clone(), *state)))
                .collect(),
        );
        let classified = classify_scoped_historical_second(
            &ledger,
            wallet,
            &records,
            &dropped,
            certified_problem_second,
            quality,
            &|market: &MarketId| history.contains(&market.to_string()),
            payout,
        )
        .map_err(|error| BootstrapError::Invalid {
            message: format!(
                "scoped classifier failed for {} at {second}: {error}",
                certificate.wallet_hex
            ),
        })?;
        for problem in classified.problems {
            let drop = ScopeDrop {
                cause: problem.cause,
                dropped_at_unix: second,
                scope_id: problem.scope.id.clone(),
                scope_kind: problem.scope.kind,
            };
            // The shared owner chooses (trigger, cause); a certified cause wins a tied second.
            if drops
                .get(&problem.scope)
                .is_none_or(|previous| second < previous.dropped_at_unix)
            {
                drops.insert(problem.scope, drop);
            }
        }
        for (_, activity_type) in classified.ignored {
            let count = ignored
                .entry(activity_type.as_str().to_owned())
                .or_insert(0);
            *count = checked_activity_count(*count, 1)?;
        }
        for decision in &classified.decisions {
            if decision.entry != EntryClassification::Admitted
                || decision.action_order_dependent
                || decision.amount == ShareAmount::ZERO
                || !payout
                    .tokens
                    .get(&decision.market_id.to_string())
                    .is_some_and(|(_, _, eligible)| *eligible)
            {
                continue;
            }
            let aggregate = aggregates
                .iter()
                .find(|aggregate| aggregate.group_id.key() == &decision.source_trade_id)
                .ok_or(BootstrapError::Internal)?;
            let components = aggregate.group_id.components();
            let (_, end, vector) = payout
                .markets
                .get(&decision.market_id.to_string())
                .ok_or(BootstrapError::Internal)?;
            rows.push(serde_json::json!({
                "activity_generation": certificate.generation, "asset": components.asset.as_ref().map(ToString::to_string),
                "classifier_version": RANKER_CLASSIFIER_VERSION, "condition_id": decision.market_id.to_string(),
                "end_date_unix": end, "outcome_id": decision.outcome_id.0, "payout_vector_json": vector,
                "price_weighted_share_amount_str": aggregate.price_weighted_share_sum.0.to_string(),
                "share_amount_str": aggregate.share_sum.to_decimal().to_string(), "side": "buy",
                "source_time_unix": second, "source_trade_id": decision.source_trade_id.0,
                "source_usdc_amount_str": aggregate.source_usdc_sum.to_decimal().to_string(),
                "wallet_hex": certificate.wallet_hex,
            }));
        }
        ledger
            .apply_all_or_none(&classified.apply)
            .map_err(|error| BootstrapError::Invalid {
                message: format!(
                    "scoped classifier application failed for {} at {second}: {error}",
                    certificate.wallet_hex
                ),
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
    rows.sort_by(|a, b| {
        (
            a["source_time_unix"].as_i64(),
            a["source_trade_id"].as_str(),
        )
            .cmp(&(
                b["source_time_unix"].as_i64(),
                b["source_trade_id"].as_str(),
            ))
    });
    let mut causes = BTreeMap::new();
    for drop in drops.values() {
        let cause = serde_json::to_value(drop.cause)?
            .as_str()
            .ok_or(BootstrapError::Internal)?
            .to_owned();
        let count = causes.entry(cause).or_insert(0);
        *count = checked_activity_count(*count, 1)?;
    }
    certificate.scope_drops_json = canonical_json(&drops.into_values().collect::<Vec<_>>())?;
    Ok(ClassifiedWallet {
        certificate,
        rows,
        ignored,
        causes,
    })
}

pub(super) fn spool_path(cache_path: &Path) -> PathBuf {
    sidecar_path(cache_path, ".projection-v3.jsonl")
}

pub(super) fn verify_spool(
    cache_path: &Path,
    spool: &SpoolCommitment,
    count: u64,
) -> Result<(), BootstrapError> {
    let path = spool_path(cache_path);
    if path.file_name().and_then(|name| name.to_str()) != Some(&spool.path_name)
        || spool.lines != count
    {
        return invalid("projection spool path/count commitment mismatch".to_owned());
    }
    require_regular_file(&path, "committed projection spool")?;
    validate_hex_sha256(&spool.sha256, "projection spool sha256")?;
    if std::fs::metadata(&path)?.len() != spool.size_bytes || sha256_file(&path)? != spool.sha256 {
        return invalid("committed projection spool size/SHA-256 mismatch".to_owned());
    }
    Ok(())
}

fn remove_uncommitted_spools(path: &Path) -> Result<(), BootstrapError> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(BootstrapError::Internal)?;
    let prefix = format!(".{name}.");
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if entry.path() == path || (name.starts_with(&prefix) && name.ends_with(".tmp")) {
            require_regular_file(&entry.path(), "uncommitted projection spool")?;
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

pub(super) fn rebuild_ranker_projection(
    transaction: &rusqlite::Transaction<'_>,
    cache_path: &Path,
    identity: &FreshCollectionIdentity,
) -> Result<(u64, String, SpoolCommitment, ClassificationReport), BootstrapError> {
    let path = spool_path(cache_path);
    remove_uncommitted_spools(&path)?;
    let temp = atomic_write_temp_path(&path);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let mut writer = std::io::BufWriter::new(file);
    let payout = BoundPayouts::read(transaction)?;
    let proof = incremental::HistoryProof::load(transaction, identity)?;
    let mut report = ClassificationReport { neg_risk_markets_without_group_id: to_u64(transaction.query_row(
        "SELECT COUNT(*) FROM clob_payout_evidence_v2 WHERE neg_risk = 1 AND neg_risk_market_id IS NULL", [], |row| row.get(0),
    )?, "neg-risk markets without group")?, ..Default::default() };
    let mut digest = JsonArrayDigest::new();
    let mut hash = Sha256::new();
    let mut count = 0;
    let mut size = 0;
    thread::scope(|scope| {
        let (send_input, input) =
            sync_channel::<(incremental::HistoryCertificate, Vec<ActivityAggregate>)>(1);
        let (send_output, output) = sync_channel::<Result<ClassifiedWallet, BootstrapError>>(1);
        thread::Builder::new()
            .name("projection-classify".to_owned())
            .spawn_scoped(scope, move || {
                while let Ok((certificate, aggregates)) = input.recv() {
                    if send_output
                        .send(classify_loaded_wallet(certificate, &aggregates, &payout))
                        .is_err()
                    {
                        break;
                    }
                }
            })?;
        let mut write_wallet = || -> Result<(), BootstrapError> {
            let classified = output.recv().map_err(|_| BootstrapError::Internal)??;
            for row in classified.rows {
                let json = canonical_json(&row)?;
                digest.push_json(json.as_bytes());
                writer.write_all(json.as_bytes())?;
                writer.write_all(b"\n")?;
                hash.update(json.as_bytes());
                hash.update(b"\n");
                count = checked_activity_count(count, 1)?;
                size = checked_activity_count(
                    size,
                    u64::try_from(json.len()).map_err(|_| BootstrapError::Internal)? + 1,
                )?;
            }
            for (kind, added) in classified.ignored {
                let total = report.ignored_by_activity_type.entry(kind).or_insert(0);
                *total = checked_activity_count(*total, added)?;
            }
            for (cause, added) in classified.causes {
                let total = report.drops_by_cause.entry(cause).or_insert(0);
                *total = checked_activity_count(*total, added)?;
            }
            classified.certificate.write(transaction)
        };
        let pass = aggregate_scan::scoped(|scan| {
            let mut pending = false;
            for wallet in &identity.wallets {
                let receipt = incremental::receipt_for_identity(transaction, identity, wallet)?;
                if receipt.excluded() {
                    continue;
                }
                let old = incremental::HistoryCertificate::load(transaction, wallet)?;
                let chain = incremental::HistoryChain::load(
                    transaction,
                    wallet,
                    &proof,
                    identity.generation,
                )?;
                let mut check = chain.check();
                let mut aggregates = Vec::new();
                let held = scan
                    .for_each_history(transaction, wallet, |aggregate, json| {
                        if let Some(json) = json {
                            check.push(&aggregate, json)?;
                        }
                        aggregates.push(aggregate);
                        Ok(())
                    })
                    .map_err(|error| BootstrapError::Invalid {
                        message: format!("activity history decoding failed for {wallet}: {error}"),
                    })?;
                if let Some(error) = held {
                    return Err(error);
                }
                let certificate = check.finish(
                    identity.generation,
                    old.map_or_else(|| "[]".to_owned(), |old| old.scope_drops_json),
                )?;
                if pending {
                    write_wallet()?;
                }
                send_input
                    .send((certificate, aggregates))
                    .map_err(|_| BootstrapError::Internal)?;
                pending = true;
            }
            if pending {
                write_wallet()?;
            }
            Ok(())
        });
        drop(send_input);
        for _ in output {}
        pass
    })?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    std::fs::rename(&temp, &path).map_err(map_rename_error)?;
    sync_parent(&path)?;
    let spool = SpoolCommitment {
        path_name: path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(BootstrapError::Internal)?
            .to_owned(),
        size_bytes: size,
        lines: count,
        sha256: format!("{:x}", hash.finalize()),
    };
    Ok((count, digest.finish(), spool, report))
}

pub(super) fn read_export_manifest(
    connection: &Connection,
    path: &Path,
    inputs: &RankerProjectionInputs,
    count: u64,
    digest: &str,
) -> Result<(String, ExportProjectionSummary), BootstrapError> {
    require_regular_file(path, "export manifest")?;
    let bytes = std::fs::read(path)?;
    let manifest: ExportManifest = serde_json::from_slice(&bytes)?;
    let spool = inputs
        .projection_spool
        .as_ref()
        .ok_or(BootstrapError::Internal)?;
    let expected = ExportProjectionSummary {
        count,
        digest: digest.to_owned(),
        classifier_version: RANKER_CLASSIFIER_VERSION,
        oracle_version: 6,
        activity_generation: inputs.activity_generation,
        spool_sha256: spool.sha256.clone(),
    };
    if manifest.version != 3
        || manifest.activity_scope != "projection_spool"
        || manifest.projection != expected
        || manifest.tables.len() != 2
        || !manifest.tables.contains_key("clob_payout_evidence_v2")
        || manifest
            .tables
            .get("projection")
            .is_none_or(|table| table.count != count)
        || manifest.tables["clob_payout_evidence_v2"].count
            != to_u64(
                connection.query_row(
                    "SELECT COUNT(*) FROM clob_payout_evidence_v2",
                    [],
                    |row| row.get(0),
                )?,
                "payout evidence count",
            )?
    {
        return invalid("export manifest-v3 does not match finalized projection".to_owned());
    }
    for (name, table) in manifest.tables {
        validate_hex_sha256(&table.sha256, "export table sha256")?;
        let file = path
            .parent()
            .unwrap_or(Path::new("."))
            .join(format!("{name}.parquet"));
        require_regular_file(&file, "exported Parquet")?;
        if sha256_file(&file)? != table.sha256 {
            return invalid(format!("exported {name} Parquet SHA-256 mismatch"));
        }
    }
    Ok((sha256_bytes(&bytes), manifest.projection))
}

pub(super) fn verify_recorded_state(
    connection: &Connection,
    generation: ClassifierGeneration,
    projection: ProjectionDigest<'_>,
) -> Result<(), BootstrapError> {
    let (phase, count, digest, classifier): FinalizedProjectionState = connection.query_row(
        "SELECT phase, ranker_projection_count, ranker_projection_digest, ranker_classifier_version FROM cache_v2_migration_state WHERE singleton = 1", [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let inputs = recorded_inputs(connection)?;
    let identity = fresh_collection_record(connection)?.ok_or(BootstrapError::Internal)?;
    if identity.version != 4
        || inputs.activity_generation != identity.generation
        || inputs.payout_generation
            != to_u64(
                required_max(
                    connection,
                    "clob_payout_coverage_manifests_v2",
                    "generation",
                )?,
                "payout generation",
            )?
    {
        return invalid("finalized projection generations differ from recorded head".to_owned());
    }
    let count = to_u64(count.ok_or(BootstrapError::Internal)?, "projection count")?;
    let digest = digest.ok_or(BootstrapError::Internal)?;
    validate_hex_sha256(&digest, "projection digest")?;
    let spool = inputs.projection_spool.ok_or(BootstrapError::Internal)?;
    validate_hex_sha256(&spool.sha256, "projection spool sha256")?;
    if phase != "finalized"
        || classifier != Some(i64::from(RANKER_CLASSIFIER_VERSION))
        || inputs.oracle_version != Some(6)
        || spool.lines != count
    {
        return invalid(
            "installed cache omitted or changed its frozen/activity/ranker proof".to_owned(),
        );
    }
    if matches!(generation, ClassifierGeneration::Current) {
        let ProjectionDigest::Recorded(record) = projection else {
            return invalid(
                "history-format-three activation requires --final-stage-record with export summary"
                    .to_owned(),
            );
        };
        let expected = ExportProjectionSummary {
            count,
            digest,
            classifier_version: RANKER_CLASSIFIER_VERSION,
            oracle_version: 6,
            activity_generation: inputs.activity_generation,
            spool_sha256: spool.sha256,
        };
        if record.export_projection.as_ref() != Some(&expected)
            || record.export_manifest_sha256.is_none()
            || record.ranker_projection_count != count
            || record.ranker_projection_digest != expected.digest
            || record.activity_coverage_generation != inputs.activity_generation
            || record.ranker_classifier_version != RANKER_CLASSIFIER_VERSION
        {
            return invalid(
                "final-stage record export summary differs from finalized state".to_owned(),
            );
        }
        validate_hex_sha256(
            record
                .export_manifest_sha256
                .as_deref()
                .ok_or(BootstrapError::Internal)?,
            "export manifest sha256",
        )?;
    }
    Ok(())
}

pub(super) fn recorded_inputs(
    connection: &Connection,
) -> Result<RankerProjectionInputs, BootstrapError> {
    let json: String = connection.query_row(
        "SELECT ranker_projection_inputs_json FROM cache_v2_migration_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

pub(super) fn finalize(
    mut connection: Connection,
    cache_path: &Path,
    stage_record_path: Option<&Path>,
    export_manifest: Option<&Path>,
    finalized_at_unix: i64,
    sealed_generation: i64,
    payout_generation: i64,
) -> Result<Option<CacheFinalStageRecord>, BootstrapError> {
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let authorization = authorize_history_writes(&transaction)?;
    verify_history_certificates(&transaction)?;
    let identity = fresh_collection_record(&transaction)?.ok_or(BootstrapError::Internal)?;
    let manifest = completed_activity_manifest(
        &transaction,
        identity.generation,
        &identity.digest,
        identity.fixed_end_unix,
        &identity.wallets,
    )?
    .ok_or_else(|| BootstrapError::Invalid {
        message: "format-three finalize requires sealed activity coverage".to_owned(),
    })?;
    let phase: String = transaction.query_row(
        "SELECT phase FROM cache_v2_migration_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let mut inputs = RankerProjectionInputs::read(
        &transaction,
        &manifest,
        &activity_identity(&transaction)?,
        payout_generation,
    )?;
    let (count, digest, report) = if phase == "finalized" {
        let recorded = recorded_inputs(&transaction)?;
        inputs.projection_spool = recorded.projection_spool.clone();
        inputs.certificate_digest = recorded.certificate_digest.clone();
        if inputs != recorded {
            return invalid("finalized ranker projection input binding changed (activity identity/manifest or payout coverage/evidence)".to_owned());
        }
        let (count, digest, classifier): (i64, String, i64) = transaction.query_row(
            "SELECT ranker_projection_count, ranker_projection_digest, ranker_classifier_version FROM cache_v2_migration_state WHERE singleton = 1", [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if classifier != i64::from(RANKER_CLASSIFIER_VERSION) {
            return invalid("format-three finalized classifier is not current".to_owned());
        }
        let count = to_u64(count, "ranker projection count")?;
        verify_spool(
            cache_path,
            recorded
                .projection_spool
                .as_ref()
                .ok_or(BootstrapError::Internal)?,
            count,
        )?;
        incremental::verify_record_chain(&transaction, &identity)?;
        (count, digest, None)
    } else {
        let (count, digest, spool, report) =
            rebuild_ranker_projection(&transaction, cache_path, &identity)?;
        inputs.projection_spool = Some(spool);
        inputs.certificate_digest = Some(digests::certificate_digest(&transaction)?);
        transaction.execute("UPDATE cache_v2_migration_state SET phase = 'finalized', ranker_projection_count = ?1,
            ranker_projection_digest = ?2, ranker_classifier_version = ?3, ranker_projection_inputs_json = ?4,
            updated_at_unix = ?5 WHERE singleton = 1",
            params![to_i64(count, "ranker projection count")?, digest, i64::from(RANKER_CLASSIFIER_VERSION), canonical_json(&inputs)?, finalized_at_unix])?;
        tracing::info!(report = %canonical_json(&report)?, count, "classifier 6 verified pass finalized");
        (count, digest, Some(report))
    };
    let export = export_manifest
        .map(|path| read_export_manifest(&transaction, path, &inputs, count, &digest))
        .transpose()?;
    if stage_record_path.is_some() && export.is_none() {
        return invalid("history-format-three stage record requires --export-manifest".to_owned());
    }
    drop(authorization);
    transaction.commit()?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(cache_path)?;
    sync_file_and_parent(cache_path)?;
    let Some(stage_record_path) = stage_record_path else {
        return Ok(None);
    };
    let (export_sha256, export_projection) = export.ok_or(BootstrapError::Internal)?;
    let record = CacheFinalStageRecord {
        version: FINAL_STAGE_RECORD_VERSION,
        cache_path: std::fs::canonicalize(cache_path)?,
        cache_sha256: sha256_file(cache_path)?,
        schema_version: CACHE_SCHEMA_VERSION_V2,
        sealed_generation: to_u64(sealed_generation, "sealed generation")?,
        activity_coverage_generation: identity.generation,
        payout_coverage_generation: to_u64(payout_generation, "payout generation")?,
        ranker_projection_count: count,
        ranker_projection_digest: digest,
        ranker_classifier_version: RANKER_CLASSIFIER_VERSION,
        export_manifest_sha256: Some(export_sha256),
        export_projection: Some(export_projection),
        classification_report: report,
    };
    atomic_write_json(stage_record_path, &record)?;
    Ok(Some(record))
}
