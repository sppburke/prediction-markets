//! The verified history pass and its durable, flat projection commitment.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, SyncSender};

use super::*;
use pe_copy_signal_engine::PositionState;
use pe_position_ledger::{
    ACTIVITY_ROW_ANOMALY_NAMESPACE, DropCause, MarketLookup, Scope, ScopeKind, ScopeLookups,
    SecondRecord, activity_row_anomaly_trigger, classify_scoped_historical_second,
    classify_scoped_historical_second_with_anomalies,
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
    pub(super) cause: DropCause,
    pub(super) dropped_at_unix: i64,
    pub(super) scope_id: String,
    pub(super) scope_kind: ScopeKind,
}

impl ScopeDrop {
    pub(super) fn scope(&self) -> Scope {
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved_anomalies: Vec<UnresolvedAnomaly>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedAnomaly {
    pub wallet_hex: String,
    pub source_time_unix: i64,
    pub kind: pe_source_polymarket_public::ActivityRowAnomalyKind,
    pub field: String,
    pub condition_id: Option<pe_core_types::PolymarketConditionId>,
    pub asset: Option<pe_core_types::PolymarketTokenId>,
    pub raw_row_hash: String,
}

/// Merge sorted aggregate and anomaly streams without fabricating ledger rows.
/// Callers pass complete seconds, including the tail at a chunk boundary.
pub(super) fn historical_seconds<'a>(
    mut aggregates: &'a [ActivityAggregate],
    mut anomalies: &'a [pe_source_polymarket_public::ActivityRowAnomaly],
) -> impl Iterator<
    Item = (
        i64,
        &'a [ActivityAggregate],
        &'a [pe_source_polymarket_public::ActivityRowAnomaly],
    ),
> {
    std::iter::from_fn(move || {
        let at = aggregates
            .first()
            .map(|a| a.source_time.0.unix_timestamp())
            .into_iter()
            .chain(anomalies.first().map(|a| a.source_time.0.unix_timestamp()))
            .min()?;
        let n = aggregates.partition_point(|a| a.source_time.0.unix_timestamp() == at);
        let m = anomalies.partition_point(|a| a.source_time.0.unix_timestamp() == at);
        let (second, rest) = aggregates.split_at(n);
        let (problems, remaining) = anomalies.split_at(m);
        aggregates = rest;
        anomalies = remaining;
        Some((at, second, problems))
    })
}

const PROJECTION_CHUNK_AGGREGATES: usize = 100_000;

type PayoutMarket = (Option<String>, Option<i64>, Option<String>);

pub(super) struct BoundPayouts {
    pub(super) tokens: PayoutTokens,
    pub(super) markets: BTreeMap<String, PayoutMarket>,
    token_markets: HashMap<String, String>,
    groups: HashSet<String>,
    neg_risk_markets_without_group_id: u64,
}

impl BoundPayouts {
    pub(super) fn read(connection: &Connection) -> Result<Self, BootstrapError> {
        let mut bound = Self {
            tokens: BTreeMap::new(),
            markets: BTreeMap::new(),
            token_markets: HashMap::new(),
            groups: HashSet::new(),
            neg_risk_markets_without_group_id: 0,
        };
        let mut statement = connection.prepare(
            "SELECT market_id, tokens_json, raw_page_sha256, neg_risk_market_id,
                    end_date_unix, payout_vector_json,
                    CASE WHEN end_date_unix IS NOT NULL
                      AND payout_status = 'resolved'
                      AND payout_vector_json IN ('[\"1\",\"0\"]','[\"0\",\"1\"]','[\"0.5\",\"0.5\"]')
                    THEN 1 ELSE 0 END, neg_risk
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
            if group.is_none() && row.get::<_, Option<bool>>(7)? == Some(true) {
                bound.neg_risk_markets_without_group_id =
                    checked_activity_count(bound.neg_risk_markets_without_group_id, 1)?;
            }
            bound
                .tokens
                .insert(market.clone(), (tokens, row.get(2)?, row.get(6)?));
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
    pub(super) unresolved_anomalies: Vec<UnresolvedAnomaly>,
}

struct WalletClassifier {
    wallet: WalletAddress,
    wallet_hex: String,
    generation: u64,
    drops: BTreeMap<Scope, ScopeDrop>,
    /// The scopes whose drop is at or before the last second classified. A
    /// drop only moves earlier and seconds only advance, so a scope never
    /// leaves: this is every later second's dropped set, kept as drops arrive.
    dropped: std::collections::BTreeSet<Scope>,
    /// Stored drops not yet in `dropped`, latest first, so the next due is last.
    upcoming: Vec<(i64, Scope)>,
    positions: HashMap<MarketOutcomeId, PositionState>,
    history: HashSet<String>,
    ignored: BTreeMap<String, u64>,
    quality: ReconstructionQuality,
    classifier_version: u32,
    anomalies: std::collections::VecDeque<pe_source_polymarket_public::ActivityRowAnomaly>,
    unresolved_anomalies: Vec<UnresolvedAnomaly>,
}

impl WalletClassifier {
    fn new(
        wallet_hex: &str,
        generation: u64,
        scope_drops_json: &str,
        classifier_version: u32,
        anomalies: Vec<pe_source_polymarket_public::ActivityRowAnomaly>,
    ) -> Result<Self, BootstrapError> {
        let wallet =
            WalletAddress::from_hex(wallet_hex).map_err(|error| BootstrapError::Invalid {
                message: format!("invalid certified wallet {wallet_hex}: {error}"),
            })?;
        let stored: Vec<ScopeDrop> = serde_json::from_str(scope_drops_json)?;
        let quality = ReconstructionQuality::new(100).map_err(|error| BootstrapError::Invalid {
            message: format!("invalid bootstrap reconstruction quality: {error}"),
        })?;
        let drops: BTreeMap<Scope, ScopeDrop> = stored
            .into_iter()
            .map(|drop| (drop.scope(), drop))
            .collect();
        let mut upcoming: Vec<_> = drops
            .iter()
            .map(|(scope, drop)| (drop.dropped_at_unix, scope.clone()))
            .collect();
        upcoming.sort_unstable_by(|a, b| b.cmp(a));
        Ok(Self {
            wallet,
            wallet_hex: wallet_hex.to_owned(),
            generation,
            drops,
            dropped: std::collections::BTreeSet::new(),
            upcoming,
            positions: HashMap::new(),
            history: HashSet::new(),
            ignored: BTreeMap::new(),
            quality,
            classifier_version,
            anomalies: anomalies.into(),
            unresolved_anomalies: Vec::new(),
        })
    }

    /// Brings `dropped` up to `second`, a second later than every one before
    /// it, and tells whether a drop is at exactly `second`. That can only be a
    /// stored drop no problem has moved: recorded drops are all earlier.
    fn advance_to(&mut self, second: i64) -> Result<bool, BootstrapError> {
        let mut certified = false;
        while self.upcoming.last().is_some_and(|(at, _)| *at <= second) {
            let (at, scope) = self.upcoming.pop().ok_or(BootstrapError::Internal)?;
            certified |= at == second
                && self
                    .drops
                    .get(&scope)
                    .is_some_and(|drop| drop.dropped_at_unix == second);
            self.dropped.insert(scope);
        }
        Ok(certified)
    }

    /// Records a problem's drop at the current second unless the scope already
    /// has one at or before it. The shared owner chooses (trigger, cause); a
    /// certified cause wins a tied second.
    fn record_drop(&mut self, scope: Scope, drop: ScopeDrop) {
        if self
            .drops
            .get(&scope)
            .is_none_or(|previous| drop.dropped_at_unix < previous.dropped_at_unix)
        {
            self.dropped.insert(scope.clone());
            self.drops.insert(scope, drop);
        }
    }

    fn seconds(
        &mut self,
        aggregates: &[ActivityAggregate],
        payout: &BoundPayouts,
        last: bool,
    ) -> Result<Vec<Value>, BootstrapError> {
        let mut rows = Vec::new();
        let end = aggregates.last().map(|a| a.source_time.0.unix_timestamp());
        let mut anomalies = Vec::new();
        while self
            .anomalies
            .front()
            .is_some_and(|a| last || end.is_some_and(|end| a.source_time.0.unix_timestamp() <= end))
        {
            anomalies.push(self.anomalies.pop_front().ok_or(BootstrapError::Internal)?);
        }
        for (second, aggregates, anomalies) in historical_seconds(aggregates, &anomalies) {
            let certified_problem_second = self.advance_to(second)?;
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
                self.wallet,
                keys.iter()
                    .filter_map(|key| self.positions.get(key).map(|state| (key.clone(), *state)))
                    .collect(),
            );
            let classified = if self.classifier_version == 7 {
                classify_scoped_historical_second_with_anomalies(
                    &ledger,
                    self.wallet,
                    second,
                    &records,
                    anomalies,
                    &self.dropped,
                    certified_problem_second,
                    self.quality,
                    &|market: &MarketId| self.history.contains(&market.to_string()),
                    payout,
                )
            } else {
                classify_scoped_historical_second(
                    &ledger,
                    self.wallet,
                    &records,
                    &self.dropped,
                    certified_problem_second,
                    self.quality,
                    &|market: &MarketId| self.history.contains(&market.to_string()),
                    payout,
                )
            }
            .map_err(|error| BootstrapError::Invalid {
                message: format!(
                    "scoped classifier failed for {} at {second}: {error}",
                    self.wallet_hex
                ),
            })?;
            for problem in classified.problems {
                let drop = ScopeDrop {
                    cause: problem.cause,
                    dropped_at_unix: second,
                    scope_id: problem.scope.id.clone(),
                    scope_kind: problem.scope.kind,
                };
                self.record_drop(problem.scope, drop);
            }
            for (id, activity_type) in classified.ignored {
                if id.0.starts_with(ACTIVITY_ROW_ANOMALY_NAMESPACE) {
                    let anomaly = anomalies
                        .iter()
                        .find(|a| activity_row_anomaly_trigger(a) == id)
                        .ok_or(BootstrapError::Internal)?;
                    self.unresolved_anomalies.push(UnresolvedAnomaly {
                        wallet_hex: self.wallet_hex.clone(),
                        source_time_unix: second,
                        kind: anomaly.kind,
                        field: anomaly.field.clone(),
                        condition_id: anomaly.condition_id.clone(),
                        asset: anomaly.asset.clone(),
                        raw_row_hash: anomaly.raw_row_hash.clone(),
                    });
                }
                let count = self
                    .ignored
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
                "activity_generation": self.generation, "asset": components.asset.as_ref().map(ToString::to_string),
                "classifier_version": self.classifier_version, "condition_id": decision.market_id.to_string(),
                "end_date_unix": end, "outcome_id": decision.outcome_id.0, "payout_vector_json": vector,
                "price_weighted_share_amount_str": aggregate.price_weighted_share_sum.0.to_string(),
                "share_amount_str": aggregate.share_sum.to_decimal().to_string(), "side": "buy",
                "source_time_unix": second, "source_trade_id": decision.source_trade_id.0,
                "source_usdc_amount_str": aggregate.source_usdc_sum.to_decimal().to_string(),
                "wallet_hex": self.wallet_hex,
            }));
            }
            ledger
                .apply_all_or_none(&classified.apply)
                .map_err(|error| BootstrapError::Invalid {
                    message: format!(
                        "scoped classifier application failed for {} at {second}: {error}",
                        self.wallet_hex
                    ),
                })?;
            if let Some(snapshot) = ledger.position(&self.wallet) {
                self.positions.extend(
                    snapshot
                        .positions
                        .iter()
                        .map(|(key, state)| (key.clone(), *state)),
                );
            }
            self.history.extend(
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
        Ok(rows)
    }

    fn finish(
        self,
        mut certificate: incremental::HistoryCertificate,
    ) -> Result<ClassifiedWallet, BootstrapError> {
        let mut causes = BTreeMap::new();
        for drop in self.drops.values() {
            let cause = serde_json::to_value(drop.cause)?
                .as_str()
                .ok_or(BootstrapError::Internal)?
                .to_owned();
            let count = causes.entry(cause).or_insert(0);
            *count = checked_activity_count(*count, 1)?;
        }
        certificate.scope_drops_json =
            canonical_json(&self.drops.into_values().collect::<Vec<_>>())?;
        Ok(ClassifiedWallet {
            certificate,
            rows: Vec::new(),
            ignored: self.ignored,
            causes,
            unresolved_anomalies: self.unresolved_anomalies,
        })
    }
}

#[cfg(feature = "scenario")]
pub(super) fn classify_loaded_wallet(
    certificate: incremental::HistoryCertificate,
    aggregates: &[ActivityAggregate],
    payout: &BoundPayouts,
    classifier_version: u32,
    anomalies: Vec<pe_source_polymarket_public::ActivityRowAnomaly>,
) -> Result<ClassifiedWallet, BootstrapError> {
    let mut classifier = WalletClassifier::new(
        &certificate.wallet_hex,
        certificate.generation,
        &certificate.scope_drops_json,
        classifier_version,
        anomalies,
    )?;
    let rows = classifier.seconds(aggregates, payout, true)?;
    let mut classified = classifier.finish(certificate)?;
    classified.rows = rows;
    Ok(classified)
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

struct ClassificationChunk {
    classifier: Option<WalletClassifier>,
    aggregates: Vec<ActivityAggregate>,
    certificate: Option<incremental::HistoryCertificate>,
}

enum ClassifiedChunk {
    Rows(Vec<Value>),
    Finished(ClassifiedWallet),
}

type ClassifierOutput = Result<ClassifiedChunk, BootstrapError>;

/// A four-core host runs this many classifiers beside the activity scan's
/// reader and its three decode workers (on Forge on 11/3 the one classifier was
/// the verified pass's busiest thread in every sample, 64-98% busy).
const MAX_PROJECTION_CLASSIFIERS: usize = 2;

fn projection_classifiers() -> usize {
    classifiers_for_cores(thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get))
}

/// At most `MAX_PROJECTION_CLASSIFIERS`, two cores left for the reader and the
/// scan, and never none.
fn classifiers_for_cores(cores: usize) -> usize {
    MAX_PROJECTION_CLASSIFIERS
        .min(cores.saturating_sub(2))
        .max(1)
}

struct Classifier {
    input: SyncSender<ClassificationChunk>,
    output: Receiver<ClassifierOutput>,
}

/// Classifies different wallets on different threads and hands their rows back
/// in dispatch order, which is wallet order and then chunk order, so every
/// commitment sees the bytes one classifier produces. A wallet's chunks all go
/// to one classifier, which keeps the wallet between them. A classifier holds
/// at most one chunk whose output the caller has not received, so at most one
/// output per classifier waits unreceived, plus the one the caller holds while
/// it hands that classifier its next chunk.
struct Classifiers {
    workers: Vec<Classifier>,
    /// Classifiers holding a chunk whose output is not yet received, oldest
    /// first; each appears at most once.
    pending: VecDeque<usize>,
}

/// A classifier thread's body: it takes chunks from the first channel and returns their outputs
/// on the second.
type ClassifierBody =
    fn(&Receiver<ClassificationChunk>, &SyncSender<ClassifierOutput>, &BoundPayouts);

impl Classifiers {
    fn spawn<'scope>(
        scope: &'scope thread::Scope<'scope, '_>,
        count: usize,
        payout: &'scope BoundPayouts,
    ) -> Result<Self, BootstrapError> {
        Self::spawn_with(scope, count, payout, classify_chunks)
    }

    fn spawn_with<'scope>(
        scope: &'scope thread::Scope<'scope, '_>,
        count: usize,
        payout: &'scope BoundPayouts,
        body: ClassifierBody,
    ) -> Result<Self, BootstrapError> {
        if count == 0 {
            return invalid("projection needs at least one classifier".to_owned());
        }
        let workers = (0..count)
            .map(|index| {
                let (send_input, input) = sync_channel::<ClassificationChunk>(1);
                let (send_output, output) = sync_channel::<ClassifierOutput>(1);
                thread::Builder::new()
                    .name(format!("projection-classify-{index}"))
                    .spawn_scoped(scope, move || body(&input, &send_output, payout))?;
                Ok(Classifier {
                    input: send_input,
                    output,
                })
            })
            .collect::<Result<Vec<_>, BootstrapError>>()?;
        Ok(Self {
            workers,
            pending: VecDeque::new(),
        })
    }

    /// Hands `chunk` to `worker`. If the worker still holds a chunk, every
    /// chunk dispatched before that one is written first, then the worker's
    /// output is received; it is written once the worker holds the new chunk,
    /// so the worker classifies while it is written. An error returned here is
    /// the first failure among the dispatched chunks: if the send fails, the
    /// held output and every other pending chunk, all older, are written before
    /// the send's failure is reported.
    fn dispatch(
        &mut self,
        worker: usize,
        chunk: ClassificationChunk,
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        let previous = if self.pending.contains(&worker) {
            while self.pending.front() != Some(&worker) {
                self.write_oldest(write)?;
            }
            self.pending.pop_front();
            Some(self.receive(worker)?)
        } else {
            None
        };
        let sent = self
            .workers
            .get(worker)
            .ok_or(BootstrapError::Internal)
            .and_then(|classifier| {
                classifier
                    .input
                    .send(chunk)
                    .map_err(|_| BootstrapError::Internal)
            });
        if sent.is_ok() {
            self.pending.push_back(worker);
        }
        if let Some(previous) = previous {
            write(previous)?;
        }
        if sent.is_err() {
            self.finish(write)?;
        }
        sent
    }

    /// Writes every pending chunk's rows, in dispatch order.
    fn finish(
        &mut self,
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        while !self.pending.is_empty() {
            self.write_oldest(write)?;
        }
        Ok(())
    }

    /// The error to report for `error`, found by the caller while reading:
    /// every pending chunk was dispatched before it, so they are written first
    /// and the first of their failures, in classifying or in writing, is
    /// reported. Without an `active` transaction nothing pending can be written
    /// or committed any more: the loss is reported with `error` as its cause,
    /// ahead of any pending chunk's own failure, and the pending chunks are
    /// left unexamined to `close`.
    fn fail(
        &mut self,
        error: BootstrapError,
        active: bool,
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> BootstrapError {
        if !active {
            return transaction_lost(&error);
        }
        match self.finish(write) {
            Ok(()) => error,
            Err(earlier) => earlier,
        }
    }

    fn write_oldest(
        &mut self,
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        let worker = self.pending.pop_front().ok_or(BootstrapError::Internal)?;
        write(self.receive(worker)?)
    }

    fn receive(&self, worker: usize) -> ClassifierOutput {
        self.workers
            .get(worker)
            .ok_or(BootstrapError::Internal)?
            .output
            .recv()
            .map_err(|_| BootstrapError::Internal)?
    }

    /// Closing the inputs ends every classifier's loop; draining the outputs
    /// leaves none waiting on a send when the scope joins.
    fn close(self) {
        let (inputs, outputs): (Vec<_>, Vec<_>) = self
            .workers
            .into_iter()
            .map(|worker| (worker.input, worker.output))
            .unzip();
        drop(inputs);
        for output in outputs {
            for _ in output {}
        }
    }
}

/// SQLite rolls a transaction back by itself after some errors (an I/O error,
/// a full disk); a statement after that would commit on its own, outside the
/// finalize transaction.
fn transaction_lost(error: &BootstrapError) -> BootstrapError {
    BootstrapError::Invalid {
        message: format!("finalize transaction is no longer active after: {error}"),
    }
}

/// One classifier's loop. It keeps the wallet it was last given until that
/// wallet's final chunk, which carries the history certificate.
fn classify_chunks(
    input: &Receiver<ClassificationChunk>,
    output: &SyncSender<ClassifierOutput>,
    payout: &BoundPayouts,
) {
    let mut classifier = None;
    while let Ok(chunk) = input.recv() {
        if output
            .send(classify_chunk(&mut classifier, chunk, payout))
            .is_err()
        {
            break;
        }
    }
}

/// Classifies one chunk with the wallet's classifier, which the wallet's
/// first chunk brings and its last chunk, carrying the certificate, finishes.
fn classify_chunk(
    classifier: &mut Option<WalletClassifier>,
    input: ClassificationChunk,
    payout: &BoundPayouts,
) -> ClassifierOutput {
    if let Some(start) = input.classifier {
        *classifier = Some(start);
    }
    let rows = classifier
        .as_mut()
        .ok_or(BootstrapError::Internal)?
        .seconds(&input.aggregates, payout, input.certificate.is_some())?;
    drop(input.aggregates);
    match input.certificate {
        Some(certificate) => {
            let mut classified = classifier
                .take()
                .ok_or(BootstrapError::Internal)?
                .finish(certificate)?;
            classified.rows = rows;
            Ok(ClassifiedChunk::Finished(classified))
        }
        None => Ok(ClassifiedChunk::Rows(rows)),
    }
}

fn take_chunk(
    aggregates: &mut Vec<ActivityAggregate>,
    second_start: &mut usize,
    source_time: &SourceTimestamp,
    chunk: usize,
) -> Option<Vec<ActivityAggregate>> {
    if aggregates
        .last()
        .is_some_and(|last| last.source_time != *source_time)
    {
        if aggregates.len() >= chunk {
            *second_start = 0;
            return Some(std::mem::take(aggregates));
        }
        *second_start = aggregates.len();
    }
    // Cut before a second that would exceed the target; a large second then
    // grows alone until the next second arrives.
    if aggregates.len() == chunk && *second_start > 0 {
        let second = aggregates.split_off(*second_start);
        *second_start = 0;
        return Some(std::mem::replace(aggregates, second));
    }
    None
}

/// Writes classified chunks, in the order it is given them, to the projection
/// spool and its commitments, and each finished wallet's report counts,
/// certificate and carried-drop consumption to the finalize transaction.
struct ProjectionWriter<'a, 'c, W> {
    transaction: &'a rusqlite::Transaction<'c>,
    spool: W,
    carried: bool,
    digest: JsonArrayDigest,
    hash: Sha256,
    count: u64,
    size: u64,
    report: ClassificationReport,
}

impl<W: std::io::Write> ProjectionWriter<'_, '_, W> {
    fn write(&mut self, classified: ClassifiedChunk) -> Result<(), BootstrapError> {
        // Nothing may be written once SQLite has rolled the transaction back:
        // a later statement would commit on its own.
        if self.transaction.is_autocommit() {
            return invalid("finalize transaction is no longer active".to_owned());
        }
        let (rows, finished) = match classified {
            ClassifiedChunk::Rows(rows) => (rows, None),
            ClassifiedChunk::Finished(mut classified) => {
                (std::mem::take(&mut classified.rows), Some(classified))
            }
        };
        for row in rows {
            let json = canonical_json(&row)?;
            self.digest.push_json(json.as_bytes());
            self.spool.write_all(json.as_bytes())?;
            self.spool.write_all(b"\n")?;
            self.hash.update(json.as_bytes());
            self.hash.update(b"\n");
            self.count = checked_activity_count(self.count, 1)?;
            self.size = checked_activity_count(
                self.size,
                u64::try_from(json.len()).map_err(|_| BootstrapError::Internal)? + 1,
            )?;
        }
        let Some(classified) = finished else {
            return Ok(());
        };
        for (kind, added) in classified.ignored {
            let total = self
                .report
                .ignored_by_activity_type
                .entry(kind)
                .or_insert(0);
            *total = checked_activity_count(*total, added)?;
        }
        for (cause, added) in classified.causes {
            let total = self.report.drops_by_cause.entry(cause).or_insert(0);
            *total = checked_activity_count(*total, added)?;
        }
        self.report
            .unresolved_anomalies
            .extend(classified.unresolved_anomalies);
        classified.certificate.write(self.transaction)?;
        if self.carried {
            seed::consume_seed_drops(self.transaction, &classified.certificate.wallet_hex)?;
        }
        Ok(())
    }
}

/// One wallet's stored history as the activity scan delivers it: every row is
/// checked against the wallet's history chain, and every completed
/// whole-second chunk goes to the wallet's classifier.
struct WalletStream<'a, 'c, 'h> {
    transaction: &'a rusqlite::Transaction<'c>,
    check: incremental::HistoryCheck<'h>,
    classifier: Option<WalletClassifier>,
    worker: usize,
    chunk: usize,
    aggregates: Vec<ActivityAggregate>,
    second_start: usize,
    /// An earlier chunk's failure, met while handing over the next one. It is
    /// already first in dispatch order and is not this wallet's decoding failure.
    handed_over: Option<BootstrapError>,
}

impl WalletStream<'_, '_, '_> {
    fn row(
        &mut self,
        aggregate: ActivityAggregate,
        json: Option<&[u8]>,
        classifiers: &mut Classifiers,
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        // A failed read can roll the transaction back, and the scan still
        // delivers the rows read before it. Those are dropped, so nothing more
        // is handed over or written and the scan ends as it would on its own;
        // the loss is reported with the error it returns.
        if self.transaction.is_autocommit() {
            return Ok(());
        }
        if let Some(json) = json {
            self.check.push(&aggregate, json)?;
        }
        if let Some(complete) = take_chunk(
            &mut self.aggregates,
            &mut self.second_start,
            &aggregate.source_time,
            self.chunk,
        ) {
            let next = ClassificationChunk {
                classifier: self.classifier.take(),
                aggregates: complete,
                certificate: None,
            };
            if let Err(error) = classifiers.dispatch(self.worker, next, write) {
                self.handed_over = Some(error);
                // Stops the scan; `handed_over` is what is reported.
                return Err(BootstrapError::Internal);
            }
        }
        self.aggregates.push(aggregate);
        Ok(())
    }
}

pub(super) fn rebuild_ranker_projection(
    transaction: &rusqlite::Transaction<'_>,
    cache_path: &Path,
    identity: &FreshCollectionIdentity,
    proof: &incremental::HistoryProof,
    chunk: usize,
    classifier_count: usize,
) -> Result<(u64, String, SpoolCommitment, ClassificationReport), BootstrapError> {
    let started = Instant::now();
    let path = spool_path(cache_path);
    remove_uncommitted_spools(&path)?;
    let temp = atomic_write_temp_path(&path);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let payout = BoundPayouts::read(transaction)?;
    let mut projection = ProjectionWriter {
        transaction,
        spool: std::io::BufWriter::new(file),
        carried: seed::has_seed_drops(transaction)?,
        digest: JsonArrayDigest::new(),
        hash: Sha256::new(),
        count: 0,
        size: 0,
        report: ClassificationReport {
            neg_risk_markets_without_group_id: payout.neg_risk_markets_without_group_id,
            ..Default::default()
        },
    };
    thread::scope(|scope| {
        let mut classifiers = Classifiers::spawn(scope, classifier_count, &payout)?;
        let mut write = |classified| projection.write(classified);
        let pass = aggregate_scan::scoped(|scan| {
            let mut slot = 0_usize;
            for wallet in &identity.wallets {
                // A failure found while reading follows every pending chunk in
                // wallet order, so `fail` reports theirs first.
                let started = (|| -> Result<Option<_>, BootstrapError> {
                    let receipt = incremental::receipt_for_identity(transaction, identity, wallet)?;
                    if receipt.excluded() {
                        return Ok(None);
                    }
                    let drops = match incremental::HistoryCertificate::load(transaction, wallet)? {
                        Some(old) => old.scope_drops_json,
                        None => seed::seed_drops(transaction, wallet)?,
                    };
                    let classifier = WalletClassifier::new(
                        wallet,
                        identity.generation,
                        &drops,
                        identity.classifier_version()?,
                        if identity.version == 5 {
                            proof.effective_anomalies(transaction, wallet, identity.generation)?
                        } else {
                            Vec::new()
                        },
                    )?;
                    let chain = incremental::HistoryChain::load(
                        transaction,
                        wallet,
                        proof,
                        identity.generation,
                    )?;
                    Ok(Some((drops, classifier, chain)))
                })();
                let (drops, classifier, chain) = match started {
                    Ok(Some(started)) => started,
                    Ok(None) => continue,
                    Err(error) => {
                        return Err(classifiers.fail(
                            error,
                            !transaction.is_autocommit(),
                            &mut write,
                        ));
                    }
                };
                let worker = slot % classifier_count;
                slot += 1;
                let mut stream = WalletStream {
                    transaction,
                    check: chain.check(),
                    classifier: Some(classifier),
                    worker,
                    chunk,
                    aggregates: Vec::new(),
                    second_start: 0,
                    handed_over: None,
                };
                let scanned = scan.for_each_history(transaction, wallet, |aggregate, json| {
                    stream.row(aggregate, json, &mut classifiers, &mut write)
                });
                let WalletStream {
                    check,
                    classifier,
                    aggregates,
                    handed_over,
                    ..
                } = stream;
                if let Some(error) = handed_over {
                    return Err(error);
                }
                let held = match scanned {
                    Ok(held) => held,
                    Err(error) => {
                        let error = BootstrapError::Invalid {
                            message: format!(
                                "activity history decoding failed for {wallet}: {error}"
                            ),
                        };
                        return Err(classifiers.fail(
                            error,
                            !transaction.is_autocommit(),
                            &mut write,
                        ));
                    }
                };
                if let Some(error) = held {
                    return Err(classifiers.fail(error, !transaction.is_autocommit(), &mut write));
                }
                let certificate = match check.finish(identity.generation, drops) {
                    Ok(certificate) => certificate,
                    Err(error) => {
                        return Err(classifiers.fail(
                            error,
                            !transaction.is_autocommit(),
                            &mut write,
                        ));
                    }
                };
                let last = ClassificationChunk {
                    classifier,
                    aggregates,
                    certificate: Some(certificate),
                };
                classifiers.dispatch(worker, last, &mut write)?;
            }
            classifiers.finish(&mut write)
        });
        classifiers.close();
        pass
    })?;
    phase_completed("verified_pass", started);
    let started = Instant::now();
    let ProjectionWriter {
        spool: mut writer,
        digest,
        hash,
        count,
        size,
        report,
        ..
    } = projection;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    std::fs::rename(&temp, &path).map_err(map_rename_error)?;
    sync_parent(&path)?;
    phase_completed("spool_sync", started);
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
        classifier_version: fresh_collection_record(connection)?
            .ok_or(BootstrapError::Internal)?
            .classifier_version()?,
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
    if !identity.history_format_three()
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
    let classifier_version = identity.classifier_version()?;
    let count = to_u64(count.ok_or(BootstrapError::Internal)?, "projection count")?;
    let digest = digest.ok_or(BootstrapError::Internal)?;
    validate_hex_sha256(&digest, "projection digest")?;
    let spool = inputs.projection_spool.ok_or(BootstrapError::Internal)?;
    validate_hex_sha256(&spool.sha256, "projection spool sha256")?;
    if phase != "finalized"
        || classifier != Some(i64::from(classifier_version))
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
            classifier_version,
            oracle_version: 6,
            activity_generation: inputs.activity_generation,
            spool_sha256: spool.sha256,
        };
        if record.export_projection.as_ref() != Some(&expected)
            || record.export_manifest_sha256.is_none()
            || record.ranker_projection_count != count
            || record.ranker_projection_digest != expected.digest
            || record.activity_coverage_generation != inputs.activity_generation
            || record.ranker_classifier_version != classifier_version
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

fn phase_completed(phase: &'static str, started: Instant) {
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(phase, elapsed_ms, "cache finalize phase completed");
}

pub(super) fn open_finalize(cache_path: &Path) -> Result<(Connection, i64, i64), BootstrapError> {
    let started = Instant::now();
    let connection = open_existing_rw(cache_path)?;
    connection.pragma_update(None, "cache_size", -1_048_576_i64)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    ensure_lane_a_v2_schema(&connection)?;
    let sealed_generation = required_max(&connection, "sealed_generation_manifests", "generation")?;
    let payout_generation = required_max(
        &connection,
        "clob_payout_coverage_manifests_v2",
        "generation",
    )?;
    verify_payout_coverage(&connection, payout_generation)?;
    phase_completed("open_checks", started);
    Ok((connection, sealed_generation, payout_generation))
}

#[expect(
    clippy::too_many_arguments,
    reason = "finalization carries explicit artifact roles and the scenario boundary hook"
)]
pub(super) fn finalize(
    connection: Connection,
    cache_path: &Path,
    stage_record_path: Option<&Path>,
    export_manifest: Option<&Path>,
    ready_file: Option<&Path>,
    finalized_at_unix: i64,
    sealed_generation: i64,
    payout_generation: i64,
    ready_hook: Option<&mut dyn FnMut()>,
) -> Result<Option<CacheFinalStageRecord>, BootstrapError> {
    finalize_with_shape(
        connection,
        cache_path,
        stage_record_path,
        export_manifest,
        ready_file,
        finalized_at_unix,
        (sealed_generation, payout_generation),
        (PROJECTION_CHUNK_AGGREGATES, projection_classifiers()),
        ready_hook,
    )
    .map(|(record, _)| record)
}

#[cfg(feature = "scenario")]
pub fn finalize_cache_v2_with_projection_chunk_for_test(
    cache_path: &Path,
    finalized_at_unix: i64,
    chunk: usize,
) -> Result<Option<ClassificationReport>, BootstrapError> {
    finalize_cache_v2_with_projection_shape_for_test(
        cache_path,
        finalized_at_unix,
        chunk,
        projection_classifiers(),
    )
}

#[cfg(feature = "scenario")]
pub fn finalize_cache_v2_with_projection_shape_for_test(
    cache_path: &Path,
    finalized_at_unix: i64,
    chunk: usize,
    classifiers: usize,
) -> Result<Option<ClassificationReport>, BootstrapError> {
    if chunk == 0 {
        return invalid("projection chunk must contain at least one aggregate".to_owned());
    }
    // Refused here too, since a finalized cache reuses its spool without
    // starting any classifier.
    if classifiers == 0 {
        return invalid("projection needs at least one classifier".to_owned());
    }
    let (connection, sealed_generation, payout_generation) = open_finalize(cache_path)?;
    finalize_with_shape(
        connection,
        cache_path,
        None,
        None,
        None,
        finalized_at_unix,
        (sealed_generation, payout_generation),
        (chunk, classifiers),
        None,
    )
    .map(|(_, report)| report)
}

/// `shape` is the projection chunk target and the classifier count.
#[expect(
    clippy::too_many_arguments,
    reason = "finalization carries explicit artifact roles, projection shape and the scenario boundary hook"
)]
fn finalize_with_shape(
    mut connection: Connection,
    cache_path: &Path,
    stage_record_path: Option<&Path>,
    export_manifest: Option<&Path>,
    ready_file: Option<&Path>,
    finalized_at_unix: i64,
    generations: (i64, i64),
    shape: (usize, usize),
    ready_hook: Option<&mut dyn FnMut()>,
) -> Result<(Option<CacheFinalStageRecord>, Option<ClassificationReport>), BootstrapError> {
    let started = Instant::now();
    let (sealed_generation, payout_generation) = generations;
    let (chunk, classifiers) = shape;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let authorization = authorize_history_writes(&transaction)?;
    let identity = fresh_collection_record(&transaction)?.ok_or(BootstrapError::Internal)?;
    let classifier_version = identity.classifier_version()?;
    let proof = CollectionProof::load(&transaction, identity.generation)?
        .ok_or(BootstrapError::Internal)?;
    let manifest = completed_activity_manifest(
        &transaction,
        identity.generation,
        &identity.digest,
        identity.fixed_end_unix,
        &identity.wallets,
        Some(&proof),
    )?
    .ok_or_else(|| BootstrapError::Invalid {
        message: "format-three finalize requires sealed activity coverage".to_owned(),
    })?;
    phase_completed("history_preflight", started);
    let started = Instant::now();
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
        if classifier != i64::from(classifier_version) {
            return invalid("format-three finalized classifier is not current".to_owned());
        }
        let count = to_u64(count, "ranker projection count")?;
        phase_completed("input_binding", started);
        let started = Instant::now();
        verify_spool(
            cache_path,
            recorded
                .projection_spool
                .as_ref()
                .ok_or(BootstrapError::Internal)?,
            count,
        )?;
        phase_completed("spool_verification", started);
        (count, digest, None)
    } else {
        phase_completed("input_binding", started);
        let (count, digest, spool, report) = rebuild_ranker_projection(
            &transaction,
            cache_path,
            &identity,
            proof.history.as_deref().ok_or(BootstrapError::Internal)?,
            chunk,
            classifiers,
        )?;
        inputs.projection_spool = Some(spool);
        let started = Instant::now();
        inputs.certificate_digest = Some(digests::certificate_digest(&transaction)?);
        phase_completed("certificate_digest", started);
        transaction.execute("UPDATE cache_v2_migration_state SET phase = 'finalized', ranker_projection_count = ?1,
            ranker_projection_digest = ?2, ranker_classifier_version = ?3, ranker_projection_inputs_json = ?4,
            updated_at_unix = ?5 WHERE singleton = 1",
            params![to_i64(count, "ranker projection count")?, digest, i64::from(classifier_version), canonical_json(&inputs)?, finalized_at_unix])?;
        tracing::info!(report = %canonical_json(&report)?, count, classifier_version, "scoped classifier verified pass finalized");
        (count, digest, Some(report))
    };
    let started = Instant::now();
    let export = export_manifest
        .map(|path| read_export_manifest(&transaction, path, &inputs, count, &digest))
        .transpose()?;
    if export_manifest.is_some() {
        phase_completed("export_verification", started);
    }
    if stage_record_path.is_some() && export.is_none() {
        return invalid("history-format-three stage record requires --export-manifest".to_owned());
    }
    let started = Instant::now();
    drop(authorization);
    transaction.commit()?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(cache_path)?;
    sync_file_and_parent(cache_path)?;
    phase_completed("commit_checkpoint_sync", started);
    let Some(stage_record_path) = stage_record_path else {
        return Ok((None, report));
    };
    let (export_sha256, export_projection) = export.ok_or(BootstrapError::Internal)?;
    let started = Instant::now();
    let canonical_cache_path = std::fs::canonicalize(cache_path)?;
    write_ready_file(
        ready_file,
        &canonical_cache_path,
        stage_record_path,
        ready_hook,
    )?;
    let cache_sha256 = sha256_file(cache_path)?;
    phase_completed("cache_hash", started);
    let started = Instant::now();
    let record = CacheFinalStageRecord {
        version: FINAL_STAGE_RECORD_VERSION,
        cache_path: canonical_cache_path,
        cache_sha256,
        schema_version: CACHE_SCHEMA_VERSION_V2,
        sealed_generation: to_u64(sealed_generation, "sealed generation")?,
        activity_coverage_generation: identity.generation,
        payout_coverage_generation: to_u64(payout_generation, "payout generation")?,
        ranker_projection_count: count,
        ranker_projection_digest: digest,
        ranker_classifier_version: classifier_version,
        export_manifest_sha256: Some(export_sha256),
        export_projection: Some(export_projection),
        classification_report: report.clone(),
    };
    atomic_write_json(stage_record_path, &record)?;
    phase_completed("stage_record", started);
    Ok((Some(record), report))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "a broken fixture must fail its test")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use super::super::activity_fixtures::{built_aggregates, wallet_hex};
    use super::*;
    use pe_core_types::Side;

    #[test]
    fn projection_chunks_keep_whole_seconds_within_the_target_or_largest_second() {
        let wallet = wallet_hex(1);
        let seconds = [1, 2, 4, 513, 3, 2, 1];
        let mut aggregates = built_aggregates(&wallet, seconds.iter().sum());
        let mut index = 0;
        for (second, size) in seconds.iter().enumerate() {
            for aggregate in &mut aggregates[index..index + size] {
                aggregate.source_time = SourceTimestamp(
                    OffsetDateTime::from_unix_timestamp(
                        1_788_000_000 + i64::try_from(second).unwrap(),
                    )
                    .unwrap(),
                );
            }
            index += size;
        }
        for target in [1, 3, 100_000] {
            let mut pending = Vec::new();
            let mut second_start = 0;
            let mut chunks = Vec::new();
            for aggregate in &aggregates {
                if let Some(chunk) = take_chunk(
                    &mut pending,
                    &mut second_start,
                    &aggregate.source_time,
                    target,
                ) {
                    chunks.push(chunk);
                }
                pending.push(aggregate.clone());
            }
            chunks.push(pending);
            assert!(chunks.iter().all(|chunk| chunk.len() <= target.max(513)));
            assert!(
                chunks
                    .windows(2)
                    .all(|pair| pair[0].last().unwrap().source_time != pair[1][0].source_time)
            );
            assert_eq!(
                chunks.iter().flatten().collect::<Vec<_>>(),
                aggregates.iter().collect::<Vec<_>>()
            );
            if target < aggregates.len() {
                assert!(chunks.len() > 3);
                assert!(chunks.iter().any(|chunk| chunk.len() == 513));
            } else {
                assert_eq!(chunks.len(), 1);
            }
        }
    }

    #[test]
    fn wallet_classifier_returns_each_chunks_rows_and_none_at_finish() {
        let wallet = wallet_hex(1);
        let mut aggregates = built_aggregates(&wallet, 5);
        let mut payout = BoundPayouts {
            tokens: BTreeMap::new(),
            markets: BTreeMap::new(),
            token_markets: HashMap::new(),
            groups: HashSet::new(),
            neg_risk_markets_without_group_id: 0,
        };
        for (index, aggregate) in aggregates.iter_mut().enumerate() {
            let market = format!("0x{index:x}");
            let mut components = aggregate.group_id.components().clone();
            components.condition_id = Some(pe_core_types::PolymarketConditionId(market.clone()));
            components.side = Some(Side::Buy);
            aggregate.group_id = SourceActivityGroupId::derive(components).unwrap();
            payout.tokens.insert(
                market.clone(),
                (vec!["123".to_owned()], "a".repeat(64), true),
            );
            payout.markets.insert(
                market,
                (None, Some(1_900_000_000), Some("[\"1\",\"0\"]".to_owned())),
            );
        }
        let mut classifier = WalletClassifier::new(&wallet, 1, "[]", 6, Vec::new()).unwrap();
        for chunk in [&aggregates[..2], &aggregates[2..3], &aggregates[3..]] {
            let rows = classifier.seconds(chunk, &payout, false).unwrap();
            assert_eq!(rows.len(), chunk.len());
            for (row, aggregate) in rows.iter().zip(chunk) {
                assert_eq!(row["source_trade_id"], aggregate.group_id.key().0);
                assert_eq!(
                    row["source_time_unix"],
                    aggregate.source_time.0.unix_timestamp()
                );
            }
        }
        let certificate = incremental::HistoryCertificate {
            wallet_hex: wallet,
            generation: 1,
            newest_source_unix: aggregates
                .last()
                .map(|aggregate| aggregate.source_time.0.unix_timestamp()),
            newest_trade_unix: aggregates
                .last()
                .map(|aggregate| aggregate.source_time.0.unix_timestamp()),
            aggregate_count: 5,
            source_row_count: 5,
            ordered_digest: aggregate_digest(&aggregates).unwrap(),
            scope_drops_json: "[]".to_owned(),
        };
        let finished = classifier.finish(certificate).unwrap();
        assert!(finished.rows.is_empty());
        assert!(finished.ignored.is_empty());
        assert!(finished.causes.is_empty());
        assert_eq!(finished.certificate.aggregate_count, 5);
    }

    /// The rule `dropped` replaces: every scope whose drop is at or before
    /// `second`, and whether any drop is at exactly `second`.
    fn per_second_rule(
        drops: &BTreeMap<Scope, ScopeDrop>,
        second: i64,
    ) -> (std::collections::BTreeSet<Scope>, bool) {
        (
            drops
                .iter()
                .filter(|(_, drop)| drop.dropped_at_unix <= second)
                .map(|(scope, _)| scope.clone())
                .collect(),
            drops.values().any(|drop| drop.dropped_at_unix == second),
        )
    }

    fn market_drop(id: &str, cause: DropCause, at: i64) -> ScopeDrop {
        ScopeDrop {
            cause,
            dropped_at_unix: at,
            scope_id: id.to_owned(),
            scope_kind: ScopeKind::Market,
        }
    }

    fn market_scope(id: &str) -> Scope {
        Scope {
            kind: ScopeKind::Market,
            id: id.to_owned(),
        }
    }

    /// One classified second: the scopes dropped there, whether a drop is at
    /// exactly that second, and the problems' drops recorded in it.
    struct Step {
        second: i64,
        dropped: &'static [&'static str],
        tie: bool,
        recorded: &'static [(&'static str, DropCause)],
    }

    #[test]
    fn dropped_scopes_follow_stored_and_recorded_drops_second_by_second() {
        // Stored drops before the first second, exactly at one, between two,
        // moved earlier by a problem, and after the last, and one scope twice
        // (the later entry wins).
        let stored = [
            market_drop("0xa", DropCause::Underflow, 5),
            market_drop("0xb", DropCause::Underflow, 20),
            market_drop("0xc", DropCause::Underflow, 25),
            market_drop("0xd", DropCause::Underflow, 100),
            market_drop("0xe", DropCause::Underflow, 15),
            market_drop("0xe", DropCause::Overflow, 45),
            market_drop("0xff", DropCause::Underflow, 500),
        ];
        let mut classifier = WalletClassifier::new(
            &wallet_hex(1),
            1,
            &serde_json::to_string(&stored).unwrap(),
            7,
            Vec::new(),
        )
        .unwrap();
        const ALL: &[&str] = &["0xa", "0xb", "0xc", "0xd", "0xe", "0xf"];
        let steps = [
            Step {
                second: 10,
                dropped: &["0xa"],
                tie: false,
                recorded: &[],
            },
            // The stored drop at 20 is certified there and keeps its cause.
            Step {
                second: 20,
                dropped: &["0xa", "0xb"],
                tie: true,
                recorded: &[("0xb", DropCause::Conversion)],
            },
            Step {
                second: 30,
                dropped: &["0xa", "0xb", "0xc"],
                tie: false,
                recorded: &[("0xf", DropCause::Underflow)],
            },
            // 0xd's stored drop at 100 moves to 40.
            Step {
                second: 40,
                dropped: &["0xa", "0xb", "0xc", "0xf"],
                tie: false,
                recorded: &[("0xd", DropCause::Overflow)],
            },
            Step {
                second: 50,
                dropped: ALL,
                tie: false,
                recorded: &[],
            },
            // Nothing is left at 100 once 0xd's drop moved to 40.
            Step {
                second: 100,
                dropped: ALL,
                tie: false,
                recorded: &[],
            },
            Step {
                second: 110,
                dropped: ALL,
                tie: false,
                recorded: &[],
            },
        ];
        for step in steps {
            let second = step.second;
            let expected = per_second_rule(&classifier.drops, second);
            let certified = classifier.advance_to(second).unwrap();
            assert_eq!(
                (classifier.dropped.clone(), certified),
                expected,
                "second {second}"
            );
            assert_eq!(
                classifier.dropped,
                step.dropped
                    .iter()
                    .copied()
                    .map(market_scope)
                    .collect::<std::collections::BTreeSet<_>>(),
                "second {second}"
            );
            assert_eq!(certified, step.tie, "second {second}");
            for (id, cause) in step.recorded {
                classifier.record_drop(market_scope(id), market_drop(id, *cause, second));
            }
        }
        // The drop after the last second is still due, and is certified as stored.
        assert_eq!(classifier.upcoming, [(500, market_scope("0xff"))]);
        let drops: Vec<_> = classifier
            .drops
            .values()
            .map(|drop| (drop.scope_id.as_str(), drop.cause, drop.dropped_at_unix))
            .collect();
        assert_eq!(
            drops,
            [
                ("0xa", DropCause::Underflow, 5),
                ("0xb", DropCause::Underflow, 20),
                ("0xc", DropCause::Underflow, 25),
                ("0xd", DropCause::Overflow, 40),
                ("0xe", DropCause::Overflow, 45),
                ("0xf", DropCause::Underflow, 30),
                ("0xff", DropCause::Underflow, 500),
            ]
        );
    }

    #[test]
    fn dropped_scopes_match_the_per_second_rule_on_generated_histories() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let causes = [
            DropCause::Underflow,
            DropCause::Overflow,
            DropCause::Conversion,
        ];
        for case in 0..400 {
            let pool = 1 + next(12);
            let stored: Vec<_> = (0..next(10))
                .map(|_| {
                    market_drop(
                        &format!("0x{:x}", next(pool)),
                        causes[usize::try_from(next(3)).unwrap()],
                        i64::try_from(next(80)).unwrap(),
                    )
                })
                .collect();
            let mut classifier = WalletClassifier::new(
                &wallet_hex(1),
                1,
                &serde_json::to_string(&stored).unwrap(),
                7,
                Vec::new(),
            )
            .unwrap();
            let mut second = i64::try_from(next(5)).unwrap();
            for _ in 0..next(30) {
                second += 1 + i64::try_from(next(6)).unwrap();
                let expected = per_second_rule(&classifier.drops, second);
                let certified = classifier.advance_to(second).unwrap();
                assert_eq!(
                    (classifier.dropped.clone(), certified),
                    expected,
                    "case {case} second {second}"
                );
                for _ in 0..next(3) {
                    let id = format!("0x{:x}", next(pool));
                    let cause = causes[usize::try_from(next(3)).unwrap()];
                    classifier.record_drop(market_scope(&id), market_drop(&id, cause, second));
                }
            }
        }
    }

    fn empty_payouts() -> BoundPayouts {
        BoundPayouts {
            tokens: BTreeMap::new(),
            markets: BTreeMap::new(),
            token_markets: HashMap::new(),
            groups: HashSet::new(),
            neg_risk_markets_without_group_id: 0,
        }
    }

    /// `count` buys of `wallet` in stored order, each in its own market that
    /// `payout` pays out, so every aggregate classifies into exactly one row.
    fn paid_buys(wallet: &str, count: usize, payout: &mut BoundPayouts) -> Vec<ActivityAggregate> {
        let mut aggregates = built_aggregates(wallet, count);
        for (index, aggregate) in aggregates.iter_mut().enumerate() {
            let market = format!("0x{}{index:x}", &wallet[2..6]);
            let mut components = aggregate.group_id.components().clone();
            components.condition_id = Some(pe_core_types::PolymarketConditionId(market.clone()));
            components.side = Some(Side::Buy);
            aggregate.group_id = SourceActivityGroupId::derive(components).unwrap();
            payout.tokens.insert(
                market.clone(),
                (vec!["123".to_owned()], "a".repeat(64), true),
            );
            payout.markets.insert(
                market,
                (None, Some(1_900_000_000), Some("[\"1\",\"0\"]".to_owned())),
            );
        }
        // The new identities change the stored order within a second.
        aggregates.sort_by(|a, b| {
            (a.source_time.0, &a.group_id.key().0).cmp(&(b.source_time.0, &b.group_id.key().0))
        });
        aggregates
    }

    fn certificate(
        wallet: &str,
        aggregates: &[ActivityAggregate],
    ) -> incremental::HistoryCertificate {
        incremental::HistoryCertificate {
            wallet_hex: wallet.to_owned(),
            generation: 1,
            newest_source_unix: None,
            newest_trade_unix: None,
            aggregate_count: u64::try_from(aggregates.len()).unwrap(),
            source_row_count: u64::try_from(aggregates.len()).unwrap(),
            ordered_digest: aggregate_digest(aggregates).unwrap(),
            scope_drops_json: "[]".to_owned(),
        }
    }

    /// Dispatches each wallet's chunks as the verified pass does: the n-th
    /// wallet on classifier n mod `count`, its classifier with the first chunk
    /// and its certificate with the last.
    fn dispatch_wallets(
        classifiers: &mut Classifiers,
        count: usize,
        wallets: &[(String, Vec<Vec<ActivityAggregate>>)],
        write: &mut impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        for (slot, (wallet, chunks)) in wallets.iter().enumerate() {
            let mut classifier = Some(WalletClassifier::new(wallet, 1, "[]", 7, Vec::new())?);
            let all = chunks.concat();
            for (index, aggregates) in chunks.iter().enumerate() {
                let last = index + 1 == chunks.len();
                let chunk = ClassificationChunk {
                    classifier: classifier.take(),
                    aggregates: aggregates.clone(),
                    certificate: last.then(|| certificate(wallet, &all)),
                };
                classifiers.dispatch(slot % count, chunk, write)?;
            }
        }
        Ok(())
    }

    #[test]
    fn classifiers_write_rows_in_wallet_then_chunk_order_for_every_count() {
        let mut payout = empty_payouts();
        // Chunk counts per wallet: one, three, one, two, one, four.
        let shapes: [&[usize]; 6] = [&[2], &[1, 3, 2], &[1], &[2, 2], &[3], &[1, 1, 2, 1]];
        let wallets = shapes
            .iter()
            .enumerate()
            .map(|(index, sizes)| {
                let wallet = wallet_hex(u8::try_from(index + 1).unwrap());
                let mut buys = paid_buys(&wallet, sizes.iter().sum(), &mut payout).into_iter();
                let chunks = sizes
                    .iter()
                    .map(|size| buys.by_ref().take(*size).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                (wallet, chunks)
            })
            .collect::<Vec<_>>();
        let expected = wallets
            .iter()
            .flat_map(|(wallet, chunks)| {
                chunks
                    .iter()
                    .flatten()
                    .map(|aggregate| (wallet.clone(), aggregate.group_id.key().0.clone()))
            })
            .collect::<Vec<_>>();
        for count in [1, 2, 3] {
            let mut rows = Vec::new();
            let mut finished = Vec::new();
            let mut write = |classified: ClassifiedChunk| -> Result<(), BootstrapError> {
                let chunk_rows = match classified {
                    ClassifiedChunk::Rows(chunk_rows) => chunk_rows,
                    ClassifiedChunk::Finished(classified) => {
                        finished.push(classified.certificate.wallet_hex.clone());
                        classified.rows
                    }
                };
                rows.extend(chunk_rows.iter().map(|row| {
                    (
                        row["wallet_hex"].as_str().unwrap().to_owned(),
                        row["source_trade_id"].as_str().unwrap().to_owned(),
                    )
                }));
                Ok(())
            };
            thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, count, &payout).unwrap();
                dispatch_wallets(&mut classifiers, count, &wallets, &mut write).unwrap();
                classifiers.finish(&mut write).unwrap();
                classifiers.close();
            });
            assert_eq!(rows, expected, "{count} classifiers");
            assert_eq!(
                finished,
                wallets
                    .iter()
                    .map(|(wallet, _)| wallet.clone())
                    .collect::<Vec<_>>(),
                "{count} classifiers"
            );
        }
    }

    #[test]
    fn classifiers_report_the_first_failure_in_wallet_order() {
        let mut payout = empty_payouts();
        let first = wallet_hex(1);
        let second = wallet_hex(2);
        let first_buys = paid_buys(&first, 4, &mut payout);
        let second_buys = paid_buys(&second, 2, &mut payout);
        let later = || BootstrapError::Invalid {
            message: "later failure".to_owned(),
        };
        let whole = |wallet: &str, aggregates: &[ActivityAggregate]| ClassificationChunk {
            classifier: Some(WalletClassifier::new(wallet, 1, "[]", 7, Vec::new()).unwrap()),
            aggregates: aggregates.to_vec(),
            certificate: Some(certificate(wallet, aggregates)),
        };
        // A chunk without its wallet's classifier fails in the classifier.
        let broken =
            |wallet: &str, aggregates: &[ActivityAggregate], last: bool| ClassificationChunk {
                classifier: None,
                aggregates: aggregates.to_vec(),
                certificate: last.then(|| certificate(wallet, aggregates)),
            };
        for count in [1, 2, 3] {
            // An earlier wallet's classifier failure precedes a later wallet's
            // read failure.
            let mut written = 0;
            let mut write = |_: ClassifiedChunk| -> Result<(), BootstrapError> {
                written += 1;
                Ok(())
            };
            let error = thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, count, &payout).unwrap();
                let error = classifiers
                    .dispatch(0, broken(&first, &first_buys, true), &mut write)
                    .and_then(|()| {
                        classifiers.dispatch(1 % count, whole(&second, &second_buys), &mut write)
                    })
                    .map_or_else(
                        |error| error,
                        |()| classifiers.fail(later(), true, &mut write),
                    );
                classifiers.close();
                error
            });
            assert!(
                matches!(error, BootstrapError::Internal),
                "{count}: {error}"
            );
            assert_eq!(written, 0, "{count} classifiers");

            // Within one wallet, a pending chunk's failure precedes a read
            // failure later in that wallet's history.
            let error = thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, count, &payout).unwrap();
                let mut write = |_: ClassifiedChunk| -> Result<(), BootstrapError> { Ok(()) };
                let error = classifiers
                    .dispatch(0, broken(&first, &first_buys[..2], false), &mut write)
                    .map_or_else(
                        |error| error,
                        |()| classifiers.fail(later(), true, &mut write),
                    );
                classifiers.close();
                error
            });
            assert!(
                matches!(error, BootstrapError::Internal),
                "{count}: {error}"
            );

            // With no earlier failure, the pending rows are written, then the
            // reader's own failure is reported.
            let mut written = 0;
            let mut write = |_: ClassifiedChunk| -> Result<(), BootstrapError> {
                written += 1;
                Ok(())
            };
            let wallets = [
                (first.clone(), vec![first_buys.clone()]),
                (second.clone(), vec![second_buys.clone()]),
            ];
            let error = thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, count, &payout).unwrap();
                dispatch_wallets(&mut classifiers, count, &wallets, &mut write).unwrap();
                let error = classifiers.fail(later(), true, &mut write);
                classifiers.close();
                error
            });
            assert_eq!(
                error.to_string(),
                later().to_string(),
                "{count} classifiers"
            );
            assert_eq!(written, 2, "{count} classifiers");

            // An earlier wallet's write failure precedes a later read failure,
            // and nothing after it is written.
            let mut written = 0;
            let mut write = |_: ClassifiedChunk| -> Result<(), BootstrapError> {
                written += 1;
                Err(BootstrapError::Invalid {
                    message: "write failure".to_owned(),
                })
            };
            let error = thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, count, &payout).unwrap();
                let error = dispatch_wallets(&mut classifiers, count, &wallets, &mut write)
                    .map_or_else(
                        |error| error,
                        |()| classifiers.fail(later(), true, &mut write),
                    );
                classifiers.close();
                error
            });
            assert!(
                error.to_string().contains("write failure"),
                "{count}: {error}"
            );
            assert_eq!(written, 1, "{count} classifiers");
        }
    }

    /// Classifiers with no threads behind them. Each worker's output already
    /// holds its given result, if any, and its input is closed unless it is
    /// open; `pending` lists the workers holding a dispatched chunk, oldest first.
    fn detached(
        workers: Vec<(Option<ClassifierOutput>, bool)>,
        pending: &[usize],
    ) -> (Classifiers, Vec<Receiver<ClassificationChunk>>) {
        let mut open = Vec::new();
        let workers = workers
            .into_iter()
            .map(|(held, keep)| {
                let (input, receiver) = sync_channel::<ClassificationChunk>(1);
                let (sender, output) = sync_channel::<ClassifierOutput>(1);
                if let Some(held) = held {
                    sender.send(held).unwrap();
                }
                if keep {
                    open.push(receiver);
                }
                Classifier { input, output }
            })
            .collect();
        let classifiers = Classifiers {
            workers,
            pending: pending.iter().copied().collect(),
        };
        (classifiers, open)
    }

    fn marked(marker: &str) -> ClassifierOutput {
        Ok(ClassifiedChunk::Rows(vec![
            serde_json::json!({"marker": marker}),
        ]))
    }

    fn markers(
        written: &mut Vec<String>,
    ) -> impl FnMut(ClassifiedChunk) -> Result<(), BootstrapError> + '_ {
        |classified| {
            if let ClassifiedChunk::Rows(rows) = classified {
                written.extend(
                    rows.iter()
                        .map(|row| row["marker"].as_str().unwrap().to_owned()),
                );
            }
            Ok(())
        }
    }

    fn next_chunk() -> ClassificationChunk {
        ClassificationChunk {
            classifier: None,
            aggregates: Vec::new(),
            certificate: None,
        }
    }

    #[test]
    fn classifiers_settle_every_older_chunk_before_reporting_a_failed_send() {
        // Reused worker: worker 0 holds A, worker 1 holds B; worker 0's input
        // has closed. Handing worker 0 the next chunk writes A, then B, and only
        // then reports the failed send.
        let (mut classifiers, _open) = detached(
            vec![(Some(marked("A")), false), (Some(marked("B")), true)],
            &[0, 1],
        );
        let mut written = Vec::new();
        let error = classifiers
            .dispatch(0, next_chunk(), &mut markers(&mut written))
            .unwrap_err();
        assert!(matches!(error, BootstrapError::Internal), "{error}");
        assert_eq!(written, ["A", "B"]);
        assert!(classifiers.pending.is_empty());

        // The same, with B failed in its classifier: B's failure is the first.
        let failed = Err(BootstrapError::Invalid {
            message: "B failed".to_owned(),
        });
        let (mut classifiers, _open) = detached(
            vec![(Some(marked("A")), false), (Some(failed), true)],
            &[0, 1],
        );
        let mut written = Vec::new();
        let error = classifiers
            .dispatch(0, next_chunk(), &mut markers(&mut written))
            .unwrap_err();
        assert!(error.to_string().contains("B failed"), "{error}");
        assert_eq!(written, ["A"]);

        // Idle worker: worker 0 holds A; worker 1 has nothing and a closed
        // input. Its failed send is reported after A is written.
        let (mut classifiers, _open) =
            detached(vec![(Some(marked("A")), true), (None, false)], &[0]);
        let mut written = Vec::new();
        let error = classifiers
            .dispatch(1, next_chunk(), &mut markers(&mut written))
            .unwrap_err();
        assert!(matches!(error, BootstrapError::Internal), "{error}");
        assert_eq!(written, ["A"]);

        // The held output's own write failure precedes everything after it.
        let (mut classifiers, _open) = detached(
            vec![(Some(marked("A")), false), (Some(marked("B")), true)],
            &[0, 1],
        );
        let mut writes = 0;
        let error = classifiers
            .dispatch(0, next_chunk(), &mut |_| {
                writes += 1;
                Err(BootstrapError::Invalid {
                    message: "write failure".to_owned(),
                })
            })
            .unwrap_err();
        assert!(error.to_string().contains("write failure"), "{error}");
        assert_eq!(writes, 1);

        // A classifier that stopped without an output fails its pending chunk.
        let (mut classifiers, _open) = detached(vec![(None, true)], &[0]);
        let mut written = Vec::new();
        let later = BootstrapError::Invalid {
            message: "later failure".to_owned(),
        };
        let error = classifiers.fail(later, true, &mut markers(&mut written));
        assert!(matches!(error, BootstrapError::Internal), "{error}");
    }

    #[test]
    fn classifiers_report_a_lost_transaction_without_writing_anything_pending() {
        // Pending outputs that succeeded, failed in their classifier, or never
        // came: the loss is reported with its cause, and nothing is written.
        let failed = || {
            Some(Err(BootstrapError::Invalid {
                message: "B failed".to_owned(),
            }))
        };
        for workers in [
            vec![(Some(marked("A")), true), (Some(marked("B")), true)],
            vec![(failed(), true), (Some(marked("B")), true)],
            vec![(None, false), (Some(marked("B")), true)],
        ] {
            let (mut classifiers, _open) = detached(workers, &[0, 1]);
            let mut written = Vec::new();
            let read = BootstrapError::Invalid {
                message: "disk I/O error".to_owned(),
            };
            let error = classifiers.fail(read, false, &mut markers(&mut written));
            assert!(written.is_empty());
            let message = error.to_string();
            assert!(message.contains("no longer active"), "{message}");
            assert!(message.contains("disk I/O error"), "{message}");
        }
    }

    /// The activity scan over a stored history whose read fails, with the
    /// finalize transaction rolled back by the time rows are delivered, as
    /// SQLite does after an I/O error: every delivered row is dropped while
    /// other wallets' outputs are pending, nothing is written, and the loss is
    /// reported with exactly the error the scan returns. With a decoding failure
    /// in a batch it read ahead, that is the decoding failure (the scan's own
    /// precedence), not the failed read.
    #[test]
    fn a_rolled_back_scan_reports_the_loss_with_the_error_the_scan_returns() {
        use super::super::activity_fixtures::{GENERATION, seeded};
        let damage = |connection: &Connection, sql: &str, key: &str| {
            connection.execute(sql, [key]).unwrap();
        };
        for decoding_failure in [true, false] {
            let (mut connection, wallets) = seeded(&[2_000]);
            let wallet = wallets[0].clone();
            let mut keys = Vec::new();
            aggregate_scan::scoped(|scan| {
                scan.for_each(&connection, GENERATION, &wallet, |aggregate, _| {
                    keys.push(aggregate.group_id.key().0.clone());
                    Ok(())
                })
            })
            .unwrap();
            // A row of the third batch cannot be read; a decoding failure in the
            // second batch, which the scan has read ahead by then, precedes it.
            damage(
                &connection,
                "UPDATE activity_groups_v2 SET components_json = CAST(components_json AS BLOB)
                 WHERE source_trade_id = ?1",
                &keys[1_100],
            );
            if decoding_failure {
                damage(
                    &connection,
                    "UPDATE activity_groups_v2
                     SET components_json = json_set(components_json, '$.transaction_hash', 'changed')
                     WHERE source_trade_id = ?1",
                    &keys[600],
                );
            }
            let expected = aggregate_scan::scoped(|scan| {
                scan.for_each(&connection, GENERATION, &wallet, |_, _| Ok(()))
            })
            .unwrap_err()
            .to_string();
            assert_eq!(
                expected.contains("activity component identity mismatch"),
                decoding_failure,
                "{expected}"
            );
            let mut payout = empty_payouts();
            let (first, second) = (wallet_hex(1), wallet_hex(2));
            let first_buys = paid_buys(&first, 1, &mut payout);
            let second_buys = paid_buys(&second, 1, &mut payout);
            let chain = incremental::HistoryChain::without_parts_for_test(&wallet);
            let transaction = connection.transaction().unwrap();
            let message = thread::scope(|scope| {
                let mut classifiers = Classifiers::spawn(scope, 2, &payout).unwrap();
                let mut writes = 0;
                let mut write = |_: ClassifiedChunk| -> Result<(), BootstrapError> {
                    writes += 1;
                    Ok(())
                };
                // Pending: the first wallet's output succeeds and the second's
                // fails in its classifier (a chunk without its wallet's classifier).
                let first_chunk = ClassificationChunk {
                    classifier: Some(
                        WalletClassifier::new(&first, 1, "[]", 7, Vec::new()).unwrap(),
                    ),
                    aggregates: first_buys.clone(),
                    certificate: Some(certificate(&first, &first_buys)),
                };
                let second_chunk = ClassificationChunk {
                    classifier: None,
                    aggregates: second_buys.clone(),
                    certificate: Some(certificate(&second, &second_buys)),
                };
                classifiers.dispatch(0, first_chunk, &mut write).unwrap();
                classifiers.dispatch(1, second_chunk, &mut write).unwrap();
                // Target 1: every delivered row would complete a chunk for the
                // classifier holding the first wallet.
                let mut stream = WalletStream {
                    transaction: &transaction,
                    check: chain.check(),
                    classifier: Some(
                        WalletClassifier::new(&wallet, 1, "[]", 7, Vec::new()).unwrap(),
                    ),
                    worker: 0,
                    chunk: 1,
                    aggregates: Vec::new(),
                    second_start: 0,
                    handed_over: None,
                };
                let mut rolled_back = false;
                let scanned = aggregate_scan::scoped(|scan| {
                    scan.for_each(&transaction, GENERATION, &wallet, |aggregate, json| {
                        if !rolled_back {
                            transaction.execute_batch("ROLLBACK")?;
                            rolled_back = true;
                        }
                        stream.row(aggregate, json, &mut classifiers, &mut write)
                    })
                });
                let error = BootstrapError::Invalid {
                    message: format!(
                        "activity history decoding failed for {wallet}: {}",
                        scanned.unwrap_err()
                    ),
                };
                assert!(rolled_back);
                assert!(stream.handed_over.is_none());
                assert_eq!(classifiers.pending, [0, 1]);
                let reported = classifiers.fail(error, !transaction.is_autocommit(), &mut write);
                assert_eq!(writes, 0);
                classifiers.close();
                reported.to_string()
            });
            assert!(message.contains("no longer active"), "{message}");
            assert!(
                message.contains(&format!(
                    "activity history decoding failed for {wallet}: {expected}"
                )),
                "{message}"
            );
        }
    }

    /// One finished wallet whose certificate the writer records.
    fn finished(wallet: &str) -> ClassifiedChunk {
        ClassifiedChunk::Finished(ClassifiedWallet {
            certificate: certificate(wallet, &[]),
            rows: vec![serde_json::json!({"wallet_hex": wallet})],
            ignored: BTreeMap::new(),
            causes: BTreeMap::new(),
            unresolved_anomalies: Vec::new(),
        })
    }

    #[test]
    fn projection_writer_writes_nothing_once_sqlite_rolled_the_transaction_back() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("cache.db");
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(&format!(
                "{HISTORY_V3_SCHEMA};
                 CREATE TABLE activity_seed_drops_v3 (wallet_hex TEXT PRIMARY KEY, scope_drops_json TEXT NOT NULL);
                 INSERT INTO activity_seed_drops_v3 VALUES ('{first}', '[]'), ('{second}', '[]');",
                first = wallet_hex(1),
                second = wallet_hex(2),
            ))
            .unwrap();
        let transaction = connection.transaction().unwrap();
        let mut projection = ProjectionWriter {
            transaction: &transaction,
            spool: Vec::new(),
            carried: true,
            digest: JsonArrayDigest::new(),
            hash: Sha256::new(),
            count: 0,
            size: 0,
            report: ClassificationReport::default(),
        };
        projection.write(finished(&wallet_hex(1))).unwrap();
        assert_eq!(projection.count, 1);
        // What SQLite does by itself after an I/O error or a full disk.
        transaction.execute_batch("ROLLBACK").unwrap();
        let error = projection.write(finished(&wallet_hex(2))).unwrap_err();
        assert!(error.to_string().contains("no longer active"), "{error}");
        assert_eq!(projection.count, 1);
        drop(projection);
        drop(transaction);
        drop(connection);
        let connection = Connection::open(&path).unwrap();
        let certificates: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM activity_wallet_history_v3",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let carried: i64 = connection
            .query_row("SELECT COUNT(*) FROM activity_seed_drops_v3", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!((certificates, carried), (0, 2));
    }

    static PRODUCED: AtomicUsize = AtomicUsize::new(0);

    /// `classify_chunks`, counting every output it produces.
    fn counted(
        input: &Receiver<ClassificationChunk>,
        output: &SyncSender<ClassifierOutput>,
        payout: &BoundPayouts,
    ) {
        let mut classifier = None;
        while let Ok(chunk) = input.recv() {
            let classified = classify_chunk(&mut classifier, chunk, payout);
            PRODUCED.fetch_add(1, AtomicOrdering::SeqCst);
            if output.send(classified).is_err() {
                break;
            }
        }
    }

    #[test]
    fn classifiers_hold_one_output_more_than_classifiers_while_the_writer_waits() {
        PRODUCED.store(0, AtomicOrdering::SeqCst);
        let mut payout = empty_payouts();
        let wallets = (1..=3)
            .map(|index| {
                let wallet = wallet_hex(index);
                let buys = paid_buys(&wallet, 2, &mut payout);
                (wallet, vec![buys])
            })
            .collect::<Vec<_>>();
        let mut order = Vec::new();
        let mut reached = false;
        let mut write = |classified: ClassifiedChunk| -> Result<(), BootstrapError> {
            if order.is_empty() {
                // The first wallet's output is held while its classifier works
                // on the third wallet and the other on the second: two waiting
                // outputs beside the held one.
                let deadline = Instant::now() + Duration::from_secs(30);
                while PRODUCED.load(AtomicOrdering::SeqCst) < 3 && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(1));
                }
                reached = PRODUCED.load(AtomicOrdering::SeqCst) == 3;
            }
            if let ClassifiedChunk::Finished(classified) = classified {
                order.push(classified.certificate.wallet_hex);
            }
            Ok(())
        };
        thread::scope(|scope| {
            let mut classifiers = Classifiers::spawn_with(scope, 2, &payout, counted).unwrap();
            dispatch_wallets(&mut classifiers, 2, &wallets, &mut write).unwrap();
            assert!(classifiers.pending.len() <= 2);
            classifiers.finish(&mut write).unwrap();
            classifiers.close();
        });
        assert!(reached, "three outputs coexisted unwritten");
        assert_eq!(PRODUCED.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(
            order,
            wallets
                .iter()
                .map(|(wallet, _)| wallet.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn classifier_count_leaves_two_cores_and_never_falls_to_zero() {
        for (cores, classifiers) in [(0, 1), (1, 1), (2, 1), (3, 1), (4, 2), (8, 2), (64, 2)] {
            assert_eq!(classifiers_for_cores(cores), classifiers, "{cores} cores");
        }
        let payout = empty_payouts();
        thread::scope(|scope| {
            let error = Classifiers::spawn(scope, 0, &payout).err().unwrap();
            assert!(
                error.to_string().contains("at least one classifier"),
                "{error}"
            );
        });
    }
}
