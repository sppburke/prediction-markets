//! Acquisition and atomic carry-forward for complete activity generations.

use super::*;

#[derive(Debug, Clone)]
pub(super) struct HistoryCertificate {
    pub(super) wallet_hex: String,
    pub(super) generation: u64,
    pub(super) newest_source_unix: Option<i64>,
    pub(super) newest_trade_unix: Option<i64>,
    pub(super) aggregate_count: u64,
    pub(super) source_row_count: u64,
    pub(super) ordered_digest: String,
    pub(super) scope_drops_json: String,
}

impl HistoryCertificate {
    pub(super) fn load(
        connection: &Connection,
        wallet: &str,
    ) -> Result<Option<Self>, BootstrapError> {
        let mut statement = connection.prepare_cached(
            "SELECT generation, newest_source_unix, newest_trade_unix, aggregate_count,
                    source_row_count, ordered_digest, scope_drops_json
             FROM activity_wallet_history_v3 WHERE wallet_hex = ?1",
        )?;
        let mut rows = statement.query([wallet])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let certificate = Self {
            wallet_hex: wallet.to_owned(),
            generation: to_u64(row.get(0)?, "certificate generation")?,
            newest_source_unix: row.get(1)?,
            newest_trade_unix: row.get(2)?,
            aggregate_count: to_u64(row.get(3)?, "certificate aggregate count")?,
            source_row_count: to_u64(row.get(4)?, "certificate source count")?,
            ordered_digest: row.get(5)?,
            scope_drops_json: row.get(6)?,
        };
        validate_hex_sha256(&certificate.ordered_digest, "certified wallet digest")?;
        Ok(Some(certificate))
    }

    pub(super) fn write(
        &self,
        transaction: &rusqlite::Transaction<'_>,
    ) -> Result<(), BootstrapError> {
        transaction.execute(
            "INSERT INTO activity_wallet_history_v3
             (wallet_hex, generation, newest_source_unix, newest_trade_unix, aggregate_count,
              source_row_count, ordered_digest, scope_drops_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(wallet_hex) DO UPDATE SET generation = excluded.generation,
             newest_source_unix = excluded.newest_source_unix, newest_trade_unix = excluded.newest_trade_unix,
             aggregate_count = excluded.aggregate_count, source_row_count = excluded.source_row_count,
             ordered_digest = excluded.ordered_digest, scope_drops_json = excluded.scope_drops_json",
            params![self.wallet_hex, to_i64(self.generation, "certificate generation")?, self.newest_source_unix,
                self.newest_trade_unix, to_i64(self.aggregate_count, "certificate aggregates")?,
                to_i64(self.source_row_count, "certificate source rows")?, self.ordered_digest, self.scope_drops_json],
        )?;
        Ok(())
    }
}

#[derive(Clone)]
struct HistoryPart {
    start: i64,
    end: i64,
    digest: String,
    count: u64,
    source_rows: u64,
}

/// One owner for effective history. Exclusions remain audit records; only a
/// complete full read or an explicit repair supersedes earlier commitments.
pub(super) struct HistoryChain {
    wallet: String,
    parts: Vec<HistoryPart>,
}

impl HistoryChain {
    pub(super) fn load(
        connection: &Connection,
        wallet: &str,
        proof: &HistoryProof,
        through_generation: u64,
    ) -> Result<Self, BootstrapError> {
        let certificate = HistoryCertificate::load(connection, wallet)?;
        let mut chain = Self {
            wallet: wallet.to_owned(),
            parts: Vec::new(),
        };
        let mut certified_generation = 0;
        if let Some(certificate) = certificate {
            if certificate.generation > through_generation {
                return invalid(format!(
                    "history certificate is ahead of receipt chain for {wallet}"
                ));
            }
            certified_generation = certificate.generation;
            let identity =
                proof
                    .identity(certificate.generation)
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: format!("certified history identity missing for {wallet}"),
                    })?;
            chain.parts.push(HistoryPart {
                start: 0,
                end: identity.fixed_end_unix,
                digest: certificate.ordered_digest,
                count: certificate.aggregate_count,
                source_rows: certificate.source_row_count,
            });
        }
        for (_, (identity, manifest)) in proof.records.range((
            std::ops::Bound::Excluded(certified_generation),
            std::ops::Bound::Included(through_generation),
        )) {
            if identity
                .wallets
                .binary_search_by(|w| w.as_str().cmp(wallet))
                .is_err()
            {
                continue;
            }
            let receipt = match manifest {
                Some(manifest) => predecessor_receipt(connection, manifest, identity, wallet)?,
                None => receipt_for_identity(connection, identity, wallet)?,
            };
            let repair = identity.repair_wallets.as_ref().is_some_and(|repairs| {
                repairs.binary_search_by(|w| w.as_str().cmp(wallet)).is_ok()
            });
            if repair && receipt.excluded() {
                chain.parts.clear();
                chain.parts.push(HistoryPart {
                    start: 0,
                    end: identity.fixed_end_unix,
                    digest: aggregate_digest(&[])?,
                    count: 0,
                    source_rows: 0,
                });
            } else if !receipt.excluded() {
                let acquisition =
                    receipt
                        .acquisition
                        .as_ref()
                        .ok_or_else(|| BootstrapError::Invalid {
                            message: format!("receipt chain acquisition missing for {wallet}"),
                        })?;
                if acquisition.mode == ActivityReadMode::Full {
                    chain.parts.clear();
                } else if chain.parts.is_empty() {
                    return invalid(format!(
                        "incremental receipt chain has no history proof for {wallet}"
                    ));
                }
                let (digest, count, source_rows) = if acquisition.version == 3 {
                    (
                        acquisition
                            .fetched_aggregate_digest
                            .clone()
                            .ok_or(BootstrapError::Internal)?,
                        acquisition
                            .fetched_aggregate_count
                            .ok_or(BootstrapError::Internal)?,
                        acquisition.fetched_source_row_count,
                    )
                } else if acquisition.mode == ActivityReadMode::Full {
                    (
                        receipt.ordered_aggregate_digest,
                        receipt.aggregate_count,
                        receipt.source_row_count,
                    )
                } else {
                    return invalid(format!(
                        "uncertified legacy incremental history for {wallet}"
                    ));
                };
                if chain
                    .parts
                    .last()
                    .is_some_and(|part| part.end > acquisition.start_exclusive)
                {
                    return invalid(format!("overlapping receipt chain for {wallet}"));
                }
                chain.parts.push(HistoryPart {
                    start: acquisition.start_exclusive,
                    end: acquisition.fixed_end_unix,
                    digest,
                    count,
                    source_rows,
                });
            }
        }
        Ok(chain)
    }

    pub(super) fn has_history_proof(&self) -> bool {
        !self.parts.is_empty()
    }

    pub(super) fn check(&self) -> HistoryCheck<'_> {
        HistoryCheck {
            chain: self,
            observed: self
                .parts
                .iter()
                .map(|_| (JsonArrayDigest::new(), 0, 0))
                .collect(),
            outside: false,
            whole: JsonArrayDigest::new(),
            count: 0,
            source_rows: 0,
            newest_source_unix: None,
            newest_trade_unix: None,
        }
    }

    fn matches_fetched(&self, aggregates: &[ActivityAggregate]) -> Result<bool, BootstrapError> {
        let mut check = self.check();
        for aggregate in aggregates {
            check.push(aggregate, canonical_json(aggregate)?.as_bytes())?;
        }
        Ok(check.matches())
    }

    pub(super) fn verify_stored(
        &self,
        scan: &mut aggregate_scan::Scan,
        connection: &Connection,
        rows_verified: &mut u64,
    ) -> Result<HistoryCertificate, BootstrapError> {
        let mut check = self.check();
        let held = scan
            .for_each_history(connection, &self.wallet, |aggregate, json| {
                *rows_verified = checked_activity_count(*rows_verified, 1)?;
                if let Some(json) = json {
                    check.push(&aggregate, json)?;
                }
                Ok(())
            })
            .map_err(|error| BootstrapError::Invalid {
                message: format!(
                    "activity history decoding failed for {}: {error}",
                    self.wallet
                ),
            })?;
        if let Some(error) = held {
            return invalid(format!(
                "activity history serialization failed for {}: {error}",
                self.wallet
            ));
        }
        check.finish(0, "[]".to_owned())
    }
}

/// Feed the finalize pass's already-decoded rows here, then certify the whole
/// only after every prefix and fetched partition reproduces its commitment.
pub(super) struct HistoryCheck<'chain> {
    chain: &'chain HistoryChain,
    observed: Vec<(JsonArrayDigest, u64, u64)>,
    outside: bool,
    whole: JsonArrayDigest,
    count: u64,
    source_rows: u64,
    newest_source_unix: Option<i64>,
    newest_trade_unix: Option<i64>,
}

impl HistoryCheck<'_> {
    pub(super) fn push(
        &mut self,
        aggregate: &ActivityAggregate,
        json: &[u8],
    ) -> Result<(), BootstrapError> {
        let time = aggregate.source_time.0.unix_timestamp();
        let part = self
            .chain
            .parts
            .iter()
            .position(|part| time > part.start && time <= part.end);
        if let Some(index) = part {
            let (digest, count, source_rows) = &mut self.observed[index];
            digest.push_json(json);
            *count = checked_activity_count(*count, 1)?;
            *source_rows = checked_activity_count(*source_rows, aggregate.row_count)?;
        } else {
            self.outside = true;
        }
        self.whole.push_json(json);
        self.count = checked_activity_count(self.count, 1)?;
        self.source_rows = checked_activity_count(self.source_rows, aggregate.row_count)?;
        self.newest_source_unix = self.newest_source_unix.max(Some(time));
        if aggregate.group_id.components().activity_type == ActivityType::Trade {
            self.newest_trade_unix = self.newest_trade_unix.max(Some(time));
        }
        Ok(())
    }

    fn matches(self) -> bool {
        !self.outside
            && self.observed.into_iter().zip(&self.chain.parts).all(
                |((digest, count, source), expected)| {
                    digest.finish() == expected.digest
                        && count == expected.count
                        && source == expected.source_rows
                },
            )
    }

    pub(super) fn finish(
        self,
        generation: u64,
        scope_drops_json: String,
    ) -> Result<HistoryCertificate, BootstrapError> {
        let certificate = HistoryCertificate {
            wallet_hex: self.chain.wallet.clone(),
            generation,
            newest_source_unix: self.newest_source_unix,
            newest_trade_unix: self.newest_trade_unix,
            aggregate_count: self.count,
            source_row_count: self.source_rows,
            ordered_digest: self.whole.finish(),
            scope_drops_json,
        };
        let mut mismatches = Vec::new();
        for ((digest, count, source), expected) in self.observed.into_iter().zip(&self.chain.parts)
        {
            let observed = digest.finish();
            if observed != expected.digest
                || count != expected.count
                || source != expected.source_rows
            {
                mismatches.push(format!(
                    "({},{}]: expected {} ({},{}), observed {observed} ({count},{source})",
                    expected.start,
                    expected.end,
                    expected.digest,
                    expected.count,
                    expected.source_rows
                ));
            }
        }
        if self.outside || !mismatches.is_empty() {
            return invalid(format!(
                "activity history chain mismatch for {}: outside={}, {}",
                self.chain.wallet,
                self.outside,
                mismatches.join("; ")
            ));
        }
        Ok(certificate)
    }
}

pub(super) fn receipt_for_identity(
    connection: &Connection,
    identity: &FreshCollectionIdentity,
    wallet: &str,
) -> Result<ActivityWalletReceiptProof, BootstrapError> {
    let mut statement = connection.prepare_cached(
        "SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
         ordered_aggregate_digest, source_row_count, aggregate_count, schema_version,
         parser_version, acquisition_json, exclusion_reason
         FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1 AND wallet_hex = ?2",
    )?;
    let mut rows = statement.query(params![
        to_i64(identity.generation, "receipt generation")?,
        wallet
    ])?;
    decode_receipt(
        rows.next()?.ok_or(BootstrapError::Internal)?,
        &identity.digest,
        identity.fixed_end_unix,
        identity.version,
    )
}

pub(super) fn receipt_marker_v2() -> Value {
    serde_json::json!({"receipt_storage":"activity_wallet_coverage_staging_v2","version":2})
}

pub(super) fn has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, BootstrapError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
        params![table, column],
        |row| row.get(0),
    )?)
}

pub(super) fn generation_identity(
    connection: &Connection,
    generation: u64,
) -> Result<Option<FreshCollectionIdentity>, BootstrapError> {
    if let Some(record) = fresh_collection_record(connection)?
        && record.generation == generation
    {
        return Ok(Some(record));
    }
    if !has_column(
        connection,
        "activity_coverage_manifests_v2",
        "collection_identity_json",
    )? {
        return Ok(None);
    }
    let json: Option<String> = connection.query_row(
        "SELECT collection_identity_json FROM activity_coverage_manifests_v2 WHERE generation = ?1",
        params![to_i64(generation, "activity generation")?], |row| row.get(0)).optional()?.flatten();
    let record = json.map(|json| decode_fresh_identity(&json)).transpose()?;
    if record
        .as_ref()
        .is_some_and(|record| record.generation != generation)
    {
        return invalid("archived collection identity generation mismatch".to_owned());
    }
    Ok(record)
}

pub(super) fn manifest_link(
    manifest: &ActivityCoverageManifestV2,
    identity: &FreshCollectionIdentity,
) -> Result<String, BootstrapError> {
    Ok(sha256_bytes(
        canonical_json(&serde_json::json!({
            "version":1, "manifest":manifest, "collection_identity":identity,
        }))?
        .as_bytes(),
    ))
}

pub(super) fn archive_identity(
    connection: &Connection,
    generation: u64,
    identity: &FreshCollectionIdentity,
) -> Result<(), BootstrapError> {
    let stored: Option<String> = connection.query_row(
        "SELECT collection_identity_json FROM activity_coverage_manifests_v2 WHERE generation = ?1",
        params![to_i64(generation, "activity generation")?],
        |row| row.get(0),
    )?;
    if let Some(stored) = stored {
        if decode_fresh_identity(&stored)? != *identity {
            return invalid("completed collection identity changed".to_owned());
        }
    } else {
        connection.execute("UPDATE activity_coverage_manifests_v2 SET collection_identity_json = ?2 WHERE generation = ?1",
            params![to_i64(generation, "activity generation")?, canonical_json(identity)?])?;
    }
    Ok(())
}

pub(super) fn record_completed_manifest(
    transaction: &rusqlite::Transaction<'_>,
    manifest: &ActivityCoverageManifestV2,
) -> Result<(), BootstrapError> {
    if let Some(stored) = stored_activity_manifest(transaction, manifest.generation)? {
        if stored != *manifest {
            return invalid("conflicting completed activity manifest".to_owned());
        }
    } else {
        transaction.execute(
            "INSERT INTO activity_coverage_manifests_v2
             (generation, reference_sha256, wallet_count, receipt_set_digest, aggregate_digest,
              source_row_count, source_bounds_json, cursors_json, page_hashes_json, group_count,
              schema_version, parser_version, completed_at_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                to_i64(manifest.generation, "activity generation")?,
                manifest.reference_sha256,
                to_i64(manifest.wallet_count, "wallet count")?,
                manifest.receipt_set_digest,
                manifest.aggregate_digest,
                to_i64(manifest.source_row_count, "source rows")?,
                canonical_json(&manifest.source_bounds)?,
                canonical_json(&manifest.cursors)?,
                canonical_json(&manifest.page_hashes)?,
                to_i64(manifest.group_count, "group count")?,
                i64::from(manifest.schema_version),
                i64::from(manifest.parser_version),
                manifest.completed_at_unix
            ],
        )?;
    }
    if let Some(identity) = fresh_collection_record(transaction)? {
        if identity.generation != manifest.generation
            || identity.digest != manifest.reference_sha256
        {
            return invalid("completed manifest does not match current collection".to_owned());
        }
        archive_identity(transaction, manifest.generation, &identity)?;
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct CollectionProof {
    pub(super) identity: FreshCollectionIdentity,
    pub(super) bulk_root: bool,
    base: Option<(ActivityCoverageManifestV2, FreshCollectionIdentity)>,
    base_link: Option<String>,
    pub(super) history: Option<Arc<HistoryProof>>,
    data_version: i64,
}

impl CollectionProof {
    pub(super) fn load(
        connection: &Connection,
        generation: u64,
    ) -> Result<Option<Self>, BootstrapError> {
        // The collector loads outside a transaction. Bracket every proof read
        // so an external commit cannot become the baseline for stale fields.
        let data_version: i64 =
            connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
        let Some(identity) = generation_identity(connection, generation)? else {
            return Ok(None);
        };
        if identity.version == 1 {
            reject_bulk_root(connection)?;
            return Ok(None);
        }
        let bulk_root = connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?
            == CACHE_SCHEMA_VERSION_BULK_ROOT;
        let history = if identity.version == 4 {
            verify_history_certificates(connection)?;
            Some(Arc::new(HistoryProof::load(connection, &identity)?))
        } else {
            None
        };
        if bulk_root {
            require_bulk_root_state(connection, &identity)?;
        }
        let base = if let Some(base) = identity.base_generation {
            let record =
                generation_identity(connection, base)?.ok_or_else(|| BootstrapError::Invalid {
                    message: "predecessor collection identity is missing".to_owned(),
                })?;
            let manifest = stored_activity_manifest(connection, base)?.ok_or_else(|| {
                BootstrapError::Invalid {
                    message: "predecessor manifest is missing".to_owned(),
                }
            })?;
            if record.digest != manifest.reference_sha256
                || Some(record.fixed_end_unix) != identity.start_exclusive
                || Some(manifest_link(&manifest, &record)?) != identity.base_manifest_sha256
            {
                return invalid("predecessor manifest commitment mismatch".to_owned());
            }
            let head: Option<i64> = connection.query_row(
                "SELECT MAX(generation) FROM activity_coverage_manifests_v2 WHERE generation < ?1",
                params![to_i64(generation, "activity generation")?],
                |row| row.get(0),
            )?;
            if head != Some(to_i64(base, "base generation")?) {
                return invalid(
                    "incremental predecessor is not the preceding completed head".to_owned(),
                );
            }
            if history.is_none() {
                verify_historical_receipts(connection, &manifest, &record)?;
            }
            Some((manifest, record))
        } else {
            None
        };
        let base_link = base
            .as_ref()
            .map(|(manifest, identity)| manifest_link(manifest, identity))
            .transpose()?;
        let after: i64 = connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
        if after != data_version {
            return invalid("collection changed externally while loading proof".to_owned());
        }
        Ok(Some(Self {
            identity,
            bulk_root,
            base,
            base_link,
            history,
            data_version,
        }))
    }

    pub(super) fn verify_unchanged(&self, connection: &Connection) -> Result<(), BootstrapError> {
        let current: i64 = connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
        if current != self.data_version {
            return invalid("collection changed externally while loading proof".to_owned());
        }
        Ok(())
    }

    pub(super) fn mode(&self, wallet: &str) -> ActivityReadMode {
        if self
            .identity
            .full_read_wallets
            .as_ref()
            .is_some_and(|full| full.binary_search_by(|w| w.as_str().cmp(wallet)).is_ok())
            || self.deferred(wallet)
        {
            ActivityReadMode::Full
        } else {
            ActivityReadMode::Incremental
        }
    }

    pub(super) fn deferred(&self, wallet: &str) -> bool {
        self.identity
            .deferred_wallets
            .as_ref()
            .is_some_and(|deferred| {
                deferred
                    .binary_search_by(|w| w.as_str().cmp(wallet))
                    .is_ok()
            })
    }

    pub(super) fn start(&self, wallet: &str) -> i64 {
        if self.mode(wallet) == ActivityReadMode::Full {
            0
        } else {
            self.identity.start_exclusive.unwrap_or(0)
        }
    }

    fn predecessor(
        &self,
        connection: &Connection,
        wallet: &str,
        carried: bool,
    ) -> Result<Option<ActivityPredecessor>, BootstrapError> {
        if self.identity.version == 4 {
            return self.certified_predecessor(connection, wallet, carried);
        }
        let Some((manifest, identity)) = &self.base else {
            return Ok(None);
        };
        if identity
            .wallets
            .binary_search_by(|w| w.as_str().cmp(wallet))
            .is_err()
        {
            return Ok(None);
        }
        let receipt = predecessor_receipt(connection, manifest, identity, wallet)?;
        if carried && receipt.excluded() {
            return invalid("excluded predecessor cannot be carried".to_owned());
        }
        Ok(Some(ActivityPredecessor {
            generation: identity.generation,
            manifest_sha256: self.base_link.clone().ok_or(BootstrapError::Internal)?,
            wallet_receipt_sha256: wallet_receipt_digest(identity, &receipt)?,
            ordered_aggregate_digest: receipt.ordered_aggregate_digest,
            aggregate_count: receipt.aggregate_count,
            source_row_count: receipt.source_row_count,
            carried,
        }))
    }

    fn certified_predecessor(
        &self,
        connection: &Connection,
        wallet: &str,
        carried: bool,
    ) -> Result<Option<ActivityPredecessor>, BootstrapError> {
        let Some(base_generation) = self.identity.base_generation else {
            return Ok(None);
        };
        let history = self.history.as_ref().ok_or(BootstrapError::Internal)?;
        let certified = HistoryCertificate::load(connection, wallet)?;
        let snapshot = if let Some(certificate) = certified {
            Some((
                certificate.generation,
                certificate.ordered_digest,
                certificate.aggregate_count,
                certificate.source_row_count,
            ))
        } else {
            let mut snapshot = None;
            for (&generation, (identity, manifest)) in
                history.records.range(..=base_generation).rev()
            {
                if identity
                    .wallets
                    .binary_search_by(|w| w.as_str().cmp(wallet))
                    .is_err()
                {
                    continue;
                }
                let receipt = match manifest {
                    Some(manifest) => predecessor_receipt(connection, manifest, identity, wallet)?,
                    None => receipt_for_identity(connection, identity, wallet)?,
                };
                let repaired = identity.repair_wallets.as_ref().is_some_and(|repairs| {
                    repairs.binary_search_by(|w| w.as_str().cmp(wallet)).is_ok()
                });
                if repaired
                    || (!receipt.excluded()
                        && receipt
                            .acquisition
                            .as_ref()
                            .is_some_and(|acquisition| acquisition.mode == ActivityReadMode::Full))
                {
                    snapshot = Some((
                        generation,
                        receipt.ordered_aggregate_digest,
                        receipt.aggregate_count,
                        receipt.source_row_count,
                    ));
                    break;
                }
            }
            snapshot
        };
        let Some((generation, digest, count, source_rows)) = snapshot else {
            return Ok(None);
        };
        let identity = history
            .identity(generation)
            .ok_or(BootstrapError::Internal)?;
        let manifest = history
            .manifest(generation)
            .ok_or(BootstrapError::Internal)?;
        let receipt = predecessor_receipt(connection, manifest, identity, wallet)?;
        Ok(Some(ActivityPredecessor {
            generation,
            manifest_sha256: history
                .link(generation)
                .ok_or(BootstrapError::Internal)?
                .to_owned(),
            wallet_receipt_sha256: wallet_receipt_digest(identity, &receipt)?,
            ordered_aggregate_digest: digest,
            aggregate_count: count,
            source_row_count: source_rows,
            carried,
        }))
    }

    pub(super) fn validate_receipt(
        &self,
        connection: &Connection,
        receipt: &ActivityWalletReceiptProof,
    ) -> Result<(), BootstrapError> {
        let acquisition = receipt
            .acquisition
            .as_ref()
            .ok_or_else(|| BootstrapError::Invalid {
                message: "version-two activity receipt omitted acquisition proof".to_owned(),
            })?;
        let complete = acquisition.disposition == ActivityDisposition::Complete;
        let deferred = self.deferred(&receipt.wallet_hex);
        // A frozen deferral is exactly an unattempted read with its own reason; the
        // not-attempted arm below refuses page evidence.
        if deferred
            != (acquisition.exclusion_reason == Some(ActivityExclusionReason::DormantDeferred))
            || (deferred && acquisition.aggregation_status != AggregationStatus::NotAttempted)
        {
            return invalid("activity deferral disagrees with frozen identity".to_owned());
        }
        let carried = complete && acquisition.mode == ActivityReadMode::Incremental;
        if acquisition.version != if self.identity.version == 4 { 3 } else { 2 }
            || acquisition.mode != self.mode(&receipt.wallet_hex)
            || acquisition.start_exclusive != self.start(&receipt.wallet_hex)
            || acquisition.fixed_end_unix != self.identity.fixed_end_unix
            || (self.identity.version != 4
                && acquisition.predecessor
                    != self.predecessor(connection, &receipt.wallet_hex, carried)?)
        {
            return invalid(format!(
                "activity acquisition identity mismatch for {}",
                receipt.wallet_hex
            ));
        }
        if self.identity.version == 4 {
            // Authenticate the immutable snapshot's links, never regenerate it
            // from a certificate finalization may already have advanced.
            if let Some(base) = &acquisition.predecessor {
                let history = self.history.as_ref().ok_or(BootstrapError::Internal)?;
                let identity = history
                    .identity(base.generation)
                    .ok_or(BootstrapError::Internal)?;
                let manifest = history
                    .manifest(base.generation)
                    .ok_or(BootstrapError::Internal)?;
                let prior =
                    predecessor_receipt(connection, manifest, identity, &receipt.wallet_hex)?;
                if Some(base.generation) > self.identity.base_generation
                    || base.carried != carried
                    || Some(base.manifest_sha256.as_str()) != history.link(base.generation)
                    || base.wallet_receipt_sha256 != wallet_receipt_digest(identity, &prior)?
                {
                    return invalid(format!(
                        "activity predecessor snapshot mismatch for {}",
                        receipt.wallet_hex
                    ));
                }
                validate_hex_sha256(&base.ordered_aggregate_digest, "predecessor history digest")?;
            }
        }
        if acquisition.mode == ActivityReadMode::Incremental && acquisition.predecessor.is_none() {
            return invalid("incremental wallet has no predecessor receipt".to_owned());
        }
        let rows = if acquisition.aggregation_status == AggregationStatus::NotAttempted {
            if !receipt.pages.is_empty() {
                return invalid("failed acquisition cannot claim page evidence".to_owned());
            }
            0
        } else {
            validate_pages(
                &receipt.wallet_hex,
                &receipt.pages,
                acquisition.start_exclusive,
                acquisition.fixed_end_unix,
            )?
        };
        if rows != acquisition.fetched_source_row_count
            || read_digest(&receipt.wallet_hex, &receipt.pages, acquisition)?
                != acquisition.read_sha256
        {
            return invalid("activity read commitment mismatch".to_owned());
        }
        let fetched = match acquisition.aggregation_status {
            AggregationStatus::Complete => {
                let digest = acquisition
                    .fetched_aggregate_digest
                    .as_ref()
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "fetched digest missing".to_owned(),
                    })?;
                validate_hex_sha256(digest, "fetched aggregate digest")?;
                let count =
                    acquisition
                        .fetched_aggregate_count
                        .ok_or_else(|| BootstrapError::Invalid {
                            message: "fetched count missing".to_owned(),
                        })?;
                if count > rows
                    || (count == 0) != (rows == 0)
                    || (count == 0 && *digest != aggregate_digest(&[])?)
                {
                    return invalid("invalid fetched aggregate count shape".to_owned());
                }
                count
            }
            AggregationStatus::NotAttempted => {
                if complete
                    || acquisition.fetched_aggregate_count.is_some()
                    || acquisition.fetched_aggregate_digest.is_some()
                    || acquisition.exclusion_reason
                        != Some(if deferred {
                            ActivityExclusionReason::DormantDeferred
                        } else {
                            ActivityExclusionReason::AcquisitionFailure
                        })
                    || receipt
                        .exclusion_reason
                        .as_deref()
                        .is_none_or(str::is_empty)
                {
                    return invalid("invalid failed acquisition proof".to_owned());
                }
                0
            }
            AggregationStatus::Failed => {
                if complete
                    || acquisition.fetched_aggregate_count.is_some()
                    || acquisition.fetched_aggregate_digest.is_some()
                    || acquisition.exclusion_reason
                        != Some(ActivityExclusionReason::AggregationFailure)
                    || rows == 0
                {
                    return invalid("invalid failed aggregation proof".to_owned());
                }
                0
            }
        };
        if complete {
            if acquisition.exclusion_reason.is_some() || receipt.exclusion_reason.is_some() {
                return invalid("complete wallet has exclusion reason".to_owned());
            }
            let (groups, source) = acquisition
                .predecessor
                .as_ref()
                .filter(|base| base.carried && acquisition.version == 2)
                .map_or((0, 0), |base| (base.aggregate_count, base.source_row_count));
            if receipt.aggregate_count != checked_activity_count(groups, fetched)?
                || receipt.source_row_count != checked_activity_count(source, rows)?
            {
                return invalid(
                    "complete history counts do not equal carried plus fetched".to_owned(),
                );
            }
            if acquisition.version == 3
                && Some(&receipt.ordered_aggregate_digest)
                    != acquisition.fetched_aggregate_digest.as_ref()
            {
                return invalid("receipt digest differs from fetched history".to_owned());
            }
        } else {
            if receipt.aggregate_count != 0
                || receipt.source_row_count != 0
                || receipt.ordered_aggregate_digest != aggregate_digest(&[])?
                || acquisition.exclusion_reason.is_none()
                || (acquisition.aggregation_status == AggregationStatus::Complete
                    && (acquisition.exclusion_reason
                        != Some(ActivityExclusionReason::CrossBoundaryCollision)
                        || acquisition.mode != ActivityReadMode::Incremental
                        || fetched == 0))
            {
                return invalid("invalid excluded activity receipt".to_owned());
            }
        }
        Ok(())
    }
}

// Historical manifests no longer name independently queryable physical rows.
// Their complete receipt set still authenticates each predecessor wallet proof.
fn verify_historical_receipts(
    connection: &Connection,
    manifest: &ActivityCoverageManifestV2,
    identity: &FreshCollectionIdentity,
) -> Result<(), BootstrapError> {
    verify_historical_receipts_with(connection, manifest, identity, |_| {})
}

fn verify_historical_receipts_with(
    connection: &Connection,
    manifest: &ActivityCoverageManifestV2,
    identity: &FreshCollectionIdentity,
    mut observe: impl FnMut(&ActivityWalletReceiptProof),
) -> Result<(), BootstrapError> {
    if (identity.version >= 2) != (manifest.cursors == receipt_marker_v2()) {
        return invalid("historical receipt marker disagrees with its identity".to_owned());
    }
    let mut digest = ReceiptSetDigest::new(
        identity.generation,
        &identity.digest,
        identity.fixed_end_unix,
    )?;
    let (mut wallets, mut groups, mut source_rows) = (0_usize, 0, 0);
    let mut fetched = JsonArrayDigest::new();
    let mut visit = |receipt: ActivityWalletReceiptProof| -> Result<(), BootstrapError> {
        if identity.wallets.get(wallets) != Some(&receipt.wallet_hex) {
            return invalid("historical receipt membership mismatch".to_owned());
        }
        observe(&receipt);
        digest.push(&receipt)?;
        fetched.push(&receipt.ordered_aggregate_digest)?;
        wallets += 1;
        groups = checked_activity_count(groups, receipt.aggregate_count)?;
        source_rows = checked_activity_count(source_rows, receipt.source_row_count)?;
        Ok(())
    };
    if uses_retained_receipts(&manifest.cursors)? {
        let column = if has_column(
            connection,
            "activity_wallet_coverage_staging_v2",
            "acquisition_json",
        )? {
            "acquisition_json"
        } else {
            "NULL"
        };
        let reason = receipt_reason_column(connection)?;
        let mut statement = connection.prepare(&format!("SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
            ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, {column}, {reason}
            FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1 ORDER BY wallet_hex"))?;
        let mut rows = statement.query(params![to_i64(identity.generation, "base generation")?])?;
        while let Some(row) = rows.next()? {
            visit(decode_receipt(
                row,
                &identity.digest,
                identity.fixed_end_unix,
                identity.version,
            )?)?;
        }
    } else {
        for receipt in
            serde_json::from_value::<Vec<ActivityWalletReceiptProof>>(manifest.cursors.clone())?
        {
            if receipt.acquisition.is_some() {
                return invalid("embedded receipt has incremental acquisition".to_owned());
            }
            visit(receipt)?;
        }
    }
    if wallets != identity.wallets.len()
        || u64::try_from(wallets).map_err(|_| BootstrapError::Internal)? != manifest.wallet_count
        || groups != manifest.group_count
        || source_rows != manifest.source_row_count
        || digest.finish() != manifest.receipt_set_digest
        || (identity.version == 4 && fetched.finish() != manifest.aggregate_digest)
    {
        return invalid("historical receipt-set commitment mismatch".to_owned());
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct HistoryProof {
    records: BTreeMap<u64, (FreshCollectionIdentity, Option<ActivityCoverageManifestV2>)>,
    links: BTreeMap<u64, String>,
    last_fetched: BTreeMap<String, (u64, Option<String>)>,
}

impl HistoryProof {
    pub(super) fn load(
        connection: &Connection,
        head: &FreshCollectionIdentity,
    ) -> Result<Self, BootstrapError> {
        let mut records = BTreeMap::new();
        let mut links = BTreeMap::new();
        let mut last_fetched = BTreeMap::<String, (u64, Option<String>)>::new();
        let mut identity = head.clone();
        loop {
            let manifest = stored_activity_manifest(connection, identity.generation)?;
            if let Some(manifest) = &manifest {
                links.insert(identity.generation, manifest_link(manifest, &identity)?);
                verify_archived_identity(connection, identity.generation, &identity, true)?;
                verify_historical_receipts_with(connection, manifest, &identity, |receipt| {
                    if !receipt.excluded()
                        && let Some(acquisition) = receipt.acquisition.as_ref()
                        && (acquisition.mode == ActivityReadMode::Full
                            || acquisition
                                .fetched_aggregate_count
                                .is_some_and(|count| count > 0))
                    {
                        // Decision 4: an unchanged full read commits only its receipt.
                        // Its fetched rows do not make a certified quiet history active.
                        let change = (
                            identity.generation,
                            (acquisition.mode == ActivityReadMode::Full)
                                .then(|| receipt.ordered_aggregate_digest.clone()),
                        );
                        let latest = last_fetched
                            .entry(receipt.wallet_hex.clone())
                            .or_insert_with(|| change.clone());
                        if change.0 > latest.0 {
                            *latest = change;
                        }
                    }
                })?;
            }
            records.insert(identity.generation, (identity.clone(), manifest));
            let Some(base) = identity.base_generation else {
                break;
            };
            let prior =
                generation_identity(connection, base)?.ok_or_else(|| BootstrapError::Invalid {
                    message: "history chain identity missing".to_owned(),
                })?;
            let manifest = stored_activity_manifest(connection, base)?.ok_or_else(|| {
                BootstrapError::Invalid {
                    message: "history chain manifest missing".to_owned(),
                }
            })?;
            if Some(manifest_link(&manifest, &prior)?) != identity.base_manifest_sha256
                || Some(prior.fixed_end_unix) != identity.start_exclusive
            {
                return invalid("history chain manifest commitment mismatch".to_owned());
            }
            identity = prior;
        }
        Ok(Self {
            records,
            links,
            last_fetched,
        })
    }

    pub(super) fn has_new_rows(&self, wallet: &str, certificate: &HistoryCertificate) -> bool {
        self.last_fetched
            .get(wallet)
            .is_some_and(|(generation, full_digest)| {
                *generation > certificate.generation
                    && full_digest
                        .as_ref()
                        .is_none_or(|digest| digest != &certificate.ordered_digest)
            })
    }

    fn identity(&self, generation: u64) -> Option<&FreshCollectionIdentity> {
        self.records.get(&generation).map(|(identity, _)| identity)
    }

    fn manifest(&self, generation: u64) -> Option<&ActivityCoverageManifestV2> {
        self.records
            .get(&generation)
            .and_then(|(_, manifest)| manifest.as_ref())
    }

    fn link(&self, generation: u64) -> Option<&str> {
        self.links.get(&generation).map(String::as_str)
    }
}

pub(super) fn verify_record_chain(
    connection: &Connection,
    head: &FreshCollectionIdentity,
) -> Result<(), BootstrapError> {
    HistoryProof::load(connection, head).map(|_| ())
}

pub(super) fn predecessor_receipt(
    connection: &Connection,
    manifest: &ActivityCoverageManifestV2,
    identity: &FreshCollectionIdentity,
    wallet: &str,
) -> Result<ActivityWalletReceiptProof, BootstrapError> {
    if !uses_retained_receipts(&manifest.cursors)? {
        return serde_json::from_value::<Vec<ActivityWalletReceiptProof>>(
            manifest.cursors.clone(),
        )?
        .into_iter()
        .find(|receipt| receipt.wallet_hex == wallet)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "predecessor embedded receipt missing".to_owned(),
        });
    }
    let acquisition = if has_column(
        connection,
        "activity_wallet_coverage_staging_v2",
        "acquisition_json",
    )? {
        "acquisition_json"
    } else {
        "NULL"
    };
    let reason = receipt_reason_column(connection)?;
    let sql = format!("SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
        ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, {acquisition}, {reason}
        FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1 AND wallet_hex = ?2");
    let mut statement = connection.prepare_cached(&sql)?;
    let mut rows = statement.query(params![
        to_i64(identity.generation, "base generation")?,
        wallet
    ])?;
    let row = rows.next()?.ok_or_else(|| BootstrapError::Invalid {
        message: "predecessor wallet receipt missing".to_owned(),
    })?;
    decode_receipt(
        row,
        &identity.digest,
        identity.fixed_end_unix,
        identity.version,
    )
}

pub(super) fn decode_receipt(
    row: &rusqlite::Row<'_>,
    reference: &str,
    end: i64,
    version: u32,
) -> Result<ActivityWalletReceiptProof, BootstrapError> {
    let receipt = ActivityWalletReceiptProof {
        wallet_hex: row.get(0)?,
        pages: serde_json::from_str(&row.get::<_, String>(3)?)?,
        ordered_aggregate_digest: row.get(4)?,
        source_row_count: to_u64(row.get(5)?, "receipt source rows")?,
        aggregate_count: to_u64(row.get(6)?, "receipt aggregate count")?,
        schema_version: u32::try_from(row.get::<_, i64>(7)?)
            .map_err(|_| BootstrapError::Internal)?,
        parser_version: u32::try_from(row.get::<_, i64>(8)?)
            .map_err(|_| BootstrapError::Internal)?,
        exclusion_reason: row.get(10)?,
        acquisition: row
            .get::<_, Option<String>>(9)?
            .map(|json| serde_json::from_str(&json))
            .transpose()?,
    };
    if row.get::<_, String>(1)? != reference
        || row.get::<_, i64>(2)? != end
        || receipt.schema_version != ACTIVITY_SCHEMA_VERSION
        || receipt.parser_version != ACTIVITY_PARSER_VERSION
        || receipt.pages.iter().any(|p| {
            p.schema_version != ACTIVITY_SCHEMA_VERSION
                || p.parser_version != ACTIVITY_PARSER_VERSION
        })
        || (receipt.aggregate_count == 0 && receipt.source_row_count != 0)
        || (receipt.exclusion_reason.is_some() && receipt.aggregate_count != 0)
        || (version >= 2) != receipt.acquisition.is_some()
    {
        return invalid(format!(
            "activity receipt identity mismatch for {}",
            receipt.wallet_hex
        ));
    }
    validate_hex_sha256(
        &receipt.ordered_aggregate_digest,
        "ordered aggregate digest",
    )?;
    Ok(receipt)
}

fn wallet_receipt_digest(
    identity: &FreshCollectionIdentity,
    receipt: &ActivityWalletReceiptProof,
) -> Result<String, BootstrapError> {
    Ok(sha256_bytes(canonical_json(&serde_json::json!({
        "version":identity.version, "generation":identity.generation,
        "reference_sha256":identity.digest, "fixed_end_unix":identity.fixed_end_unix, "receipt":receipt,
    }))?.as_bytes()))
}

fn read_digest(
    wallet: &str,
    pages: &[ReconciliationPageEvidence],
    acquisition: &ActivityAcquisition,
) -> Result<String, BootstrapError> {
    Ok(sha256_bytes(canonical_json(&serde_json::json!({
        "version":2, "wallet_hex":wallet, "mode":acquisition.mode,
        "start_exclusive":acquisition.start_exclusive, "fixed_end_unix":acquisition.fixed_end_unix,
        "pages":pages, "aggregation_status":acquisition.aggregation_status,
        "ordered_aggregate_digest":acquisition.fetched_aggregate_digest,
        "aggregate_count":acquisition.fetched_aggregate_count, "source_row_count":acquisition.fetched_source_row_count,
    }))?.as_bytes()))
}

// Saturated probe pages remain committed but their rows are not admitted twice.
// Only terminal, unsaturated windows contribute to the fetched row total.
fn validate_pages(
    wallet: &str,
    pages: &[ReconciliationPageEvidence],
    start: i64,
    end: i64,
) -> Result<u64, BootstrapError> {
    use pe_source_polymarket_public::{ACTIVITY_MAX_OFFSET, RECONCILIATION_PAGE_LIMIT};
    let mut windows = BTreeMap::<(i64, i64), Vec<&ReconciliationPageEvidence>>::new();
    for page in pages {
        let bounds = page.bounds.ok_or_else(|| BootstrapError::Invalid {
            message: "activity page omitted window".to_owned(),
        })?;
        let lo = bounds.start.ok_or_else(|| BootstrapError::Invalid {
            message: "activity page omitted positive wire start".to_owned(),
        })?;
        if lo < start
            || lo >= bounds.end
            || bounds.end > end
            || page.partition.is_some()
            || page.row_count > RECONCILIATION_PAGE_LIMIT
            || page.offset > ACTIVITY_MAX_OFFSET
        {
            return invalid("activity page is outside the frozen read window".to_owned());
        }
        validate_hex_sha256(&page.raw_page_hash, "raw page hash")?;
        validate_hex_sha256(&page.canonical_page_hash, "canonical page hash")?;
        let url =
            reqwest::Url::parse(&page.request_url).map_err(|error| BootstrapError::Invalid {
                message: format!("invalid page URL: {error}"),
            })?;
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        let wire_start = lo
            .checked_add(1)
            .ok_or(BootstrapError::Internal)?
            .to_string();
        if query.get("user").map(String::as_str) != Some(wallet)
            || query.get("start") != Some(&wire_start)
            || query.get("end") != Some(&bounds.end.to_string())
            || query.get("offset") != Some(&page.offset.to_string())
            || query.get("limit") != Some(&RECONCILIATION_PAGE_LIMIT.to_string())
            || query.get("sortDirection").map(String::as_str) != Some("DESC")
            || query.get("type").map(String::as_str) != Some("TRADE,SPLIT,MERGE,REDEEM,CONVERSION")
        {
            return invalid("activity page request does not match its proof".to_owned());
        }
        windows.entry((lo, bounds.end)).or_default().push(page);
    }
    if !windows.contains_key(&(start, end)) {
        return invalid("activity proof omitted original read window".to_owned());
    }
    let mut leaves = Vec::new();
    for ((lo, hi), mut pages) in windows {
        pages.sort_by_key(|page| page.offset);
        let mut offset = 0;
        let mut rows = 0;
        for (index, page) in pages.iter().enumerate() {
            if page.offset != offset
                || (index + 1 < pages.len() && page.row_count != RECONCILIATION_PAGE_LIMIT)
            {
                return invalid("activity page offsets are incomplete".to_owned());
            }
            offset = offset
                .checked_add(RECONCILIATION_PAGE_LIMIT)
                .ok_or(BootstrapError::Internal)?;
            rows = checked_activity_count(rows, u64::from(page.row_count))?;
        }
        let last = pages.last().ok_or(BootstrapError::Internal)?;
        if last.row_count < RECONCILIATION_PAGE_LIMIT {
            leaves.push((lo, hi, rows));
        } else if last.offset != ACTIVITY_MAX_OFFSET {
            return invalid("activity page proof has no terminal page".to_owned());
        }
    }
    leaves.sort_unstable();
    let mut cursor = start;
    let mut rows = 0;
    for (lo, hi, count) in leaves {
        if lo != cursor {
            return invalid("activity terminal windows have a gap or overlap".to_owned());
        }
        cursor = hi;
        rows = checked_activity_count(rows, count)?;
    }
    if cursor != end {
        return invalid("activity terminal windows do not reach fixed end".to_owned());
    }
    Ok(rows)
}

pub(super) fn verify_archived_identity(
    connection: &Connection,
    generation: u64,
    identity: &FreshCollectionIdentity,
    required: bool,
) -> Result<(), BootstrapError> {
    let stored: Option<String> = connection.query_row(
        "SELECT collection_identity_json FROM activity_coverage_manifests_v2 WHERE generation = ?1",
        params![to_i64(generation, "activity generation")?],
        |row| row.get(0),
    )?;
    let archived = stored
        .map(|json| decode_fresh_identity(&json))
        .transpose()?;
    if archived.as_ref().is_some_and(|record| record != identity)
        || (required && archived.is_none())
    {
        return invalid("completed collection identity is missing or changed".to_owned());
    }
    Ok(())
}

/// One wallet's acquisition partition, fed one row at a time.
///
/// A row's failure is held rather than returned so that `finish` reports the
/// same failure, for the same wallet, that a whole-slice pass reports: the
/// caller's receipt comparison still comes first, and the first offending row
/// still wins over every later one.
pub(super) struct AcquisitionPartition<'receipt> {
    receipt: &'receipt ActivityWalletReceiptProof,
    acquisition: Option<&'receipt ActivityAcquisition>,
    rows_seen: bool,
    carried: JsonArrayDigest,
    fetched: JsonArrayDigest,
    carried_count: u64,
    carried_source: u64,
    fetched_count: u64,
    fetched_source: u64,
    failure: Option<BootstrapError>,
}

impl<'receipt> AcquisitionPartition<'receipt> {
    pub(super) fn new(receipt: &'receipt ActivityWalletReceiptProof) -> Self {
        Self {
            receipt,
            acquisition: if receipt.excluded() {
                None
            } else {
                receipt.acquisition.as_ref()
            },
            rows_seen: false,
            carried: JsonArrayDigest::new(),
            fetched: JsonArrayDigest::new(),
            carried_count: 0,
            carried_source: 0,
            fetched_count: 0,
            fetched_source: 0,
            failure: None,
        }
    }

    /// `json` is the row's canonical JSON, already serialized for the wallet
    /// and generation commitments.
    pub(super) fn push(&mut self, row: &ActivityAggregate, json: &[u8]) {
        self.rows_seen = true;
        if self.failure.is_some() {
            return;
        }
        let Some(acquisition) = self.acquisition else {
            return;
        };
        let epoch = row.source_time.0.unix_timestamp();
        if epoch <= 0 || epoch > acquisition.fixed_end_unix {
            self.failure = Some(BootstrapError::Invalid {
                message: "complete activity row outside certified history".to_owned(),
            });
            return;
        }
        let (digest, count, source) = if acquisition.mode == ActivityReadMode::Incremental
            && epoch <= acquisition.start_exclusive
        {
            (
                &mut self.carried,
                &mut self.carried_count,
                &mut self.carried_source,
            )
        } else {
            (
                &mut self.fetched,
                &mut self.fetched_count,
                &mut self.fetched_source,
            )
        };
        digest.push_json(json);
        match (
            checked_activity_count(*count, 1),
            checked_activity_count(*source, row.row_count),
        ) {
            (Ok(rows), Ok(source_rows)) => {
                *count = rows;
                *source = source_rows;
            }
            (Err(error), _) | (_, Err(error)) => self.failure = Some(error),
        }
    }

    pub(super) fn finish(self) -> Result<(), BootstrapError> {
        if self.receipt.excluded() {
            if self.rows_seen {
                return invalid("excluded wallet has current-generation rows".to_owned());
            }
            return Ok(());
        }
        let Some(acquisition) = self.acquisition else {
            return Ok(());
        };
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        let (carried, fetched) = (self.carried, self.fetched);
        let (carried_count, carried_source) = (self.carried_count, self.carried_source);
        let (fetched_count, fetched_source) = (self.fetched_count, self.fetched_source);
        validate_partitions(
            acquisition,
            carried,
            fetched,
            (carried_count, carried_source),
            (fetched_count, fetched_source),
        )
    }
}

fn validate_partitions(
    acquisition: &ActivityAcquisition,
    carried: JsonArrayDigest,
    fetched: JsonArrayDigest,
    (carried_count, carried_source): (u64, u64),
    (fetched_count, fetched_source): (u64, u64),
) -> Result<(), BootstrapError> {
    if let Some(base) = acquisition.predecessor.as_ref().filter(|base| base.carried) {
        if carried.finish() != base.ordered_aggregate_digest
            || carried_count != base.aggregate_count
            || carried_source != base.source_row_count
        {
            return invalid("carried history does not reproduce predecessor receipt".to_owned());
        }
    } else if carried_count != 0 {
        return invalid("unbound carried activity rows".to_owned());
    }
    if Some(fetched.finish()) != acquisition.fetched_aggregate_digest
        || Some(fetched_count) != acquisition.fetched_aggregate_count
        || fetched_source != acquisition.fetched_source_row_count
    {
        return invalid("fetched history does not reproduce acquisition receipt".to_owned());
    }
    Ok(())
}

// The carry reads, decodes and serializes on the certification path's worker
// pool (`aggregate_scan`), which bounds the rows held in memory; the writer
// keeps the ordered digests and the one re-stamp inside the wallet transaction.
fn verify_and_carry_wallet(
    scan: &mut aggregate_scan::Scan,
    transaction: &rusqlite::Transaction<'_>,
    wallet: &str,
    generation: Option<i64>,
    end: i64,
    base: &ActivityPredecessor,
    history: &mut JsonArrayDigest,
) -> Result<(), BootstrapError> {
    let base_generation = to_i64(base.generation, "base generation")?;
    let mut digest = JsonArrayDigest::new();
    let (mut count, mut source_rows) = (0, 0);
    let unserializable = scan.for_each(transaction, base_generation, wallet, |row, json| {
        if row.source_time.0.unix_timestamp() <= 0 || row.source_time.0.unix_timestamp() > end {
            return invalid("carried activity outside predecessor window".to_owned());
        }
        // One serialization feeds both commitments (#670).
        if let Some(json) = json {
            digest.push_json(json);
            history.push_json(json);
        }
        count = checked_activity_count(count, 1)?;
        source_rows = checked_activity_count(source_rows, row.row_count)?;
        Ok(())
    })?;
    if let Some(error) = unserializable {
        return Err(error);
    }
    if digest.finish() != base.ordered_aggregate_digest
        || count != base.aggregate_count
        || source_rows != base.source_row_count
    {
        return invalid("carried history does not match predecessor receipt".to_owned());
    }
    if let Some(generation) = generation {
        let changed = transaction.execute(
            "UPDATE activity_groups_v2 SET coverage_generation = ?1
             WHERE wallet_hex = ?2 AND coverage_generation = ?3",
            params![generation, wallet, base_generation],
        )?;
        if u64::try_from(changed).ok() != Some(count) {
            return invalid("carry update count changed".to_owned());
        }
    }
    tracing::debug!(
        wallet,
        count,
        carried = generation.is_some(),
        "activity predecessor verified"
    );
    Ok(())
}

fn check_collisions(
    connection: &Connection,
    proof: &CollectionProof,
    completion: &WalletActivityCompletion,
) -> Result<bool, BootstrapError> {
    // These are the only indexed-identity probes omitted by the private root.
    // Wallet ownership, data_version, receipts and strict inserts still run.
    if proof.bulk_root {
        return Ok(false);
    }
    if proof.identity.version == 4 {
        if proof.mode(&completion.wallet_hex) == ActivityReadMode::Full {
            return Ok(false);
        }
        let mut collision = false;
        let mut statement = connection.prepare_cached(
            "SELECT wallet_hex FROM activity_groups_v2 WHERE source_trade_id = ?1",
        )?;
        for aggregate in &completion.aggregates {
            let existing: Option<String> = statement
                .query_row(params![aggregate.group_id.key().0], |row| row.get(0))
                .optional()?;
            if let Some(wallet) = existing {
                if wallet != completion.wallet_hex {
                    return invalid("foreign-wallet activity identity collision".to_owned());
                }
                collision = true;
            }
        }
        return Ok(collision);
    }
    let mut collision = false;
    let mut statement = connection.prepare_cached(
        "SELECT wallet_hex, coverage_generation FROM activity_groups_v2 WHERE source_trade_id = ?1",
    )?;
    for aggregate in &completion.aggregates {
        let existing: Option<(String, i64)> = statement
            .query_row(params![aggregate.group_id.key().0], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?;
        if let Some((wallet, generation)) = existing {
            if wallet != completion.wallet_hex {
                return invalid("foreign-wallet activity identity collision".to_owned());
            }
            if proof.mode(&wallet) == ActivityReadMode::Incremental {
                if Some(to_u64(generation, "retained generation")?)
                    != proof.identity.base_generation
                {
                    return invalid(
                        "activity identity collides with a non-predecessor row".to_owned(),
                    );
                }
                collision = true;
            }
        }
    }
    Ok(collision)
}

pub(super) fn commit_incremental_wallet(
    scan: &mut aggregate_scan::Scan,
    connection: &mut Connection,
    proof: &CollectionProof,
    completed_at: i64,
    completion: &WalletActivityCompletion,
) -> (CollectionWriteCounts, Result<(), BootstrapError>) {
    if proof.bulk_root {
        let mut ids = BTreeSet::new();
        for aggregate in &completion.aggregates {
            if !ids.insert(&aggregate.group_id.key().0) {
                return (
                    CollectionWriteCounts::default(),
                    invalid("duplicate source_trade_id in bulk-root wallet batch".to_owned()),
                );
            }
        }
    }
    if proof.identity.version == 4 {
        commit_history_wallet(scan, connection, proof, completed_at, completion)
    } else {
        (
            CollectionWriteCounts::default(),
            commit_incremental_wallet_v2(scan, connection, proof, completed_at, completion),
        )
    }
}

fn commit_incremental_wallet_v2(
    scan: &mut aggregate_scan::Scan,
    connection: &mut Connection,
    proof: &CollectionProof,
    completed_at: i64,
    completion: &WalletActivityCompletion,
) -> Result<(), BootstrapError> {
    let began = std::time::Instant::now();
    let wallet = &completion.wallet_hex;
    let identity = &proof.identity;
    let generation = to_i64(identity.generation, "activity generation")?;
    let collision = check_collisions(connection, proof, completion)?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    // The exclusive mutation lock and this sole writer bind the frozen identity.
    // Own commits do not change data_version; an external commit does. Checking
    // it inside BEGIN IMMEDIATE rechecks that binding without parsing the entire
    // generation-sized wallet union once per wallet.
    let data_version: i64 =
        transaction.pragma_query_value(None, "data_version", |row| row.get(0))?;
    if data_version != proof.data_version
        || collision != check_collisions(&transaction, proof, completion)?
    {
        return invalid("collection changed externally before wallet commit".to_owned());
    }
    let unexpected: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM activity_groups_v2 WHERE wallet_hex = ?1 AND coverage_generation = ?2)
         OR EXISTS(SELECT 1 FROM activity_wallet_coverage_staging_v2 WHERE wallet_hex = ?1 AND generation = ?2)",
        params![wallet, generation], |row| row.get(0))?;
    if unexpected {
        return invalid(
            "unexpected current-generation rows or receipt before wallet commit".to_owned(),
        );
    }
    let mode = proof.mode(wallet);
    let incomplete = completion.aggregation_status != AggregationStatus::Complete;
    let excluded = incomplete || collision;
    let predecessor = proof.predecessor(
        &transaction,
        wallet,
        !excluded && mode == ActivityReadMode::Incremental,
    )?;
    if mode == ActivityReadMode::Incremental {
        let base = predecessor
            .as_ref()
            .ok_or_else(|| BootstrapError::Invalid {
                message: "incremental wallet omitted predecessor".to_owned(),
            })?;
        let foreign: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM activity_groups_v2 WHERE wallet_hex = ?1 AND coverage_generation != ?2)",
            params![wallet, to_i64(base.generation, "base generation")?], |row| row.get(0))?;
        if foreign {
            return invalid("wallet has contradictory retained generations".to_owned());
        }
    }
    let mut acquisition = ActivityAcquisition {
        version: 2,
        mode: mode.clone(),
        start_exclusive: proof.start(wallet),
        fixed_end_unix: identity.fixed_end_unix,
        aggregation_status: completion.aggregation_status.clone(),
        fetched_aggregate_digest: if incomplete {
            None
        } else {
            Some(aggregate_digest(&completion.aggregates)?)
        },
        fetched_aggregate_count: if incomplete {
            None
        } else {
            Some(u64::try_from(completion.aggregates.len()).map_err(|_| BootstrapError::Internal)?)
        },
        fetched_source_row_count: completion.fetched_source_row_count,
        read_sha256: String::new(),
        predecessor,
        disposition: if excluded {
            ActivityDisposition::Excluded
        } else {
            ActivityDisposition::Complete
        },
        exclusion_reason: if completion.aggregation_status == AggregationStatus::NotAttempted {
            Some(if proof.deferred(wallet) {
                ActivityExclusionReason::DormantDeferred
            } else {
                ActivityExclusionReason::AcquisitionFailure
            })
        } else if incomplete {
            Some(ActivityExclusionReason::AggregationFailure)
        } else if collision {
            Some(ActivityExclusionReason::CrossBoundaryCollision)
        } else {
            None
        },
    };
    if excluded && mode == ActivityReadMode::Incremental {
        // An exclusion must not hide damaged predecessor history on restart.
        // Verify the same bounded bytes, but leave all retained rows untouched.
        verify_and_carry_wallet(
            scan,
            &transaction,
            wallet,
            None,
            acquisition.start_exclusive,
            acquisition
                .predecessor
                .as_ref()
                .ok_or(BootstrapError::Internal)?,
            &mut JsonArrayDigest::new(),
        )?;
    }
    acquisition.read_sha256 = read_digest(wallet, &completion.pages, &acquisition)?;
    let mut history = JsonArrayDigest::new();
    let (mut count, mut source_rows) = (0, 0);
    if !excluded {
        if mode == ActivityReadMode::Full {
            transaction.execute(
                "DELETE FROM activity_groups_v2 WHERE wallet_hex = ?1",
                params![wallet],
            )?;
        } else {
            let base = acquisition
                .predecessor
                .as_ref()
                .ok_or(BootstrapError::Internal)?;
            verify_and_carry_wallet(
                scan,
                &transaction,
                wallet,
                Some(generation),
                acquisition.start_exclusive,
                base,
                &mut history,
            )?;
            count = base.aggregate_count;
            source_rows = base.source_row_count;
        }
        for aggregate in &completion.aggregates {
            let epoch = aggregate.source_time.0.unix_timestamp();
            if epoch <= acquisition.start_exclusive || epoch > acquisition.fixed_end_unix {
                return invalid("fetched aggregate outside acquisition window".to_owned());
            }
            // Ordinary uniqueness is immediate. A bulk root proves global
            // uniqueness when sealing; neither mode permits an upsert.
            insert_activity_aggregate_strict(&transaction, generation, wallet, aggregate)?;
            history.push(aggregate)?;
            count = checked_activity_count(count, 1)?;
            source_rows = checked_activity_count(source_rows, aggregate.row_count)?;
        }
    }
    let receipt = ActivityWalletReceiptProof {
        wallet_hex: wallet.clone(),
        pages: completion.pages.clone(),
        ordered_aggregate_digest: history.finish(),
        source_row_count: source_rows,
        aggregate_count: count,
        schema_version: ACTIVITY_SCHEMA_VERSION,
        parser_version: ACTIVITY_PARSER_VERSION,
        acquisition: Some(acquisition),
        exclusion_reason: completion.exclusion_reason.clone(),
    };
    proof.validate_receipt(&transaction, &receipt)?;
    transaction.execute("INSERT INTO activity_wallet_coverage_staging_v2
        (generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
         ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, completed_at_unix, acquisition_json, exclusion_reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![generation, wallet, identity.digest, identity.fixed_end_unix, canonical_json(&receipt.pages)?, receipt.ordered_aggregate_digest,
            to_i64(source_rows, "source row count")?, to_i64(count, "aggregate count")?, i64::from(ACTIVITY_SCHEMA_VERSION),
            i64::from(ACTIVITY_PARSER_VERSION), completed_at, canonical_json(&receipt.acquisition)?, receipt.exclusion_reason])?;
    transaction.commit()?;
    if excluded && !proof.deferred(wallet) {
        tracing::warn!(wallet, generation, reason = ?receipt.acquisition.as_ref().and_then(|a| a.exclusion_reason.as_ref()), "activity wallet excluded from generation");
    }
    tracing::debug!(
        wallet,
        generation,
        elapsed_ms = began.elapsed().as_millis(),
        "activity wallet transaction committed"
    );
    Ok(())
}

fn commit_history_wallet(
    scan: &mut aggregate_scan::Scan,
    connection: &mut Connection,
    proof: &CollectionProof,
    completed_at: i64,
    completion: &WalletActivityCompletion,
) -> (CollectionWriteCounts, Result<(), BootstrapError>) {
    let mut counts = CollectionWriteCounts::default();
    let result = (|| {
        let wallet = &completion.wallet_hex;
        let identity = &proof.identity;
        let generation = to_i64(identity.generation, "activity generation")?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let _authorization = authorize_history_writes(&transaction)?;
        let data_version: i64 =
            transaction.pragma_query_value(None, "data_version", |row| row.get(0))?;
        if data_version != proof.data_version {
            return invalid("collection changed externally before wallet commit".to_owned());
        }
        let unexpected: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM activity_wallet_coverage_staging_v2 WHERE wallet_hex = ?1 AND generation = ?2)",
        params![wallet, generation], |row| row.get(0),
    )?;
        if unexpected {
            return invalid("unexpected receipt before wallet commit".to_owned());
        }
        let mode = proof.mode(wallet);
        let repair = identity
            .repair_wallets
            .as_ref()
            .is_some_and(|repairs| repairs.binary_search(wallet).is_ok());
        let collision = check_collisions(&transaction, proof, completion)?;
        let incomplete = completion.aggregation_status != AggregationStatus::Complete;
        let excluded = incomplete || collision;
        let chain = HistoryChain::load(
            &transaction,
            wallet,
            proof.history.as_ref().ok_or(BootstrapError::Internal)?,
            identity.base_generation.unwrap_or(0),
        )?;
        let predecessor = proof.predecessor(
            &transaction,
            wallet,
            !excluded && mode == ActivityReadMode::Incremental,
        )?;
        let mut acquisition = ActivityAcquisition {
            version: 3,
            mode: mode.clone(),
            start_exclusive: proof.start(wallet),
            fixed_end_unix: identity.fixed_end_unix,
            aggregation_status: completion.aggregation_status.clone(),
            fetched_aggregate_digest: if incomplete {
                None
            } else {
                Some(aggregate_digest(&completion.aggregates)?)
            },
            fetched_aggregate_count: if incomplete {
                None
            } else {
                Some(
                    u64::try_from(completion.aggregates.len())
                        .map_err(|_| BootstrapError::Internal)?,
                )
            },
            fetched_source_row_count: completion.fetched_source_row_count,
            read_sha256: String::new(),
            predecessor,
            disposition: if excluded {
                ActivityDisposition::Excluded
            } else {
                ActivityDisposition::Complete
            },
            exclusion_reason: if completion.aggregation_status == AggregationStatus::NotAttempted {
                Some(if proof.deferred(wallet) {
                    ActivityExclusionReason::DormantDeferred
                } else {
                    ActivityExclusionReason::AcquisitionFailure
                })
            } else if incomplete {
                Some(ActivityExclusionReason::AggregationFailure)
            } else if collision {
                Some(ActivityExclusionReason::CrossBoundaryCollision)
            } else {
                None
            },
        };
        let unchanged_full = !excluded
            && !repair
            && mode == ActivityReadMode::Full
            && chain.has_history_proof()
            && chain.matches_fetched(&completion.aggregates)?;
        for aggregate in &completion.aggregates {
            let time = aggregate.source_time.0.unix_timestamp();
            if time <= acquisition.start_exclusive || time > acquisition.fixed_end_unix {
                return invalid("fetched aggregate outside acquisition window".to_owned());
            }
        }
        if repair {
            let certificate = HistoryCertificate::load(&transaction, wallet)?;
            let mut digest = JsonArrayDigest::new();
            let observed = scan.for_each_history(&transaction, wallet, |_, json| {
                counts.rows_verified = checked_activity_count(counts.rows_verified, 1)?;
                if let Some(json) = json {
                    digest.push_json(json);
                }
                Ok(())
            });
            match observed {
                Ok(None) => {
                    tracing::warn!(wallet, stored_digest = digest.finish(), certified_digest = ?certificate.as_ref().map(|certificate| &certificate.ordered_digest), "replacing explicitly repaired wallet history")
                }
                Ok(Some(error))
                | Err(
                    error @ (BootstrapError::Invalid { .. }
                    | BootstrapError::Json(_)
                    | BootstrapError::Parse { .. }
                    | BootstrapError::Sqlite(
                        rusqlite::Error::InvalidColumnType(..)
                        | rusqlite::Error::FromSqlConversionFailure(..),
                    )),
                ) => {
                    tracing::warn!(wallet, stored_digest_error = %error, certified_digest = ?certificate.as_ref().map(|certificate| &certificate.ordered_digest), "replacing unreadable explicitly repaired wallet history");
                }
                Err(error) => return Err(error),
            }
            counts.rows_deleted = u64::try_from(transaction.execute(
                "DELETE FROM activity_groups_v2 WHERE wallet_hex = ?1",
                [wallet],
            )?)
            .map_err(|_| BootstrapError::Internal)?;
        } else if excluded {
            if !proof.deferred(wallet) && chain.has_history_proof() {
                chain.verify_stored(scan, &transaction, &mut counts.rows_verified)?;
            }
        } else if mode == ActivityReadMode::Full && !unchanged_full {
            if chain.has_history_proof() {
                chain.verify_stored(scan, &transaction, &mut counts.rows_verified)?;
            }
            counts.rows_deleted = u64::try_from(transaction.execute(
                "DELETE FROM activity_groups_v2 WHERE wallet_hex = ?1",
                [wallet],
            )?)
            .map_err(|_| BootstrapError::Internal)?;
        }
        if !excluded && !unchanged_full {
            for aggregate in &completion.aggregates {
                insert_activity_aggregate_strict(&transaction, generation, wallet, aggregate)?;
            }
        }
        acquisition.read_sha256 = read_digest(wallet, &completion.pages, &acquisition)?;
        let receipt = ActivityWalletReceiptProof {
            wallet_hex: wallet.clone(),
            pages: completion.pages.clone(),
            ordered_aggregate_digest: if excluded {
                aggregate_digest(&[])?
            } else {
                acquisition
                    .fetched_aggregate_digest
                    .clone()
                    .ok_or(BootstrapError::Internal)?
            },
            source_row_count: if excluded {
                0
            } else {
                completion.fetched_source_row_count
            },
            aggregate_count: if excluded {
                0
            } else {
                acquisition
                    .fetched_aggregate_count
                    .ok_or(BootstrapError::Internal)?
            },
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            acquisition: Some(acquisition),
            exclusion_reason: completion.exclusion_reason.clone(),
        };
        proof.validate_receipt(&transaction, &receipt)?;
        transaction.execute(
        "INSERT INTO activity_wallet_coverage_staging_v2
         (generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
          ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version,
          completed_at_unix, acquisition_json, exclusion_reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![generation, wallet, identity.digest, identity.fixed_end_unix, canonical_json(&receipt.pages)?,
            receipt.ordered_aggregate_digest, to_i64(receipt.source_row_count, "receipt source rows")?,
            to_i64(receipt.aggregate_count, "receipt aggregate count")?, i64::from(ACTIVITY_SCHEMA_VERSION),
            i64::from(ACTIVITY_PARSER_VERSION), completed_at, canonical_json(&receipt.acquisition)?, receipt.exclusion_reason],
    )?;
        drop(_authorization);
        transaction.commit()?;
        counts.rows_inserted = if !excluded && !unchanged_full {
            receipt.aggregate_count
        } else {
            0
        };
        if proof.deferred(wallet) {
            counts.deferred_wallets = 1;
        } else if excluded {
            counts.excluded_wallets = 1;
        } else if mode == ActivityReadMode::Incremental {
            counts.incremental_wallets = 1;
        } else if unchanged_full {
            counts.unchanged_full_wallets = 1;
        } else {
            counts.differing_full_wallets = 1;
        }
        if excluded && !proof.deferred(wallet) {
            tracing::warn!(wallet, generation, reason = ?receipt.acquisition.as_ref().and_then(|acquisition| acquisition.exclusion_reason.as_ref()), "activity wallet excluded from generation");
        }
        Ok(())
    })();
    if result.is_err() {
        counts.rows_deleted = 0;
    }
    (counts, result)
}

pub(super) fn log_writer_settings(connection: &Connection) -> Result<(), BootstrapError> {
    let journal: String = connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let read = |name| connection.pragma_query_value(None, name, |row| row.get::<_, i64>(0));
    let (version, build): (String, String) =
        connection.query_row("SELECT sqlite_version(), sqlite_source_id()", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    tracing::info!(
        journal_mode = journal,
        synchronous = read("synchronous")?,
        wal_autocheckpoint = read("wal_autocheckpoint")?,
        page_size = read("page_size")?,
        cache_size = read("cache_size")?,
        mmap_size = read("mmap_size")?,
        temp_store = read("temp_store")?,
        sqlite_version = version,
        sqlite_source_id = build,
        "activity writer effective SQLite settings"
    );
    Ok(())
}
