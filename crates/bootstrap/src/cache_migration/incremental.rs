//! Acquisition and atomic carry-forward for complete activity generations.

use super::*;

pub(super) fn read_manifest(
    connection: &Connection,
    generation: u64,
) -> Result<Option<ActivityCoverageManifestV2>, BootstrapError> {
    let stored = connection
        .query_row(
            "SELECT reference_sha256, wallet_count, receipt_set_digest, aggregate_digest,
                    source_row_count, source_bounds_json, cursors_json, page_hashes_json,
                    group_count, schema_version, parser_version, completed_at_unix
             FROM activity_coverage_manifests_v2 WHERE generation = ?1",
            params![to_i64(generation, "activity generation")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                ))
            },
        )
        .optional()?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let manifest = ActivityCoverageManifestV2 {
        generation,
        reference_sha256: stored.0,
        wallet_count: to_u64(stored.1, "activity wallet count")?,
        receipt_set_digest: stored.2,
        aggregate_digest: stored.3,
        source_row_count: to_u64(stored.4, "activity source-row count")?,
        source_bounds: serde_json::from_str(&stored.5)?,
        cursors: serde_json::from_str(&stored.6)?,
        page_hashes: serde_json::from_str(&stored.7)?,
        group_count: to_u64(stored.8, "activity group count")?,
        schema_version: u32::try_from(stored.9).map_err(|_| BootstrapError::Invalid {
            message: "invalid activity schema version".to_owned(),
        })?,
        parser_version: u32::try_from(stored.10).map_err(|_| BootstrapError::Invalid {
            message: "invalid activity parser version".to_owned(),
        })?,
        completed_at_unix: stored.11,
    };
    Ok(Some(manifest))
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
    if let Some(stored) = read_manifest(transaction, manifest.generation)? {
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
    base: Option<(ActivityCoverageManifestV2, FreshCollectionIdentity)>,
    base_link: Option<String>,
    data_version: i64,
}

impl CollectionProof {
    pub(super) fn load(
        connection: &Connection,
        generation: u64,
    ) -> Result<Option<Self>, BootstrapError> {
        let Some(identity) = generation_identity(connection, generation)? else {
            return Ok(None);
        };
        if identity.version == 1 {
            return Ok(None);
        }
        let base = if let Some(base) = identity.base_generation {
            let record =
                generation_identity(connection, base)?.ok_or_else(|| BootstrapError::Invalid {
                    message: "predecessor collection identity is missing".to_owned(),
                })?;
            let manifest =
                read_manifest(connection, base)?.ok_or_else(|| BootstrapError::Invalid {
                    message: "predecessor manifest is missing".to_owned(),
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
            verify_historical_receipts(connection, &manifest, &record)?;
            Some((manifest, record))
        } else {
            None
        };
        let base_link = base
            .as_ref()
            .map(|(manifest, identity)| manifest_link(manifest, identity))
            .transpose()?;
        let data_version = connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
        Ok(Some(Self {
            identity,
            base,
            base_link,
            data_version,
        }))
    }

    pub(super) fn mode(&self, wallet: &str) -> ActivityReadMode {
        if self
            .identity
            .full_read_wallets
            .as_ref()
            .is_some_and(|full| full.binary_search_by(|w| w.as_str().cmp(wallet)).is_ok())
        {
            ActivityReadMode::Full
        } else {
            ActivityReadMode::Incremental
        }
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
        let carried = complete && acquisition.mode == ActivityReadMode::Incremental;
        if acquisition.version != 2
            || acquisition.mode != self.mode(&receipt.wallet_hex)
            || acquisition.start_exclusive != self.start(&receipt.wallet_hex)
            || acquisition.fixed_end_unix != self.identity.fixed_end_unix
            || acquisition.predecessor
                != self.predecessor(connection, &receipt.wallet_hex, carried)?
        {
            return invalid(format!(
                "activity acquisition identity mismatch for {}",
                receipt.wallet_hex
            ));
        }
        if acquisition.mode == ActivityReadMode::Incremental && acquisition.predecessor.is_none() {
            return invalid("incremental wallet has no predecessor receipt".to_owned());
        }
        let rows = validate_pages(
            &receipt.wallet_hex,
            &receipt.pages,
            acquisition.start_exclusive,
            acquisition.fixed_end_unix,
        )?;
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
            if acquisition.exclusion_reason.is_some() {
                return invalid("complete wallet has exclusion reason".to_owned());
            }
            let (groups, source) = acquisition
                .predecessor
                .as_ref()
                .filter(|base| base.carried)
                .map_or((0, 0), |base| (base.aggregate_count, base.source_row_count));
            if receipt.aggregate_count != checked_activity_count(groups, fetched)?
                || receipt.source_row_count != checked_activity_count(source, rows)?
            {
                return invalid(
                    "complete history counts do not equal carried plus fetched".to_owned(),
                );
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
    if (identity.version == 2) != (manifest.cursors == receipt_marker_v2()) {
        return invalid("historical receipt marker disagrees with its identity".to_owned());
    }
    let mut digest = ReceiptSetDigest::new(
        identity.generation,
        &identity.digest,
        identity.fixed_end_unix,
    )?;
    let (mut wallets, mut groups, mut source_rows) = (0_usize, 0, 0);
    let mut visit = |receipt: ActivityWalletReceiptProof| -> Result<(), BootstrapError> {
        if identity.wallets.get(wallets) != Some(&receipt.wallet_hex) {
            return invalid("historical receipt membership mismatch".to_owned());
        }
        digest.push(&receipt)?;
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
        let mut statement = connection.prepare(&format!("SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
            ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, {column}
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
    {
        return invalid("historical receipt-set commitment mismatch".to_owned());
    }
    Ok(())
}

fn predecessor_receipt(
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
    let sql = format!("SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
        ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, {acquisition}
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
        || (version == 2) != receipt.acquisition.is_some()
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

pub(super) fn validate_result(
    receipt: &ActivityWalletReceiptProof,
    rows: &[ActivityAggregate],
) -> Result<(), BootstrapError> {
    let Some(acquisition) = &receipt.acquisition else {
        return Ok(());
    };
    if acquisition.disposition == ActivityDisposition::Excluded {
        if !rows.is_empty() {
            return invalid("excluded wallet has current-generation rows".to_owned());
        }
        return Ok(());
    }
    let mut carried = JsonArrayDigest::new();
    let mut fetched = JsonArrayDigest::new();
    let (mut carried_count, mut carried_source, mut fetched_count, mut fetched_source) =
        (0, 0, 0, 0);
    for row in rows {
        let epoch = row.source_time.0.unix_timestamp();
        if epoch <= 0 || epoch > acquisition.fixed_end_unix {
            return invalid("complete activity row outside certified history".to_owned());
        }
        if acquisition.mode == ActivityReadMode::Incremental && epoch <= acquisition.start_exclusive
        {
            carried.push(row)?;
            carried_count = checked_activity_count(carried_count, 1)?;
            carried_source = checked_activity_count(carried_source, row.row_count)?;
        } else {
            fetched.push(row)?;
            fetched_count = checked_activity_count(fetched_count, 1)?;
            fetched_source = checked_activity_count(fetched_source, row.row_count)?;
        }
    }
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

// One bounded batch is resident; every batch stays in the same wallet transaction.
const CARRY_BATCH_SIZE: i64 = 512;

fn verify_and_carry_wallet(
    transaction: &rusqlite::Transaction<'_>,
    wallet: &str,
    generation: Option<i64>,
    end: i64,
    base: &ActivityPredecessor,
    history: &mut JsonArrayDigest,
) -> Result<(), BootstrapError> {
    let mut previous: Option<(i64, String)> = None;
    let mut digest = JsonArrayDigest::new();
    let (mut count, mut source_rows, mut batches) = (0, 0, 0_u64);
    loop {
        let lower = if previous.is_some() {
            "AND (source_time_unix, source_trade_id) > (?3, ?4)"
        } else {
            ""
        };
        let sql = format!("SELECT source_trade_id, semantic_revision, components_json, row_count,
            share_amount_str, price_weighted_share_amount_str, source_usdc_amount_str, source_time_unix, is_combo
            FROM activity_groups_v2 WHERE wallet_hex = ?1 AND coverage_generation = ?2 {lower}
            ORDER BY source_time_unix, source_trade_id LIMIT {CARRY_BATCH_SIZE}");
        let mut statement = transaction.prepare_cached(&sql)?;
        let mut rows = if let Some((time, id)) = &previous {
            statement.query(params![
                wallet,
                to_i64(base.generation, "base generation")?,
                time,
                id
            ])?
        } else {
            statement.query(params![wallet, to_i64(base.generation, "base generation")?])?
        };
        let mut batch = Vec::new();
        while let Some(row) = rows.next()? {
            batch.push(decode_activity_aggregate(
                (
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ),
                wallet,
            )?);
        }
        drop(rows);
        drop(statement);
        let Some(last) = batch.last() else {
            break;
        };
        let last_key = (
            last.source_time.0.unix_timestamp(),
            last.group_id.key().0.clone(),
        );
        for row in &batch {
            if row.source_time.0.unix_timestamp() <= 0 || row.source_time.0.unix_timestamp() > end {
                return invalid("carried activity outside predecessor window".to_owned());
            }
            digest.push(row)?;
            history.push(row)?;
            count = checked_activity_count(count, 1)?;
            source_rows = checked_activity_count(source_rows, row.row_count)?;
        }
        if let Some(generation) = generation {
            let lower = if previous.is_some() {
                "AND (source_time_unix, source_trade_id) > (?6, ?7)"
            } else {
                ""
            };
            let update = format!(
                "UPDATE activity_groups_v2 SET coverage_generation = ?1
            WHERE wallet_hex = ?2 AND coverage_generation = ?3
            AND (source_time_unix, source_trade_id) <= (?4, ?5) {lower}"
            );
            let changed = if let Some((time, id)) = &previous {
                transaction.execute(
                    &update,
                    params![
                        generation,
                        wallet,
                        to_i64(base.generation, "base generation")?,
                        last_key.0,
                        last_key.1,
                        time,
                        id
                    ],
                )?
            } else {
                transaction.execute(
                    &update,
                    params![
                        generation,
                        wallet,
                        to_i64(base.generation, "base generation")?,
                        last_key.0,
                        last_key.1
                    ],
                )?
            };
            if changed != batch.len() {
                return invalid("carry update count changed".to_owned());
            }
        }
        batches = checked_activity_count(batches, 1)?;
        previous = Some(last_key);
    }
    if digest.finish() != base.ordered_aggregate_digest
        || count != base.aggregate_count
        || source_rows != base.source_row_count
    {
        return invalid("carried history does not match predecessor receipt".to_owned());
    }
    tracing::debug!(
        wallet,
        count,
        batches,
        carried = generation.is_some(),
        "activity predecessor verified with advancing wallet batches"
    );
    Ok(())
}

fn check_collisions(
    connection: &Connection,
    proof: &CollectionProof,
    completion: &WalletActivityCompletion,
) -> Result<bool, BootstrapError> {
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
    let excluded = completion.aggregation_failed || collision;
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
        aggregation_status: if completion.aggregation_failed {
            AggregationStatus::Failed
        } else {
            AggregationStatus::Complete
        },
        fetched_aggregate_digest: if completion.aggregation_failed {
            None
        } else {
            Some(aggregate_digest(&completion.aggregates)?)
        },
        fetched_aggregate_count: if completion.aggregation_failed {
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
        exclusion_reason: if completion.aggregation_failed {
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
            // The PK was checked above; strict INSERT prevents an equal-revision upsert.
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
    };
    proof.validate_receipt(&transaction, &receipt)?;
    transaction.execute("INSERT INTO activity_wallet_coverage_staging_v2
        (generation, wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
         ordered_aggregate_digest, source_row_count, aggregate_count, schema_version, parser_version, completed_at_unix, acquisition_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![generation, wallet, identity.digest, identity.fixed_end_unix, canonical_json(&receipt.pages)?, receipt.ordered_aggregate_digest,
            to_i64(source_rows, "source row count")?, to_i64(count, "aggregate count")?, i64::from(ACTIVITY_SCHEMA_VERSION),
            i64::from(ACTIVITY_PARSER_VERSION), completed_at, canonical_json(&receipt.acquisition)?])?;
    transaction.commit()?;
    if excluded {
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
        sqlite_version = version,
        sqlite_source_id = build,
        "activity writer effective SQLite settings"
    );
    Ok(())
}
