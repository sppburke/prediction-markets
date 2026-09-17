//! Resumable, generation-sealed wallet-cache migration and fixed-path cutover (#544, #545).
//!
//! Version one remains available only as `*_v1_sealed` audit tables. Version-two
//! consumers use the typed `activity_groups_v2` and CLOB payout APIs; no Rust API
//! unions the generations. This makes a cross-generation ranking/state read a
//! schema/API error instead of a filter callers can forget.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::{StreamExt as _, stream};
use pe_core_types::{
    CollateralAmount, MarketId, ReconstructionQuality, ShareAmount, SourceTimestamp, WalletAddress,
};
use pe_position_ledger::{
    EntryClassification, LedgerEffect, LedgerMutation, PositionLedger, SecondVerdict,
    classify_complete_historical_second,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivitySemanticRevision,
    PriceWeightedShareAmount, ReconciliationPageEvidence, SourceActivityGroupComponents,
    SourceActivityGroupId,
};
use pe_source_polymarket_public::{
    ActivityReadError, CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION,
    ClobCoverageManifest, ReconciliationFetcher, fetch_complete_activity,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

mod digests;
use digests::{JsonArrayDigest, ReceiptSetDigest};

use crate::cache::{CACHE_SCHEMA_VERSION_V1, CACHE_SCHEMA_VERSION_V2, REQUIRED_TRADES_INDEXES};
use crate::error::BootstrapError;
use crate::lock::{ForgeActivationLocks, ForgeLockHandoff};
use crate::reclamation_evidence::{
    ReclamationEvidenceReport, capture as capture_reclamation_evidence, eval_results_dir_for_cache,
};

const CACHE_BUILD_MANIFEST_VERSION: u32 = 1;
const FROZEN_PAYLOAD_REFERENCE_VERSION: u32 = 1;
const FINAL_STAGE_RECORD_VERSION: u32 = 2;
const RANKER_CLASSIFIER_VERSION: u32 = 2;
const FRESH_COLLECTION_VERSION: u32 = 1;
const MAX_ACTIVITY_WALLET_FETCHES: usize = 16;

const V2_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sealed_generation_manifests (
    generation                 INTEGER PRIMARY KEY NOT NULL CHECK(generation = 1),
    input_manifest_sha256      TEXT    NOT NULL,
    source_bounds_json         TEXT    NOT NULL,
    cursors_json               TEXT    NOT NULL,
    hashes_json                TEXT    NOT NULL,
    legacy_trade_count         INTEGER NOT NULL,
    legacy_resolution_count    INTEGER NOT NULL,
    legacy_cursor_count        INTEGER NOT NULL,
    sealed_at_unix             INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_coverage_manifests_v2 (
    generation          INTEGER PRIMARY KEY NOT NULL,
    reference_sha256    TEXT    NOT NULL,
    wallet_count        INTEGER NOT NULL,
    receipt_set_digest  TEXT    NOT NULL,
    aggregate_digest    TEXT    NOT NULL,
    source_row_count    INTEGER NOT NULL,
    source_bounds_json  TEXT    NOT NULL,
    cursors_json        TEXT    NOT NULL,
    page_hashes_json    TEXT    NOT NULL,
    group_count         INTEGER NOT NULL,
    schema_version      INTEGER NOT NULL CHECK(schema_version = 2),
    parser_version      INTEGER NOT NULL CHECK(parser_version = 2),
    completed_at_unix   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_wallet_coverage_staging_v2 (
    generation                INTEGER NOT NULL,
    wallet_hex               TEXT    NOT NULL,
    reference_sha256         TEXT    NOT NULL,
    fixed_end_unix           INTEGER NOT NULL,
    page_evidence_json       TEXT    NOT NULL,
    ordered_aggregate_digest TEXT    NOT NULL,
    source_row_count         INTEGER NOT NULL,
    aggregate_count          INTEGER NOT NULL,
    schema_version           INTEGER NOT NULL CHECK(schema_version = 2),
    parser_version           INTEGER NOT NULL CHECK(parser_version = 2),
    completed_at_unix        INTEGER NOT NULL,
    -- Set when the venue's history for this wallet could not be read
    -- deterministically; the wallet is excluded from this generation (#588).
    exclusion_reason         TEXT    NULL,
    PRIMARY KEY (generation, wallet_hex)
);

CREATE TABLE IF NOT EXISTS activity_groups_v2 (
    source_trade_id                  TEXT    PRIMARY KEY NOT NULL
        CHECK(substr(source_trade_id, 1, 3) = 'g2:'),
    coverage_generation             INTEGER NOT NULL,
    semantic_revision               TEXT    NOT NULL,
    components_json                 TEXT    NOT NULL,
    wallet_hex                      TEXT    NOT NULL,
    transaction_hash                TEXT    NOT NULL,
    activity_type                   TEXT    NOT NULL,
    condition_id                    TEXT    NULL,
    asset                           TEXT    NULL,
    outcome_id                      INTEGER NULL,
    side                            TEXT    NULL CHECK(side IN ('buy','sell')),
    row_count                       INTEGER NOT NULL CHECK(row_count > 0),
    share_amount_str                TEXT    NOT NULL,
    price_weighted_share_amount_str TEXT    NOT NULL,
    source_usdc_amount_str          TEXT    NOT NULL,
    source_time_unix                INTEGER NOT NULL,
    is_combo                        INTEGER NOT NULL CHECK(is_combo IN (0,1)),
    schema_version                  INTEGER NOT NULL CHECK(schema_version = 2),
    parser_version                  INTEGER NOT NULL CHECK(parser_version = 2)
);
CREATE INDEX IF NOT EXISTS idx_activity_groups_v2_wallet_time
    ON activity_groups_v2(wallet_hex, source_time_unix, source_trade_id);
CREATE INDEX IF NOT EXISTS idx_activity_groups_v2_condition
    ON activity_groups_v2(condition_id, outcome_id, source_time_unix);

CREATE TABLE IF NOT EXISTS ranker_entries_v2 (
    source_trade_id       TEXT PRIMARY KEY NOT NULL
        CHECK(substr(source_trade_id, 1, 3) = 'g2:'),
    activity_generation  INTEGER NOT NULL,
    classifier_version   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_frozen_payload_verifications (
    reference_sha256          TEXT PRIMARY KEY NOT NULL,
    active_wallets_json       TEXT    NOT NULL,
    freshness_json            TEXT    NOT NULL,
    legacy_trade_count        INTEGER NOT NULL,
    cross_generation_matches  INTEGER NOT NULL CHECK(cross_generation_matches = 0),
    activity_generation       INTEGER NULL,
    fixed_end_unix            INTEGER NULL,
    verified_at_unix          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_v2_migration_state (
    singleton              INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
    phase                  TEXT    NOT NULL CHECK(phase IN (
        'schema_sealed','frozen_payload_verified','finalized'
    )),
    input_manifest_sha256  TEXT    NOT NULL,
    ranker_projection_count INTEGER NULL,
    ranker_projection_digest TEXT NULL,
    ranker_classifier_version INTEGER NULL,
    fresh_collection_json  TEXT    NULL,
    updated_at_unix        INTEGER NOT NULL
);
";

/// Operator-captured identity of the verified online-backup input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheV2BuildManifest {
    pub manifest_version: u32,
    pub backup_sha256: String,
    pub source_bounds: Value,
    pub cursors: Value,
    pub hashes: BTreeMap<String, String>,
    pub sealed_at_unix: i64,
}

/// Complete activity walk needed before a v2 side cache can be finalized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCoverageManifestV2 {
    pub generation: u64,
    pub reference_sha256: String,
    pub wallet_count: u64,
    pub receipt_set_digest: String,
    pub aggregate_digest: String,
    pub source_row_count: u64,
    pub group_count: u64,
    pub source_bounds: Value,
    pub cursors: Value,
    pub page_hashes: Vec<String>,
    pub completed_at_unix: i64,
    pub schema_version: u32,
    pub parser_version: u32,
}

/// Exact cache evidence captured with the accepted publication payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCacheFreshness {
    pub newest_trade_unix: i64,
    pub newest_resolution_fetch_unix: i64,
    pub clob_cursor: String,
    pub clob_cursor_updated_at: i64,
}

type FinalizedProjectionState = (String, Option<i64>, Option<String>, Option<i64>);

/// Versioned identity of one fresh activity collection recorded in the
/// candidate's migration-state singleton (#588). `digest` binds the other
/// fields and is the `reference_sha256` of that generation's receipts and
/// activity manifest; projection rows bind it indirectly through their
/// activity generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FreshCollectionIdentity {
    version: u32,
    generation: u64,
    fixed_end_unix: i64,
    wallets: Vec<String>,
    digest: String,
}

impl FreshCollectionIdentity {
    fn new(
        generation: u64,
        fixed_end_unix: i64,
        wallets: Vec<String>,
    ) -> Result<Self, BootstrapError> {
        let digest = fresh_collection_digest(generation, fixed_end_unix, &wallets)?;
        Ok(Self {
            version: FRESH_COLLECTION_VERSION,
            generation,
            fixed_end_unix,
            wallets,
            digest,
        })
    }

    fn verified(self) -> Result<Self, BootstrapError> {
        if self.version != FRESH_COLLECTION_VERSION {
            return invalid(format!(
                "fresh collection identity version {} is unsupported",
                self.version
            ));
        }
        if self.wallets.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid("fresh collection wallet list is not sorted and unique".to_owned());
        }
        for wallet in &self.wallets {
            validate_wallet_hex(wallet)?;
        }
        if self.digest
            != fresh_collection_digest(self.generation, self.fixed_end_unix, &self.wallets)?
        {
            return invalid("fresh collection identity digest mismatch".to_owned());
        }
        Ok(self)
    }
}

fn fresh_collection_digest(
    generation: u64,
    fixed_end_unix: i64,
    wallets: &[String],
) -> Result<String, BootstrapError> {
    Ok(sha256_bytes(
        canonical_json(&serde_json::json!({
            "version": FRESH_COLLECTION_VERSION,
            "generation": generation,
            "fixed_end_unix": fixed_end_unix,
            "wallets": wallets,
        }))?
        .as_bytes(),
    ))
}

/// The one activity identity a schema-two cache is bound to: a fresh
/// collection record when present, otherwise the legacy frozen-payload binding.
struct ActivityIdentity {
    generation: u64,
    reference_sha256: String,
    fixed_end_unix: i64,
    wallets: Vec<String>,
}

/// Exclusive lower bound that reaches a wallet's full history: it goes on the
/// wire as `start=1`, whereas an omitted `start` returns only the venue's
/// default recent window (docs/15, checked 2026-09-12).
const FULL_HISTORY_START_EXCLUSIVE: Option<i64> = Some(0);

/// Byte-exact cycle staging receipt for the fixed cache (#588).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheStageReport {
    pub fixed_path: PathBuf,
    pub prior_path: PathBuf,
    pub side_path: PathBuf,
    pub prior_schema: i64,
    /// Differs from `prior_schema` only after the initial candidate was sealed.
    pub side_schema: i64,
    pub prior_sha256: Option<String>,
    pub side_sha256: Option<String>,
    pub resumed: bool,
}

/// Supplied frozen reference used to rerun the production active filter against
/// the sealed v1 tables. `ranked_wallets` is the pre-filter universe and
/// `active_wallets` is the accepted set, both canonical lowercase addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenPayloadReference {
    pub version: u32,
    pub process_now_unix: i64,
    pub active_window_hours: u64,
    pub max_cache_staleness_hours: u64,
    pub ranked_wallets: Vec<String>,
    pub active_wallets: Vec<String>,
    pub freshness: FrozenCacheFreshness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheMigrationReport {
    pub cache_path: PathBuf,
    pub input_manifest_sha256: String,
    pub legacy_trade_count: u64,
    pub legacy_resolution_count: u64,
    pub legacy_cursor_count: u64,
    pub resumed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrozenPayloadVerification {
    pub reference_sha256: String,
    pub active_wallets: Vec<String>,
    pub freshness: FrozenCacheFreshness,
    pub legacy_trade_count: u64,
    pub cross_generation_matches: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFinalStageRecord {
    pub version: u32,
    pub cache_path: PathBuf,
    pub cache_sha256: String,
    pub schema_version: i64,
    pub sealed_generation: u64,
    pub activity_coverage_generation: u64,
    pub payout_coverage_generation: u64,
    pub ranker_projection_count: u64,
    pub ranker_projection_digest: String,
    pub ranker_classifier_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheActivationRequest {
    pub fixed_path: PathBuf,
    pub side_path: PathBuf,
    pub prior_cache_backup_path: PathBuf,
    pub expected_side_sha256: String,
}

#[derive(Debug, Serialize)]
pub struct CacheActivationReport {
    pub installed_path: PathBuf,
    pub installed_sha256: String,
    pub prior_cache_backup_path: PathBuf,
    pub prior_cache_sha256: String,
    pub prior_cache_schema: i64,
    pub activation_evidence: Option<ReclamationEvidenceReport>,
    pub resumed: bool,
}

/// Hash/schema binding recorded from activation for a pre-publication restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorCacheBinding {
    pub sha256: String,
    pub schema_version: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurablePublishRequest {
    version: u32,
    batch: Value,
    entries: Vec<Value>,
    keep_batches: u64,
    cache_activation: DurableCacheActivation,
    publish_key: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableCacheActivation {
    side_path: PathBuf,
    fixed_path: PathBuf,
    prior_cache_backup_path: PathBuf,
    expected_sha256: String,
}

/// Authoritative lookup used to decide whether a hash-bound publication was
/// consumed. Failure to obtain an answer must be returned as an error.
pub trait PublicationConsumptionProbe {
    fn was_published<'a>(
        &'a self,
        publish_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, BootstrapError>> + Send + 'a>>;
}

/// Supabase `ranking_batches.publish_key` lookup for prior-cache recovery.
pub struct SupabasePublicationProbe {
    client: reqwest::Client,
    base_url: String,
    key: String,
}

impl SupabasePublicationProbe {
    pub fn new(base_url: String, key: String) -> Result<Self, BootstrapError> {
        if base_url.trim().is_empty() || key.trim().is_empty() {
            return invalid("publication authority credentials are unavailable".to_owned());
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|error| BootstrapError::Invalid {
                    message: format!("publication authority client unavailable: {error}"),
                })?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            key,
        })
    }
}

impl PublicationConsumptionProbe for SupabasePublicationProbe {
    fn was_published<'a>(
        &'a self,
        publish_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, BootstrapError>> + Send + 'a>> {
        Box::pin(async move {
            let response = self
                .client
                .get(format!(
                    "{}/rest/v1/ranking_batches?select=batch_id&publish_key=eq.{}&limit=1",
                    self.base_url, publish_key
                ))
                .header("apikey", &self.key)
                .bearer_auth(&self.key)
                .send()
                .await
                .map_err(|error| BootstrapError::Invalid {
                    message: format!("publication authority unavailable: {error}"),
                })?;
            if !response.status().is_success() {
                return invalid(format!(
                    "publication authority returned HTTP {}",
                    response.status()
                ));
            }
            let rows =
                response
                    .json::<Vec<Value>>()
                    .await
                    .map_err(|error| BootstrapError::Invalid {
                        message: format!(
                            "publication authority returned malformed evidence: {error}"
                        ),
                    })?;
            Ok(!rows.is_empty())
        })
    }
}

/// Populate per-wallet v2 activity checkpoints for one verified frozen universe.
///
/// The frozen reference is revalidated before any source call. Up to sixteen
/// wallet reads run concurrently through the caller's one shared fetcher; only
/// each wallet's aggregate-and-receipt SQLite transaction is serialized here.
pub async fn populate_activity_v2(
    cache_path: &Path,
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    frozen_reference_path: &Path,
    fixed_end_unix: i64,
    generation: u64,
    completed_at_unix: i64,
) -> Result<ActivityCoverageManifestV2, BootstrapError> {
    let schema_connection = open_existing_rw(cache_path)?;
    require_schema(&schema_connection, CACHE_SCHEMA_VERSION_V2)?;
    ensure_lane_a_v2_schema(&schema_connection)?;
    schema_connection.close().map_err(|(_, error)| error)?;
    let verification =
        verify_frozen_payload_v1(cache_path, frozen_reference_path, completed_at_unix)?;
    let mut wallets = verification.active_wallets;
    wallets.sort();
    wallets.dedup();

    let connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    bind_frozen_activity_identity(
        &connection,
        &verification.reference_sha256,
        generation,
        fixed_end_unix,
    )?;
    let identity = ActivityIdentity {
        generation,
        reference_sha256: verification.reference_sha256,
        fixed_end_unix,
        wallets,
    };
    // The legacy reference reads keep their authentic request shape so a
    // resumed legacy collection matches its recorded receipts.
    collect_activity_v2(
        connection,
        fetcher,
        base_url,
        &identity,
        None,
        completed_at_unix,
    )
    .await
}

/// Collect complete activity for a fresh private generation without a frozen
/// payload reference (#588).
///
/// A generation that is not yet recorded is started atomically: its wallet
/// union and end are recorded in the migration-state singleton, finalization is
/// invalidated, and the candidate's superseded projection, activity rows,
/// receipts and manifests are cleared. Repeating a recorded generation keeps
/// its recorded end and wallet list and fetches only wallets without a valid
/// receipt; a completed generation returns its manifest without any source
/// call. `new_generation_end_unix` bounds only a newly started generation.
pub async fn populate_activity_fresh_v2(
    cache_path: &Path,
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    generation: u64,
    new_generation_end_unix: i64,
    completed_at_unix: i64,
) -> Result<ActivityCoverageManifestV2, BootstrapError> {
    let mut connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    ensure_lane_a_v2_schema(&connection)?;
    let record = begin_or_resume_fresh_collection(
        &mut connection,
        generation,
        new_generation_end_unix,
        completed_at_unix,
    )?;
    let identity = ActivityIdentity {
        generation: record.generation,
        reference_sha256: record.digest,
        fixed_end_unix: record.fixed_end_unix,
        wallets: record.wallets,
    };
    collect_activity_v2(
        connection,
        fetcher,
        base_url,
        &identity,
        FULL_HISTORY_START_EXCLUSIVE,
        completed_at_unix,
    )
    .await
}

fn begin_or_resume_fresh_collection(
    connection: &mut Connection,
    generation: u64,
    fixed_end_unix: i64,
    started_at_unix: i64,
) -> Result<FreshCollectionIdentity, BootstrapError> {
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let recorded = fresh_collection_record(&transaction)?;
    if let Some(record) = recorded.as_ref() {
        if record.generation == generation {
            return Ok(record.clone());
        }
        // Starting a generation clears the candidate's retained activity, so an
        // unfinished collection can only be resumed: its frozen universe is
        // otherwise no longer derivable from this copy. Only a manifest that
        // proves the recorded identity counts as complete.
        let completed = completed_activity_manifest(
            &transaction,
            record.generation,
            &record.digest,
            record.fixed_end_unix,
            &record.wallets,
        )?;
        if completed.is_none() {
            return invalid(format!(
                "fresh activity generation {} is incomplete; resume it instead of starting {generation}",
                record.generation
            ));
        }
    }
    let generation_i64 = to_i64(generation, "fresh activity generation")?;
    let mut known = recorded
        .as_ref()
        .map(|record| to_i64(record.generation, "fresh activity generation"))
        .transpose()?;
    for sql in [
        "SELECT MAX(generation) FROM activity_coverage_manifests_v2",
        "SELECT MAX(generation) FROM activity_wallet_coverage_staging_v2",
        "SELECT MAX(activity_generation) FROM cache_frozen_payload_verifications",
    ] {
        let value: Option<i64> = transaction.query_row(sql, [], |row| row.get(0))?;
        known = known.max(value);
    }
    if known.is_some_and(|known| generation_i64 <= known) {
        return invalid(format!(
            "fresh activity generation {generation} must exceed the recorded generation {}",
            known.unwrap_or_default()
        ));
    }

    // The union is read before any clearing: current acquisition candidates plus
    // every wallet with retained history in this byte copy of the immutable
    // prior (activity from a prior fresh/legacy collection, else the sealed
    // schema-one trades of an initial candidate).
    let mut wallets = BTreeSet::new();
    let retained_activity = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM activity_coverage_manifests_v2)",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    let retained_sql = if retained_activity {
        "SELECT DISTINCT wallet_hex FROM activity_groups_v2"
    } else {
        "SELECT DISTINCT wallet_hex FROM trades_v1_sealed"
    };
    for sql in [
        "SELECT wallet_hex FROM active_tradeable_wallets",
        retained_sql,
    ] {
        let mut statement = transaction.prepare(sql)?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for wallet in rows {
            let wallet = wallet?.to_ascii_lowercase();
            validate_wallet_hex(&wallet)?;
            wallets.insert(wallet);
        }
    }
    // A wallet the prior's collection excluded (`collect_activity_v2`) retains
    // no history but keeps its place in the union, so the exclusion stays
    // local to the generation that recorded it and the next collection reads
    // the wallet's history again. The prior's newest manifest carries that
    // generation's receipts.
    let prior_manifest: Option<(i64, String)> = transaction
        .query_row(
            "SELECT generation, cursors_json FROM activity_coverage_manifests_v2
             ORDER BY generation DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((generation, cursors)) = prior_manifest {
        // Bind the selected manifest to the activity identity before choosing
        // its receipt representation. Fresh predecessors were verified above;
        // frozen predecessors use the same verifier even for legacy arrays.
        let identity = activity_identity(&transaction)?;
        if to_i64(identity.generation, "activity generation")? != generation {
            return invalid("prior activity manifest generation mismatch".to_owned());
        }
        if recorded.is_none() {
            completed_activity_manifest(
                &transaction,
                identity.generation,
                &identity.reference_sha256,
                identity.fixed_end_unix,
                &identity.wallets,
            )?
            .ok_or_else(|| BootstrapError::Invalid {
                message: "prior activity manifest is missing".to_owned(),
            })?;
        }
        let cursors: Value = serde_json::from_str(&cursors)?;
        let mut retain_excluded =
            |wallet: String, count, pages: &[ReconciliationPageEvidence], reason: Option<&str>| {
                if is_excluded_receipt(count, pages, reason) {
                    let wallet = wallet.to_ascii_lowercase();
                    validate_wallet_hex(&wallet)?;
                    wallets.insert(wallet);
                }
                Ok::<_, BootstrapError>(())
            };
        if uses_retained_receipts(&cursors)? {
            let mut statement = transaction.prepare(
                "SELECT wallet_hex, aggregate_count, page_evidence_json, exclusion_reason
                 FROM activity_wallet_coverage_staging_v2
                 WHERE generation = ?1 ORDER BY wallet_hex",
            )?;
            let mut rows = statement.query(params![generation])?;
            while let Some(row) = rows.next()? {
                let pages: Vec<ReconciliationPageEvidence> =
                    serde_json::from_str(&row.get::<_, String>(2)?)?;
                let reason: Option<String> = row.get(3)?;
                retain_excluded(
                    row.get(0)?,
                    to_u64(row.get(1)?, "activity aggregate count")?,
                    &pages,
                    reason.as_deref(),
                )?;
            }
        } else {
            let receipts: Vec<ActivityWalletReceiptProof> = serde_json::from_value(cursors)?;
            for receipt in receipts {
                retain_excluded(
                    receipt.wallet_hex,
                    receipt.aggregate_count,
                    &receipt.pages,
                    receipt.exclusion_reason.as_deref(),
                )?;
            }
        }
    }
    let record =
        FreshCollectionIdentity::new(generation, fixed_end_unix, wallets.into_iter().collect())?;
    transaction.execute(
        "UPDATE cache_v2_migration_state
         SET fresh_collection_json = ?1, phase = 'schema_sealed',
             ranker_projection_count = NULL, ranker_projection_digest = NULL,
             ranker_classifier_version = NULL, updated_at_unix = ?2
         WHERE singleton = 1",
        params![canonical_json(&record)?, started_at_unix],
    )?;
    transaction.execute_batch(
        "DELETE FROM ranker_entries_v2;
         DELETE FROM activity_groups_v2;
         DELETE FROM activity_wallet_coverage_staging_v2;
         DELETE FROM activity_coverage_manifests_v2;",
    )?;
    transaction.commit()?;
    Ok(record)
}

fn fresh_collection_record(
    connection: &Connection,
) -> Result<Option<FreshCollectionIdentity>, BootstrapError> {
    // Historical caches finalized before #588 lack the column and are read
    // without being upgraded; they carry only the legacy identity.
    let column_present: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('cache_v2_migration_state')
                       WHERE name = 'fresh_collection_json')",
        [],
        |row| row.get(0),
    )?;
    if !column_present {
        return Ok(None);
    }
    let stored: Option<String> = connection
        .query_row(
            "SELECT fresh_collection_json FROM cache_v2_migration_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    stored
        .map(|json| serde_json::from_str::<FreshCollectionIdentity>(&json)?.verified())
        .transpose()
}

async fn collect_activity_v2(
    mut connection: Connection,
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    identity: &ActivityIdentity,
    history_start_exclusive: Option<i64>,
    completed_at_unix: i64,
) -> Result<ActivityCoverageManifestV2, BootstrapError> {
    let ActivityIdentity {
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    } = identity;
    let (generation, fixed_end_unix) = (*generation, *fixed_end_unix);
    if let Some(manifest) = completed_activity_manifest(
        &connection,
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    )? {
        return Ok(manifest);
    }
    // Wallet writes are atomic. An intact receipt now skips its wallet even if
    // a group was later deleted: completion re-verifies every wallet's content.
    let completed = validate_activity_receipts(
        &connection,
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    )?;
    let missing = wallets
        .iter()
        .filter(|wallet| !completed.contains(*wallet))
        .cloned()
        .collect::<Vec<_>>();

    let reads = stream::iter(missing.into_iter().map(|wallet_hex| async move {
        let wallet =
            WalletAddress::from_hex(&wallet_hex).map_err(|error| BootstrapError::Invalid {
                message: format!("frozen universe contains invalid wallet {wallet_hex}: {error}"),
            })?;
        // A venue payload this parser cannot represent (observed: a TRADE row
        // priced 3.1968021978, outside the unit interval) excludes the wallet
        // like an unaggregatable history: the read failed before any page
        // evidence survived, so the receipt's reason is the whole record.
        let complete = match fetch_complete_activity(
            fetcher,
            base_url,
            wallet,
            history_start_exclusive,
            fixed_end_unix,
        )
        .await
        {
            Ok(complete) => complete,
            Err(error) if excludes_wallet(&error) => {
                tracing::warn!(
                    wallet = %wallet_hex,
                    generation,
                    %error,
                    "activity wallet excluded from the generation: venue history cannot be read"
                );
                return Ok::<_, BootstrapError>(WalletActivityCompletion {
                    wallet_hex,
                    pages: Vec::new(),
                    aggregates: Vec::new(),
                    source_row_count: 0,
                    exclusion_reason: Some(error.to_string()),
                });
            }
            Err(error) => return Err(activity_read_failure(&wallet_hex, error)),
        };
        let mut exclusion_reason: Option<String> = None;
        let (aggregates, source_row_count) = match complete.buckets() {
            Ok(buckets) => {
                let mut aggregates = buckets.into_iter().flatten().collect::<Vec<_>>();
                aggregates.sort_by(|left, right| {
                    left.source_time
                        .0
                        .unix_timestamp()
                        .cmp(&right.source_time.0.unix_timestamp())
                        .then_with(|| left.group_id.key().0.cmp(&right.group_id.key().0))
                });
                let source_row_count = u64::try_from(complete.rows.len()).map_err(|_| {
                    BootstrapError::Invalid {
                        message: format!("activity source-row count overflow for {wallet_hex}"),
                    }
                })?;
                (aggregates, source_row_count)
            }
            // A history the aggregator cannot bucket deterministically (observed:
            // one fill reported as two rows with different venue timestamps)
            // excludes this wallet from the generation instead of failing the
            // cycle: its receipt keeps the fetched page evidence with zero
            // aggregates (`is_excluded_receipt`), so the ranker never sees the
            // wallet, the resume does not refetch it, every other wallet keeps
            // collecting, and the next generation reads the wallet again.
            Err(error) if excludes_wallet(&error) => {
                tracing::warn!(
                    wallet = %wallet_hex,
                    generation,
                    %error,
                    "activity wallet excluded from the generation: history cannot be aggregated deterministically"
                );
                exclusion_reason = Some(error.to_string());
                (Vec::new(), 0)
            }
            Err(error) => {
                return Err(BootstrapError::Polymarket {
                    wallet: wallet_hex,
                    message: error.to_string(),
                });
            }
        };
        Ok::<_, BootstrapError>(WalletActivityCompletion {
            wallet_hex,
            pages: complete.pages,
            aggregates,
            source_row_count,
            exclusion_reason,
        })
    }))
    .buffer_unordered(MAX_ACTIVITY_WALLET_FETCHES);
    // One reader batch can queue behind the serial writer. A full queue pauses
    // polling the bounded reader stream; no wallet completion is dropped on success.
    let (sender, mut receiver) = mpsc::channel(MAX_ACTIVITY_WALLET_FETCHES);
    let (finished_sender, mut finished_receiver) = oneshot::channel();
    // Arbitrate at the point of failure, including a writer failure while the
    // producer is polling a read. A later drain failure cannot replace it.
    let first_error = Arc::new(OnceLock::new());
    let writer_error = Arc::clone(&first_error);
    let writer_reference = reference_sha256.clone();
    let writer = std::thread::Builder::new()
        .name("activity-cache-writer".to_owned())
        .spawn(move || {
            while let Some(completion) = receiver.blocking_recv() {
                if let Err(error) = commit_activity_wallet_v2(
                    &mut connection,
                    generation,
                    &writer_reference,
                    fixed_end_unix,
                    completed_at_unix,
                    &completion,
                ) {
                    let _ = writer_error.set(error);
                    break;
                }
            }
            // Free any queued completions before signalling, so the collector's
            // synchronous join never waits on their destruction.
            drop(receiver);
            let _ = finished_sender.send(());
            connection
        })?;
    let writer_finished = {
        let produce = async {
            futures::pin_mut!(reads);
            while let Some(completion) = reads.next().await {
                match completion {
                    Ok(completion) => {
                        if sender.send(completion).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = first_error.set(error);
                        break;
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = &mut finished_receiver => true,
            () = produce => false,
        }
    };
    // Stop reads, close the queue and drain accepted wallets after a read error.
    // Await termination before joining so SQLite cannot block the async runtime.
    // No return path after spawn may bypass this join: the caller holds the lock.
    drop(sender);
    if !writer_finished {
        let _ = finished_receiver.await;
    }
    let joined = writer.join();
    if let Some(error) = Arc::try_unwrap(first_error)
        .map_err(|_| BootstrapError::Internal)?
        .into_inner()
    {
        return Err(error);
    }
    let connection = joined.map_err(|_| BootstrapError::Cache {
        message: "activity cache writer thread panicked".to_owned(),
    })?;

    let staged = validate_activity_staging(
        &connection,
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    )?;
    let excluded = staged.excluded_count;
    if excluded > 0 {
        tracing::warn!(
            generation,
            excluded,
            "activity wallets excluded from the generation: their histories could not be aggregated deterministically"
        );
    }
    Ok(staged.into_manifest(
        generation,
        reference_sha256.clone(),
        fixed_end_unix,
        completed_at_unix,
    ))
}

// A source read that exhausted the fetcher's transient retries, or that the
// venue rate-limited, is the supervised temporary failure (exit 75), the same
// classification `clob.rs`/`events.rs` use; every completed wallet keeps its
// durable receipt, so the retry fetches only the remainder.
// A venue payload this collector cannot represent deterministically excludes
// one wallet; transport failures and locally generated requests do not (#588).
fn excludes_wallet(error: &ActivityReadError) -> bool {
    matches!(
        error,
        ActivityReadError::Parse(_)
            | ActivityReadError::Aggregate(_)
            | ActivityReadError::Identity(_)
            | ActivityReadError::RowOutsideBounds { .. }
    )
}

fn activity_read_failure(wallet_hex: &str, error: ActivityReadError) -> BootstrapError {
    match error {
        ActivityReadError::Fetch {
            source: SourceError::Transient { .. } | SourceError::RateLimited { .. },
            ..
        } => BootstrapError::TransientSource {
            source_name: "polymarket-activity",
            message: format!("{wallet_hex}: {error}"),
        },
        other => BootstrapError::Polymarket {
            wallet: wallet_hex.to_owned(),
            message: other.to_string(),
        },
    }
}

/// Seal a verified hash-qualified online backup as generation two.
///
/// `PRAGMA quick_check` runs before mutation. A WAL backup is checkpointed
/// with `wal_checkpoint(TRUNCATE)` and must report `busy=0` and every frame
/// checkpointed; otherwise migration refuses rather than guessing whether the
/// main file contains the committed tail.
pub fn migrate_cache_v2(
    cache_path: &Path,
    manifest_path: &Path,
) -> Result<CacheMigrationReport, BootstrapError> {
    require_regular_file(cache_path, "cache online backup")?;
    require_regular_file(manifest_path, "cache build manifest")?;
    let manifest_bytes = std::fs::read(manifest_path)?;
    let manifest: CacheV2BuildManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.manifest_version != CACHE_BUILD_MANIFEST_VERSION {
        return invalid(format!(
            "cache build manifest version {} is unsupported",
            manifest.manifest_version
        ));
    }
    validate_hex_sha256(&manifest.backup_sha256, "backup_sha256")?;
    let before_hash = sha256_file(cache_path)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);

    let mut connection = open_existing_rw(cache_path)?;
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found == CACHE_SCHEMA_VERSION_V2 {
        ensure_lane_a_v2_schema(&connection)?;
        quick_check(&connection)?;
        checkpoint_truncate(&connection)?;
        let stored: Option<(String, i64, i64, i64)> = connection
            .query_row(
                "SELECT input_manifest_sha256, legacy_trade_count, legacy_resolution_count, \
                        legacy_cursor_count FROM sealed_generation_manifests WHERE generation = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((stored_hash, trades, resolutions, cursors)) = stored else {
            return invalid("v2 cache omitted its sealed-generation manifest".to_owned());
        };
        if stored_hash != manifest_sha256 {
            return invalid("v2 cache was built from a different manifest".to_owned());
        }
        connection.close().map_err(|(_, error)| error)?;
        sync_file_and_parent(cache_path)?;
        return Ok(CacheMigrationReport {
            cache_path: std::fs::canonicalize(cache_path)?,
            input_manifest_sha256: manifest_sha256,
            legacy_trade_count: to_u64(trades, "legacy trade count")?,
            legacy_resolution_count: to_u64(resolutions, "legacy resolution count")?,
            legacy_cursor_count: to_u64(cursors, "legacy cursor count")?,
            resumed: true,
        });
    }
    if before_hash != manifest.backup_sha256 {
        return invalid(format!(
            "online-backup hash mismatch: manifest {}, file {before_hash}",
            manifest.backup_sha256
        ));
    }
    verify_manifest_wal_binding(cache_path, &manifest)?;
    quick_check(&connection)?;
    checkpoint_truncate(&connection)?;
    require_reclamation_ready(&connection)?;
    if found != 0 && found != CACHE_SCHEMA_VERSION_V1 {
        return invalid(format!("cannot migrate cache schema version {found}"));
    }
    for table in ["trades", "market_resolutions", "source_cursor"] {
        require_table(&connection, table)?;
    }

    let legacy_trade_count = count_rows(&connection, "trades")?;
    let legacy_resolution_count = count_rows(&connection, "market_resolutions")?;
    let legacy_cursor_count = count_rows(&connection, "source_cursor")?;
    let source_bounds_json = canonical_json(&manifest.source_bounds)?;
    let cursors_json = canonical_json(&manifest.cursors)?;
    let hashes_json = canonical_json(&manifest.hashes)?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "ALTER TABLE trades RENAME TO trades_v1_sealed;
         ALTER TABLE market_resolutions RENAME TO market_resolutions_v1_sealed;
         ALTER TABLE source_cursor RENAME TO source_cursor_v1_sealed;",
    )?;
    transaction.execute_batch(V2_SCHEMA)?;
    ensure_lane_a_v2_schema(&transaction)?;
    transaction.execute(
        "INSERT INTO sealed_generation_manifests
             (generation, input_manifest_sha256, source_bounds_json, cursors_json, hashes_json,
              legacy_trade_count, legacy_resolution_count, legacy_cursor_count, sealed_at_unix)
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            manifest_sha256,
            source_bounds_json,
            cursors_json,
            hashes_json,
            legacy_trade_count,
            legacy_resolution_count,
            legacy_cursor_count,
            manifest.sealed_at_unix,
        ],
    )?;
    transaction.execute(
        "INSERT INTO cache_v2_migration_state
             (singleton, phase, input_manifest_sha256, updated_at_unix)
         VALUES (1, 'schema_sealed', ?1, ?2)",
        params![manifest_sha256, manifest.sealed_at_unix],
    )?;
    transaction.pragma_update(None, "user_version", CACHE_SCHEMA_VERSION_V2)?;
    transaction.commit()?;
    quick_check(&connection)?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    sync_file_and_parent(cache_path)?;

    Ok(CacheMigrationReport {
        cache_path: std::fs::canonicalize(cache_path)?,
        input_manifest_sha256: manifest_sha256,
        legacy_trade_count: to_u64(legacy_trade_count, "legacy trade count")?,
        legacy_resolution_count: to_u64(legacy_resolution_count, "legacy resolution count")?,
        legacy_cursor_count: to_u64(legacy_cursor_count, "legacy cursor count")?,
        resumed: false,
    })
}

struct WalletActivityCompletion {
    wallet_hex: String,
    pages: Vec<ReconciliationPageEvidence>,
    aggregates: Vec<ActivityAggregate>,
    /// Why this wallet is excluded from the generation, when it is. A read that
    /// failed while parsing has no page evidence to keep, so the reason is the
    /// only durable marker of the exclusion.
    exclusion_reason: Option<String>,
    /// Rows that entered `aggregates`; zero for a wallet excluded from the
    /// generation, whose `pages` still record the rows the venue returned.
    source_row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityWalletReceiptProof {
    wallet_hex: String,
    pages: Vec<ReconciliationPageEvidence>,
    ordered_aggregate_digest: String,
    source_row_count: u64,
    aggregate_count: u64,
    schema_version: u32,
    parser_version: u32,
    /// Absent for every ordinary receipt, so a proof's bytes are unchanged
    /// unless the wallet was excluded (#588).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exclusion_reason: Option<String>,
}

/// A wallet excluded from its generation by `collect_activity_v2`: its receipt
/// records the reason, or (for receipts written before that column existed)
/// carries page rows that produced no aggregates. A wallet with no history has
/// zero aggregates too, but no reason and no page rows.
fn is_excluded_receipt(
    aggregate_count: u64,
    pages: &[ReconciliationPageEvidence],
    exclusion_reason: Option<&str>,
) -> bool {
    exclusion_reason.is_some()
        || (aggregate_count == 0 && pages.iter().any(|page| page.row_count > 0))
}

fn retained_receipt_marker() -> Value {
    serde_json::json!({"receipt_storage": "activity_wallet_coverage_staging_v2", "version": 1})
}

// Arrays alone select authentic historical manifests. No unknown object can
// fall back to that reader, even when its retained rows are absent or damaged.
fn uses_retained_receipts(cursors: &Value) -> Result<bool, BootstrapError> {
    if cursors.is_array() {
        Ok(false)
    } else if cursors == &retained_receipt_marker() {
        Ok(true)
    } else {
        invalid("unknown activity receipt storage marker".to_owned())
    }
}

struct ValidatedActivityStaging {
    wallet_count: u64,
    excluded_count: u64,
    source_row_count: u64,
    group_count: u64,
    aggregate_digest: String,
    receipt_set_digest: String,
}

impl ValidatedActivityStaging {
    fn into_manifest(
        self,
        generation: u64,
        reference_sha256: String,
        fixed_end_unix: i64,
        completed_at_unix: i64,
    ) -> ActivityCoverageManifestV2 {
        ActivityCoverageManifestV2 {
            generation,
            reference_sha256,
            wallet_count: self.wallet_count,
            receipt_set_digest: self.receipt_set_digest,
            aggregate_digest: self.aggregate_digest,
            source_row_count: self.source_row_count,
            group_count: self.group_count,
            source_bounds: serde_json::json!({
                "start_exclusive": null,
                "end_inclusive": fixed_end_unix,
                "wallet_count": self.wallet_count,
            }),
            cursors: retained_receipt_marker(),
            page_hashes: Vec::new(),
            completed_at_unix,
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
        }
    }
}

// Both staging and completed representations validate wallet content through
// this owner. Only a wallet's typed aggregates and serialization are resident.
struct ActivityValidation {
    wallet_count: u64,
    excluded_count: u64,
    source_row_count: u64,
    group_count: u64,
    aggregates: JsonArrayDigest,
    receipts: ReceiptSetDigest,
}

impl ActivityValidation {
    fn new(generation: u64, reference: &str, end: i64) -> Result<Self, BootstrapError> {
        Ok(Self {
            wallet_count: 0,
            excluded_count: 0,
            source_row_count: 0,
            group_count: 0,
            aggregates: JsonArrayDigest::new(),
            receipts: ReceiptSetDigest::new(generation, reference, end)?,
        })
    }

    fn visit(
        &mut self,
        connection: &Connection,
        generation: i64,
        receipt: &ActivityWalletReceiptProof,
    ) -> Result<(), BootstrapError> {
        let aggregates = load_activity_aggregates(connection, generation, &receipt.wallet_hex)?;
        let source_rows = aggregates.iter().try_fold(0_u64, |total, aggregate| {
            checked_activity_count(total, aggregate.row_count)
        })?;
        let json = canonical_json(&aggregates)?;
        if receipt.aggregate_count
            != u64::try_from(aggregates.len()).map_err(|_| BootstrapError::Internal)?
            || receipt.source_row_count != source_rows
            || receipt.ordered_aggregate_digest != sha256_bytes(json.as_bytes())
        {
            return invalid(format!(
                "activity receipt aggregate mismatch for {}",
                receipt.wallet_hex
            ));
        }
        self.aggregates.extend_array(&json)?;
        self.receipts.push(receipt)?;
        self.wallet_count = checked_activity_count(self.wallet_count, 1)?;
        self.excluded_count = checked_activity_count(
            self.excluded_count,
            u64::from(is_excluded_receipt(
                receipt.aggregate_count,
                &receipt.pages,
                receipt.exclusion_reason.as_deref(),
            )),
        )?;
        self.source_row_count = checked_activity_count(self.source_row_count, source_rows)?;
        self.group_count = checked_activity_count(self.group_count, receipt.aggregate_count)?;
        Ok(())
    }

    fn finish(self) -> ValidatedActivityStaging {
        ValidatedActivityStaging {
            wallet_count: self.wallet_count,
            excluded_count: self.excluded_count,
            source_row_count: self.source_row_count,
            group_count: self.group_count,
            aggregate_digest: self.aggregates.finish(),
            receipt_set_digest: self.receipts.finish(),
        }
    }
}

fn checked_activity_count(total: u64, count: u64) -> Result<u64, BootstrapError> {
    total
        .checked_add(count)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "activity count overflow".to_owned(),
        })
}

fn bind_frozen_activity_identity(
    connection: &Connection,
    reference_sha256: &str,
    generation: u64,
    fixed_end_unix: i64,
) -> Result<(), BootstrapError> {
    let generation = to_i64(generation, "activity generation")?;
    let changed = connection.execute(
        "UPDATE cache_frozen_payload_verifications
         SET activity_generation = COALESCE(activity_generation, ?2),
             fixed_end_unix = COALESCE(fixed_end_unix, ?3)
         WHERE reference_sha256 = ?1
           AND (activity_generation IS NULL OR activity_generation = ?2)
           AND (fixed_end_unix IS NULL OR fixed_end_unix = ?3)",
        params![reference_sha256, generation, fixed_end_unix],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        invalid("frozen payload was rebound to another activity generation or end".to_owned())
    }
}

fn commit_activity_wallet_v2(
    connection: &mut Connection,
    generation: u64,
    reference_sha256: &str,
    fixed_end_unix: i64,
    completed_at_unix: i64,
    completion: &WalletActivityCompletion,
) -> Result<(), BootstrapError> {
    let generation_i64 = to_i64(generation, "activity generation")?;
    let aggregate_count =
        u64::try_from(completion.aggregates.len()).map_err(|_| BootstrapError::Invalid {
            message: format!(
                "activity aggregate count overflow for {}",
                completion.wallet_hex
            ),
        })?;
    let digest = aggregate_digest(&completion.aggregates)?;
    let page_evidence_json = canonical_json(&completion.pages)?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "DELETE FROM activity_groups_v2
         WHERE coverage_generation = ?1 AND wallet_hex = ?2",
        params![generation_i64, completion.wallet_hex],
    )?;
    for aggregate in &completion.aggregates {
        insert_activity_aggregate(
            &transaction,
            generation_i64,
            &completion.wallet_hex,
            aggregate,
        )?;
    }
    let changed = transaction.execute(
        "INSERT INTO activity_wallet_coverage_staging_v2
             (generation, wallet_hex, reference_sha256, fixed_end_unix,
              page_evidence_json, ordered_aggregate_digest, source_row_count,
              aggregate_count, schema_version, parser_version, completed_at_unix,
              exclusion_reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(generation, wallet_hex) DO UPDATE SET
             reference_sha256 = excluded.reference_sha256,
             fixed_end_unix = excluded.fixed_end_unix,
             page_evidence_json = excluded.page_evidence_json,
             ordered_aggregate_digest = excluded.ordered_aggregate_digest,
             source_row_count = excluded.source_row_count,
             aggregate_count = excluded.aggregate_count,
             schema_version = excluded.schema_version,
             parser_version = excluded.parser_version,
             completed_at_unix = excluded.completed_at_unix,
             exclusion_reason = excluded.exclusion_reason
         WHERE activity_wallet_coverage_staging_v2.reference_sha256 = excluded.reference_sha256
           AND activity_wallet_coverage_staging_v2.fixed_end_unix = excluded.fixed_end_unix
           AND activity_wallet_coverage_staging_v2.page_evidence_json = excluded.page_evidence_json
           AND activity_wallet_coverage_staging_v2.ordered_aggregate_digest = excluded.ordered_aggregate_digest
           AND activity_wallet_coverage_staging_v2.source_row_count = excluded.source_row_count
           AND activity_wallet_coverage_staging_v2.aggregate_count = excluded.aggregate_count
           AND activity_wallet_coverage_staging_v2.schema_version = excluded.schema_version
           AND activity_wallet_coverage_staging_v2.parser_version = excluded.parser_version
           AND COALESCE(activity_wallet_coverage_staging_v2.exclusion_reason, '')
               = COALESCE(excluded.exclusion_reason, '')",
        params![
            generation_i64,
            completion.wallet_hex,
            reference_sha256,
            fixed_end_unix,
            page_evidence_json,
            digest,
            to_i64(completion.source_row_count, "activity source-row count")?,
            to_i64(aggregate_count, "activity aggregate count")?,
            i64::from(ACTIVITY_SCHEMA_VERSION),
            i64::from(ACTIVITY_PARSER_VERSION),
            completed_at_unix,
            completion.exclusion_reason,
        ],
    )?;
    if changed != 1 {
        return invalid(format!(
            "activity wallet {} was replayed with different receipt evidence",
            completion.wallet_hex
        ));
    }
    transaction.commit()?;
    Ok(())
}

fn insert_activity_aggregate(
    transaction: &rusqlite::Transaction<'_>,
    generation: i64,
    expected_wallet: &str,
    aggregate: &ActivityAggregate,
) -> Result<(), BootstrapError> {
    let source_trade_id = aggregate.group_id.key();
    validate_g2_id(&source_trade_id.0)?;
    let components = aggregate.group_id.components();
    if components.wallet.to_string() != expected_wallet {
        return invalid(format!(
            "activity aggregate {} belongs to another wallet",
            source_trade_id.0
        ));
    }
    let side = components.side.map(|value| match value {
        pe_core_types::Side::Buy => "buy",
        pe_core_types::Side::Sell => "sell",
    });
    let mut statement = transaction.prepare_cached(
        "INSERT INTO activity_groups_v2
             (source_trade_id, coverage_generation, semantic_revision, components_json,
              wallet_hex, transaction_hash, activity_type, condition_id, asset, outcome_id,
              side, row_count, share_amount_str, price_weighted_share_amount_str,
              source_usdc_amount_str, source_time_unix, is_combo, schema_version,
              parser_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                 ?15, ?16, ?17, ?18, ?19)
         ON CONFLICT(source_trade_id) DO UPDATE SET
             coverage_generation = excluded.coverage_generation,
             semantic_revision = excluded.semantic_revision,
             components_json = excluded.components_json,
             wallet_hex = excluded.wallet_hex,
             transaction_hash = excluded.transaction_hash,
             activity_type = excluded.activity_type,
             condition_id = excluded.condition_id,
             asset = excluded.asset,
             outcome_id = excluded.outcome_id,
             side = excluded.side,
             row_count = excluded.row_count,
             share_amount_str = excluded.share_amount_str,
             price_weighted_share_amount_str = excluded.price_weighted_share_amount_str,
             source_usdc_amount_str = excluded.source_usdc_amount_str,
             source_time_unix = excluded.source_time_unix,
             is_combo = excluded.is_combo,
             schema_version = excluded.schema_version,
             parser_version = excluded.parser_version
         WHERE activity_groups_v2.semantic_revision = excluded.semantic_revision",
    )?;
    let changed = statement.execute(params![
        source_trade_id.0,
        generation,
        aggregate.semantic_revision.as_str(),
        canonical_json(components)?,
        expected_wallet,
        components.transaction_hash,
        components.activity_type.as_str(),
        components.condition_id.as_ref().map(ToString::to_string),
        components.asset.as_ref().map(ToString::to_string),
        components.outcome.map(|value| i64::from(value.0)),
        side,
        to_i64(aggregate.row_count, "activity row count")?,
        aggregate.share_sum.to_decimal().to_string(),
        aggregate.price_weighted_share_sum.0.to_string(),
        aggregate.source_usdc_sum.to_decimal().to_string(),
        aggregate.source_time.0.unix_timestamp(),
        i64::from(aggregate.is_combo),
        i64::from(ACTIVITY_SCHEMA_VERSION),
        i64::from(ACTIVITY_PARSER_VERSION),
    ])?;
    if changed == 1 {
        Ok(())
    } else {
        invalid(format!(
            "activity group {} was replayed with a different semantic revision",
            source_trade_id.0
        ))
    }
}

fn validate_activity_receipts(
    connection: &Connection,
    generation: u64,
    reference_sha256: &str,
    fixed_end_unix: i64,
    wallets: &[String],
) -> Result<BTreeSet<String>, BootstrapError> {
    let mut completed = BTreeSet::new();
    visit_activity_receipts(
        connection,
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
        |receipt| {
            completed.insert(receipt.wallet_hex);
            Ok(())
        },
    )?;
    Ok(completed)
}

// Stream one receipt at a time through the same identity/shape checks for both
// resume and full content validation. This owner never reads aggregate rows.
fn visit_activity_receipts(
    connection: &Connection,
    generation: u64,
    reference_sha256: &str,
    fixed_end_unix: i64,
    expected: &[String],
    mut visit: impl FnMut(ActivityWalletReceiptProof) -> Result<(), BootstrapError>,
) -> Result<(), BootstrapError> {
    let generation_i64 = to_i64(generation, "activity generation")?;
    let mut statement = connection.prepare(
        "SELECT wallet_hex, reference_sha256, fixed_end_unix, page_evidence_json,
                ordered_aggregate_digest, source_row_count, aggregate_count,
                schema_version, parser_version, exclusion_reason
         FROM activity_wallet_coverage_staging_v2
         WHERE generation = ?1 ORDER BY wallet_hex",
    )?;
    let rows = statement.query_map(params![generation_i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, Option<String>>(9)?,
        ))
    })?;
    for row in rows {
        let row = row?;
        if expected.binary_search(&row.0).is_err() {
            return invalid(format!(
                "activity receipt contains wallet outside frozen universe: {}",
                row.0
            ));
        }
        if row.1 != reference_sha256
            || row.2 != fixed_end_unix
            || row.7 != i64::from(ACTIVITY_SCHEMA_VERSION)
            || row.8 != i64::from(ACTIVITY_PARSER_VERSION)
        {
            return invalid(format!("activity receipt identity mismatch for {}", row.0));
        }
        validate_hex_sha256(&row.4, "ordered aggregate digest")?;
        let pages: Vec<ReconciliationPageEvidence> = serde_json::from_str(&row.3)?;
        if pages.iter().any(|page| {
            page.schema_version != ACTIVITY_SCHEMA_VERSION
                || page.parser_version != ACTIVITY_PARSER_VERSION
        }) {
            return invalid(format!("activity page version mismatch for {}", row.0));
        }
        let aggregate_count = to_u64(row.6, "activity receipt aggregate count")?;
        let source_row_count = to_u64(row.5, "activity receipt source-row count")?;
        if aggregate_count == 0 && source_row_count != 0 {
            return invalid(format!("activity receipt aggregate mismatch for {}", row.0));
        }
        // An excluded wallet contributes no rows; a reason beside aggregates is
        // contradictory evidence.
        if row.9.is_some() && aggregate_count != 0 {
            return invalid(format!(
                "activity receipt records an exclusion reason with aggregates for {}",
                row.0
            ));
        }
        visit(ActivityWalletReceiptProof {
            wallet_hex: row.0,
            pages,
            ordered_aggregate_digest: row.4,
            source_row_count,
            aggregate_count,
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            exclusion_reason: row.9,
        })?;
    }
    Ok(())
}

fn validate_activity_staging(
    connection: &Connection,
    generation: u64,
    reference_sha256: &str,
    fixed_end_unix: i64,
    wallets: &[String],
) -> Result<ValidatedActivityStaging, BootstrapError> {
    let generation_i64 = to_i64(generation, "activity generation")?;
    let mut validation = ActivityValidation::new(generation, reference_sha256, fixed_end_unix)?;
    visit_activity_receipts(
        connection,
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
        |receipt| validation.visit(connection, generation_i64, &receipt),
    )?;
    // The table's primary key makes wallets unique, and the visitor rejects
    // every wallet outside the sorted identity. Equal counts prove completeness.
    if validation.wallet_count
        != u64::try_from(wallets.len()).map_err(|_| BootstrapError::Internal)?
    {
        return invalid("activity coverage is missing frozen wallets".to_owned());
    }
    Ok(validation.finish())
}

fn load_activity_aggregates(
    connection: &Connection,
    generation: i64,
    wallet_hex: &str,
) -> Result<Vec<ActivityAggregate>, BootstrapError> {
    let mut statement = connection.prepare(
        "SELECT source_trade_id, semantic_revision, components_json, row_count,
                share_amount_str, price_weighted_share_amount_str, source_usdc_amount_str,
                source_time_unix, is_combo
         FROM activity_groups_v2
         WHERE coverage_generation = ?1 AND wallet_hex = ?2
         ORDER BY source_time_unix, source_trade_id",
    )?;
    let rows = statement.query_map(params![generation, wallet_hex], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
        ))
    })?;
    let mut aggregates = Vec::new();
    for row in rows {
        let row = row?;
        validate_g2_id(&row.0)?;
        let components: SourceActivityGroupComponents = serde_json::from_str(&row.2)?;
        if components.wallet.to_string() != wallet_hex {
            return invalid(format!("activity component wallet mismatch for {}", row.0));
        }
        let group_id =
            SourceActivityGroupId::derive(components).map_err(|error| BootstrapError::Invalid {
                message: format!("activity identity reconstruction failed: {error}"),
            })?;
        if group_id.key().0 != row.0 {
            return invalid(format!(
                "activity component identity mismatch for {}",
                row.0
            ));
        }
        let semantic_revision: ActivitySemanticRevision =
            serde_json::from_value(Value::String(row.1))?;
        let share_decimal =
            rust_decimal::Decimal::from_str(&row.4).map_err(|error| BootstrapError::Invalid {
                message: format!("invalid activity share amount for {}: {error}", row.0),
            })?;
        let source_usdc_decimal =
            rust_decimal::Decimal::from_str(&row.6).map_err(|error| BootstrapError::Invalid {
                message: format!("invalid activity collateral amount for {}: {error}", row.0),
            })?;
        let weighted =
            rust_decimal::Decimal::from_str(&row.5).map_err(|error| BootstrapError::Invalid {
                message: format!("invalid activity weighted amount for {}: {error}", row.0),
            })?;
        let source_time = OffsetDateTime::from_unix_timestamp(row.7).map_err(|error| {
            BootstrapError::Invalid {
                message: format!("invalid activity timestamp for {}: {error}", row.0),
            }
        })?;
        aggregates.push(ActivityAggregate {
            group_id,
            row_count: to_u64(row.3, "activity aggregate row count")?,
            share_sum: ShareAmount::from_decimal_exact(share_decimal).map_err(|error| {
                BootstrapError::Invalid {
                    message: format!("invalid exact activity shares for {}: {error}", row.0),
                }
            })?,
            price_weighted_share_sum: PriceWeightedShareAmount(weighted),
            source_usdc_sum: CollateralAmount::from_decimal_exact(source_usdc_decimal).map_err(
                |error| BootstrapError::Invalid {
                    message: format!("invalid exact activity collateral for {}: {error}", row.0),
                },
            )?,
            source_time: SourceTimestamp(source_time),
            is_combo: match row.8 {
                0 => false,
                1 => true,
                value => return invalid(format!("invalid activity combo flag {value}")),
            },
            semantic_revision,
        });
    }
    Ok(aggregates)
}

fn aggregate_digest(aggregates: &[ActivityAggregate]) -> Result<String, BootstrapError> {
    Ok(sha256_bytes(canonical_json(aggregates)?.as_bytes()))
}

fn completed_activity_manifest(
    connection: &Connection,
    generation: u64,
    reference_sha256: &str,
    fixed_end_unix: i64,
    wallets: &[String],
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
    verify_activity_manifest(
        connection,
        &manifest,
        reference_sha256,
        fixed_end_unix,
        wallets,
    )?;
    Ok(Some(manifest))
}

fn verify_activity_manifest(
    connection: &Connection,
    manifest: &ActivityCoverageManifestV2,
    reference_sha256: &str,
    fixed_end_unix: i64,
    wallets: &[String],
) -> Result<(), BootstrapError> {
    validate_hex_sha256(&manifest.receipt_set_digest, "receipt set digest")?;
    validate_hex_sha256(&manifest.aggregate_digest, "activity aggregate digest")?;
    let wallet_count = u64::try_from(wallets.len()).map_err(|_| BootstrapError::Internal)?;
    let expected_bounds = serde_json::json!({
        "start_exclusive": null,
        "end_inclusive": fixed_end_unix,
        "wallet_count": wallet_count,
    });
    if manifest.reference_sha256 != reference_sha256
        || manifest.wallet_count != wallet_count
        || manifest.schema_version != ACTIVITY_SCHEMA_VERSION
        || manifest.parser_version != ACTIVITY_PARSER_VERSION
        || canonical_json(&manifest.source_bounds)? != canonical_json(&expected_bounds)?
    {
        return invalid("activity coverage manifest identity mismatch".to_owned());
    }
    let validated = if uses_retained_receipts(&manifest.cursors)? {
        if !manifest.page_hashes.is_empty() {
            return invalid("retained activity manifest has embedded page hashes".to_owned());
        }
        validate_activity_staging(
            connection,
            manifest.generation,
            reference_sha256,
            fixed_end_unix,
            wallets,
        )?
    } else {
        let staged_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1",
            params![to_i64(manifest.generation, "activity generation")?],
            |row| row.get(0),
        )?;
        if staged_count != 0 {
            return invalid("legacy activity manifest retained staging receipts".to_owned());
        }
        let receipts: Vec<ActivityWalletReceiptProof> =
            serde_json::from_value(manifest.cursors.clone())?;
        if receipts.iter().map(|r| &r.wallet_hex).ne(wallets.iter()) {
            return invalid("activity coverage manifest identity mismatch".to_owned());
        }
        let mut expected_page_hashes = receipts
            .iter()
            .flat_map(|receipt| {
                receipt
                    .pages
                    .iter()
                    .map(move |page| format!("{}:{}", receipt.wallet_hex, page.raw_page_hash))
            })
            .collect::<Vec<_>>();
        expected_page_hashes.sort();
        if manifest.page_hashes != expected_page_hashes {
            return invalid("activity coverage manifest identity mismatch".to_owned());
        }
        let mut validation =
            ActivityValidation::new(manifest.generation, reference_sha256, fixed_end_unix)?;
        for receipt in receipts {
            if receipt.schema_version != ACTIVITY_SCHEMA_VERSION
                || receipt.parser_version != ACTIVITY_PARSER_VERSION
                || receipt.pages.iter().any(|page| {
                    page.schema_version != ACTIVITY_SCHEMA_VERSION
                        || page.parser_version != ACTIVITY_PARSER_VERSION
                })
            {
                return invalid("activity manifest receipt version mismatch".to_owned());
            }
            validation.visit(
                connection,
                to_i64(manifest.generation, "activity generation")?,
                &receipt,
            )?;
        }
        validation.finish()
    };
    if manifest.receipt_set_digest != validated.receipt_set_digest {
        return invalid("activity coverage manifest identity mismatch".to_owned());
    }
    if manifest.group_count != validated.group_count
        || manifest.source_row_count != validated.source_row_count
        || manifest.aggregate_digest != validated.aggregate_digest
    {
        return invalid("activity coverage manifest aggregate digest mismatch".to_owned());
    }
    Ok(())
}

/// Re-run the exact active/freshness filter against the sealed v1 generation.
pub fn verify_frozen_payload_v1(
    cache_path: &Path,
    reference_path: &Path,
    verified_at_unix: i64,
) -> Result<FrozenPayloadVerification, BootstrapError> {
    let reference_bytes = std::fs::read(reference_path)?;
    let reference: FrozenPayloadReference = serde_json::from_slice(&reference_bytes)?;
    if reference.version != FROZEN_PAYLOAD_REFERENCE_VERSION {
        return invalid(format!(
            "frozen payload reference version {} is unsupported",
            reference.version
        ));
    }
    let reference_sha256 = sha256_bytes(&reference_bytes);
    let connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    let cutoff = reference
        .process_now_unix
        .checked_sub(hours_to_seconds(reference.active_window_hours)?)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "active-filter cutoff overflow".to_owned(),
        })?;
    let staleness = hours_to_seconds(reference.max_cache_staleness_hours)?;

    let mut active = Vec::new();
    for wallet in &reference.ranked_wallets {
        validate_wallet_hex(wallet)?;
        let last: Option<i64> = connection.query_row(
            "SELECT MAX(timestamp_unix) FROM trades_v1_sealed WHERE wallet_hex = ?1",
            params![wallet],
            |row| row.get(0),
        )?;
        if last.is_some_and(|timestamp| timestamp >= cutoff) {
            active.push(wallet.clone());
        }
    }
    let mut expected = reference.active_wallets.clone();
    for wallet in &expected {
        validate_wallet_hex(wallet)?;
    }
    active.sort();
    active.dedup();
    expected.sort();
    expected.dedup();
    if active != expected {
        return invalid(format!(
            "sealed v1 active-filter mismatch: expected {expected:?}, got {active:?}"
        ));
    }

    let actual_freshness = frozen_freshness(&connection)?;
    if actual_freshness != reference.freshness {
        return invalid(format!(
            "sealed v1 freshness mismatch: expected {:?}, got {actual_freshness:?}",
            reference.freshness
        ));
    }
    for (label, timestamp) in [
        ("trade", actual_freshness.newest_trade_unix),
        ("resolution", actual_freshness.newest_resolution_fetch_unix),
        ("CLOB cursor", actual_freshness.clob_cursor_updated_at),
    ] {
        if reference.process_now_unix.saturating_sub(timestamp) > staleness {
            return invalid(format!(
                "sealed v1 {label} freshness exceeds the supplied bound"
            ));
        }
    }
    if !actual_freshness.clob_cursor.is_empty() {
        return invalid("sealed v1 CLOB cursor is not terminal".to_owned());
    }

    let legacy_trade_count = count_rows(&connection, "trades_v1_sealed")?;
    let cross_generation_matches: i64 = connection.query_row(
        "SELECT COUNT(*) FROM activity_groups_v2 v2
         JOIN trades_v1_sealed v1 ON v1.source_trade_id = v2.source_trade_id",
        [],
        |row| row.get(0),
    )?;
    if cross_generation_matches != 0 {
        return invalid("a sealed v1 trade identity entered activity_groups_v2".to_owned());
    }
    let active_json = canonical_json(&active)?;
    let freshness_json = canonical_json(&actual_freshness)?;
    connection.execute(
        "INSERT INTO cache_frozen_payload_verifications
             (reference_sha256, active_wallets_json, freshness_json, legacy_trade_count,
              cross_generation_matches, verified_at_unix)
         VALUES (?1, ?2, ?3, ?4, 0, ?5)
         ON CONFLICT(reference_sha256) DO UPDATE SET
             active_wallets_json = excluded.active_wallets_json,
             freshness_json = excluded.freshness_json,
             legacy_trade_count = excluded.legacy_trade_count,
             cross_generation_matches = 0",
        params![
            reference_sha256,
            active_json,
            freshness_json,
            legacy_trade_count,
            verified_at_unix,
        ],
    )?;
    connection.execute(
        "UPDATE cache_v2_migration_state
         SET phase = CASE WHEN phase = 'schema_sealed' THEN 'frozen_payload_verified' ELSE phase END,
             updated_at_unix = ?1 WHERE singleton = 1",
        params![verified_at_unix],
    )?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    sync_file_and_parent(cache_path)?;
    Ok(FrozenPayloadVerification {
        reference_sha256,
        active_wallets: active,
        freshness: actual_freshness,
        legacy_trade_count: to_u64(legacy_trade_count, "legacy trade count")?,
        cross_generation_matches: to_u64(cross_generation_matches, "cross-generation match count")?,
    })
}

fn activity_identity(connection: &Connection) -> Result<ActivityIdentity, BootstrapError> {
    if let Some(record) = fresh_collection_record(connection)? {
        return Ok(ActivityIdentity {
            generation: record.generation,
            reference_sha256: record.digest,
            fixed_end_unix: record.fixed_end_unix,
            wallets: record.wallets,
        });
    }
    let rows = connection
        .prepare(
            "SELECT activity_generation, reference_sha256, fixed_end_unix, active_wallets_json
             FROM cache_frozen_payload_verifications
             WHERE activity_generation IS NOT NULL AND fixed_end_unix IS NOT NULL
             ORDER BY activity_generation DESC, reference_sha256",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let Some(selected) = rows.first() else {
        return invalid("frozen activity identity is missing".to_owned());
    };
    if rows.iter().skip(1).any(|row| row.0 == selected.0) {
        return invalid("activity generation is bound to multiple frozen references".to_owned());
    }
    let mut wallets: Vec<String> = serde_json::from_str(&selected.3)?;
    wallets.sort();
    wallets.dedup();
    Ok(ActivityIdentity {
        generation: to_u64(selected.0, "activity generation")?,
        reference_sha256: selected.1.clone(),
        fixed_end_unix: selected.2,
        wallets,
    })
}

fn install_activity_manifest(
    transaction: &rusqlite::Transaction<'_>,
    finalized_at_unix: i64,
) -> Result<ActivityCoverageManifestV2, BootstrapError> {
    let ActivityIdentity {
        generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    } = activity_identity(transaction)?;
    if let Some(manifest) = completed_activity_manifest(
        transaction,
        generation,
        &reference_sha256,
        fixed_end_unix,
        &wallets,
    )? {
        return Ok(manifest);
    }
    let manifest = validate_activity_staging(
        transaction,
        generation,
        &reference_sha256,
        fixed_end_unix,
        &wallets,
    )?
    .into_manifest(
        generation,
        reference_sha256,
        fixed_end_unix,
        finalized_at_unix,
    );
    transaction.execute(
        "INSERT INTO activity_coverage_manifests_v2
             (generation, reference_sha256, wallet_count, receipt_set_digest,
              aggregate_digest, source_row_count, source_bounds_json, cursors_json,
              page_hashes_json, group_count, schema_version, parser_version,
              completed_at_unix)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            to_i64(manifest.generation, "activity generation")?,
            manifest.reference_sha256,
            to_i64(manifest.wallet_count, "activity wallet count")?,
            manifest.receipt_set_digest,
            manifest.aggregate_digest,
            to_i64(manifest.source_row_count, "activity source-row count")?,
            canonical_json(&manifest.source_bounds)?,
            canonical_json(&manifest.cursors)?,
            canonical_json(&manifest.page_hashes)?,
            to_i64(manifest.group_count, "activity group count")?,
            i64::from(manifest.schema_version),
            i64::from(manifest.parser_version),
            manifest.completed_at_unix,
        ],
    )?;
    Ok(manifest)
}

fn rebuild_ranker_projection(
    transaction: &rusqlite::Transaction<'_>,
    activity_generation: u64,
    wallets: &[String],
) -> Result<(u64, String), BootstrapError> {
    let generation = to_i64(activity_generation, "activity generation")?;
    let payout_markets = transaction
        .prepare(
            "SELECT market_id FROM clob_payout_evidence_v2
             WHERE end_date_unix IS NOT NULL
               AND payout_status = 'resolved'
               AND payout_vector_json IN ('[\"1\",\"0\"]','[\"0\",\"1\"]','[\"0.5\",\"0.5\"]')
             ORDER BY market_id",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()?;
    let quality = ReconstructionQuality::new(100).map_err(|error| BootstrapError::Invalid {
        message: format!("bootstrap reconstruction quality is invalid: {error}"),
    })?;
    transaction.execute("DELETE FROM ranker_entries_v2", [])?;
    let mut insert = transaction.prepare(
        "INSERT INTO ranker_entries_v2
             (source_trade_id, activity_generation, classifier_version)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(source_trade_id) DO NOTHING",
    )?;
    for wallet_hex in wallets {
        let wallet =
            WalletAddress::from_hex(wallet_hex).map_err(|error| BootstrapError::Invalid {
                message: format!("frozen universe contains invalid wallet {wallet_hex}: {error}"),
            })?;
        let aggregates = load_activity_aggregates(transaction, generation, wallet_hex)?;
        let mut buckets = BTreeMap::<i64, Vec<ActivityAggregate>>::new();
        for aggregate in aggregates {
            buckets
                .entry(aggregate.source_time.0.unix_timestamp())
                .or_default()
                .push(aggregate);
        }
        let mut ledger = PositionLedger::new();
        let mut history = BTreeSet::<String>::new();
        for aggregates in buckets.into_values() {
            let mutations = match aggregates
                .iter()
                .map(LedgerMutation::from_activity)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(mutations) => mutations,
                Err(_) => break,
            };
            if mutations
                .iter()
                .any(|mutation| matches!(mutation.effect.effective(), LedgerEffect::RequiresAnchor))
            {
                break;
            }
            let decisions = match classify_complete_historical_second(
                &ledger,
                wallet,
                &mutations,
                quality,
                &|market: &MarketId| history.contains(&market.to_string()),
            ) {
                Ok(SecondVerdict::OrderIndependent { decisions, .. }) => decisions,
                Ok(SecondVerdict::OrderDependent { .. }) | Err(_) => break,
            };
            for decision in decisions {
                if decision.entry != EntryClassification::Admitted
                    || decision.amount == ShareAmount::ZERO
                    || !payout_markets.contains(&decision.market_id.to_string())
                {
                    continue;
                }
                let complete_identifiers = aggregates.iter().any(|aggregate| {
                    aggregate.group_id.key() == &decision.source_trade_id
                        && aggregate.group_id.components().condition_id.is_some()
                        && aggregate.group_id.components().asset.is_some()
                        && aggregate.group_id.components().outcome.is_some()
                        && aggregate.group_id.components().side.is_some()
                });
                if complete_identifiers {
                    validate_g2_id(&decision.source_trade_id.0)?;
                    insert.execute(params![
                        decision.source_trade_id.0,
                        generation,
                        i64::from(RANKER_CLASSIFIER_VERSION)
                    ])?;
                }
            }
            if ledger.apply_all_or_none(&mutations).is_err() {
                break;
            }
            for mutation in &mutations {
                for key in mutation.touched_keys() {
                    history.insert(key.market().to_string());
                }
            }
        }
    }

    let digest = ranker_projection_digest(transaction, activity_generation)?;
    let count: i64 =
        transaction.query_row("SELECT COUNT(*) FROM ranker_entries_v2", [], |row| {
            row.get(0)
        })?;
    let count = to_u64(count, "ranker projection count")?;
    Ok((count, digest))
}

fn ranker_projection_digest(
    connection: &Connection,
    activity_generation: u64,
) -> Result<String, BootstrapError> {
    let mut statement = connection.prepare(
        "SELECT ranker.source_trade_id, ranker.activity_generation,
                ranker.classifier_version, groups_v2.wallet_hex,
                groups_v2.condition_id, groups_v2.asset, groups_v2.outcome_id,
                groups_v2.side, groups_v2.share_amount_str,
                groups_v2.price_weighted_share_amount_str,
                groups_v2.source_usdc_amount_str, groups_v2.source_time_unix,
                payout.payout_vector_json, payout.end_date_unix
         FROM ranker_entries_v2 ranker
         JOIN activity_groups_v2 groups_v2
           ON groups_v2.source_trade_id = ranker.source_trade_id
          AND groups_v2.coverage_generation = ranker.activity_generation
         JOIN clob_payout_evidence_v2 payout
           ON payout.market_id = groups_v2.condition_id
         WHERE ranker.activity_generation = ?1
         ORDER BY ranker.source_trade_id",
    )?;
    let rows = statement.query_map(
        params![to_i64(activity_generation, "activity generation")?],
        |row| {
            Ok(serde_json::json!({
                "source_trade_id": row.get::<_, String>(0)?,
                "activity_generation": row.get::<_, i64>(1)?,
                "classifier_version": row.get::<_, i64>(2)?,
                "wallet_hex": row.get::<_, String>(3)?,
                "condition_id": row.get::<_, String>(4)?,
                "asset": row.get::<_, String>(5)?,
                "outcome_id": row.get::<_, i64>(6)?,
                "side": row.get::<_, String>(7)?,
                "share_amount_str": row.get::<_, String>(8)?,
                "price_weighted_share_amount_str": row.get::<_, String>(9)?,
                "source_usdc_amount_str": row.get::<_, String>(10)?,
                "source_time_unix": row.get::<_, i64>(11)?,
                "payout_vector_json": row.get::<_, String>(12)?,
                "end_date_unix": row.get::<_, i64>(13)?,
            }))
        },
    )?;
    let mut digest = JsonArrayDigest::new();
    for row in rows {
        digest.push(&row?)?;
    }
    Ok(digest.finish())
}

/// Close and seal a complete v2 side cache, then emit a hash-bound stage record.
pub fn finalize_cache_v2(
    cache_path: &Path,
    stage_record_path: &Path,
    finalized_at_unix: i64,
) -> Result<CacheFinalStageRecord, BootstrapError> {
    let mut connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    ensure_lane_a_v2_schema(&connection)?;
    quick_check(&connection)?;
    let sealed_generation = required_max(&connection, "sealed_generation_manifests", "generation")?;
    let payout_generation = required_max(
        &connection,
        "clob_payout_coverage_manifests_v2",
        "generation",
    )?;
    verify_payout_coverage(&connection, payout_generation)?;
    let transaction = connection.transaction()?;
    let activity_manifest = install_activity_manifest(&transaction, finalized_at_unix)?;
    let wallets = activity_identity(&transaction)?.wallets;
    let (ranker_projection_count, ranker_projection_digest) =
        rebuild_ranker_projection(&transaction, activity_manifest.generation, &wallets)?;
    transaction.execute(
        "UPDATE cache_v2_migration_state
         SET phase = 'finalized', ranker_projection_count = ?1,
             ranker_projection_digest = ?2, ranker_classifier_version = ?3,
             updated_at_unix = ?4 WHERE singleton = 1",
        params![
            to_i64(ranker_projection_count, "ranker projection count")?,
            ranker_projection_digest,
            i64::from(RANKER_CLASSIFIER_VERSION),
            finalized_at_unix,
        ],
    )?;
    transaction.commit()?;
    checkpoint_truncate(&connection)?;
    quick_check(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(cache_path)?;
    sync_file_and_parent(cache_path)?;
    let record = CacheFinalStageRecord {
        version: FINAL_STAGE_RECORD_VERSION,
        cache_path: std::fs::canonicalize(cache_path)?,
        cache_sha256: sha256_file(cache_path)?,
        schema_version: CACHE_SCHEMA_VERSION_V2,
        sealed_generation: to_u64(sealed_generation, "sealed generation")?,
        activity_coverage_generation: activity_manifest.generation,
        payout_coverage_generation: to_u64(payout_generation, "payout generation")?,
        ranker_projection_count,
        ranker_projection_digest,
        ranker_classifier_version: RANKER_CLASSIFIER_VERSION,
    };
    atomic_write_json(stage_record_path, &record)?;
    Ok(record)
}

/// Stage one cycle's immutable prior and private candidate from the fixed
/// cache as byte-exact copies (#588).
///
/// Under the cache mutation lock the fixed main is checkpointed, integrity
/// checked and closed, then copied to `prior_path` through the private
/// `.pending` → fsync → rename → parent-sync pattern and hash-verified; the
/// candidate is created from that prior the same way. A completed prior is
/// never rewritten and an existing candidate is returned untouched so a retry
/// resumes the cycle's own copy; a candidate without its prior is refused.
pub fn stage_cache_cycle_v2(
    fixed_path: &Path,
    prior_path: &Path,
    side_path: &Path,
    build_manifest_path: Option<&Path>,
) -> Result<CacheStageReport, BootstrapError> {
    require_regular_file(fixed_path, "current fixed cache")?;
    require_same_device(fixed_path, prior_path, side_path)?;
    // The copies delete and rename their `.pending` names, so those must be
    // independent of every role as well.
    let prior_pending = pending_path_for(prior_path);
    let side_pending = pending_path_for(side_path);
    // SQLite creates, truncates or deletes the fixed cache's and the
    // candidate's write-ahead, shared-memory and rollback-journal sidecars
    // when staging opens them, so those names are roles as well.
    let sidecars: Vec<(&str, PathBuf)> = [
        ("current fixed cache sidecar", fixed_path),
        ("private candidate sidecar", side_path),
    ]
    .into_iter()
    .flat_map(|(label, path)| {
        ["-wal", "-shm", "-journal"].map(move |suffix| (label, sidecar_path(path, suffix)))
    })
    .collect();
    let mut roles = vec![
        ("current fixed cache", fixed_path),
        ("immutable prior cache", prior_path),
        ("private candidate cache", side_path),
        ("immutable prior staging file", prior_pending.as_path()),
        ("private candidate staging file", side_pending.as_path()),
    ];
    roles.extend(
        sidecars
            .iter()
            .map(|(label, path)| (*label, path.as_path())),
    );
    // Taking the cache lock creates and rewrites the lock file, so the roles
    // are validated before the lock is taken.
    let lock_target = lock_target_path(fixed_path)?;
    roles.push(("cache mutation lock", lock_target.as_path()));
    // The commands that follow staging take the candidate's own lock.
    let side_lock_target = lock_target_path(side_path)?;
    roles.push(("private candidate lock", side_lock_target.as_path()));
    // The manifest is checked and written under one spelling: its resolved
    // identity, whose parent directory exists. The writer's temporary file
    // beside it is a role too.
    let build_manifest_path = build_manifest_path
        .map(canonical_intended_path)
        .transpose()?;
    let build_manifest_temp = build_manifest_path.as_deref().map(atomic_write_temp_path);
    if let Some(path) = build_manifest_path.as_deref() {
        roles.push(("cache build manifest", path));
    }
    if let Some(path) = build_manifest_temp.as_deref() {
        roles.push(("cache build manifest staging file", path));
    }
    require_distinct_files(&roles)?;
    let _lock = crate::lock::CacheMutationLock::acquire(fixed_path)?;
    if side_path.exists() {
        require_regular_file(side_path, "private candidate cache")?;
        if !prior_path.exists() {
            return invalid(format!(
                "private candidate {} exists without its immutable prior {}",
                side_path.display(),
                prior_path.display()
            ));
        }
        require_regular_file(prior_path, "immutable prior cache")?;
        let report = stage_report(fixed_path, prior_path, side_path, None, true)?;
        // An unsealed initial candidate whose manifest went missing gets it
        // back from the immutable prior; a sealed candidate keeps its recorded
        // input hash in `sealed_generation_manifests` and needs no file.
        if let Some(path) = build_manifest_path.as_deref()
            && report.side_schema != CACHE_SCHEMA_VERSION_V2
            && !path.exists()
        {
            write_build_manifest(prior_path, &sha256_file(prior_path)?, path)?;
        }
        return Ok(report);
    }
    let prior_sha256 = if prior_path.exists() {
        require_regular_file(prior_path, "immutable prior cache")?;
        sha256_file(prior_path)?
    } else {
        let current = open_existing_rw(fixed_path)?;
        checkpoint_truncate(&current)?;
        quick_check(&current)?;
        current.close().map_err(|(_, error)| error)?;
        let fixed_sha256 = sha256_file(fixed_path)?;
        copy_file_atomic_verified(fixed_path, prior_path, Some(&fixed_sha256))?;
        fixed_sha256
    };
    // An initial schema-one candidate is sealed by `cache-migrate-v2` against
    // this authentic hash-bound build manifest; the candidate is a byte copy
    // of the verified prior, so the prior hash is the backup hash. Written
    // before the candidate is adopted so an interrupted cycle resumes with it.
    if let Some(path) = build_manifest_path.as_deref()
        && verified_user_version(prior_path)? != CACHE_SCHEMA_VERSION_V2
        && !path.exists()
    {
        write_build_manifest(prior_path, &prior_sha256, path)?;
    }
    copy_file_atomic_verified(prior_path, side_path, Some(&prior_sha256))?;
    stage_report(fixed_path, prior_path, side_path, Some(prior_sha256), false)
}

fn write_build_manifest(
    prior_path: &Path,
    prior_sha256: &str,
    path: &Path,
) -> Result<(), BootstrapError> {
    let prior = open_immutable(prior_path)?;
    let newest_trade: Option<i64> =
        prior.query_row("SELECT MAX(timestamp_unix) FROM trades", [], |row| {
            row.get(0)
        })?;
    let newest_resolution: Option<i64> = prior.query_row(
        "SELECT MAX(fetched_at_unix) FROM market_resolutions",
        [],
        |row| row.get(0),
    )?;
    let clob_cursor: Option<String> = prior
        .query_row(
            "SELECT value FROM source_cursor WHERE key = 'clob_closed'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    prior.close().map_err(|(_, error)| error)?;
    atomic_write_json(
        path,
        &CacheV2BuildManifest {
            manifest_version: CACHE_BUILD_MANIFEST_VERSION,
            backup_sha256: prior_sha256.to_owned(),
            source_bounds: serde_json::json!({
                "newest_trade_unix": newest_trade,
                "newest_resolution_fetch_unix": newest_resolution,
            }),
            cursors: serde_json::json!({ "clob_closed": clob_cursor }),
            hashes: BTreeMap::new(),
            sealed_at_unix: OffsetDateTime::now_utc().unix_timestamp(),
        },
    )
}

/// Open a checkpointed and closed main for reading exactly as its bytes are.
/// SQLite's immutable mode takes no locks, creates no write-ahead or
/// shared-memory sidecar beside the file and ignores any it finds, so the
/// immutable prior is inspected without being touched.
fn open_immutable(path: &Path) -> Result<Connection, BootstrapError> {
    // SQLite's URI rules want a canonical absolute path: repeated separators
    // (a leading `//` would read as a URI authority) and links resolved.
    let canonical = std::fs::canonicalize(path)?;
    let text = canonical.to_str().ok_or_else(|| BootstrapError::Invalid {
        message: format!("{} is not a UTF-8 path", path.display()),
    })?;
    let mut uri = String::from("file:");
    for character in text.chars() {
        match character {
            '%' => uri.push_str("%25"),
            '?' => uri.push_str("%3F"),
            '#' => uri.push_str("%23"),
            other => uri.push(other),
        }
    }
    uri.push_str("?immutable=1");
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(BootstrapError::from)
}

/// The immutable prior's `user_version`, read without touching the file.
fn verified_user_version(path: &Path) -> Result<i64, BootstrapError> {
    let connection = open_immutable(path)?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    connection.close().map_err(|(_, error)| error)?;
    Ok(version)
}

/// Read the candidate's `user_version` through SQLite so a seal committed to
/// the write-ahead log but not yet checkpointed is honored. The read-write
/// connection only reads; SQLite removes the sidecars it created when this is
/// the last connection to close and leaves them to any connection still open.
fn candidate_user_version(path: &Path) -> Result<i64, BootstrapError> {
    let connection = open_existing_rw(path)?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    connection.close().map_err(|(_, error)| error)?;
    Ok(version)
}

/// The staged roles must be independent files: the same path, a hard link or a
/// symbolic link would let candidate writes reach the prior or the fixed cache.
fn require_distinct_files(roles: &[(&str, &Path)]) -> Result<(), BootstrapError> {
    let mut identities: Vec<(PathBuf, Option<(u64, u64)>)> = Vec::new();
    for (label, path) in roles {
        let canonical = canonical_intended_path(path).map_err(|error| match error {
            BootstrapError::Invalid { message } => BootstrapError::Invalid {
                message: format!("{label}: {message}"),
            },
            other => other,
        })?;
        let inode = file_identity(path)?;
        for (other_path, other_inode) in &identities {
            if *other_path == canonical || (inode.is_some() && inode == *other_inode) {
                return invalid(format!(
                    "{label} {} is not an independent file",
                    path.display()
                ));
            }
        }
        identities.push((canonical, inode));
    }
    Ok(())
}

/// The file the cache lock will create or rewrite: the lock name with links
/// followed, including a link whose target does not exist yet (the documented
/// alias layout links the physical lock name to the repository's lock file
/// before either exists).
fn lock_target_path(fixed_path: &Path) -> Result<PathBuf, BootstrapError> {
    link_target_path(crate::lock::lock_path_for(fixed_path))
}

/// A path with symbolic links followed one by one, including a final link
/// whose target does not exist yet: the file that creating or rewriting the
/// path would actually touch.
fn link_target_path(mut path: PathBuf) -> Result<PathBuf, BootstrapError> {
    let spelled = path.clone();
    for _ in 0..16 {
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = std::fs::read_link(&path)?;
                path = match path.parent() {
                    Some(parent) if !target.is_absolute() => parent.join(target),
                    _ => target,
                };
            }
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(path),
            Err(error) => return Err(error.into()),
        }
    }
    invalid(format!(
        "{} resolves through too many links",
        spelled.display()
    ))
}

/// Identity of a staged path: its parent directory resolved through the file
/// system, joined with its file name, itself resolved when it already exists
/// so a linked alias of a role collides; a link without a target is refused.
/// The parent must already exist.
/// Staging never creates directories, so a spelling that would put a
/// directory or a file at a cache role is refused before anything is written.
fn canonical_intended_path(path: &Path) -> Result<PathBuf, BootstrapError> {
    let spelled = path.as_os_str().as_encoded_bytes();
    if spelled.ends_with(b"/") || spelled.ends_with(b"/.") {
        return invalid(format!("{} names a directory, not a file", path.display()));
    }
    let name = path.file_name().ok_or_else(|| BootstrapError::Invalid {
        message: format!("{} has no file name", path.display()),
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = std::fs::canonicalize(parent).map_err(|error| BootstrapError::Invalid {
        message: format!(
            "{}: parent directory is not available: {error}",
            path.display()
        ),
    })?;
    let intended = parent.join(name);
    match std::fs::canonicalize(&intended) {
        Ok(existing) => Ok(existing),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A link without a target is not an absent file: a copy made
            // later could give it one and turn it into an alias.
            match std::fs::symlink_metadata(&intended) {
                Ok(_) => invalid(format!(
                    "{} is a symbolic link without a target",
                    path.display()
                )),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(intended),
                Err(error) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
/// Device and inode of the file a spelling opens, links followed.
fn file_identity(path: &Path) -> Result<Option<(u64, u64)>, BootstrapError> {
    use std::os::unix::fs::MetadataExt as _;
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some((metadata.dev(), metadata.ino()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn file_identity(_path: &Path) -> Result<Option<(u64, u64)>, BootstrapError> {
    Ok(None)
}

fn stage_report(
    fixed_path: &Path,
    prior_path: &Path,
    side_path: &Path,
    prior_sha256: Option<String>,
    resumed: bool,
) -> Result<CacheStageReport, BootstrapError> {
    let prior_schema = verified_user_version(prior_path)?;
    let side_schema = candidate_user_version(side_path)?;
    Ok(CacheStageReport {
        fixed_path: std::fs::canonicalize(fixed_path)?,
        prior_path: std::fs::canonicalize(prior_path)?,
        side_path: std::fs::canonicalize(side_path)?,
        prior_schema,
        side_schema,
        side_sha256: prior_sha256.clone(),
        prior_sha256,
        resumed,
    })
}

/// Install a finalized v2 main at the fixed Forge path under the reviewed
/// loop → one-shot run → cache lock order.
pub fn activate_cache_v2(
    request: &CacheActivationRequest,
) -> Result<CacheActivationReport, BootstrapError> {
    activate_cache_v2_with_handoff(request, None)
}

/// Install a finalized cache while accepting a verified shell lock handoff.
/// Direct callers use [`activate_cache_v2`] and retain the full Rust-owned
/// loop/run/cache lock stack.
pub fn activate_cache_v2_with_handoff(
    request: &CacheActivationRequest,
    handoff: Option<&ForgeLockHandoff>,
) -> Result<CacheActivationReport, BootstrapError> {
    let _locks = ForgeActivationLocks::acquire_with_handoff(&request.fixed_path, handoff)?;
    require_regular_file(&request.fixed_path, "current fixed cache")?;
    validate_hex_sha256(&request.expected_side_sha256, "expected side sha256")?;
    if !request.side_path.exists() {
        let installed_hash = sha256_file(&request.fixed_path)?;
        if installed_hash != request.expected_side_sha256 {
            return invalid(
                "v2 side cache is missing and the fixed cache has another hash".to_owned(),
            );
        }
        require_regular_file(&request.prior_cache_backup_path, "prior cache backup")?;
        let prior_cache_sha256 = sha256_file(&request.prior_cache_backup_path)?;
        let prior_cache_schema = verified_cache_schema(&request.prior_cache_backup_path)?;
        let installed = open_existing_ro(&request.fixed_path)?;
        require_schema(&installed, CACHE_SCHEMA_VERSION_V2)?;
        quick_check(&installed)?;
        verify_finalized_v2_manifests(&installed, ClassifierGeneration::Current)?;
        installed.close().map_err(|(_, error)| error)?;
        reject_nonempty_sidecars(&request.fixed_path)?;
        return Ok(CacheActivationReport {
            installed_path: std::fs::canonicalize(&request.fixed_path)?,
            installed_sha256: installed_hash,
            prior_cache_backup_path: std::fs::canonicalize(&request.prior_cache_backup_path)?,
            prior_cache_sha256,
            prior_cache_schema,
            activation_evidence: None,
            resumed: true,
        });
    }
    require_regular_file(&request.side_path, "version-two side cache")?;
    reject_nonempty_activation_sidecars(&request.side_path)?;
    if sha256_file(&request.side_path)? != request.expected_side_sha256 {
        return invalid("version-two side-cache hash changed after finalization".to_owned());
    }
    require_same_device(
        &request.fixed_path,
        &request.side_path,
        &request.prior_cache_backup_path,
    )?;

    let current = open_existing_rw(&request.fixed_path)?;
    checkpoint_truncate(&current)?;
    quick_check(&current)?;
    let current_version: i64 =
        current.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match current_version {
        0 | CACHE_SCHEMA_VERSION_V1 => require_reclamation_ready(&current)?,
        CACHE_SCHEMA_VERSION_V2 => {
            verify_finalized_v2_manifests(&current, ClassifierGeneration::Historical)?
        }
        other => return invalid(format!("unsupported prior cache schema {other}")),
    }
    current.close().map_err(|(_, error)| error)?;
    let prior_cache_sha256 = sha256_file(&request.fixed_path)?;
    let activation_evidence = if current_version == 0 || current_version == CACHE_SCHEMA_VERSION_V1
    {
        let evidence = capture_reclamation_evidence(
            &request.fixed_path,
            &eval_results_dir_for_cache(&request.fixed_path),
        )?;
        if !evidence.activation_ready {
            return invalid(
                "current cache failed the locked reclamation/index activation gate".to_owned(),
            );
        }
        Some(evidence)
    } else {
        None
    };

    if request.prior_cache_backup_path.exists() {
        if sha256_file(&request.prior_cache_backup_path)? != prior_cache_sha256 {
            return invalid(format!(
                "existing prior-cache backup differs from the fixed cache: {}",
                request.prior_cache_backup_path.display()
            ));
        }
    } else {
        preserve_main_and_sidecars(&request.fixed_path, &request.prior_cache_backup_path)?;
    }
    if verified_cache_schema(&request.prior_cache_backup_path)? != current_version {
        return invalid("prior-cache backup schema changed during activation".to_owned());
    }

    // The side cache must remain the exact immutable artifact that was finalized. In particular,
    // never checkpoint an unmanifested WAL into its main file: all stale-evidence checks complete
    // against the side path before the authoritative fixed path is replaced.
    require_regular_file(&request.side_path, "version-two side cache")?;
    reject_nonempty_activation_sidecars(&request.side_path)?;
    let side = open_existing_ro(&request.side_path)?;
    require_schema(&side, CACHE_SCHEMA_VERSION_V2)?;
    quick_check(&side)?;
    verify_finalized_v2_manifests(&side, ClassifierGeneration::Current)?;
    side.close().map_err(|(_, error)| error)?;
    // The read-only validation of a WAL-mode main may itself allocate an SHM index. With the
    // pre-open sidecar rejection above complete, only nonempty WAL frames can add durable state;
    // reject those and remove validation-created empty WAL/SHM files.
    reject_nonempty_sidecars(&request.side_path)?;
    let installed_hash = sha256_file(&request.side_path)?;
    if installed_hash != request.expected_side_sha256 {
        return invalid("version-two side-cache hash changed after finalization".to_owned());
    }

    remove_sidecars(&request.fixed_path)?;
    std::fs::rename(&request.side_path, &request.fixed_path).map_err(map_rename_error)?;
    sync_parent(&request.fixed_path)?;
    Ok(CacheActivationReport {
        installed_path: std::fs::canonicalize(&request.fixed_path)?,
        installed_sha256: installed_hash,
        prior_cache_backup_path: std::fs::canonicalize(&request.prior_cache_backup_path)?,
        prior_cache_sha256,
        prior_cache_schema: current_version,
        activation_evidence,
        resumed: false,
    })
}

/// Restore the hash-bound prior cache only when the durable pending request is
/// intact and authoritative `ranking_batches.publish_key` evidence proves that
/// exact publication has never been consumed.
pub async fn restore_prior_cache(
    fixed_path: &Path,
    prior_cache_backup_path: &Path,
    displaced_cache_backup_path: &Path,
    binding: &PriorCacheBinding,
    publication_request_path: &Path,
    pending_pointer_path: &Path,
    publication_probe: &dyn PublicationConsumptionProbe,
) -> Result<(), BootstrapError> {
    // Taking the lock stack creates and rewrites three lock files, the
    // displaced copy and its sidecars are written through `.pending` names
    // and the fixed cache's sidecars are removed, so none of those names may
    // be another role; checked before any lock is taken.
    let displaced_pending = pending_path_for(displaced_cache_backup_path);
    let sidecars: Vec<(&str, PathBuf)> = [
        ("corrected fixed cache sidecar", fixed_path),
        (
            "displaced-cache backup sidecar",
            displaced_cache_backup_path,
        ),
    ]
    .into_iter()
    .flat_map(|(label, path)| {
        ["-wal", "-shm", "-journal"].map(move |suffix| (label, sidecar_path(path, suffix)))
    })
    .collect();
    let sidecar_pendings = ["-wal", "-shm"]
        .map(|suffix| pending_path_for(&sidecar_path(displaced_cache_backup_path, suffix)));
    let [loop_lock, run_lock] = crate::lock::forge_named_lock_paths(fixed_path);
    let lock_targets = [
        link_target_path(loop_lock)?,
        link_target_path(run_lock)?,
        lock_target_path(fixed_path)?,
    ];
    let mut roles = vec![
        ("corrected fixed cache", fixed_path),
        ("prior cache restore main", prior_cache_backup_path),
        ("displaced-cache backup", displaced_cache_backup_path),
        ("displaced-cache staging file", displaced_pending.as_path()),
        ("publication request", publication_request_path),
        ("pending publication pointer", pending_pointer_path),
    ];
    roles.extend(
        sidecars
            .iter()
            .map(|(label, path)| (*label, path.as_path())),
    );
    roles.extend(
        sidecar_pendings
            .iter()
            .map(|path| ("displaced-cache sidecar staging file", path.as_path())),
    );
    roles.extend(
        lock_targets
            .iter()
            .map(|path| ("Forge lock file", path.as_path())),
    );
    require_distinct_files(&roles)?;
    let _locks = ForgeActivationLocks::acquire(fixed_path)?;
    let request = verified_pending_publication(
        publication_request_path,
        pending_pointer_path,
        fixed_path,
        prior_cache_backup_path,
    )?;
    if publication_probe
        .was_published(&request.publish_key)
        .await?
    {
        return invalid(
            "prior-cache restore is retired after the bound ranking publication was consumed"
                .to_owned(),
        );
    }
    validate_hex_sha256(&binding.sha256, "prior cache sha256")?;
    require_regular_file(prior_cache_backup_path, "prior cache restore main")?;
    if sha256_file(prior_cache_backup_path)? != binding.sha256
        || verified_cache_schema(prior_cache_backup_path)? != binding.schema_version
    {
        return invalid("prior cache does not match its recorded schema/hash".to_owned());
    }
    require_same_device(
        fixed_path,
        prior_cache_backup_path,
        displaced_cache_backup_path,
    )?;
    if fixed_path.is_file() {
        let current = open_existing_rw(fixed_path)?;
        checkpoint_truncate(&current)?;
        quick_check(&current)?;
        current.close().map_err(|(_, error)| error)?;
        if displaced_cache_backup_path.exists() {
            if sha256_file(displaced_cache_backup_path)? != sha256_file(fixed_path)? {
                return invalid(format!(
                    "existing displaced-cache backup differs from the fixed cache: {}",
                    displaced_cache_backup_path.display()
                ));
            }
        } else {
            preserve_main_and_sidecars(fixed_path, displaced_cache_backup_path)?;
        }
    }
    remove_sidecars(fixed_path)?;
    std::fs::rename(prior_cache_backup_path, fixed_path).map_err(map_rename_error)?;
    sync_parent(fixed_path)?;
    reject_nonempty_sidecars(fixed_path)?;
    if sha256_file(fixed_path)? != binding.sha256
        || verified_cache_schema(fixed_path)? != binding.schema_version
    {
        return invalid("restored cache does not match its recorded schema/hash".to_owned());
    }
    Ok(())
}

fn verified_pending_publication(
    request_path: &Path,
    pending_pointer_path: &Path,
    fixed_path: &Path,
    prior_cache_backup_path: &Path,
) -> Result<DurablePublishRequest, BootstrapError> {
    require_regular_file(request_path, "publication request")?;
    require_regular_file(pending_pointer_path, "pending publication pointer")?;
    require_regular_file(fixed_path, "corrected fixed cache")?;
    require_regular_file(prior_cache_backup_path, "prior cache restore main")?;
    let pending = std::fs::read_to_string(pending_pointer_path)?;
    let lines = pending.lines().collect::<Vec<_>>();
    if lines.len() != 1 || lines[0].is_empty() || Path::new(lines[0]).is_absolute() {
        return invalid(
            "pending publication pointer must contain one repository-relative request path"
                .to_owned(),
        );
    }
    if std::fs::canonicalize(lines[0])? != std::fs::canonicalize(request_path)? {
        return invalid("pending publication pointer names another request".to_owned());
    }

    let request: DurablePublishRequest = serde_json::from_slice(&std::fs::read(request_path)?)?;
    if request.version != 1 || request.entries.is_empty() {
        return invalid("publication request has an unsupported or empty shape".to_owned());
    }
    validate_hex_sha256(&request.publish_key, "publication request publish_key")?;
    validate_hex_sha256(
        &request.cache_activation.expected_sha256,
        "publication request cache activation hash",
    )?;
    let mut identity = BTreeMap::new();
    identity.insert("batch", request.batch.clone());
    identity.insert(
        "cache_activation",
        serde_json::to_value(&request.cache_activation)?,
    );
    identity.insert("entries", Value::Array(request.entries.clone()));
    let identity = serde_json::to_value(identity)?;
    if sha256_bytes(python_canonical_json(&identity)?.as_bytes()) != request.publish_key {
        return invalid("publication request content hash mismatch".to_owned());
    }
    if std::fs::canonicalize(&request.cache_activation.fixed_path)?
        != std::fs::canonicalize(fixed_path)?
        || std::fs::canonicalize(&request.cache_activation.prior_cache_backup_path)?
            != std::fs::canonicalize(prior_cache_backup_path)?
    {
        return invalid("publication request is bound to another cache activation".to_owned());
    }
    if sha256_file(fixed_path)? != request.cache_activation.expected_sha256 {
        return invalid("fixed cache no longer matches the bound corrected cache".to_owned());
    }
    Ok(request)
}

fn frozen_freshness(connection: &Connection) -> Result<FrozenCacheFreshness, BootstrapError> {
    let newest_trade_unix: Option<i64> = connection.query_row(
        "SELECT MAX(timestamp_unix) FROM trades_v1_sealed",
        [],
        |row| row.get(0),
    )?;
    let newest_resolution_fetch_unix: Option<i64> = connection.query_row(
        "SELECT MAX(fetched_at_unix) FROM market_resolutions_v1_sealed",
        [],
        |row| row.get(0),
    )?;
    let cursor: Option<(String, i64)> = connection
        .query_row(
            "SELECT value, updated_at FROM source_cursor_v1_sealed WHERE key = 'clob_closed'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((clob_cursor, clob_cursor_updated_at)) = cursor else {
        return invalid("sealed v1 CLOB cursor is missing".to_owned());
    };
    Ok(FrozenCacheFreshness {
        newest_trade_unix: newest_trade_unix.ok_or_else(|| BootstrapError::Invalid {
            message: "sealed v1 trade watermark is missing".to_owned(),
        })?,
        newest_resolution_fetch_unix: newest_resolution_fetch_unix.ok_or_else(|| {
            BootstrapError::Invalid {
                message: "sealed v1 resolution watermark is missing".to_owned(),
            }
        })?,
        clob_cursor,
        clob_cursor_updated_at,
    })
}

fn require_reclamation_ready(connection: &Connection) -> Result<(), BootstrapError> {
    let pending: Option<String> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'reclamation_pending'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if pending.is_some() {
        return invalid("cache reclamation_pending marker is present".to_owned());
    }
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'trades'")?;
    let present = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let missing = REQUIRED_TRADES_INDEXES
        .iter()
        .filter(|required| !present.iter().any(|actual| actual == **required))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return invalid(format!("required trades indexes are missing: {missing:?}"));
    }
    Ok(())
}

fn verify_manifest_wal_binding(
    cache_path: &Path,
    manifest: &CacheV2BuildManifest,
) -> Result<(), BootstrapError> {
    let wal = sidecar_path(cache_path, "-wal");
    let wal_is_nonempty = wal.is_file() && std::fs::metadata(&wal)?.len() != 0;
    let expected = manifest.hashes.get("backup_wal_sha256");
    match (wal_is_nonempty, expected) {
        (true, Some(expected_hash)) => {
            validate_hex_sha256(expected_hash, "hashes.backup_wal_sha256")?;
            let actual = sha256_file(&wal)?;
            if &actual != expected_hash {
                return invalid(format!(
                    "online-backup WAL hash mismatch: manifest {expected_hash}, file {actual}"
                ));
            }
            Ok(())
        }
        (true, None) => invalid(
            "online backup has uncheckpointed WAL frames without hashes.backup_wal_sha256"
                .to_owned(),
        ),
        (false, Some(_)) => invalid(
            "cache build manifest binds a WAL but the online backup has no non-empty WAL"
                .to_owned(),
        ),
        (false, None) => Ok(()),
    }
}

/// SQLite's quick check: a linear-time structural verification (page
/// allocation and coverage, freelist, malformed records, overflow chains,
/// rowid ordering) that fails closed at each lifecycle point. It skips the
/// full `integrity_check`'s per-row index probes, so index-to-row content
/// agreement and UNIQUE validation are not verified here; on Forge's
/// production cache (150 GB, 275 million trades, four trade indexes) one full
/// check exceeded seven hours, which no per-cycle step can afford (#643).
fn quick_check(connection: &Connection) -> Result<(), BootstrapError> {
    let result: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if result == "ok" {
        Ok(())
    } else {
        invalid(format!("SQLite quick_check failed: {result}"))
    }
}

fn ensure_lane_a_v2_schema(connection: &Connection) -> Result<(), BootstrapError> {
    connection.execute_batch(V2_SCHEMA)?;
    for (table, column, definition) in [
        (
            "activity_coverage_manifests_v2",
            "reference_sha256",
            "TEXT NULL",
        ),
        (
            "activity_coverage_manifests_v2",
            "wallet_count",
            "INTEGER NULL",
        ),
        (
            "activity_coverage_manifests_v2",
            "receipt_set_digest",
            "TEXT NULL",
        ),
        (
            "activity_coverage_manifests_v2",
            "aggregate_digest",
            "TEXT NULL",
        ),
        (
            "activity_coverage_manifests_v2",
            "source_row_count",
            "INTEGER NULL",
        ),
        (
            "cache_frozen_payload_verifications",
            "activity_generation",
            "INTEGER NULL",
        ),
        (
            "cache_frozen_payload_verifications",
            "fixed_end_unix",
            "INTEGER NULL",
        ),
        (
            "cache_v2_migration_state",
            "ranker_projection_count",
            "INTEGER NULL",
        ),
        (
            "cache_v2_migration_state",
            "ranker_projection_digest",
            "TEXT NULL",
        ),
        (
            "cache_v2_migration_state",
            "ranker_classifier_version",
            "INTEGER NULL",
        ),
        (
            "cache_v2_migration_state",
            "fresh_collection_json",
            "TEXT NULL",
        ),
        (
            "activity_wallet_coverage_staging_v2",
            "exclusion_reason",
            "TEXT NULL",
        ),
        ("clob_payout_evidence_v2", "end_date_unix", "INTEGER NULL"),
        (
            "clob_payout_evidence_staging_v2",
            "end_date_unix",
            "INTEGER NULL",
        ),
    ] {
        let exists: bool = connection.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM pragma_table_info('{table}') WHERE name = ?1)"),
            params![column],
            |row| row.get(0),
        )?;
        if !exists {
            connection.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))?;
        }
    }
    Ok(())
}

fn checkpoint_truncate(connection: &Connection) -> Result<(), BootstrapError> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode.eq_ignore_ascii_case("wal") {
        let (busy, log, checkpointed): (i64, i64, i64) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || log != checkpointed {
            return invalid(format!(
                "SQLite checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}"
            ));
        }
    }
    Ok(())
}

fn open_existing_rw(path: &Path) -> Result<Connection, BootstrapError> {
    // A canonical absolute path is never read as a SQLite URI (`file:` names).
    let connection = Connection::open_with_flags(
        std::fs::canonicalize(path)?,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

fn open_existing_ro(path: &Path) -> Result<Connection, BootstrapError> {
    Connection::open_with_flags(
        std::fs::canonicalize(path)?,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(BootstrapError::from)
}

fn require_schema(connection: &Connection, expected: i64) -> Result<(), BootstrapError> {
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found == expected {
        Ok(())
    } else {
        invalid(format!(
            "cache schema version is {found}, expected {expected}"
        ))
    }
}

/// Schema of a checkpointed, hash-bound main, read in immutable mode so the
/// check creates no sidecar beside it.
fn verified_cache_schema(path: &Path) -> Result<i64, BootstrapError> {
    let connection = open_immutable(path)?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match version {
        0 | CACHE_SCHEMA_VERSION_V1 => {}
        CACHE_SCHEMA_VERSION_V2 => {
            verify_finalized_v2_manifests(&connection, ClassifierGeneration::Historical)?
        }
        other => return invalid(format!("prior cache has unsupported schema {other}")),
    }
    quick_check(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    Ok(version)
}

fn require_table(connection: &Connection, table: &str) -> Result<(), BootstrapError> {
    let found: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        params![table],
        |row| row.get(0),
    )?;
    if found {
        Ok(())
    } else {
        invalid(format!("required cache table {table} is missing"))
    }
}

fn count_rows(connection: &Connection, table: &str) -> Result<i64, BootstrapError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get(0))
        .map_err(BootstrapError::from)
}

fn required_max(connection: &Connection, table: &str, column: &str) -> Result<i64, BootstrapError> {
    let sql = format!("SELECT MAX({column}) FROM {table}");
    let value: Option<i64> = connection.query_row(&sql, [], |row| row.get(0))?;
    value.ok_or_else(|| BootstrapError::Invalid {
        message: format!("required manifest table {table} is empty"),
    })
}

enum ClassifierGeneration {
    Current,
    Historical,
}

fn verify_finalized_v2_manifests(
    connection: &Connection,
    generation: ClassifierGeneration,
) -> Result<(), BootstrapError> {
    if required_max(connection, "sealed_generation_manifests", "generation")? != 1 {
        return invalid("installed cache has an invalid sealed generation".to_owned());
    }
    // A legacy identity exists only through a frozen-payload verification row
    // carrying its activity binding; a fresh collection identity needs none.
    let ActivityIdentity {
        generation: activity_generation,
        reference_sha256,
        fixed_end_unix,
        wallets,
    } = activity_identity(connection)?;
    completed_activity_manifest(
        connection,
        activity_generation,
        &reference_sha256,
        fixed_end_unix,
        &wallets,
    )?
    .ok_or_else(|| BootstrapError::Invalid {
        message: "installed cache has no matching activity manifest".to_owned(),
    })?;
    let payout_generation = required_max(
        connection,
        "clob_payout_coverage_manifests_v2",
        "generation",
    )?;
    verify_payout_coverage(connection, payout_generation)?;
    let state: Option<FinalizedProjectionState> = connection
        .query_row(
            "SELECT phase, ranker_projection_count, ranker_projection_digest,
                    ranker_classifier_version
             FROM cache_v2_migration_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((phase, projection_count, projection_digest, classifier_version)) = state else {
        return invalid("installed cache omitted its finalized migration state".to_owned());
    };
    let actual_projection_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ranker_entries_v2 WHERE activity_generation = ?1",
        params![to_i64(activity_generation, "activity generation")?],
        |row| row.get(0),
    )?;
    let actual_projection_digest = ranker_projection_digest(connection, activity_generation)?;
    let classifier_matches = match generation {
        ClassifierGeneration::Current => {
            classifier_version == Some(i64::from(RANKER_CLASSIFIER_VERSION))
        }
        ClassifierGeneration::Historical => matches!(classifier_version, Some(1 | 2)),
    };
    let mismatched_rows: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ranker_entries_v2 WHERE classifier_version IS NOT ?1)",
        params![classifier_version],
        |row| row.get(0),
    )?;
    if phase != "finalized"
        || projection_count != Some(actual_projection_count)
        || projection_digest.as_deref() != Some(actual_projection_digest.as_str())
        || !classifier_matches
        || mismatched_rows
    {
        return invalid(
            "installed cache omitted or changed its frozen/activity/ranker proof".to_owned(),
        );
    }
    Ok(())
}

fn verify_payout_coverage(connection: &Connection, generation: i64) -> Result<(), BootstrapError> {
    let (manifest_json, market_count, schema_version, parser_version): (String, i64, i64, i64) =
        connection.query_row(
            "SELECT manifest_json, market_count, schema_version, parser_version
             FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
            params![generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let manifest: ClobCoverageManifest = serde_json::from_str(&manifest_json)?;
    let rebuilt = ClobCoverageManifest::complete(manifest.generation, manifest.pages.clone())
        .map_err(|error| BootstrapError::Invalid {
            message: format!("CLOB payout coverage manifest is incomplete: {error}"),
        })?;
    if rebuilt != manifest
        || to_i64(manifest.generation, "CLOB payout generation")? != generation
        || manifest.schema_version != CLOB_RESOLUTION_SCHEMA_VERSION
        || manifest.parser_version != CLOB_RESOLUTION_PARSER_VERSION
        || schema_version != i64::from(CLOB_RESOLUTION_SCHEMA_VERSION)
        || parser_version != i64::from(CLOB_RESOLUTION_PARSER_VERSION)
        || to_i64(manifest.counts.markets, "CLOB payout market count")? != market_count
    {
        return invalid(
            "CLOB payout coverage manifest does not match its stored generation".to_owned(),
        );
    }
    let evidence_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM clob_payout_evidence_v2 WHERE coverage_generation = ?1",
        params![generation],
        |row| row.get(0),
    )?;
    if evidence_count != market_count {
        return invalid(format!(
            "CLOB payout coverage is incomplete: manifest={market_count}, evidence={evidence_count}"
        ));
    }
    Ok(())
}

fn require_regular_file(path: &Path, label: &str) -> Result<(), BootstrapError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        invalid(format!(
            "{label} is not a regular non-symlink file: {}",
            path.display()
        ))
    }
}

fn validate_wallet_hex(wallet: &str) -> Result<(), BootstrapError> {
    if wallet.len() == 42
        && wallet.starts_with("0x")
        && wallet[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "frozen payload has non-canonical wallet {wallet:?}"
        ))
    }
}

fn validate_hex_sha256(value: &str, field: &str) -> Result<(), BootstrapError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "{field} must be 64 lowercase hexadecimal characters"
        ))
    }
}

fn validate_g2_id(value: &str) -> Result<(), BootstrapError> {
    if value.len() == 67
        && value.starts_with("g2:")
        && value[3..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "non-canonical version-two activity identity {value:?}"
        ))
    }
}

fn canonical_json(value: &(impl Serialize + ?Sized)) -> Result<String, BootstrapError> {
    serde_json::to_string(value).map_err(BootstrapError::from)
}

// `publish_key` is owned by Python's `json.dumps(sort_keys=True, separators=(",", ":"))`.
// Serde and Python choose different display forms for otherwise-equal JSON floats around
// the scientific-notation thresholds. Reproduce Python's spelling from serde_json's exact
// decimal rendering instead of parsing financial values into Rust floating point.
fn python_canonical_json(value: &Value) -> Result<String, BootstrapError> {
    fn append(value: &Value, output: &mut String) -> Result<(), BootstrapError> {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => output.push_str(&python_json_number(value)?),
            Value::String(value) => output.push_str(&serde_json::to_string(value)?),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    append(value, output)?;
                }
                output.push(']');
            }
            Value::Object(values) => {
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                output.push('{');
                for (index, key) in keys.into_iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    output.push_str(&serde_json::to_string(key)?);
                    output.push(':');
                    append(&values[key], output)?;
                }
                output.push('}');
            }
        }
        Ok(())
    }

    let mut output = String::new();
    append(value, &mut output)?;
    Ok(output)
}

fn python_json_number(value: &serde_json::Number) -> Result<String, BootstrapError> {
    let rendered = value.to_string();
    if value.is_i64() || value.is_u64() {
        return Ok(rendered);
    }
    if let Some((mantissa, exponent)) = rendered.split_once('e') {
        let (sign, digits) = exponent.strip_prefix('-').map_or_else(
            || ('+', exponent.strip_prefix('+').unwrap_or(exponent)),
            |v| ('-', v),
        );
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        return Ok(format!("{mantissa}e{sign}{digits:0>2}"));
    }

    let (sign, magnitude) = rendered
        .strip_prefix('-')
        .map_or(("", rendered.as_str()), |value| ("-", value));
    let Some(fraction) = magnitude.strip_prefix("0.") else {
        return Ok(rendered);
    };
    let Some(first_nonzero) = fraction.find(|character| character != '0') else {
        return Ok(rendered);
    };
    let exponent_magnitude =
        first_nonzero
            .checked_add(1)
            .ok_or_else(|| BootstrapError::Invalid {
                message: "publication request number exponent overflow".to_owned(),
            })?;
    if exponent_magnitude <= 4 {
        return Ok(rendered);
    }
    let digits = &fraction[first_nonzero..];
    let (first, remainder) = digits.split_at(1);
    let mantissa = if remainder.is_empty() {
        first.to_owned()
    } else {
        format!("{first}.{remainder}")
    };
    Ok(format!("{sign}{mantissa}e-{exponent_magnitude:0>2}"))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn sha256_file(path: &Path) -> Result<String, BootstrapError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn hours_to_seconds(hours: u64) -> Result<i64, BootstrapError> {
    let seconds = hours
        .checked_mul(3_600)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "hour bound overflow".to_owned(),
        })?;
    i64::try_from(seconds).map_err(|_| BootstrapError::Invalid {
        message: "hour bound exceeds i64".to_owned(),
    })
}

fn to_i64(value: u64, field: &str) -> Result<i64, BootstrapError> {
    i64::try_from(value).map_err(|_| BootstrapError::Invalid {
        message: format!("{field} exceeds SQLite integer range"),
    })
}

fn to_u64(value: i64, field: &str) -> Result<u64, BootstrapError> {
    u64::try_from(value).map_err(|_| BootstrapError::Invalid {
        message: format!("{field} is negative"),
    })
}

fn sync_file_and_parent(path: &Path) -> Result<(), BootstrapError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), BootstrapError> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// The temporary file [`atomic_write_json`] renames onto `path`.
fn atomic_write_temp_path(path: &Path) -> PathBuf {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("stage"),
        std::process::id()
    ))
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<(), BootstrapError> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = atomic_write_temp_path(path);
    let rendered = serde_json::to_vec_pretty(value)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&rendered)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&temp, path).map_err(map_rename_error)?;
    sync_parent(path)
}

fn pending_path_for(target: &Path) -> PathBuf {
    sidecar_path(target, ".pending")
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn reject_nonempty_sidecars(path: &Path) -> Result<(), BootstrapError> {
    let wal = sidecar_path(path, "-wal");
    if wal.is_file() && std::fs::metadata(&wal)?.len() != 0 {
        return invalid(format!("non-empty SQLite WAL remains: {}", wal.display()));
    }
    if wal.exists() {
        std::fs::remove_file(wal)?;
    }
    // SQLite's shared-memory index has a fixed non-zero allocation even after
    // TRUNCATE checkpoints. Once every connection is closed and WAL is empty,
    // it contains no durable state and must be removed, not installed.
    let shared_memory = sidecar_path(path, "-shm");
    if shared_memory.exists() {
        std::fs::remove_file(shared_memory)?;
    }
    Ok(())
}

fn reject_nonempty_activation_sidecars(path: &Path) -> Result<(), BootstrapError> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sidecar_path(path, suffix);
        if sidecar.exists() && std::fs::metadata(&sidecar)?.len() != 0 {
            return invalid(format!(
                "non-empty SQLite activation sidecar remains: {}",
                sidecar.display()
            ));
        }
    }
    Ok(())
}

fn remove_sidecars(path: &Path) -> Result<(), BootstrapError> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sidecar_path(path, suffix);
        match std::fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn preserve_main_and_sidecars(source: &Path, target: &Path) -> Result<(), BootstrapError> {
    if target.exists() {
        return invalid(format!(
            "immutable backup target exists: {}",
            target.display()
        ));
    }
    copy_file_atomic(source, target)?;
    for suffix in ["-wal", "-shm"] {
        let source_sidecar = sidecar_path(source, suffix);
        if source_sidecar.exists() {
            let target_sidecar = sidecar_path(target, suffix);
            copy_file_atomic(&source_sidecar, &target_sidecar)?;
        }
    }
    sync_parent(target)
}

fn copy_file_atomic(source: &Path, target: &Path) -> Result<(), BootstrapError> {
    copy_file_atomic_verified(source, target, None)
}

/// Copy through a private `.pending` file, synchronize it, optionally require
/// its bytes to hash to `expected_sha256` before it is renamed into place, then
/// synchronize the parent directory.
fn copy_file_atomic_verified(
    source: &Path,
    target: &Path,
    expected_sha256: Option<&str>,
) -> Result<(), BootstrapError> {
    let pending = pending_path_for(target);
    match std::fs::remove_file(&pending) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::copy(source, &pending)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pending)?
        .sync_all()?;
    if let Some(expected) = expected_sha256 {
        let actual = sha256_file(&pending)?;
        if actual != expected {
            std::fs::remove_file(&pending)?;
            return invalid(format!(
                "staged copy hash mismatch: {} is {actual}, source is {expected}",
                pending.display()
            ));
        }
    }
    std::fs::rename(&pending, target).map_err(map_rename_error)?;
    sync_parent(target)
}

fn require_same_device(first: &Path, second: &Path, third: &Path) -> Result<(), BootstrapError> {
    let device = device_for_existing_or_parent(first)?;
    for path in [second, third] {
        let other = device_for_existing_or_parent(path)?;
        if other != device {
            return invalid(format!(
                "atomic rename refused across devices: {} is device {other}, expected {device}",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn device_for_existing_or_parent(path: &Path) -> Result<u64, BootstrapError> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = if path.exists() {
        std::fs::metadata(path)?
    } else {
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::metadata(parent)?
    };
    Ok(metadata.dev())
}

#[cfg(not(unix))]
fn device_for_existing_or_parent(_path: &Path) -> Result<u64, BootstrapError> {
    Ok(0)
}

fn map_rename_error(error: std::io::Error) -> BootstrapError {
    if error.raw_os_error() == Some(18) {
        BootstrapError::Invalid {
            message: "atomic rename refused across devices".to_owned(),
        }
    } else {
        BootstrapError::Io(error)
    }
}

fn invalid<T>(message: String) -> Result<T, BootstrapError> {
    Err(BootstrapError::Invalid { message })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod publication_json_tests {
    use super::python_canonical_json;

    #[test]
    fn matches_python_json_float_spelling_at_scientific_thresholds() {
        let value = serde_json::json!({
            "integer": 1,
            "large": 1e16,
            "ordinary": 0.0001,
            "small": 1.23e-5,
            "whole_float": 1.0,
        });
        assert_eq!(
            python_canonical_json(&value).unwrap(),
            r#"{"integer":1,"large":1e+16,"ordinary":0.0001,"small":1.23e-05,"whole_float":1.0}"#
        );
    }
}
