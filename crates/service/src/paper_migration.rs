//! Resumable paper-state v1-to-v2 boot authority transition (#544).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail, ensure};
use pe_core_types::{EventSeq, ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::envelope::{HashInput, compute_hashes};
use pe_event_log::{ContentType, EnvelopeIn, LogTailBinding, Scanner};
use pe_execution_core::LiveJournal;
use pe_paper_state::{
    DurableLogBindings, LEGACY_EXACT_MIGRATION_VERSION, MigrationMetadata, MigrationPhase,
    MigrationRecord, PaperMainSeal, PaperPositionRow, PaperStateDb, SCHEMA_VERSION,
    verify_log_bindings,
};
use pe_strategy_winner_follow::PerTradeCap;
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::runtime_config::RuntimeConfig;
use crate::source_event_sink::SourceEventSink;
use crate::trade_poller::ReconciliationObligations;

const LEGACY_HISTORY_SOURCE_ID: &str = "paper-migration-legacy-wallet-history-v1";
const LEGACY_HISTORY_SCHEMA_VERSION: u32 = 1;
const LEGACY_HISTORY_PARSER_VERSION: u32 = 1;
const LEGACY_HISTORY_OBSERVED_AT_KEY: &str = "legacy_history_observed_at_unix";
const REMOTE_AUTHORITY_SOURCE_ID: &str = "paper-migration-supabase-authority-v2";

/// Fully resolved migration inputs. The caller must construct this only after
/// the old process has exited and all of its database/log handles are closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperMigrationPaths {
    pub fixed_main: PathBuf,
    pub source_log: PathBuf,
    pub paper_log: PathBuf,
    pub live_journal: PathBuf,
    pub legacy_history: PathBuf,
    pub binary_identity: String,
}

/// Startup result: either the fixed v2 main is already authoritative, or the
/// caller must run authority reload and the causal bracket against `active_main`.
#[derive(Debug, Clone)]
pub struct PaperMigrationBoot {
    pub active_main: PathBuf,
    pub record: MigrationRecord,
    pub session: Option<PaperMigrationSession>,
}

/// In-progress state retained across the remote-authority reload and position
/// bracket. No producer may start until [`finish`](Self::finish) succeeds.
#[derive(Debug, Clone)]
pub struct PaperMigrationSession {
    paths: PaperMigrationPaths,
    side_main: PathBuf,
    version_one_backup: PathBuf,
}

/// Fail-closed #544 activation prerequisites from the already-applied runtime
/// snapshot. The migration must not rename its v2 main while the impact gate is
/// disabled or the reviewed unlimited per-trade posture changed.
pub fn validate_initial_configuration(config: &RuntimeConfig) -> Result<()> {
    ensure!(
        config.price_impact_cap_bps == 100,
        "paper migration requires applied price_impact_cap_bps=100, found {}",
        config.price_impact_cap_bps
    );
    ensure!(
        config.per_trade_cap == PerTradeCap::Unlimited,
        "paper migration requires the reviewed per_trade_cap=unlimited posture"
    );
    Ok(())
}

/// A v1-to-v2 migration must reload the complete configured remote financial
/// authority; local-only boot cannot silently substitute its subset state.
pub fn validate_migration_authority(
    paper_schema_version: i64,
    supabase_authoritative: bool,
) -> Result<()> {
    ensure!(
        paper_schema_version != 1 || supabase_authoritative,
        "paper schema-v1 migration requires supabase_authoritative=true"
    );
    Ok(())
}

/// Capture the state census of the wallets the migration bracket accepted,
/// immediately after the bracket; that set is the boot universe. Fenced and
/// deferred wallets are not part of the census, and only the accepted set is
/// guaranteed a current validation row. The side-main file hash later binds
/// this row together with the canonical cursor/fence/validation tables it
/// describes.
pub fn record_activation_facts(
    paper_state: &PaperStateDb,
    wallets: &[WalletAddress],
    obligations: &ReconciliationObligations,
    binary_identity: &str,
) -> Result<String> {
    let mut wallets = wallets.to_vec();
    wallets.sort_by_key(ToString::to_string);
    wallets.dedup();
    let mut source_bounds = Vec::with_capacity(wallets.len());
    let mut cursors = Vec::with_capacity(wallets.len());
    let mut wallet_identity = Vec::with_capacity(wallets.len());
    for wallet in &wallets {
        let validation = paper_state
            .position_validation(wallet)
            .context("read migration position validation")?
            .with_context(|| format!("migration bracket omitted wallet {wallet}"))?;
        source_bounds.push(serde_json::json!({
            "wallet": wallet.to_string(),
            "activity_bounds": serde_json::from_str::<serde_json::Value>(
                &validation.activity_bounds_json,
            )?,
            "source_log_generation": validation.source_log_generation,
        }));
        cursors.push(serde_json::json!({
            "wallet": wallet.to_string(),
            "delivery": paper_state.cursor(wallet)?,
            "activity": paper_state.activity(wallet)?,
        }));
        wallet_identity.push(serde_json::json!({
            "wallet": wallet.to_string(),
            "ledger_hash": validation.ledger_hash,
            "positions_proof_hash": validation.positions_proof_hash,
        }));
    }
    let fences = paper_state
        .wallet_fences()?
        .into_iter()
        .map(|fence| {
            Ok(serde_json::json!({
                "wallet": fence.wallet.to_string(),
                "source_trade_id": fence.source_trade_id.0,
                "cause": fence.cause,
                "proof": serde_json::from_str::<serde_json::Value>(&fence.proof_json)?,
                "fenced_at_unix": fence.fenced_at_unix,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let facts = serde_json::json!({
        "source_bounds": source_bounds,
        "cursors": cursors,
        "obligations": obligations.migration_evidence(),
        "fences": fences,
        "wallet_identity": wallet_identity,
        "binary_identity": binary_identity,
    });
    paper_state
        .record_migration_activation_facts(&facts, binary_identity)
        .context("record paper migration activation facts")
}

/// Append and synchronize the validated complete remote-authority snapshot
/// before `replace_authoritative_state` applies it to the side main.
pub fn append_remote_authority_snapshot(
    source_log_path: &Path,
    bankroll: &Decimal,
    positions: &[PaperPositionRow],
) -> Result<(), crate::supabase_state::SupabaseStateError> {
    let timestamp = OffsetDateTime::now_utc();
    let payload = serde_json::to_vec(&serde_json::json!({
        "bankroll": bankroll.to_string(),
        "positions": positions.iter().map(|position| serde_json::json!({
            "market_id": position.market_id.0,
            "outcome_id": position.outcome_id.0,
            "long_contracts": position.long.to_decimal().to_string(),
            "short_contracts": position.short.to_decimal().to_string(),
        })).collect::<Vec<_>>(),
    }))?;
    let mut sink = SourceEventSink::open(source_log_path).map_err(|error| {
        crate::supabase_state::SupabaseStateError::MigrationEvidence(error.to_string())
    })?;
    sink.append_durable(EnvelopeIn {
        source_id: SourceId(REMOTE_AUTHORITY_SOURCE_ID.to_owned()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(timestamp),
        received_at: ReceivedAt(timestamp),
        content_type: ContentType::Json,
        payload,
    })
    .map_err(|error| {
        crate::supabase_state::SupabaseStateError::MigrationEvidence(error.to_string())
    })?;
    Ok(())
}

impl PaperMigrationBoot {
    /// Prepare or resume the exact machine-owned side main selected in metadata.
    pub fn prepare(paths: PaperMigrationPaths, imported_at_unix: i64) -> Result<Self> {
        ensure!(
            paths.fixed_main.is_file(),
            "paper migration requires the existing fixed main {}",
            paths.fixed_main.display()
        );
        let schema = MigrationMetadata::schema_version(&paths.fixed_main)
            .context("inspect fixed paper-state schema before normal open")?;
        // An installed main written by a pre-#545 binary is still at the exact-migration
        // version; the ordinary writable open that follows this pre-check migrates it in
        // place (#567).
        if schema == SCHEMA_VERSION || schema == LEGACY_EXACT_MIGRATION_VERSION {
            let mut record = MigrationMetadata::read(&paths.fixed_main)
                .context("read installed paper migration record")?
                .context("v2 paper main omitted migration record")?;
            MigrationMetadata::verify_activation_facts(&paths.fixed_main)
                .context("verify installed paper migration activation census")?;
            if record.phase == MigrationPhase::ActivationTailsRecorded {
                let activation = record
                    .activation_tails
                    .as_ref()
                    .context("paper activation phase omitted final tails")?;
                let current = capture_log_bindings(&paths)?;
                verify_log_bindings(activation, &current)
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                record = MigrationMetadata::advance_phase(
                    &paths.fixed_main,
                    MigrationPhase::ActivationTailsRecorded,
                    MigrationPhase::Installed,
                )
                .context("resume paper migration after the database rename")?;
            }
            ensure!(
                record.phase == MigrationPhase::Installed,
                "v2 paper main is not installed: {}",
                record.phase
            );
            let activation = record
                .activation_tails
                .as_ref()
                .context("installed paper migration omitted activation tails")?;
            verify_boundary_prefixes(activation, &paths)
                .context("verify installed paper migration activation prefixes")?;
            return Ok(Self {
                active_main: paths.fixed_main,
                record,
                session: None,
            });
        }
        ensure!(
            schema == 1,
            "unsupported paper-state schema version {schema}"
        );

        let legacy_bytes = std::fs::read(&paths.legacy_history).with_context(|| {
            format!(
                "read captured legacy history {}",
                paths.legacy_history.display()
            )
        })?;
        let legacy_hash = blake3::hash(&legacy_bytes).to_hex().to_string();
        let mut record = match MigrationMetadata::read(&paths.fixed_main)
            .context("read paper migration metadata")?
        {
            Some(record) => record,
            None => {
                let boundary = capture_log_bindings(&paths)?;
                let version_one_hash = MigrationMetadata::seal_version_one_main(&paths.fixed_main)
                    .context("seal version-one paper main")?;
                let version_one_backup =
                    hash_qualified_path(&paths.fixed_main, "v1", &version_one_hash)?;
                MigrationMetadata::preserve_version_one_main(
                    &paths.fixed_main,
                    &version_one_backup,
                    &version_one_hash,
                )
                .context("preserve immutable version-one paper main")?;
                let side_main = hash_qualified_path(&paths.fixed_main, "v2", &version_one_hash)?;
                let built = MigrationMetadata::build_side_main_v2(&paths.fixed_main, &side_main)
                    .context("build version-two paper side main")?;
                ensure!(
                    built.version_one_main_hash == version_one_hash,
                    "version-one paper main changed while building the side main"
                );
                let mut input_hashes = BTreeMap::new();
                input_hashes.insert("version_one_main_blake3".to_owned(), version_one_hash);
                input_hashes.insert("legacy_history_blake3".to_owned(), legacy_hash.clone());
                input_hashes.insert(
                    "legacy_history_size".to_owned(),
                    legacy_bytes.len().to_string(),
                );
                input_hashes.insert(
                    LEGACY_HISTORY_OBSERVED_AT_KEY.to_owned(),
                    imported_at_unix.to_string(),
                );
                input_hashes.insert("binary_identity".to_owned(), paths.binary_identity.clone());
                let initial = MigrationRecord {
                    version_one_boundary: boundary,
                    activation_tails: None,
                    phase: MigrationPhase::BoundaryRecorded,
                    side_main_path: std::fs::canonicalize(&side_main)?,
                    input_hashes,
                };
                MigrationMetadata::record_once(&paths.fixed_main, &initial)
                    .context("record immutable paper migration boundary")?;
                MigrationMetadata::mirror_to_side(&paths.fixed_main, &side_main)
                    .context("mirror initial paper migration record")?;
                initial
            }
        };

        validate_inputs(&record, &paths, &legacy_bytes, &legacy_hash)?;
        let side_main = record.side_main_path.clone();
        if record.phase == MigrationPhase::BoundaryRecorded {
            append_legacy_input_and_select_roll_forward(&record, &paths, &legacy_bytes)?;
            record = MigrationMetadata::advance_phase(
                &paths.fixed_main,
                MigrationPhase::BoundaryRecorded,
                MigrationPhase::VersionTwoInputsAppending,
            )
            .context("select paper migration roll-forward recovery")?;
            MigrationMetadata::mirror_to_side(&paths.fixed_main, &side_main)
                .context("mirror paper roll-forward phase")?;
        }
        ensure!(
            matches!(
                record.phase,
                MigrationPhase::VersionTwoInputsAppending
                    | MigrationPhase::SideStateBuilt
                    | MigrationPhase::ActivationTailsRecorded
            ),
            "schema-v1 paper main has invalid migration phase {}",
            record.phase
        );
        verify_boundary_prefixes(&record.version_one_boundary, &paths)?;

        let side = PaperStateDb::open(&side_main).context("open version-two paper side main")?;
        let imported = side
            .import_legacy_wallet_history(&legacy_bytes, imported_at_unix)
            .context("import captured legacy wallet history")?;
        ensure!(
            imported.source_hash == legacy_hash && imported.result == "imported",
            "legacy wallet history import proof does not match the captured input"
        );
        drop(side);

        let version_one_hash = record
            .input_hashes
            .get("version_one_main_blake3")
            .context("migration record omitted version_one_main_blake3")?;
        let version_one_backup = hash_qualified_path(&paths.fixed_main, "v1", version_one_hash)?;
        MigrationMetadata::preserve_version_one_main(
            &paths.fixed_main,
            &version_one_backup,
            version_one_hash,
        )
        .context("verify immutable version-one paper backup")?;
        if record.phase == MigrationPhase::ActivationTailsRecorded {
            PaperMigrationSession {
                paths: paths.clone(),
                side_main,
                version_one_backup,
            }
            .finish()
            .context("resume paper migration from recorded activation tails")?;
            let installed = MigrationMetadata::read(&paths.fixed_main)
                .context("read resumed installed paper migration")?
                .context("resumed installed paper migration omitted metadata")?;
            return Ok(Self {
                active_main: paths.fixed_main,
                record: installed,
                session: None,
            });
        }
        Ok(Self {
            active_main: side_main.clone(),
            record,
            session: Some(PaperMigrationSession {
                paths,
                side_main,
                version_one_backup,
            }),
        })
    }
}

impl PaperMigrationSession {
    /// Complete the phase transitions after authoritative reload and the causal
    /// bracket, install only the synchronized main, and re-verify final tails.
    pub fn finish(self) -> Result<PaperMainSeal> {
        MigrationMetadata::activation_facts_hash(&self.side_main, &self.paths.binary_identity)
            .context("verify paper migration activation census")?;
        let mut record = MigrationMetadata::read(&self.paths.fixed_main)
            .context("read paper migration state before activation")?
            .context("paper migration record disappeared before activation")?;
        if record.phase == MigrationPhase::VersionTwoInputsAppending {
            record = MigrationMetadata::advance_phase(
                &self.paths.fixed_main,
                MigrationPhase::VersionTwoInputsAppending,
                MigrationPhase::SideStateBuilt,
            )
            .context("record completed paper side-state build")?;
            MigrationMetadata::mirror_to_side(&self.paths.fixed_main, &self.side_main)
                .context("mirror completed paper side-state build")?;
        }
        if record.phase == MigrationPhase::SideStateBuilt {
            verify_boundary_prefixes(&record.version_one_boundary, &self.paths)?;
            let activation = capture_log_bindings(&self.paths)?;
            record = MigrationMetadata::record_activation_tails(&self.paths.fixed_main, activation)
                .context("record final paper activation tails")?;
            MigrationMetadata::mirror_to_side(&self.paths.fixed_main, &self.side_main)
                .context("mirror final paper activation tails")?;
        }
        ensure!(
            record.phase == MigrationPhase::ActivationTailsRecorded,
            "paper migration cannot install from phase {}",
            record.phase
        );
        let activation = record
            .activation_tails
            .clone()
            .context("paper activation phase omitted final tails")?;
        let before_install = capture_log_bindings(&self.paths)?;
        verify_log_bindings(&activation, &before_install)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let side = MigrationMetadata::finalize_side_main_v2(&self.side_main)
            .context("finalize version-two paper side main")?;
        MigrationMetadata::install_side_main_v2(
            &self.paths.fixed_main,
            &self.side_main,
            &self.version_one_backup,
            &side.hash,
        )
        .context("atomically install version-two paper main")?;
        MigrationMetadata::advance_phase(
            &self.paths.fixed_main,
            MigrationPhase::ActivationTailsRecorded,
            MigrationPhase::Installed,
        )
        .context("record installed paper authority")?;
        let current = capture_log_bindings(&self.paths)?;
        verify_log_bindings(&activation, &current)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        MigrationMetadata::finalize_side_main_v2(&self.paths.fixed_main)
            .context("seal installed version-two paper main")
    }
}

/// Offline: rebind the installed activation tails of a generation that was moved as a whole (the
/// rehearsal's private copy) to the configured paths, after proving that every configured log
/// still carries the recorded activation prefix with the installed branch's own checks (#570).
/// Returns `false` without writing when a moved copy's recorded paths already match; never touches
/// the logs. The paper-state owner refuses a main that is still in its recorded origin directory
/// (the production generation) whether or not its paths match.
pub fn update_installed_log_paths(paths: &PaperMigrationPaths) -> Result<bool> {
    let schema = MigrationMetadata::schema_version(&paths.fixed_main)
        .context("inspect fixed paper-state schema before updating migration paths")?;
    ensure!(
        schema == SCHEMA_VERSION || schema == LEGACY_EXACT_MIGRATION_VERSION,
        "paper migration paths can only be updated on an installed version-two main (schema {schema})"
    );
    let record = MigrationMetadata::read(&paths.fixed_main)
        .context("read installed paper migration record")?
        .context("v2 paper main omitted migration record")?;
    MigrationMetadata::verify_activation_facts(&paths.fixed_main)
        .context("verify installed paper migration activation census")?;
    ensure!(
        record.phase == MigrationPhase::Installed,
        "v2 paper main is not installed: {}",
        record.phase
    );
    let recorded = record
        .activation_tails
        .as_ref()
        .context("installed paper migration omitted activation tails")?;
    let configured = DurableLogBindings {
        source: LogTailBinding {
            path: std::fs::canonicalize(&paths.source_log)?,
            ..recorded.source.clone()
        },
        paper: LogTailBinding {
            path: std::fs::canonicalize(&paths.paper_log)?,
            ..recorded.paper.clone()
        },
        live_journal: LogTailBinding {
            path: std::fs::canonicalize(&paths.live_journal)?,
            ..recorded.live_journal.clone()
        },
    };
    Scanner::verify_prefix(&configured.source)
        .context("verify recorded source-log prefix at the configured path")?;
    Scanner::verify_prefix(&configured.paper)
        .context("verify recorded paper-log prefix at the configured path")?;
    LiveJournal::verified_tail(&paths.live_journal)
        .context("verify current native live-journal payloads")?;
    Scanner::verify_prefix(&configured.live_journal)
        .context("verify recorded live-journal prefix at the configured path")?;
    MigrationMetadata::update_installed_log_paths(&paths.fixed_main, &configured)
        .context("update installed paper migration paths")
}

/// Offline pre-append rollback. The fixed v1 record is the authority; all
/// three logs must still equal its immutable boundary before only the supplied
/// checkpointed v1 main is restored.
pub fn rollback_version_one(
    paths: &PaperMigrationPaths,
    version_one_backup: &Path,
    failed_side: &Path,
) -> Result<()> {
    let current = capture_log_bindings(paths)?;
    MigrationMetadata::rollback_pre_activation(
        &paths.fixed_main,
        version_one_backup,
        failed_side,
        &current,
    )
    .context("restore checkpointed version-one paper main")
}

fn capture_log_bindings(paths: &PaperMigrationPaths) -> Result<DurableLogBindings> {
    Ok(DurableLogBindings {
        source: Scanner::verify(&paths.source_log)
            .with_context(|| format!("verify source log {}", paths.source_log.display()))?,
        paper: Scanner::verify(&paths.paper_log)
            .with_context(|| format!("verify paper log {}", paths.paper_log.display()))?,
        live_journal: LiveJournal::verified_tail(&paths.live_journal)
            .with_context(|| format!("verify live journal {}", paths.live_journal.display()))?,
    })
}

fn verify_boundary_prefixes(
    boundary: &DurableLogBindings,
    paths: &PaperMigrationPaths,
) -> Result<()> {
    ensure!(
        boundary.source.path == std::fs::canonicalize(&paths.source_log)?,
        "configured source log no longer matches the migration record"
    );
    ensure!(
        boundary.paper.path == std::fs::canonicalize(&paths.paper_log)?,
        "configured paper log no longer matches the migration record"
    );
    ensure!(
        boundary.live_journal.path == std::fs::canonicalize(&paths.live_journal)?,
        "configured live journal no longer matches the migration record"
    );
    Scanner::verify_prefix(&boundary.source).context("verify recorded source-log prefix")?;
    Scanner::verify_prefix(&boundary.paper).context("verify recorded paper-log prefix")?;
    LiveJournal::verified_tail(&paths.live_journal)
        .context("verify current native live-journal payloads")?;
    Scanner::verify_prefix(&boundary.live_journal)
        .context("verify recorded live-journal prefix")?;
    Ok(())
}

fn validate_inputs(
    record: &MigrationRecord,
    paths: &PaperMigrationPaths,
    legacy_bytes: &[u8],
    legacy_hash: &str,
) -> Result<()> {
    ensure!(
        record
            .input_hashes
            .get("legacy_history_blake3")
            .is_some_and(|stored| stored == legacy_hash),
        "captured legacy wallet history hash changed"
    );
    ensure!(
        record
            .input_hashes
            .get("legacy_history_size")
            .is_some_and(|stored| stored == &legacy_bytes.len().to_string()),
        "captured legacy wallet history size changed"
    );
    ensure!(
        record
            .input_hashes
            .get("binary_identity")
            .is_some_and(|stored| stored == &paths.binary_identity),
        "paper migration binary identity changed"
    );
    if !record.side_main_path.is_file() {
        bail!(
            "recorded paper side main is missing: {}",
            record.side_main_path.display()
        );
    }
    Ok(())
}

fn append_legacy_input_and_select_roll_forward(
    record: &MigrationRecord,
    paths: &PaperMigrationPaths,
    legacy_bytes: &[u8],
) -> Result<()> {
    let observed_at_unix = record
        .input_hashes
        .get(LEGACY_HISTORY_OBSERVED_AT_KEY)
        .context("migration record omitted legacy_history_observed_at_unix")?
        .parse::<i64>()
        .context("migration record has invalid legacy_history_observed_at_unix")?;
    let timestamp = OffsetDateTime::from_unix_timestamp(observed_at_unix)
        .context("legacy-history migration timestamp is outside the supported range")?;
    let boundary = &record.version_one_boundary;
    let mut current = capture_log_bindings(paths)?;
    ensure!(
        current.paper == boundary.paper && current.live_journal == boundary.live_journal,
        "paper or live-journal log changed before the first version-two append"
    );
    Scanner::verify_prefix(&boundary.source)
        .context("verify source boundary before first append")?;

    let sequence = boundary
        .source
        .last_sequence
        .map_or(Ok(EventSeq(0)), |last| {
            last.0
                .checked_add(1)
                .map(EventSeq)
                .context("source-log sequence overflow before migration append")
        })?;
    let source_id = SourceId(LEGACY_HISTORY_SOURCE_ID.to_owned());
    let observed_at = SourceTimestamp(timestamp);
    let received_at = ReceivedAt(timestamp);
    let content_type = ContentType::Json;
    let (_, _, expected_hash) = compute_hashes(HashInput {
        seq: sequence,
        source_id: &source_id,
        schema_version: LEGACY_HISTORY_SCHEMA_VERSION,
        parser_version: LEGACY_HISTORY_PARSER_VERSION,
        observed_at: &observed_at,
        received_at: &received_at,
        content_type: &content_type,
        prev_hash: &boundary.source.last_hash,
        payload: legacy_bytes,
    })?;

    if current.source == boundary.source {
        let mut sink = SourceEventSink::open(&paths.source_log)
            .context("open normal source-log owner for the first migration append")?;
        let appended = sink
            .append_durable(EnvelopeIn {
                source_id,
                schema_version: LEGACY_HISTORY_SCHEMA_VERSION,
                parser_version: LEGACY_HISTORY_PARSER_VERSION,
                observed_at,
                received_at,
                content_type,
                payload: legacy_bytes.to_vec(),
            })
            .context("append and synchronize captured legacy history")?;
        ensure!(
            appended.sequence == sequence,
            "legacy-history migration append used an unexpected sequence"
        );
        drop(sink);
        current = capture_log_bindings(paths)?;
    }

    ensure!(
        current.source.last_sequence == Some(sequence)
            && current.source.last_hash == expected_hash
            && current.source.physical_tail > boundary.source.physical_tail,
        "source log grew by data other than the exact first version-two migration input"
    );
    ensure!(
        current.paper == boundary.paper && current.live_journal == boundary.live_journal,
        "paper or live-journal log changed during the first version-two append"
    );
    Ok(())
}

fn hash_qualified_path(fixed: &Path, generation: &str, hash: &str) -> Result<PathBuf> {
    let parent = fixed.parent().unwrap_or_else(|| Path::new("."));
    let name = fixed
        .file_name()
        .and_then(|value| value.to_str())
        .context("paper-state path has no UTF-8 file name")?;
    Ok(parent.join(format!("{name}.{generation}.{hash}.db")))
}

#[cfg(all(test, feature = "scenario"))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_event_log::Writer;
    use pe_strategy_winner_follow::PerTradeCap;
    use rusqlite::Connection;

    use crate::config::ServiceConfig;

    const V1_SCHEMA: &str = include_str!("../../paper-state/tests/fixtures/paper_state_v1.sql");

    #[test]
    fn invalid_initial_configuration_blocks_migration_activation() {
        let mut config = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert!(validate_initial_configuration(&config).is_err());
        config.price_impact_cap_bps = 100;
        config.per_trade_cap = PerTradeCap::Unlimited;
        validate_initial_configuration(&config).unwrap();
        config.per_trade_cap = PerTradeCap::Bps(100);
        assert!(validate_initial_configuration(&config).is_err());
        assert!(validate_migration_authority(1, false).is_err());
        validate_migration_authority(1, true).unwrap();
        validate_migration_authority(SCHEMA_VERSION, false).unwrap();
    }

    #[test]
    fn crash_after_first_v2_append_resumes_without_duplicate_input() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("paper_state.db");
        let source = dir.path().join("source.log");
        let paper = dir.path().join("paper.log");
        let journal = dir.path().join("live_journal.log");
        let history = dir.path().join("wallet_market_history.json");
        let history_bytes =
            br#"{"wallets":[{"wallet":"0x1111111111111111111111111111111111111111","markets":["condition-1"]}]}"#;
        Connection::open(&fixed)
            .unwrap()
            .execute_batch(V1_SCHEMA)
            .unwrap();
        Writer::open(&source).unwrap().sync().unwrap();
        Writer::open(&paper).unwrap().sync().unwrap();
        drop(LiveJournal::open(&journal).unwrap());
        std::fs::write(&history, history_bytes).unwrap();
        let paths = PaperMigrationPaths {
            fixed_main: fixed.clone(),
            source_log: source,
            paper_log: paper,
            live_journal: journal,
            legacy_history: history,
            binary_identity: "crash-fixture-build".to_owned(),
        };
        let boundary = capture_log_bindings(&paths).unwrap();
        let v1_hash = MigrationMetadata::seal_version_one_main(&fixed).unwrap();
        let v1_backup = hash_qualified_path(&fixed, "v1", &v1_hash).unwrap();
        MigrationMetadata::preserve_version_one_main(&fixed, &v1_backup, &v1_hash).unwrap();
        let side = hash_qualified_path(&fixed, "v2", &v1_hash).unwrap();
        MigrationMetadata::build_side_main_v2(&fixed, &side).unwrap();
        let legacy_hash = blake3::hash(history_bytes).to_hex().to_string();
        let record = MigrationRecord {
            version_one_boundary: boundary,
            activation_tails: None,
            phase: MigrationPhase::BoundaryRecorded,
            side_main_path: std::fs::canonicalize(&side).unwrap(),
            input_hashes: BTreeMap::from([
                ("version_one_main_blake3".to_owned(), v1_hash),
                ("legacy_history_blake3".to_owned(), legacy_hash),
                (
                    "legacy_history_size".to_owned(),
                    history_bytes.len().to_string(),
                ),
                (
                    LEGACY_HISTORY_OBSERVED_AT_KEY.to_owned(),
                    "1788192000".to_owned(),
                ),
                ("binary_identity".to_owned(), paths.binary_identity.clone()),
            ]),
        };
        MigrationMetadata::record_once(&fixed, &record).unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();

        // Model power loss after Writer::sync and before advance_phase.
        append_legacy_input_and_select_roll_forward(&record, &paths, history_bytes).unwrap();
        let appended_tail = Scanner::verify(&paths.source_log).unwrap();
        assert_eq!(
            MigrationMetadata::read(&fixed).unwrap().unwrap().phase,
            MigrationPhase::BoundaryRecorded
        );

        let resumed = PaperMigrationBoot::prepare(paths.clone(), 1_788_192_001).unwrap();
        assert_eq!(
            resumed.record.phase,
            MigrationPhase::VersionTwoInputsAppending
        );
        assert_eq!(Scanner::verify(&paths.source_log).unwrap(), appended_tail);
        drop(resumed);

        let side_state = PaperStateDb::open(&side).unwrap();
        side_state
            .record_migration_activation_facts(
                &serde_json::json!({"fixture": "complete"}),
                &paths.binary_identity,
            )
            .unwrap();
        drop(side_state);

        MigrationMetadata::advance_phase(
            &fixed,
            MigrationPhase::VersionTwoInputsAppending,
            MigrationPhase::SideStateBuilt,
        )
        .unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
        let activation = capture_log_bindings(&paths).unwrap();
        MigrationMetadata::record_activation_tails(&fixed, activation).unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();

        // Model power loss after activation-tail sync and before database rename.
        let installed = PaperMigrationBoot::prepare(paths, 1_788_192_002).unwrap();
        assert!(installed.session.is_none());
        assert_eq!(installed.record.phase, MigrationPhase::Installed);
        assert_eq!(
            MigrationMetadata::schema_version(&fixed).unwrap(),
            SCHEMA_VERSION
        );
    }
}
