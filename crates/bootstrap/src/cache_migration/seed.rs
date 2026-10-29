//! A fresh cache seeded with known wallets, carrying the old cache's certified cumulative
//! drops and its discovery exclusions.

use super::*;

/// Re-reading the venue cannot recreate a drop whose trigger has since vanished, so a
/// wallet's first finalize starts from the drops its old certificate held. The rows are
/// history-guarded and committed with the certificates until that finalize consumes them.
pub(super) const SEED_DROPS_TABLE: &str = "activity_seed_drops_v3";
const SEED_DROPS_SCHEMA: &str = "CREATE TABLE activity_seed_drops_v3 (
    wallet_hex TEXT PRIMARY KEY, scope_drops_json TEXT NOT NULL)";

#[derive(Debug, Serialize)]
pub struct SeededCacheReport {
    pub cache_path: PathBuf,
    pub wallets_sha256: String,
    pub wallets: usize,
    pub wallets_with_drops: usize,
    pub tombstones: usize,
    pub infrastructure_flags: usize,
    pub already_initialized: bool,
}

/// What the old cache hands on: every certified non-empty cumulative drop (discovery may
/// admit any of its wallets later), its tombstones and its infrastructure flags.
struct Carried {
    drops: BTreeMap<String, String>,
    tombstones: BTreeMap<String, (i64, String)>,
    infra: BTreeSet<String>,
}

/// An initialized, never-collected seeded cache: every wallet row as
/// `(source_bits, is_infra, is_active)`, the tombstones and the carried drops.
#[derive(Debug, PartialEq, Eq)]
struct SeededState {
    wallets: BTreeMap<String, (i64, bool, bool)>,
    tombstones: BTreeMap<String, (i64, String)>,
    drops: BTreeMap<String, String>,
}

/// Create a schema-one cache at `config.cache_path` whose active wallets are exactly
/// `wallets_path`'s, as market participants, carrying what the finalized format-three cache
/// `drops_from` holds (see [`Carried`]). The cache appears at its path only once complete; an
/// existing cache succeeds only while it is still exactly that initialized, uncollected cache.
pub fn init_seeded_cache(
    config: &BootstrapConfig,
    wallets_path: &Path,
    drops_from: &Path,
) -> Result<SeededCacheReport, BootstrapError> {
    let wallets = read_seed_wallets(wallets_path)?;
    let carried = Carried::read(drops_from, &wallets)?;
    let target = &config.cache_path;
    let report = |already_initialized| SeededCacheReport {
        cache_path: target.clone(),
        wallets_sha256: sha256_bytes(
            wallets
                .iter()
                .map(|wallet| format!("{wallet}\n"))
                .collect::<String>()
                .as_bytes(),
        ),
        wallets: wallets.len(),
        wallets_with_drops: carried.drops.len(),
        tombstones: carried.tombstones.len(),
        infrastructure_flags: carried.infra.len(),
        already_initialized,
    };
    if target.exists() {
        return if seeded_state(target)?.is_some_and(|stored| stored == carried.state(&wallets)) {
            Ok(report(true))
        } else {
            invalid(format!(
                "{} already exists and is not this initialized, uncollected seeded cache",
                target.display()
            ))
        };
    }
    let unfinished = sidecar_path(target, ".init");
    for path in [
        unfinished.clone(),
        sidecar_path(&unfinished, "-wal"),
        sidecar_path(&unfinished, "-shm"),
    ] {
        if path.exists() {
            require_regular_file(&path, "unfinished seeded cache")?;
            std::fs::remove_file(&path)?;
        }
    }
    let mut seeded = config.clone();
    seeded.cache_path = unfinished.clone();
    drop(WalletCache::open_configured(&seeded)?);
    // Tombstones first, so the upserts below meet the existing gate.
    let mut connection = Connection::open(&unfinished)?;
    let transaction = connection.transaction()?;
    {
        let mut insert = transaction.prepare(
            "INSERT INTO purged_wallets (wallet_hex, purged_at_unix, reason) VALUES (?1, ?2, ?3)",
        )?;
        for (wallet, (purged_at, reason)) in &carried.tombstones {
            insert.execute(params![wallet, purged_at, reason])?;
        }
    }
    transaction.commit()?;
    drop(connection);
    let mut cache = WalletCache::open_configured(&seeded)?;
    let rows: Vec<crate::cache::WalletUpsertRow> = carried
        .infra
        .iter()
        .map(|wallet| (wallet.clone(), 0, true, None, None, None, 0))
        .chain(wallets.iter().map(|wallet| {
            let source = crate::pile::SRC_MARKET_PARTICIPANT;
            (wallet.clone(), source, false, None, None, None, 0)
        }))
        .collect();
    cache.upsert_wallets_bulk(&rows)?;
    let activated = crate::pile::apply_activation_rules(&mut cache)?;
    drop(cache);
    if activated != wallets.len() {
        return invalid(format!(
            "activated {activated} of {} seeded wallets",
            wallets.len()
        ));
    }
    let mut connection = Connection::open(&unfinished)?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(SEED_DROPS_SCHEMA)?;
    {
        let mut insert =
            transaction.prepare("INSERT INTO activity_seed_drops_v3 VALUES (?1, ?2)")?;
        for (wallet, json) in &carried.drops {
            insert.execute(params![wallet, json])?;
        }
    }
    for operation in ["INSERT", "UPDATE", "DELETE"] {
        transaction.execute_batch(&history_trigger_sql(SEED_DROPS_TABLE, operation))?;
    }
    transaction.commit()?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(&unfinished)?;
    sync_file_and_parent(&unfinished)?;
    std::fs::rename(&unfinished, target).map_err(map_rename_error)?;
    sync_parent(target)?;
    Ok(report(false))
}

pub(super) fn has_seed_drops(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [SEED_DROPS_TABLE],
        |row| row.get(0),
    )
}

/// A wallet's carried drops, read by its first finalize when it has no certificate.
pub(super) fn seed_drops(connection: &Connection, wallet: &str) -> Result<String, BootstrapError> {
    if !has_seed_drops(connection)? {
        return Ok("[]".to_owned());
    }
    Ok(connection
        .query_row(
            "SELECT scope_drops_json FROM activity_seed_drops_v3 WHERE wallet_hex = ?1",
            [wallet],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or_else(|| "[]".to_owned()))
}

/// The wallet's certificate now holds its drops, so its carried row goes.
pub(super) fn consume_seed_drops(
    transaction: &rusqlite::Transaction<'_>,
    wallet: &str,
) -> Result<(), BootstrapError> {
    transaction.execute(
        "DELETE FROM activity_seed_drops_v3 WHERE wallet_hex = ?1",
        [wallet],
    )?;
    Ok(())
}

fn read_seed_wallets(path: &Path) -> Result<BTreeSet<String>, BootstrapError> {
    let mut wallets = BTreeSet::new();
    for (index, line) in std::fs::read_to_string(path)?.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let wallet = WalletAddress::from_hex(line).map_err(|error| BootstrapError::Invalid {
            message: format!("seed list line {}: {error}", index + 1),
        })?;
        wallets.insert(wallet.to_string());
    }
    if wallets.is_empty() {
        return invalid("the seed list is empty".to_owned());
    }
    Ok(wallets)
}

impl Carried {
    /// Read from a format-three cache whose certificates verify against its recorded
    /// commitment. Seeds must be eligible there: a tombstoned or infrastructure-flagged
    /// wallet cannot be activated.
    fn read(path: &Path, seeds: &BTreeSet<String>) -> Result<Self, BootstrapError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        if fresh_collection_record(&connection)?.is_none_or(|record| record.version != 4) {
            return invalid("cumulative drops come only from a format-three cache".to_owned());
        }
        verify_history_certificates(&connection)?;
        let mut drops = BTreeMap::new();
        let mut statement = connection.prepare(
            "SELECT wallet_hex, scope_drops_json FROM activity_wallet_history_v3
             WHERE scope_drops_json != '[]'",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let (wallet, json): (String, String) = (row.get(0)?, row.get(1)?);
            let parsed: Vec<projection_v3::ScopeDrop> = serde_json::from_str(&json)?;
            if canonical_json(&parsed)? != json {
                return invalid(format!("non-canonical certified drops for {wallet}"));
            }
            drops.insert(wallet, json);
        }
        let tombstones = connection
            .prepare("SELECT wallet_hex, purged_at_unix, reason FROM purged_wallets")?
            .query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?
            .collect::<Result<BTreeMap<String, (i64, String)>, _>>()?;
        let infra = connection
            .prepare("SELECT wallet_hex FROM wallets WHERE is_infra = 1")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<BTreeSet<String>, _>>()?;
        let ineligible: Vec<_> = seeds
            .iter()
            .filter(|seed| tombstones.contains_key(*seed) || infra.contains(*seed))
            .collect();
        if !ineligible.is_empty() {
            return invalid(format!(
                "seeds tombstoned or infrastructure-flagged in the old cache: {ineligible:?}"
            ));
        }
        Ok(Self {
            drops,
            tombstones,
            infra,
        })
    }

    /// The cache initialization produces: the seeds active as market participants, and
    /// every flagged wallet the tombstone gate admits (a tombstoned one gets no row).
    fn state(&self, seeds: &BTreeSet<String>) -> SeededState {
        let flagged = self
            .infra
            .iter()
            .filter(|wallet| !self.tombstones.contains_key(*wallet))
            .map(|wallet| (wallet.clone(), (0, true, false)));
        let seeded = seeds.iter().map(|wallet| {
            let source = crate::pile::SRC_MARKET_PARTICIPANT;
            (wallet.clone(), (source, false, true))
        });
        SeededState {
            wallets: flagged.chain(seeded).collect(),
            tombstones: self.tombstones.clone(),
            drops: self.drops.clone(),
        }
    }
}

/// `None` unless PATH is still schema one with no collection, legacy trades or migration
/// state: a collected cache is never accepted as initialized.
fn seeded_state(path: &Path) -> Result<Option<SeededState>, BootstrapError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let schema: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if schema != crate::cache::CACHE_SCHEMA_VERSION_V1 {
        return Ok(None);
    }
    let uncollected: bool = connection.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'cache_v2_migration_state')
         AND NOT EXISTS(SELECT 1 FROM trades)",
        [],
        |row| row.get(0),
    )?;
    if !uncollected || !has_seed_drops(&connection)? {
        return Ok(None);
    }
    let wallets = connection
        .prepare("SELECT wallet_hex, source_bits, is_infra, is_active FROM wallets")?
        .query_map([], |row| {
            Ok((row.get(0)?, (row.get(1)?, row.get(2)?, row.get(3)?)))
        })?
        .collect::<Result<_, _>>()?;
    let tombstones = connection
        .prepare("SELECT wallet_hex, purged_at_unix, reason FROM purged_wallets")?
        .query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?
        .collect::<Result<_, _>>()?;
    let drops = connection
        .prepare("SELECT wallet_hex, scope_drops_json FROM activity_seed_drops_v3")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok(Some(SeededState {
        wallets,
        tombstones,
        drops,
    }))
}
