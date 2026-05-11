//! Retrospective operator-clustering audit (issue #130).
//!
//! The audit answers: "given **today's** cluster definitions, which past
//! sweep fills would have been flagged as multi-wallet operator activity?"
//! It does **not** reproduce historical clustering state at each trade
//! decision time — the walk-forward variant is explicitly out of scope.
//!
//! Output is byte-deterministic across runs on identical inputs: any field
//! derived from a `HashSet` (notably `anti_gaming_flags`) is sorted
//! lexicographically before joining, so commit-as-fixture diffs are stable.
//!
//! The clustering kernel is reused unchanged from `pe-operator-graph`:
//! `build_operator_identities(&FundingSnapshot, &ClusteringConfig::default())`.
//! Per-wallet trade counts are not threaded into the snapshot (the audit
//! binary calls `build_operator_identities` with an empty
//! `closed_trade_counts` map), so any anti-gaming flag depending on trade
//! volume may be under-reported — this is documented in `--help`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use pe_core_types::SourceTimestamp;
use pe_operator_graph::{
    ClusteringConfig, FundingEdge, FundingSnapshot, OperatorIdentity, build_operator_identities,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use time::OffsetDateTime;

use crate::cache::WalletCache;

/// Help text for the binary's `--help` flag and a fallback for `parse_args` errors.
pub const HELP_TEXT: &str = r#"pe-operator-audit — retrospective operator clustering audit (issue #130).

Usage:
    pe-operator-audit [--cache-path PATH] [--trades-ndjson PATH]
                      [--csv-out PATH] [--min-confidence PPM]

Options:
    --cache-path PATH         Wallet cache SQLite. Falls back to env
                              PE_BOOTSTRAP_CACHE_PATH. Required.
    --trades-ndjson PATH      Optional. Path to a sweep trades.ndjson; without
                              it, every operator row shows fills=0 and
                              double_funding_flag="-" — useful for inspecting
                              the cluster graph alone.
    --csv-out PATH            CSV output path (default: operator_audit.csv).
    --min-confidence PPM      Drop OperatorIdentity rows with confidence_ppm
                              below this value before sorting (default: 0).
    --help                    Print this help to stdout and exit 0.

Notes:
  * Audit is retrospective: it clusters at now_unix using current funder edges.
    It does not reproduce historical clustering state at past trade times.
  * Anti-gaming flags depending on per-wallet trade counts may be under-
    reported — the audit calls build_operator_identities with an empty
    trade slice.
"#;

/// Parsed CLI arguments. `parse_args` resolves `cache_path` from the
/// `--cache-path` flag or the `PE_BOOTSTRAP_CACHE_PATH` env var.
#[derive(Debug, Clone)]
pub struct Args {
    pub cache_path: PathBuf,
    pub trades_ndjson: Option<PathBuf>,
    pub csv_out: PathBuf,
    pub min_confidence: u32,
}

/// Parse `std::env::args()` into [`Args`].
///
/// Returns `Ok(None)` when `--help` was passed (caller prints `HELP_TEXT`).
/// Returns `Err` on unknown flags or missing required values; the binary
/// formats the error and exits non-zero.
///
/// This is the **one** function that does not return `anyhow::Result` for its
/// success-or-help branches — the help branch is an inline exit path, not a
/// propagated error.
pub fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<Option<Args>> {
    let mut iter = args.into_iter();
    let _program = iter.next(); // discard arg0

    let mut cache_path_flag: Option<PathBuf> = None;
    let mut trades_ndjson: Option<PathBuf> = None;
    let mut csv_out: Option<PathBuf> = None;
    let mut min_confidence: u32 = 0;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--cache-path" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--cache-path requires a value"))?;
                cache_path_flag = Some(PathBuf::from(v));
            }
            "--trades-ndjson" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--trades-ndjson requires a value"))?;
                trades_ndjson = Some(PathBuf::from(v));
            }
            "--csv-out" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--csv-out requires a value"))?;
                csv_out = Some(PathBuf::from(v));
            }
            "--min-confidence" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--min-confidence requires a value"))?;
                min_confidence = v
                    .parse::<u32>()
                    .with_context(|| format!("--min-confidence: not a u32: {v}"))?;
            }
            other => bail!("unknown flag: {other}"),
        }
    }

    let cache_path = match cache_path_flag {
        Some(p) => p,
        None => match std::env::var("PE_BOOTSTRAP_CACHE_PATH") {
            Ok(v) if !v.is_empty() => PathBuf::from(v),
            _ => bail!("missing --cache-path and PE_BOOTSTRAP_CACHE_PATH not set; see --help"),
        },
    };

    Ok(Some(Args {
        cache_path,
        trades_ndjson,
        csv_out: csv_out.unwrap_or_else(|| PathBuf::from("operator_audit.csv")),
        min_confidence,
    }))
}

/// Mirror of the subset of `crates/backtest/src/report.rs::TradeFill` needed for
/// audit grouping. Inlined here to avoid the `bootstrap → backtest` reverse
/// dependency. Keep field names in sync with the canonical struct.
#[derive(Debug, Deserialize)]
struct LocalTradeFill {
    operator_id: Option<String>,
    leader_wallet: String,
}

/// Named value type for [`parse_fills`] — replaces the anonymous
/// `(u64, HashSet<String>)` tuple.
#[derive(Debug, Default, Clone)]
pub struct FillStats {
    pub count: u64,
    pub distinct_wallets: HashSet<String>,
}

/// One output row in the audit table.
#[derive(Debug, Clone)]
pub struct AuditRow {
    pub rank: u32,
    pub operator_id: String,
    pub funder_root: String,
    pub wallet_count: usize,
    pub confidence_ppm: u32,
    pub fills: u64,
    pub distinct_leader_wallets: usize,
    /// `"Y"` when `distinct_leader_wallets >= 2`, else `"-"`.
    pub double_funding_flag: &'static str,
    /// `|`-separated sorted list — byte-deterministic across runs even though
    /// the source is a `HashSet`.
    pub anti_gaming_flags: String,
}

/// Bundle returned by [`run`]. Exposed so integration tests can assert on the
/// in-memory results without re-parsing the CSV.
#[derive(Debug, Clone)]
pub struct RunOutput {
    pub rows: Vec<AuditRow>,
    pub unattributed_fills: u64,
    pub total_fills: u64,
    pub markdown: String,
    pub csv_path: PathBuf,
}

/// Open the cache, build operator identities at `now_unix` using the same
/// clustering kernel as the rest of the system, and return the resulting set.
///
/// Per-wallet `closed_trade_counts` is intentionally empty — see module doc
/// for the anti-gaming-flag caveat.
pub fn load_operator_identities(cache_path: &Path) -> Result<Vec<OperatorIdentity>> {
    let cache = WalletCache::open(cache_path)
        .with_context(|| format!("open WalletCache at {}", cache_path.display()))?;
    let raw = cache
        .load_funder_edges_with_timestamp()
        .context("load_funder_edges_with_timestamp")?;

    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let snapshot_ts = SourceTimestamp(
        OffsetDateTime::from_unix_timestamp(now_unix).unwrap_or(OffsetDateTime::UNIX_EPOCH),
    );

    let edges: Vec<FundingEdge> = raw
        .into_iter()
        .filter(|(_, _, event_at)| *event_at <= now_unix)
        .map(|(funder, funded, _)| FundingEdge {
            funder,
            funded,
            amount_usd: Decimal::ZERO,
            timestamp: snapshot_ts.clone(),
        })
        .collect();

    let snapshot = FundingSnapshot {
        edges,
        wallet_ages: HashMap::new(),
        known_external: HashMap::new(),
        closed_trade_counts: HashMap::new(),
        realized_pnl_usd: HashMap::new(),
        snapshot_at: snapshot_ts,
    };

    build_operator_identities(&snapshot, &ClusteringConfig::default())
        .context("build_operator_identities")
}

/// Parse a sweep `trades.ndjson` and group fills by `operator_id`.
///
/// Returns `(fills_map, unattributed_count, total_count)`. Fills with
/// `operator_id == None` are counted into `unattributed_count` and not added
/// to the map.
pub fn parse_fills(path: &Path) -> Result<(HashMap<String, FillStats>, u64, u64)> {
    let file =
        File::open(path).with_context(|| format!("open trades.ndjson {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut fills: HashMap<String, FillStats> = HashMap::new();
    let mut unattributed: u64 = 0;
    let mut total: u64 = 0;

    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read line {idx} of {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let fill: LocalTradeFill = serde_json::from_str(&line)
            .with_context(|| format!("parse line {} of {}: {line}", idx + 1, path.display()))?;
        total = total.saturating_add(1);
        match fill.operator_id {
            Some(op_id) => {
                let stats = fills.entry(op_id).or_default();
                stats.count = stats.count.saturating_add(1);
                stats.distinct_wallets.insert(fill.leader_wallet);
            }
            None => {
                unattributed = unattributed.saturating_add(1);
            }
        }
    }

    Ok((fills, unattributed, total))
}

/// Join operator identities with grouped fills, filter by `min_confidence`,
/// sort by `(double_funding_flag DESC, fills DESC, confidence_ppm DESC)`, and
/// assign 1-indexed `rank` after sorting.
///
/// `anti_gaming_flags` is rendered as a `|`-joined sorted list — output is
/// byte-deterministic regardless of `HashSet` iteration order.
pub fn build_rows(
    identities: Vec<OperatorIdentity>,
    fills: &HashMap<String, FillStats>,
    min_confidence: u32,
) -> Vec<AuditRow> {
    let empty = FillStats::default();
    let mut rows: Vec<AuditRow> = identities
        .into_iter()
        .filter(|id| id.confidence_ppm >= min_confidence)
        .map(|id| {
            let op_key = id.operator_id.to_string();
            let stats = fills.get(&op_key).unwrap_or(&empty);
            let distinct = stats.distinct_wallets.len();
            let double_funding_flag = if distinct >= 2 { "Y" } else { "-" };
            let mut flags: Vec<String> = id
                .anti_gaming_flags
                .iter()
                .map(|f| format!("{f:?}"))
                .collect();
            flags.sort();
            let anti_gaming_flags = flags.join("|");
            AuditRow {
                rank: 0, // assigned after sort
                operator_id: op_key,
                funder_root: id.funder_root.to_string(),
                wallet_count: id.member_wallets.len(),
                confidence_ppm: id.confidence_ppm,
                fills: stats.count,
                distinct_leader_wallets: distinct,
                double_funding_flag,
                anti_gaming_flags,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        let a_flag = a.double_funding_flag == "Y";
        let b_flag = b.double_funding_flag == "Y";
        b_flag
            .cmp(&a_flag)
            .then_with(|| b.fills.cmp(&a.fills))
            .then_with(|| b.confidence_ppm.cmp(&a.confidence_ppm))
            // Tiebreak deterministically on operator_id so identical rows
            // sort stably across runs.
            .then_with(|| a.operator_id.cmp(&b.operator_id))
    });

    for (i, row) in rows.iter_mut().enumerate() {
        row.rank = u32::try_from(i + 1).unwrap_or(u32::MAX);
    }

    rows
}

/// Single source of truth for column order — both formatters call this.
/// Returns 9 stringified columns aligned with the table header in
/// [`render_markdown`] and the CSV header in [`write_csv`].
pub fn row_fields(row: &AuditRow) -> [String; 9] {
    [
        row.rank.to_string(),
        row.operator_id.clone(),
        row.funder_root.clone(),
        row.wallet_count.to_string(),
        row.confidence_ppm.to_string(),
        row.fills.to_string(),
        row.distinct_leader_wallets.to_string(),
        row.double_funding_flag.to_string(),
        row.anti_gaming_flags.clone(),
    ]
}

/// Format a percentage as `XX.X` with one decimal. Used in the unattributed-
/// fills summary line.
fn pct(num: u64, den: u64) -> String {
    if den == 0 {
        return "0.0".to_owned();
    }
    let scaled = (num.saturating_mul(1000)) / den;
    let whole = scaled / 10;
    let frac = scaled % 10;
    format!("{whole}.{frac}")
}

const COLUMNS: [&str; 9] = [
    "rank",
    "operator_id",
    "funder_root",
    "wallet_count",
    "confidence_ppm",
    "fills",
    "distinct_leader_wallets",
    "double_funding_flag",
    "anti_gaming_flags",
];

/// Render the audit table as markdown. Summary line is printed both before
/// the table (so it's visible to `head`/`grep` consumers) and as a trailing
/// line.
pub fn render_markdown(rows: &[AuditRow], unattributed_fills: u64, total_fills: u64) -> String {
    let mut out = String::new();
    let summary = format!(
        "Unattributed fills: {unattributed_fills} / {total_fills} ({}%) — fills with no operator_id.\n",
        pct(unattributed_fills, total_fills),
    );
    out.push_str(&summary);
    out.push('\n');
    out.push_str("| ");
    out.push_str(&COLUMNS.join(" | "));
    out.push_str(" |\n|");
    for _ in 0..COLUMNS.len() {
        out.push_str("---|");
    }
    out.push('\n');
    for row in rows {
        let fields = row_fields(row);
        out.push_str("| ");
        out.push_str(&fields.join(" | "));
        out.push_str(" |\n");
    }
    out.push('\n');
    out.push_str(&summary);
    out
}

/// Write the audit table as CSV. Header includes a comment line with
/// `unattributed_fills` so consumers don't need the markdown copy.
pub fn write_csv(
    rows: &[AuditRow],
    unattributed_fills: u64,
    total_fills: u64,
    path: &Path,
) -> Result<()> {
    let mut f =
        File::create(path).with_context(|| format!("create CSV output {}", path.display()))?;
    writeln!(
        f,
        "# unattributed_fills={unattributed_fills}/{total_fills} ({}%)",
        pct(unattributed_fills, total_fills),
    )?;
    writeln!(f, "{}", COLUMNS.join(","))?;
    for row in rows {
        writeln!(f, "{}", row_fields(row).join(","))?;
    }
    Ok(())
}

/// Top-level entry point — runs the full audit and returns the in-memory
/// result. Side effect: writes `csv_out`. Stdout printing is the caller's
/// responsibility so tests can capture the markdown without spawning a
/// subprocess.
pub fn run(args: Args) -> Result<RunOutput> {
    let identities = load_operator_identities(&args.cache_path)?;
    let (fills, unattributed_fills, total_fills) = match &args.trades_ndjson {
        Some(p) => parse_fills(p)?,
        None => (HashMap::new(), 0, 0),
    };
    let rows = build_rows(identities, &fills, args.min_confidence);
    let markdown = render_markdown(&rows, unattributed_fills, total_fills);
    write_csv(&rows, unattributed_fills, total_fills, &args.csv_out)?;
    Ok(RunOutput {
        rows,
        unattributed_fills,
        total_fills,
        markdown,
        csv_path: args.csv_out,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_help_returns_none() {
        let v = vec!["pe-operator-audit".to_owned(), "--help".to_owned()];
        let out = parse_args(v).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parse_args_missing_cache_path_errors() {
        // Ensure env doesn't accidentally satisfy the requirement.
        // SAFETY: single-test, no concurrent env mutation; nextest isolates per-test envs.
        // The `unsafe` is required because env::remove_var is marked unsafe in Rust 2024.
        // Justification: this test does not run concurrently with other tests reading the
        // same var, and the workspace forbids unsafe — we use safe `set_var` alternative
        // by simply not depending on env in this branch.
        let v = vec!["pe-operator-audit".to_owned()];
        // We can't safely unset the env in a test without unsafe in 2024 edition,
        // so we conditionally skip this assertion when the env var is set externally.
        if std::env::var("PE_BOOTSTRAP_CACHE_PATH").is_err() {
            assert!(parse_args(v).is_err());
        }
    }

    #[test]
    fn parse_args_unknown_flag_errors() {
        let v = vec!["pe-operator-audit".to_owned(), "--bogus".to_owned()];
        assert!(parse_args(v).is_err());
    }

    #[test]
    fn parse_args_with_all_flags() {
        let v = vec![
            "pe-operator-audit".to_owned(),
            "--cache-path".to_owned(),
            "/tmp/c.db".to_owned(),
            "--trades-ndjson".to_owned(),
            "/tmp/t.ndjson".to_owned(),
            "--csv-out".to_owned(),
            "/tmp/o.csv".to_owned(),
            "--min-confidence".to_owned(),
            "5000".to_owned(),
        ];
        let args = parse_args(v).unwrap().unwrap();
        assert_eq!(args.cache_path, PathBuf::from("/tmp/c.db"));
        assert_eq!(args.trades_ndjson, Some(PathBuf::from("/tmp/t.ndjson")));
        assert_eq!(args.csv_out, PathBuf::from("/tmp/o.csv"));
        assert_eq!(args.min_confidence, 5000);
    }

    #[test]
    fn pct_handles_zero_denominator() {
        assert_eq!(pct(0, 0), "0.0");
        assert_eq!(pct(5, 0), "0.0");
    }

    #[test]
    fn pct_rounds_one_decimal_via_truncation() {
        assert_eq!(pct(1, 10), "10.0");
        assert_eq!(pct(1, 3), "33.3");
        assert_eq!(pct(2, 3), "66.6");
        assert_eq!(pct(0, 100), "0.0");
        assert_eq!(pct(100, 100), "100.0");
    }
}
