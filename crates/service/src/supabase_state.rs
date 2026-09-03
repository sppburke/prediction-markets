//! Authoritative Supabase paper-state client (issue #397).
//!
//! When `PE_SUPABASE_AUTHORITATIVE=1`, Supabase is the system of record for "the money and
//! the book": the new `paper_bankroll` + `paper_positions` tables and the reused
//! `paper_fills` + `settled_markets`. Mutations go through two Postgres RPCs —
//! [`SupabaseStateClient::commit_fill`] and [`SupabaseStateClient::apply_resolution`] — that
//! are atomic and idempotent (the SQL gates each money write on the dedup row newly
//! inserting and mutates `paper_bankroll` via a single self-referencing UPDATE, so the
//! concurrent fill and resolution tasks cannot lose an update). Local SQLite is mirrored
//! write-through and is the read cache; `seen_trades`/`leader_positions`/`poll_cursors`/
//! `meta` stay local-only.
//!
//! The two write-through paths are exposed as the free functions
//! [`commit_fill_authoritative`] and [`apply_resolution_authoritative`] (RPC first, SQLite
//! mirror second), generic over [`SupabaseStateTrait`] so scenario tests drive them with an
//! in-memory fake and inject RPC failures with no live network — mirroring the
//! [`crate::supabase_sink`] `SinkWriter` seam.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pe_core_types::{EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{
    FillRecord, FillRow, LeaderPositionRow, PaperPositionRow, PaperStateDb, PaperStateError,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::decision_replay::{
    AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
};
use crate::orchestrator::{pending_terminal, recorded_fill_terminal, render_pending_evidence};
use crate::supabase_reader::auth_token;
use crate::supabase_sink::{SupabaseFillRow, supabase_fill_from};

/// Errors from the authoritative Supabase boundary. On the trade path an
/// [`SupabaseStateError`] from the RPC is the fail-closed trigger (the caller skips the
/// trade; #511: the frozen record retries via v2 and the boot frame-walk converges).
#[derive(Debug, thiserror::Error)]
pub enum SupabaseStateError {
    #[error(
        "authoritative resolution committed remotely but the local mirror failed for {market}: {error}"
    )]
    LocalMirrorUncertain { market: String, error: String },
    #[error("supabase transport: {0}")]
    Transport(#[source] reqwest::Error),
    /// Non-2xx response; the body carries the PL/pgSQL `RAISE` message when the RPC errored.
    #[error("supabase status {0}: {1}")]
    Status(u16, String),
    #[error("supabase decode: {0}")]
    Decode(#[source] reqwest::Error),
    /// The RPC returned SQL `NULL` (e.g. an uninitialised `paper_bankroll`).
    #[error("supabase returned NULL for {0}")]
    Null(&'static str),
    /// An authoritative read found an absent singleton — the authority is not seeded.
    #[error(
        "supabase {0} not initialised; run --backfill-supabase before enabling PE_SUPABASE_AUTHORITATIVE"
    )]
    Uninitialised(&'static str),
    #[error("corrupt supabase value: {0}")]
    Corrupt(String),
    #[error("paper-state: {0}")]
    PaperState(#[from] PaperStateError),
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("migration evidence: {0}")]
    MigrationEvidence(String),
    /// Boot frame reconciliation stopped below the verified log head.
    #[error("authoritative boot frame walk incomplete at watermark {last_watermark}")]
    IncompleteFrameWalk { last_watermark: i64 },
}

/// Requested page size for the boot `paper_positions` pull (the server may return
/// fewer). Canonical: `docs/_GLOSSARY.md` `supabase_paper_positions_page_limit`.
const PAPER_POSITIONS_PAGE_LIMIT: usize = 1_000;

/// Inclusive bound on total pulled position rows — the runaway backstop that turns an
/// ignored/repeating offset into a loud failure instead of an unbounded loop.
/// Canonical: `docs/_GLOSSARY.md` `supabase_paper_positions_max_rows`.
const PAPER_POSITIONS_MAX_ROWS: usize = 500_000;

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// The canonical fill the authority holds for an idempotency key (#511): the
/// `commit_fill_v2` `row` payload, parsed fail-closed. On `existing`, `event_seq` is the
/// ORIGINAL frame's — the local mirror must record THESE fields, not the retry's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFill {
    pub record: FillRecord,
    pub event_seq: EventSeq,
    pub source_trade_id: String,
}

/// `commit_fill_v2` outcome (#511). Money values are authority-canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillV2Outcome {
    /// Newly applied under this key.
    Applied {
        bankroll: Decimal,
        row: CanonicalFill,
    },
    /// The key already existed (an earlier attempt landed — including ambiguously);
    /// `row` is the canonical fill to converge on.
    Existing {
        bankroll: Decimal,
        row: CanonicalFill,
    },
    /// The market is settled and the key absent: refused, nothing inserted.
    Settled { bankroll: Decimal },
}

/// `apply_resolution_v2` outcome (#511): credit is computed INSIDE the RPC from
/// `paper_positions` under the bankroll lock; on `existing` these are the CANONICAL
/// recorded values, so a crash-then-retry mirrors idempotently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionV2Outcome {
    pub applied: bool,
    pub credit: Decimal,
    pub outcome_prices: Vec<Decimal>,
    pub settled_at_unix: i64,
    pub bankroll: Decimal,
}

/// Idempotent authoritative writes. Abstracted as a trait so scenario tests drive
/// [`commit_fill_authoritative`] / [`apply_resolution_authoritative`] with an in-memory fake.
pub trait SupabaseStateTrait: Send + Sync {
    /// Commit a fill via the authoritative `commit_fill_v2` RPC (#511): typed outcome,
    /// canonical row, lock-first settled refusal.
    fn commit_fill_v2(
        &self,
        row: &SupabaseFillRow,
    ) -> impl Future<Output = Result<FillV2Outcome, SupabaseStateError>> + Send;
    /// Apply a resolution via the authoritative `apply_resolution_v2` RPC (#511): the
    /// credit is computed server-side under the bankroll lock.
    fn apply_resolution_v2(
        &self,
        market_id: &MarketId,
        outcome_prices: &[Decimal],
        settled_at_unix: i64,
    ) -> impl Future<Output = Result<ResolutionV2Outcome, SupabaseStateError>> + Send;
}

/// Complete authoritative boot reads, split from runtime RPCs for deterministic failure injection.
pub trait SupabaseBootTrait: SupabaseStateTrait {
    fn fetch_boot_bankroll(
        &self,
    ) -> impl Future<Output = Result<Option<Decimal>, SupabaseStateError>> + Send;

    fn fetch_boot_positions(
        &self,
    ) -> impl Future<Output = Result<Vec<PaperPositionRow>, SupabaseStateError>> + Send;
}

/// Production [`SupabaseStateTrait`]: PostgREST RPCs over reqwest, reusing the
/// [`auth_token`] header pattern (same token in `apikey` + `Authorization`, preferring the
/// service-role secret so writes bypass RLS). Cheap to clone (reqwest client is `Arc`-backed).
#[derive(Clone)]
pub struct SupabaseStateClient {
    client: reqwest::Client,
    base_url: String,
    token: String,
    /// Cumulative count of authoritative RPC calls (`commit_fill` + `apply_resolution`),
    /// surfaced in `status.json` so an agent can see the steady-state Supabase write rate.
    /// `Arc` so every clone (one per fill, in the orchestrator) shares the same counter.
    calls: Arc<AtomicU64>,
}

impl SupabaseStateClient {
    pub fn new(client: reqwest::Client, base_url: &str, anon_key: &str, secret_key: &str) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: auth_token(anon_key, secret_key).to_string(),
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A handle to the cumulative RPC-call counter (for the status writer). All clones of this
    /// client share it.
    pub fn call_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.calls)
    }

    /// `POST {base}/rest/v1/rpc/{func}` with a JSON args object; decode the scalar return.
    async fn post_rpc_json(
        &self,
        func: &'static str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, SupabaseStateError> {
        // Count every authoritative RPC (both RPCs route through here); fetch/upsert (boot pull
        // + one-time backfill) deliberately do not, so the counter reflects the recurring rate.
        self.calls.fetch_add(1, Ordering::Relaxed);
        let url = format!("{}/rest/v1/rpc/{}", self.base_url, func);
        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .json(body)
            .send()
            .await
            .map_err(SupabaseStateError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SupabaseStateError::Status(status.as_u16(), text));
        }
        resp.json().await.map_err(SupabaseStateError::Decode)
    }

    /// Authenticated `GET {url}`, decoding the JSON body as `T`.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<T, SupabaseStateError> {
        let resp = self
            .client
            .get(url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .send()
            .await
            .map_err(SupabaseStateError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SupabaseStateError::Status(status.as_u16(), text));
        }
        resp.json().await.map_err(SupabaseStateError::Decode)
    }

    /// `POST {base}/rest/v1/{table}?on_conflict={key}` merge-duplicates upsert (backfill).
    async fn post_upsert(
        &self,
        table: &str,
        on_conflict: &str,
        body: &serde_json::Value,
    ) -> Result<(), SupabaseStateError> {
        let url = format!(
            "{}/rest/v1/{}?on_conflict={}",
            self.base_url, table, on_conflict
        );
        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header("Prefer", "resolution=merge-duplicates")
            .json(body)
            .send()
            .await
            .map_err(SupabaseStateError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SupabaseStateError::Status(status.as_u16(), text));
        }
        Ok(())
    }

    // ── Boot pull readers ──────────────────────────────────────────────────────

    /// The authoritative bankroll, or `None` when the singleton row is absent (not yet
    /// backfilled — the boot pull then leaves the local cache untouched).
    pub async fn fetch_bankroll(&self) -> Result<Option<Decimal>, SupabaseStateError> {
        let url = format!(
            "{}/rest/v1/paper_bankroll?select=bankroll_str&id=eq.0",
            self.base_url
        );
        let rows: Vec<BankrollRow> = self.get_json(&url).await?;
        rows.first()
            .map(|r| bankroll_money(&r.bankroll_str, "paper_bankroll bankroll_str"))
            .transpose()
    }

    /// The authoritative net positions, for the boot pull into the local cache.
    ///
    /// Pages through PostgREST (#516): the server caps ANY single response at its
    /// configured `db-max-rows` (1000 on this project), so the previous bare GET
    /// silently truncated the >1000-row cumulative table. The walk advances by the
    /// ACTUAL returned length and terminates only on an EMPTY page — correct even if
    /// the server cap drops below the requested limit. Offset pagination assumes a
    /// stable ordered set, which every legitimate writer's own sequencing guarantees:
    /// this service's writers spawn only after boot completes, and backfill/reset run
    /// with the service stopped per their runbooks (docs/34/35).
    pub async fn fetch_positions(&self) -> Result<Vec<PaperPositionRow>, SupabaseStateError> {
        self.fetch_positions_paged(PAPER_POSITIONS_PAGE_LIMIT, PAPER_POSITIONS_MAX_ROWS)
            .await
    }

    async fn fetch_positions_paged(
        &self,
        page_limit: usize,
        max_rows: usize,
    ) -> Result<Vec<PaperPositionRow>, SupabaseStateError> {
        let mut raw: Vec<PositionRow> = Vec::new();
        let mut previous_first: Option<(String, i64)> = None;
        loop {
            let url = format!(
                "{}/rest/v1/paper_positions?select=market_id,outcome_id,long_contracts,short_contracts\
                 &order=market_id.asc,outcome_id.asc&limit={page_limit}&offset={}",
                self.base_url,
                raw.len()
            );
            let page: Vec<PositionRow> = self.get_json(&url).await?;
            let Some(first_row) = page.first() else {
                break;
            };
            // A server ignoring `offset` re-serves the same PK-ordered page forever; the
            // repeated first key fails it on the SECOND request instead of grinding to
            // the row bound (with no writers, a later page can never begin at an
            // already-seen key).
            let first = (first_row.market_id.clone(), first_row.outcome_id);
            if previous_first.replace(first.clone()) == Some(first) {
                return Err(SupabaseStateError::Corrupt(
                    "paper_positions page repeated; server ignored the offset".to_owned(),
                ));
            }
            raw.extend(page);
            if raw.len() > max_rows {
                return Err(SupabaseStateError::Corrupt(format!(
                    "paper_positions pull exceeded the {max_rows}-row bound"
                )));
            }
        }
        raw.into_iter()
            .map(|r| {
                Ok(PaperPositionRow {
                    market_id: MarketId(VenueMarketId(r.market_id)),
                    outcome_id: OutcomeId(u16::try_from(r.outcome_id).map_err(|_| {
                        SupabaseStateError::Corrupt(format!("outcome_id {}", r.outcome_id))
                    })?),
                    long_contracts: u64::try_from(r.long_contracts).map_err(|_| {
                        SupabaseStateError::Corrupt(format!("long {}", r.long_contracts))
                    })?,
                    short_contracts: u64::try_from(r.short_contracts).map_err(|_| {
                        SupabaseStateError::Corrupt(format!("short {}", r.short_contracts))
                    })?,
                })
            })
            .collect()
    }

    // ── One-time backfill upserts (SQLite → Supabase, service stopped) ──────────

    /// Upsert the bankroll singleton to an exact value (backfill — distinct from the
    /// delta-based `commit_fill`/`apply_resolution` RPCs; this is the only way to seed the
    /// authoritative balance from the complete local SQLite scalar).
    pub async fn upsert_bankroll(&self, value: Decimal) -> Result<(), SupabaseStateError> {
        let body = serde_json::json!([{ "id": 0, "bankroll_str": value.to_string() }]);
        self.post_upsert("paper_bankroll", "id", &body).await
    }

    /// Upsert one net position row (backfill).
    pub async fn upsert_position(&self, row: &PaperPositionRow) -> Result<(), SupabaseStateError> {
        let body = serde_json::json!([{
            "market_id": row.market_id.0.0,
            "outcome_id": row.outcome_id.0,
            "long_contracts": row.long_contracts,
            "short_contracts": row.short_contracts,
        }]);
        self.post_upsert("paper_positions", "market_id,outcome_id", &body)
            .await
    }
}

impl SupabaseBootTrait for SupabaseStateClient {
    async fn fetch_boot_bankroll(&self) -> Result<Option<Decimal>, SupabaseStateError> {
        self.fetch_bankroll().await
    }

    async fn fetch_boot_positions(&self) -> Result<Vec<PaperPositionRow>, SupabaseStateError> {
        self.fetch_positions().await
    }
}

/// Raw `commit_fill_v2` jsonb response (fail-closed parse — every field validated).
#[derive(Debug, Deserialize)]
struct FillV2Resp {
    outcome: String,
    bankroll: String,
    row: Option<FillV2RowJson>,
}

#[derive(Debug, Deserialize)]
struct FillV2RowJson {
    idempotency_key: String,
    source_trade_id: String,
    market_id: String,
    outcome_id: i64,
    side: String,
    contracts: i64,
    fill_price: String,
    event_seq: i64,
}

#[derive(Debug, Deserialize)]
struct ResolutionV2Resp {
    outcome: String,
    credit: String,
    outcome_prices: Vec<String>,
    settled_at_unix: i64,
    bankroll: String,
}

/// Parse a decimal-string money field: fail closed on malformed or negative (#511).
fn money(raw: &str, what: &'static str) -> Result<Decimal, SupabaseStateError> {
    let d = Decimal::from_str_exact(raw)
        .map_err(|e| SupabaseStateError::Corrupt(format!("{what}: {raw:?}: {e}")))?;
    if d < Decimal::ZERO {
        return Err(SupabaseStateError::Corrupt(format!(
            "{what}: negative {raw:?}"
        )));
    }
    Ok(d)
}

/// The stored bankroll is Postgres `numeric` text whose scale grows with every fill
/// (`3729.7323088775297327030000000060` on 2026-09-02), while `Decimal` holds at most
/// 28 fractional digits. Grammar is canonical and unsigned: `digits` or `digits.digits`
/// with nothing else. A value the exact parser accepts is returned unchanged; only an
/// exact-parse failure caused by excess fractional precision (`Underflow`) falls back
/// to dropping fractional digits beyond the 28th significant digit (below 1e-24 USD),
/// and a nonzero value that would truncate to zero fails closed like every other input.
fn bankroll_money(raw: &str, what: &'static str) -> Result<Decimal, SupabaseStateError> {
    let corrupt = |detail: &str| SupabaseStateError::Corrupt(format!("{what}: {raw:?}: {detail}"));
    let (integer, fraction) = match raw.split_once('.') {
        Some((integer, fraction)) if !fraction.is_empty() => (integer, fraction),
        Some(_) => return Err(corrupt("trailing decimal point")),
        None => (raw, ""),
    };
    if integer.is_empty()
        || !integer.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(corrupt("not plain unsigned decimal text"));
    }
    match Decimal::from_str_exact(raw) {
        Ok(exact) => Ok(exact),
        Err(rust_decimal::Error::Underflow) => {
            let integer_digits = integer.trim_start_matches('0').len();
            let keep = 28usize.saturating_sub(integer_digits).min(fraction.len());
            if keep == 0 {
                return Err(corrupt("integer part leaves no representable fraction"));
            }
            let normalized = format!("{integer}.{}", &fraction[..keep]);
            let truncated = Decimal::from_str_exact(&normalized)
                .map_err(|e| corrupt(&format!("excess precision fallback: {e}")))?;
            if truncated.is_zero() && fraction.bytes().any(|b| b != b'0') {
                return Err(corrupt("nonzero value below representable precision"));
            }
            Ok(truncated)
        }
        Err(e) => Err(corrupt(&e.to_string())),
    }
}

fn canonical_fill(row: FillV2RowJson) -> Result<CanonicalFill, SupabaseStateError> {
    let side = match row.side.as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        other => {
            return Err(SupabaseStateError::Corrupt(format!(
                "v2 row side {other:?}"
            )));
        }
    };
    let contracts = u64::try_from(row.contracts)
        .map_err(|_| SupabaseStateError::Corrupt(format!("v2 row contracts {}", row.contracts)))?;
    let event_seq = u64::try_from(row.event_seq)
        .map_err(|_| SupabaseStateError::Corrupt(format!("v2 row event_seq {}", row.event_seq)))?;
    let outcome_id = u16::try_from(row.outcome_id).map_err(|_| {
        SupabaseStateError::Corrupt(format!("v2 row outcome_id {}", row.outcome_id))
    })?;
    Ok(CanonicalFill {
        record: FillRecord {
            idempotency_key: row.idempotency_key,
            market_id: MarketId(pe_core_types::VenueMarketId(row.market_id)),
            outcome_id: OutcomeId(outcome_id),
            side,
            contracts,
            fill_price: Price(money(&row.fill_price, "v2 row fill_price")?),
        },
        event_seq: EventSeq(event_seq),
        source_trade_id: row.source_trade_id,
    })
}

impl SupabaseStateTrait for SupabaseStateClient {
    async fn commit_fill_v2(
        &self,
        row: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        // Decimals as strings (no f64); the RPC casts text → numeric.
        let body = serde_json::json!({
            "p_idempotency_key": row.fill.idempotency_key,
            "p_leader_wallet": row.leader_wallet,
            "p_source_trade_id": row.source_trade_id,
            "p_market_id": row.fill.market_id.0.0,
            "p_outcome_id": row.fill.outcome_id.0,
            "p_side": side_str(row.fill.side),
            "p_contracts": row.fill.contracts,
            "p_fill_price": row.fill.fill_price.0.to_string(),
            "p_entry_unix": row.entry_unix,
            "p_event_seq": row.fill.event_seq,
        });
        let value = self.post_rpc_json("commit_fill_v2", &body).await?;
        let resp: FillV2Resp = serde_json::from_value(value)
            .map_err(|e| SupabaseStateError::Corrupt(format!("commit_fill_v2 shape: {e}")))?;
        let bankroll = bankroll_money(&resp.bankroll, "commit_fill_v2 bankroll")?;
        match (resp.outcome.as_str(), resp.row) {
            ("applied", Some(row)) => Ok(FillV2Outcome::Applied {
                bankroll,
                row: canonical_fill(row)?,
            }),
            ("existing", Some(row)) => Ok(FillV2Outcome::Existing {
                bankroll,
                row: canonical_fill(row)?,
            }),
            ("settled", None) => Ok(FillV2Outcome::Settled { bankroll }),
            (outcome, row) => Err(SupabaseStateError::Corrupt(format!(
                "commit_fill_v2 outcome {outcome:?} with row.is_some()={}",
                row.is_some()
            ))),
        }
    }

    async fn apply_resolution_v2(
        &self,
        market_id: &MarketId,
        outcome_prices: &[Decimal],
        settled_at_unix: i64,
    ) -> Result<ResolutionV2Outcome, SupabaseStateError> {
        // `outcome_prices` as a jsonb array of text decimals — matches the sink's
        // `settled_markets.outcome_prices` shape (so `wallet_live_stats` reads agree).
        let prices: Vec<String> = outcome_prices.iter().map(|d| d.to_string()).collect();
        let body = serde_json::json!({
            "p_market_id": market_id.0.0,
            "p_outcome_prices": prices,
            "p_settled_at_unix": settled_at_unix,
        });
        let value = self.post_rpc_json("apply_resolution_v2", &body).await?;
        let resp: ResolutionV2Resp = serde_json::from_value(value)
            .map_err(|e| SupabaseStateError::Corrupt(format!("apply_resolution_v2 shape: {e}")))?;
        let applied = match resp.outcome.as_str() {
            "applied" => true,
            "existing" => false,
            other => {
                return Err(SupabaseStateError::Corrupt(format!(
                    "apply_resolution_v2 outcome {other:?}"
                )));
            }
        };
        let mut parsed_prices = Vec::with_capacity(resp.outcome_prices.len());
        for p in &resp.outcome_prices {
            parsed_prices.push(money(p, "apply_resolution_v2 outcome_price")?);
        }
        Ok(ResolutionV2Outcome {
            applied,
            credit: money(&resp.credit, "apply_resolution_v2 credit")?,
            outcome_prices: parsed_prices,
            settled_at_unix: resp.settled_at_unix,
            bankroll: bankroll_money(&resp.bankroll, "apply_resolution_v2 bankroll")?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BankrollRow {
    bankroll_str: String,
}

#[derive(Debug, Deserialize)]
struct PositionRow {
    market_id: String,
    outcome_id: i64,
    long_contracts: i64,
    short_contracts: i64,
}

// ── Write-through paths (RPC first, SQLite mirror second) ───────────────────────

/// Outcome of an authoritative fill commit (#511): the orchestrator marks the contract
/// filled only on `Filled`; `RefusedSettled` is terminal (seen + typed flip written).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativeFillOutcome {
    Filled(Decimal),
    RefusedSettled(Decimal),
    /// The authority committed but the local mirror could not converge after
    /// in-process retries (#544 review): the caller must treat paper durability
    /// as uncertain — fail readiness and stop producers; the boot frame-walk
    /// converges from the durable frame on restart. Carries the authority
    /// bankroll for the final status snapshot only.
    LocalDurabilityUncertain(Decimal),
}

/// Authoritative fill commit (#397, reshaped by #511): `commit_fill_v2` FIRST (fail-closed
/// — an RPC error propagates and the caller runs the frozen-retry protocol; the frame is
/// durable), then ONE local convergence transaction mirroring the CANONICAL row and the
/// authority bankroll, then — only after the local transaction commits (#511 R3) — the
/// successor-gated #510 watermark advance. A settled refusal writes the terminal refused
/// disposition instead. The RPC `.await` completes before the SQLite mutex is taken.
#[allow(clippy::too_many_arguments)]
pub async fn commit_fill_authoritative<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    paper_state: &PaperStateDb,
    source_trade_id: &SourceTradeId,
    leader: &LeaderPositionRow,
    _record: &FillRecord,
    seq: EventSeq,
    sup_row: &SupabaseFillRow,
    flip: Option<pe_paper_state::DispatchFlip<'_>>,
    pending_decision: Option<&DecisionEvidenceAccumulator>,
) -> Result<AuthoritativeFillOutcome, SupabaseStateError> {
    // #510: snapshot the watermark BEFORE the external mutation — a getter failure fails
    // the fill closed here, never after the RPC has already debited Supabase.
    let wm_before = paper_state.last_supabase_applied_event_seq()?;
    let outcome = supabase.commit_fill_v2(sup_row).await?;
    let pending = match &outcome {
        FillV2Outcome::Applied { bankroll, row } => render_pending_evidence(
            pending_decision,
            AuthorityEvidence::commit_fill_v2("applied", *bankroll),
            recorded_fill_terminal(&row.record, row.event_seq),
        ),
        FillV2Outcome::Existing { bankroll, row } => render_pending_evidence(
            pending_decision,
            AuthorityEvidence::commit_fill_v2("existing", *bankroll),
            recorded_fill_terminal(&row.record, row.event_seq),
        ),
        FillV2Outcome::Settled { bankroll } => render_pending_evidence(
            pending_decision,
            AuthorityEvidence::commit_fill_v2("settled_refusal", *bankroll),
            TerminalDispositionEvidence::settled_refusal(),
        ),
    }
    .map_err(|error| {
        SupabaseStateError::Corrupt(format!(
            "encode decision_pending authority evidence: {error}"
        ))
    })?;
    // #508 round-4: the local disposition surfaces and retries IN-PROCESS (bounded); the
    // durable backstop is the boot frame-walk (the frozen watermark marks the frame pending).
    let mut local = Ok(());
    for attempt in 1u32..=3 {
        local = match &outcome {
            FillV2Outcome::Applied { bankroll, row }
            | FillV2Outcome::Existing { bankroll, row } => paper_state
                .commit_fill_canonical_pending(
                    Some(source_trade_id),
                    Some(leader),
                    &row.record,
                    row.event_seq,
                    seq,
                    *bankroll,
                    flip,
                    pending.as_ref().map(pending_terminal),
                ),
            FillV2Outcome::Settled { .. } => paper_state.commit_refused_fill_pending(
                source_trade_id,
                Some(leader),
                seq,
                flip.map(|f| pe_paper_state::DispatchFlip {
                    dispatch_id: f.dispatch_id,
                    paper_outcome: "no_fill:market_settled",
                }),
                pending.as_ref().map(pending_terminal),
            ),
        };
        match &local {
            Ok(()) => break,
            Err(e) if attempt < 3 => {
                tracing::error!(
                    error = %e,
                    attempt,
                    "authoritative fill: local disposition failed; retrying in-process"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(_) => {}
        }
    }
    match local {
        Ok(()) => {
            // #510 successor-gated runtime advance, AFTER the local disposition (#511 R3:
            // advancing first would hide an unmirrored frame from the boot frame-walk).
            // Frames are dense from seq 0; any gap freezes the watermark and the boot
            // frame-walk — which resolves every frame above it through v2 — heals it.
            let is_successor = match wm_before {
                None => seq.0 == 0,
                Some(wm) => wm.0.checked_add(1) == Some(seq.0),
            };
            if is_successor && let Err(e) = paper_state.set_supabase_applied_event_seq(seq) {
                warn!(
                    error = %e,
                    seq = seq.0,
                    "authoritative fill: watermark advance failed (boot frame-walk re-confirms)"
                );
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                seq = seq.0,
                "authoritative fill: authority committed but local disposition failed after \
                 in-process retries; watermark left frozen (boot frame-walk converges)"
            );
            // Remote truth advanced without local convergence: surface typed
            // uncertainty instead of success (#544 review). In-memory financial
            // state must not advance past durable local state.
            let bankroll = match outcome {
                FillV2Outcome::Applied { bankroll, .. }
                | FillV2Outcome::Existing { bankroll, .. }
                | FillV2Outcome::Settled { bankroll } => bankroll,
            };
            return Ok(AuthoritativeFillOutcome::LocalDurabilityUncertain(bankroll));
        }
    }
    Ok(match outcome {
        FillV2Outcome::Applied { bankroll, .. } | FillV2Outcome::Existing { bankroll, .. } => {
            AuthoritativeFillOutcome::Filled(bankroll)
        }
        FillV2Outcome::Settled { bankroll } => AuthoritativeFillOutcome::RefusedSettled(bankroll),
    })
}

/// Authoritative resolution (#397, reshaped by #511): `apply_resolution_v2` FIRST — the
/// credit is computed INSIDE the RPC from `paper_positions` under the bankroll lock,
/// closing the read-then-resolve TOCTOU — then the SQLite mirror applies the RETURNED
/// canonical values (on `existing`, the originally recorded ones), so a crash-then-retry
/// converges with no double credit. Fail-closed: an RPC error leaves the market unsettled
/// for the next tick.
pub async fn apply_resolution_authoritative<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    store: &mut ResolutionStore,
    market_id: &MarketId,
    outcome_prices: &[Decimal],
    settled_at_unix: i64,
) -> Result<Decimal, SupabaseStateError> {
    let res = supabase
        .apply_resolution_v2(market_id, outcome_prices, settled_at_unix)
        .await?;
    if let Err(e) = store.settle_and_credit(
        market_id.clone(),
        res.outcome_prices.clone(),
        res.credit,
        res.settled_at_unix,
    ) {
        warn!(
            error = %e,
            market = %market_id,
            "authoritative resolution committed to Supabase but local SQLite mirror failed"
        );
        // Typed error instead of silent success (#544 review): the market stays
        // locally unsettled, the next tick re-drives the idempotent RPC (returns
        // `existing`), and the mirror converges — the caller retries, it does
        // not report a settled market it has not durably recorded.
        return Err(SupabaseStateError::LocalMirrorUncertain {
            market: market_id.0.0.clone(),
            error: e.to_string(),
        });
    }
    Ok(res.bankroll)
}

// ── Boot: frame-walk then pull ──────────────────────────────────────────────────

/// #511 unified boot frame-walk (replaces the #397/#510 fills-row catch-up): resolve every
/// event-log FRAME above the successor-gated Supabase watermark through `commit_fill_v2`.
/// The watermark freezes below the OLDEST unresolved frame even when later fills succeeded
/// (non-successors never advance it), so the walk provably revisits every pending frame —
/// the freeze IS the durable pending marker; no separate state exists.
///
/// Per frame: `applied`/`existing` → one local convergence transaction (canonical row +
/// authority bankroll + seen; the leader long/short mirror is not in the frame and
/// refreshes on the wallet's next processed trade); `settled` → the terminal refused
/// disposition; non-`wf|` key → skip (cannot satisfy `paper_fills.leader_wallet`). The
/// watermark advances sequentially after each LOCAL disposition commits, and the walk
/// halts at the first failure (the tail stays pending for the next boot). Returns
/// `(new_watermark, fully_resolved, resolved_count)`.
pub async fn resolve_event_frames<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    paper_state: &PaperStateDb,
    event_log_path: &std::path::Path,
) -> Result<(i64, bool, usize), SupabaseStateError> {
    let wm_start = match paper_state.last_supabase_applied_event_seq()? {
        Some(seq) => i64::try_from(seq.0).unwrap_or(i64::MAX),
        None => -1,
    };
    let mut new_wm = wm_start;
    let mut fully_resolved = true;
    let mut resolved = 0usize;
    if !event_log_path.exists() {
        return Ok((new_wm, true, 0));
    }
    let replay = pe_event_log::Reader::replay(event_log_path).map_err(|e| {
        SupabaseStateError::Corrupt(format!("open event log {}: {e}", event_log_path.display()))
    })?;
    for frame in replay {
        let (seq, envelope) =
            frame.map_err(|e| SupabaseStateError::Corrupt(format!("read event-log frame: {e}")))?;
        let seq_i = i64::try_from(seq.0).unwrap_or(i64::MAX);
        if seq_i <= new_wm {
            continue;
        }
        let fill: pe_strategy_winner_follow::PaperFill = serde_json::from_slice(&envelope.payload)
            .map_err(|e| {
                SupabaseStateError::Corrupt(format!("decode PaperFill at seq {}: {e}", seq.0))
            })?;
        let fill_row = FillRow {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            contracts: fill.intent.contracts.0,
            fill_price: fill.simulated_fill_price,
            event_seq: seq_i,
        };
        let disposition = match supabase_fill_from(&fill_row) {
            // Non-`wf|` fill (no leader): cannot write `paper_fills.leader_wallet` (NOT
            // NULL). Advance past it so it never blocks the prefix.
            None => {
                warn!(
                    key = %fill_row.idempotency_key,
                    "supabase boot frame-walk: skipping non-winner-follow fill (no leader)"
                );
                Ok(())
            }
            Some(sup) => match supabase.commit_fill_v2(&sup).await {
                Err(e) => {
                    warn!(
                        error = %e,
                        event_seq = seq.0,
                        "supabase boot frame-walk: commit_fill_v2 failed; halting at last \
                         confirmed prefix"
                    );
                    fully_resolved = false;
                    break;
                }
                Ok(outcome) => {
                    resolved += 1;
                    let trade_id = sup.source_trade_id.clone().map(SourceTradeId);
                    let pending_row = match trade_id.as_ref() {
                        Some(id) => paper_state
                            .open_decision_pending()?
                            .into_iter()
                            .find(|row| &row.source_trade_id == id),
                        None => None,
                    };
                    let pending_decision = pending_row
                        .as_ref()
                        .map(DecisionEvidenceAccumulator::from_pending_checkpoint)
                        .transpose()
                        .map_err(|error| {
                            SupabaseStateError::Corrupt(format!(
                                "recover decision_pending checkpoint at seq {}: {error}",
                                seq.0
                            ))
                        })?;
                    match outcome {
                        FillV2Outcome::Applied { bankroll, row } => {
                            let pending = render_pending_evidence(
                                pending_decision.as_ref(),
                                AuthorityEvidence::commit_fill_v2("applied", bankroll),
                                recorded_fill_terminal(&row.record, row.event_seq),
                            )
                            .map_err(|error| {
                                SupabaseStateError::Corrupt(format!(
                                    "render recovered decision evidence at seq {}: {error}",
                                    seq.0
                                ))
                            })?;
                            paper_state.commit_fill_canonical_pending(
                                trade_id.as_ref(),
                                None,
                                &row.record,
                                row.event_seq,
                                seq,
                                bankroll,
                                None,
                                pending.as_ref().map(pending_terminal),
                            )
                        }
                        FillV2Outcome::Existing { bankroll, row } => {
                            let pending = render_pending_evidence(
                                pending_decision.as_ref(),
                                AuthorityEvidence::commit_fill_v2("existing", bankroll),
                                recorded_fill_terminal(&row.record, row.event_seq),
                            )
                            .map_err(|error| {
                                SupabaseStateError::Corrupt(format!(
                                    "render recovered decision evidence at seq {}: {error}",
                                    seq.0
                                ))
                            })?;
                            paper_state.commit_fill_canonical_pending(
                                trade_id.as_ref(),
                                None,
                                &row.record,
                                row.event_seq,
                                seq,
                                bankroll,
                                None,
                                pending.as_ref().map(pending_terminal),
                            )
                        }
                        FillV2Outcome::Settled { bankroll } => match trade_id.as_ref() {
                            Some(id) => {
                                let pending = render_pending_evidence(
                                    pending_decision.as_ref(),
                                    AuthorityEvidence::commit_fill_v2("settled_refusal", bankroll),
                                    TerminalDispositionEvidence::settled_refusal(),
                                )
                                .map_err(|error| {
                                    SupabaseStateError::Corrupt(format!(
                                        "render recovered refusal evidence at seq {}: {error}",
                                        seq.0
                                    ))
                                })?;
                                paper_state.commit_refused_fill_pending(
                                    id,
                                    None,
                                    seq,
                                    None,
                                    pending.as_ref().map(pending_terminal),
                                )
                            }
                            // A `wf|` key always embeds the trade id; defensive.
                            None => Ok(()),
                        },
                    }
                    .map_err(|e| (e, seq.0))
                    .map_err(|(e, s)| {
                        SupabaseStateError::Corrupt(format!("local disposition at seq {s}: {e}"))
                    })
                }
            },
        };
        if let Err(e) = disposition {
            warn!(
                error = %e,
                event_seq = seq.0,
                "supabase boot frame-walk: local disposition failed; halting (frame stays \
                 pending; next boot retries)"
            );
            fully_resolved = false;
            break;
        }
        paper_state.set_supabase_applied_event_seq(seq)?;
        new_wm = seq_i;
    }
    Ok((new_wm, fully_resolved, resolved))
}

/// Issue #397 authoritative boot: catch up Supabase from the local event-log fills, persist
/// the advanced watermark, then pull the authoritative bankroll + positions back into SQLite
/// so values read after this reflect the Supabase source of truth.
///
/// The settled set is **not** pulled: it self-heals via the gated `apply_resolution` on the
/// next resolution tick (a market settled in Supabase but missing from SQLite is re-driven —
/// the RPC gate credits zero, the SQLite `settle_and_credit` mirror inserts+credits once),
/// which also avoids a fragile jsonb→text round-trip. Fail-closed: a pull error aborts boot
/// (refuse to run authoritative without the authority).
pub async fn supabase_authoritative_boot<S: SupabaseBootTrait + ?Sized>(
    client: &S,
    paper_state: &PaperStateDb,
    event_log_path: &std::path::Path,
) -> Result<(), SupabaseStateError> {
    supabase_authoritative_boot_observed(client, paper_state, event_log_path, |_, _| Ok(())).await
}

/// Migration form of [`supabase_authoritative_boot`]: the complete canonical
/// snapshot is synchronously observed after validation and before SQLite apply.
pub async fn supabase_authoritative_boot_observed<S, F>(
    client: &S,
    paper_state: &PaperStateDb,
    event_log_path: &std::path::Path,
    observe: F,
) -> Result<(), SupabaseStateError>
where
    S: SupabaseBootTrait + ?Sized,
    F: FnOnce(&Decimal, &[PaperPositionRow]) -> Result<(), SupabaseStateError>,
{
    // 3. #511: resolve every event-log frame above the successor-gated watermark through
    //    `commit_fill_v2` (frames the runtime confirmed are below the watermark already;
    //    an ABSENT watermark — fresh DB or SQLite loss — walks from seq 0). This REPLACES
    //    both the blind local frame replay and the fills-row catch-up in authoritative
    //    mode: local application is decided by the authority, so a refused frame can
    //    never resurrect locally.
    let wm = match paper_state.last_supabase_applied_event_seq()? {
        Some(seq) => i64::try_from(seq.0).unwrap_or(i64::MAX),
        None => -1,
    };
    let (new_wm, fully_caught_up, committed) =
        resolve_event_frames(client, paper_state, event_log_path).await?;
    // #510 change 3: one summary line so a long replay is visible (the 2026-08-12 incident
    // looked like a hang) and a no-op boot (`replayed=0`) is the instant-restart proof.
    info!(
        old_watermark = wm,
        head = new_wm,
        replayed = committed,
        "supabase authoritative boot: catch-up summary"
    );

    // A partial walk leaves authority below the durable log head. Boot must fail so no producer
    // can turn that uncertainty into seen/no-fill state (#544).
    if !fully_caught_up {
        return Err(SupabaseStateError::IncompleteFrameWalk {
            last_watermark: new_wm,
        });
    }

    // Fetch and validate the complete remote snapshot in memory before touching SQLite. The
    // paper-state owner applies bankroll + delete-all + insert-all in one transaction.
    let bankroll = client
        .fetch_boot_bankroll()
        .await?
        .ok_or(SupabaseStateError::Uninitialised("paper_bankroll"))?;
    let positions = client.fetch_boot_positions().await?;
    let n = positions.len();
    observe(&bankroll, &positions)?;
    paper_state.replace_authoritative_state(bankroll, &positions)?;
    info!(bankroll = %bankroll, "supabase authoritative boot: pulled bankroll");
    info!(
        positions = n,
        "supabase authoritative boot: pulled positions"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Query, State};
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;

    /// Fixture "table": `rows` total rows, returning at most `server_cap` per response
    /// regardless of the requested limit (models PostgREST `db-max-rows`), failing with
    /// 503 at offsets ≥ `fail_at_offset`.
    #[derive(Clone)]
    struct PositionsFixture {
        rows: usize,
        server_cap: usize,
        fail_at_offset: Option<usize>,
        ignore_offset: bool,
        offsets: Arc<Mutex<Vec<usize>>>,
    }

    async fn positions_page(
        State(fx): State<PositionsFixture>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Result<Json<Vec<serde_json::Value>>, axum::http::StatusCode> {
        assert_eq!(
            query.get("order").map(String::as_str),
            Some("market_id.asc,outcome_id.asc"),
            "stable PK ordering is part of the pagination contract"
        );
        let limit: usize = query.get("limit").unwrap().parse().unwrap();
        let mut offset: usize = query.get("offset").unwrap().parse().unwrap();
        if fx.fail_at_offset.is_some_and(|fail| offset >= fail) {
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
        fx.offsets.lock().unwrap().push(offset);
        if fx.ignore_offset {
            offset = 0;
        }
        let take = limit.min(fx.server_cap).min(fx.rows.saturating_sub(offset));
        Ok(Json(
            (0..take)
                .map(|i| {
                    serde_json::json!({
                        "market_id": format!("0x{:064x}", offset + i),
                        "outcome_id": 0,
                        "long_contracts": 1,
                        "short_contracts": 0
                    })
                })
                .collect(),
        ))
    }

    async fn bankroll_row() -> Json<Vec<serde_json::Value>> {
        Json(vec![serde_json::json!({ "bankroll_str": "123.45" })])
    }

    async fn serve(fixture: PositionsFixture) -> String {
        let app = Router::new()
            .route("/rest/v1/paper_positions", get(positions_page))
            .route("/rest/v1/paper_bankroll", get(bankroll_row))
            .with_state(fixture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    fn fixture(rows: usize, server_cap: usize) -> PositionsFixture {
        PositionsFixture {
            rows,
            server_cap,
            fail_at_offset: None,
            ignore_offset: false,
            offsets: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn client(base: &str) -> SupabaseStateClient {
        SupabaseStateClient::new(reqwest::Client::new(), base, "anon", "")
    }

    #[test]
    fn bankroll_text_is_exact_when_representable_and_truncates_only_excess_precision() {
        // Live `paper_bankroll.bankroll_str` on 2026-09-02 (32 significant digits).
        let live = "3729.7323088775297327030000000060";
        assert_eq!(
            super::bankroll_money(live, "bankroll").unwrap().to_string(),
            "3729.732308877529732703000000"
        );
        // Exactly representable inputs are returned unchanged, including the maximum.
        for exact in [
            "4422.242308877529732703",
            "3729.73",
            "0",
            "7.9228162514264337593543950335",
            "79228162514264337593543950335",
        ] {
            assert_eq!(
                super::bankroll_money(exact, "bankroll")
                    .unwrap()
                    .to_string(),
                exact
            );
        }
        // Fail closed: garbage, signs, exponent, grammar, whitespace, underflow, overflow.
        for bad in [
            "3729.7323088775297327030000000060x",
            "-0.00000000000000000000000000001",
            "-1",
            "+1",
            "1e5",
            "",
            ".5",
            "1.",
            " 1 ",
            "1_000",
            "0.0000000000000000000000000001", // 1e-28 fits; 1e-29 below must fail
        ]
        .into_iter()
        .filter(|s| *s != "0.0000000000000000000000000001")
        {
            assert!(super::bankroll_money(bad, "bankroll").is_err(), "{bad:?}");
        }
        assert!(super::bankroll_money("0.00000000000000000000000000001", "bankroll").is_err());
        assert!(super::bankroll_money("79228162514264337593543950335.9", "bankroll").is_err());
        // Exact money parsing is unchanged for every other field.
        assert!(super::money(live, "credit").is_err());
        assert!(super::money("-1", "credit").is_err());
    }

    #[tokio::test]
    async fn positions_pull_walks_pages_to_the_empty_terminator() {
        let fx = fixture(5, usize::MAX);
        let base = serve(fx.clone()).await;
        let rows = client(&base)
            .fetch_positions_paged(2, 500_000)
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        // Offsets advance by the actual returned length; the short tail page (1 row)
        // still forces one more request, which returns empty and terminates.
        assert_eq!(*fx.offsets.lock().unwrap(), vec![0, 2, 4, 5]);
    }

    #[tokio::test]
    async fn positions_pull_is_cap_agnostic_below_the_requested_limit() {
        // The server returns FEWER rows than requested while more remain (the live
        // project's db-max-rows behavior, T2-demonstrated in #516): a `len < limit`
        // terminator would silently truncate here; the empty-page rule pulls everything.
        let fx = fixture(5, 1);
        let base = serve(fx.clone()).await;
        let rows = client(&base)
            .fetch_positions_paged(2, 500_000)
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(*fx.offsets.lock().unwrap(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn positions_pull_page_failure_returns_err_with_no_partial_vector() {
        let mut fx = fixture(5, usize::MAX);
        fx.fail_at_offset = Some(2);
        let base = serve(fx.clone()).await;
        let error = client(&base).fetch_positions_paged(2, 500_000).await;
        assert!(matches!(error, Err(SupabaseStateError::Status(503, _))));
    }

    #[tokio::test]
    async fn positions_pull_row_bound_is_inclusive_and_fails_loudly() {
        let fx = fixture(10, usize::MAX);
        let base = serve(fx.clone()).await;
        // Exactly at the bound is allowed…
        assert_eq!(
            client(&base)
                .fetch_positions_paged(2, 10)
                .await
                .unwrap()
                .len(),
            10
        );
        // …one page past it fails closed.
        assert!(matches!(
            client(&base).fetch_positions_paged(2, 4).await,
            Err(SupabaseStateError::Corrupt(_))
        ));
    }

    #[tokio::test]
    async fn positions_pull_fails_fast_when_the_server_ignores_the_offset() {
        // An offset-ignoring server re-serves page 1 forever; the repeated first key
        // fails the pull on the SECOND request, not at the 500k-row backstop.
        let mut fx = fixture(10, usize::MAX);
        fx.ignore_offset = true;
        let base = serve(fx.clone()).await;
        let error = client(&base).fetch_positions_paged(2, 500_000).await;
        assert!(matches!(error, Err(SupabaseStateError::Corrupt(_))));
        assert_eq!(*fx.offsets.lock().unwrap(), vec![0, 2]);
    }

    #[tokio::test]
    async fn public_fetch_positions_pages_at_the_production_constant() {
        // 1001 rows through the PUBLIC entry point at the production 1000-row limit:
        // two nonempty pages then the empty terminator.
        let fx = fixture(1_001, 1_000);
        let base = serve(fx.clone()).await;
        let rows = client(&base).fetch_positions().await.unwrap();
        assert_eq!(rows.len(), 1_001);
        assert_eq!(*fx.offsets.lock().unwrap(), vec![0, 1_000, 1_001]);
    }

    #[tokio::test]
    async fn authoritative_boot_atomically_replaces_and_deletes_stale_positions() {
        let fx = fixture(2, 1);
        let base = serve(fx).await;
        let dir = tempfile::tempdir().unwrap();
        let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        db.set_bankroll(Decimal::ONE).unwrap();
        db.upsert_position(
            &MarketId(VenueMarketId("0xstale".to_owned())),
            OutcomeId(0),
            9,
            0,
        )
        .unwrap();

        supabase_authoritative_boot(&client(&base), &db, &dir.path().join("absent-paper.log"))
            .await
            .unwrap();

        assert_eq!(db.bankroll().unwrap(), Some(Decimal::new(12_345, 2)));
        let positions = db.paper_positions().unwrap();
        assert_eq!(positions.len(), 2);
        assert!(
            positions
                .iter()
                .all(|row| row.market_id.to_string() != "0xstale")
        );
    }

    #[tokio::test]
    async fn authoritative_boot_mid_page_failure_leaves_prior_local_state_untouched() {
        let mut fx = fixture(5, usize::MAX);
        fx.fail_at_offset = Some(2);
        let base = serve(fx).await;
        let dir = tempfile::tempdir().unwrap();
        let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let stale = MarketId(VenueMarketId("0xstale".to_owned()));
        db.set_bankroll(Decimal::ONE).unwrap();
        db.upsert_position(&stale, OutcomeId(0), 9, 0).unwrap();

        let result =
            supabase_authoritative_boot(&client(&base), &db, &dir.path().join("absent-paper.log"))
                .await;

        assert!(matches!(result, Err(SupabaseStateError::Status(503, _))));
        assert_eq!(db.bankroll().unwrap(), Some(Decimal::ONE));
        assert_eq!(db.paper_positions().unwrap()[0].market_id, stale);
    }
}
