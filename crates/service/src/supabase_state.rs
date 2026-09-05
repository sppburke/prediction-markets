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
use std::time::Duration;

use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, OutcomeId, PolymarketConditionId, Price, ReceivedAt,
    ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Reader, Writer};
use pe_execution_core::EconomicPrepared;
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{
    FillRecord, FillRow, FinancialFillRecord, LeaderPositionRow, PaperPositionRow, PaperStateDb,
    PaperStateError,
};
use pe_risk_engine::{BinaryPayout, aggregate_resolution_credit};
use pe_source_polymarket_public::{
    BinaryPayoutVector, CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION,
    ClobPayoutResolution, parse_clob_market,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::decision_replay::{
    AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
};
use crate::orchestrator::{pending_terminal, recorded_fill_terminal, render_pending_evidence};
use crate::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority, FinancialPayload,
    FinancialResult, PAPER_LOG_SCHEMA_VERSION_V2, PaperFillOperationIdentity, PaperLogFrame,
    PaperLogRecord, paper_era, scan_paper_log,
};
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
    /// Authority compared an existing/predecessor operation and rejected changed identity.
    #[error("authoritative paper protocol conflict: {reason}")]
    Conflict { reason: String },
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

/// Upper bound for one authoritative financial mutation. An elapsed request is deliberately
/// ambiguous and is recovered from the synchronized Prepared record.
const AUTHORITATIVE_MUTATION_TIMEOUT: Duration = Duration::from_secs(20);

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

/// Frozen fill request reconstructed solely from a FinancialPrepared payload and receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedFillRequest {
    pub expected_authority: ExpectedAuthority,
    pub prepared_receipt: AppendReceipt,
    pub idempotency_key: String,
    pub leader_wallet: WalletAddress,
    pub source_trade_id: SourceTradeId,
    pub market_id: String,
    pub outcome_id: u16,
    pub side: Side,
    pub quantity: ShareAmount,
    pub fill_price: Price,
    pub principal: CollateralAmount,
    pub fee: CollateralAmount,
    pub entry_unix: i64,
}

impl PreparedFillRequest {
    pub fn from_prepared(
        expected_authority: ExpectedAuthority,
        prepared_receipt: AppendReceipt,
        operation: &PaperFillOperationIdentity,
        economic: &EconomicPrepared,
    ) -> Self {
        let outcome_id = u16::from(economic.market.outcome_index);
        let idempotency_key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
            &operation.leader_wallet.to_string(),
            &operation.source_trade_id.0,
            &economic.market.market_id,
            outcome_id,
            economic.market.side,
            operation.observed_at_bucket,
        );
        Self {
            expected_authority,
            prepared_receipt,
            idempotency_key,
            leader_wallet: operation.leader_wallet,
            source_trade_id: operation.source_trade_id.clone(),
            market_id: economic.market.market_id.clone(),
            outcome_id,
            side: economic.market.side,
            quantity: economic.sizing.expected_shares,
            fill_price: economic.sizing.expected_vwap,
            principal: economic.sizing.principal,
            fee: economic.fee.expected_fee,
            entry_unix: operation.observed_at_bucket,
        }
    }
}

/// Frozen resolution request reconstructed from its Prepared payload and source envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedResolutionRequest {
    pub expected_authority: ExpectedAuthority,
    pub prepared_receipt: AppendReceipt,
    pub condition: PolymarketConditionId,
    pub payout_by_outcome_index_json: String,
    pub settled_at_unix: i64,
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

    /// #545 Start-bound, Prepared-sequenced exact fill mutation.
    fn commit_prepared_fill(
        &self,
        request: &PreparedFillRequest,
    ) -> impl Future<Output = Result<CanonicalFillResult, SupabaseStateError>> + Send;

    /// #545 Start-bound, Prepared-sequenced exact resolution mutation.
    fn apply_prepared_resolution(
        &self,
        request: &PreparedResolutionRequest,
    ) -> impl Future<Output = Result<CanonicalResolutionResult, SupabaseStateError>> + Send;
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

    /// Seed the synchronized QualificationStarted receipt in the authority singleton.
    /// Equal retries are idempotent; a distinct Start is surfaced as a typed conflict.
    pub async fn seed_financial_start(
        &self,
        start: AppendReceipt,
    ) -> Result<(), SupabaseStateError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Response {
            outcome: String,
            #[serde(default)]
            conflict_reason: Option<String>,
            start_seq: i64,
            start_hash: String,
        }

        let body = serde_json::json!({
            "p_start_seq": start.sequence.0,
            "p_start_hash": start.this_hash.to_hex().to_string(),
        });
        let value = self.post_rpc_json("seed_financial_start", &body).await?;
        let response: Response = serde_json::from_value(value).map_err(|error| {
            SupabaseStateError::Corrupt(format!("seed_financial_start shape: {error}"))
        })?;
        if response.outcome == "conflict" {
            return Err(SupabaseStateError::Conflict {
                reason: response
                    .conflict_reason
                    .unwrap_or_else(|| "authority omitted conflict_reason".to_owned()),
            });
        }
        let sequence = u64::try_from(response.start_seq).map_err(|_| {
            SupabaseStateError::Corrupt(format!("financial Start sequence {}", response.start_seq))
        })?;
        if !matches!(response.outcome.as_str(), "applied" | "existing")
            || sequence != start.sequence.0
            || response.start_hash != start.this_hash.to_hex().to_string()
        {
            return Err(SupabaseStateError::Corrupt(
                "seed_financial_start canonical identity differs".to_owned(),
            ));
        }
        Ok(())
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
            .timeout(AUTHORITATIVE_MUTATION_TIMEOUT)
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedFillResp {
    outcome: String,
    conflict_reason: Option<String>,
    bankroll: String,
    applied_prepared_seq: Option<i64>,
    row: Option<PreparedFillRow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedFillRow {
    idempotency_key: String,
    leader_wallet: String,
    source_trade_id: String,
    market_id: String,
    outcome_id: i64,
    side: String,
    quantity: String,
    fill_price: String,
    principal: String,
    fee: String,
    entry_unix: i64,
    prepared_seq: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedResolutionResp {
    outcome: String,
    conflict_reason: Option<String>,
    credit: Option<String>,
    applied_prepared_seq: Option<i64>,
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
/// to dropping fractional digits beyond the 28th significant digit, and only when every
/// dropped digit within the first 24 fractional positions is zero (so the loss is below
/// 1e-24 USD at any magnitude); a nonzero value that would truncate to zero fails closed
/// like every other input.
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
            // Only digits beyond the 24th fractional position may be dropped (the
            // documented bound of 1e-24 USD); a larger integer part shrinks `keep`,
            // so any nonzero digit between `keep` and position 24 fails closed.
            let guard_end = fraction.len().min(24);
            if keep < guard_end && fraction[keep..guard_end].bytes().any(|b| b != b'0') {
                return Err(corrupt(
                    "nonzero digit dropped within the first 24 fractional positions",
                ));
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

    async fn commit_prepared_fill(
        &self,
        request: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, SupabaseStateError> {
        let body = serde_json::json!({
            "p_start_seq": request.expected_authority.qualification_start_receipt.sequence.0,
            "p_start_hash": request.expected_authority.qualification_start_receipt.this_hash.to_hex().to_string(),
            "p_expected_prior_seq": request.expected_authority.prior_completed_prepared_sequence.map(|value| value.0),
            "p_prepared_seq": request.prepared_receipt.sequence.0,
            "p_idempotency_key": request.idempotency_key,
            "p_leader_wallet": request.leader_wallet.to_string(),
            "p_source_trade_id": request.source_trade_id.0,
            "p_market_id": request.market_id,
            "p_outcome_id": request.outcome_id,
            "p_side": side_str(request.side),
            "p_quantity": request.quantity.to_decimal().to_string(),
            "p_fill_price": request.fill_price.0.to_string(),
            "p_principal": request.principal.to_decimal().to_string(),
            "p_fee": request.fee.to_decimal().to_string(),
            "p_entry_unix": request.entry_unix,
        });
        let value = self.post_rpc_json("commit_fill_v2", &body).await?;
        let response: PreparedFillResp = serde_json::from_value(value).map_err(|error| {
            SupabaseStateError::Corrupt(format!("prepared commit_fill_v2 shape: {error}"))
        })?;
        if response.outcome == "conflict" {
            return Err(SupabaseStateError::Conflict {
                reason: response
                    .conflict_reason
                    .unwrap_or_else(|| "authority omitted conflict_reason".to_owned()),
            });
        }
        if response.outcome != "applied" && response.outcome != "existing" {
            return Err(SupabaseStateError::Corrupt(format!(
                "prepared commit_fill_v2 outcome {:?}",
                response.outcome
            )));
        }
        let applied = parse_event_seq(
            response.applied_prepared_seq,
            "prepared commit_fill_v2 applied_prepared_seq",
        )?;
        if applied != request.prepared_receipt.sequence {
            return Err(SupabaseStateError::Corrupt(
                "authority returned a different fill Prepared sequence".to_owned(),
            ));
        }
        let row = response.row.ok_or_else(|| {
            SupabaseStateError::Corrupt("prepared commit_fill_v2 omitted row".to_owned())
        })?;
        validate_prepared_fill_row(request, &row)?;
        Ok(CanonicalFillResult {
            outcome: response.outcome,
            bankroll: bankroll_money(&response.bankroll, "prepared fill bankroll")?,
            applied_prepared_seq: applied,
            quantity: request.quantity,
            principal: request.principal,
            fee: request.fee,
            fill_price: request.fill_price,
        })
    }

    async fn apply_prepared_resolution(
        &self,
        request: &PreparedResolutionRequest,
    ) -> Result<CanonicalResolutionResult, SupabaseStateError> {
        let payout: serde_json::Value = serde_json::from_str(&request.payout_by_outcome_index_json)
            .map_err(|error| {
                SupabaseStateError::Corrupt(format!("prepared resolution payout JSON: {error}"))
            })?;
        let body = serde_json::json!({
            "p_start_seq": request.expected_authority.qualification_start_receipt.sequence.0,
            "p_start_hash": request.expected_authority.qualification_start_receipt.this_hash.to_hex().to_string(),
            "p_expected_prior_seq": request.expected_authority.prior_completed_prepared_sequence.map(|value| value.0),
            "p_prepared_seq": request.prepared_receipt.sequence.0,
            "p_condition_id": request.condition.0,
            "p_payout_by_outcome_index": payout,
            "p_settled_at_unix": request.settled_at_unix,
        });
        let value = self.post_rpc_json("apply_resolution_v2", &body).await?;
        let response: PreparedResolutionResp = serde_json::from_value(value).map_err(|error| {
            SupabaseStateError::Corrupt(format!("prepared apply_resolution_v2 shape: {error}"))
        })?;
        if response.outcome == "conflict" {
            return Err(SupabaseStateError::Conflict {
                reason: response
                    .conflict_reason
                    .unwrap_or_else(|| "authority omitted conflict_reason".to_owned()),
            });
        }
        if response.outcome != "applied" && response.outcome != "existing" {
            return Err(SupabaseStateError::Corrupt(format!(
                "prepared apply_resolution_v2 outcome {:?}",
                response.outcome
            )));
        }
        let applied = parse_event_seq(
            response.applied_prepared_seq,
            "prepared apply_resolution_v2 applied_prepared_seq",
        )?;
        if applied != request.prepared_receipt.sequence {
            return Err(SupabaseStateError::Corrupt(
                "authority returned a different resolution Prepared sequence".to_owned(),
            ));
        }
        let credit = response.credit.ok_or_else(|| {
            SupabaseStateError::Corrupt("prepared apply_resolution_v2 omitted credit".to_owned())
        })?;
        Ok(CanonicalResolutionResult {
            outcome: response.outcome,
            bankroll: bankroll_money(&response.bankroll, "prepared resolution bankroll")?,
            applied_prepared_seq: applied,
            credit: parse_collateral(&credit, "prepared resolution credit")?,
            settled_at_unix: request.settled_at_unix,
        })
    }
}

fn parse_event_seq(value: Option<i64>, what: &'static str) -> Result<EventSeq, SupabaseStateError> {
    let value = value.ok_or(SupabaseStateError::Null(what))?;
    u64::try_from(value)
        .map(EventSeq)
        .map_err(|_| SupabaseStateError::Corrupt(format!("{what}: {value}")))
}

fn parse_collateral(
    value: &str,
    what: &'static str,
) -> Result<CollateralAmount, SupabaseStateError> {
    CollateralAmount::from_decimal_exact(money(value, what)?)
        .map_err(|error| SupabaseStateError::Corrupt(format!("{what}: {error}")))
}

fn parse_shares(value: &str, what: &'static str) -> Result<ShareAmount, SupabaseStateError> {
    ShareAmount::from_decimal_exact(money(value, what)?)
        .map_err(|error| SupabaseStateError::Corrupt(format!("{what}: {error}")))
}

fn validate_prepared_fill_row(
    request: &PreparedFillRequest,
    row: &PreparedFillRow,
) -> Result<(), SupabaseStateError> {
    let outcome_id = u16::try_from(row.outcome_id).map_err(|_| {
        SupabaseStateError::Corrupt(format!("prepared fill outcome_id {}", row.outcome_id))
    })?;
    let side = match row.side.as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        other => {
            return Err(SupabaseStateError::Corrupt(format!(
                "prepared fill side {other:?}"
            )));
        }
    };
    let prepared_seq = u64::try_from(row.prepared_seq).map(EventSeq).map_err(|_| {
        SupabaseStateError::Corrupt(format!("prepared fill sequence {}", row.prepared_seq))
    })?;
    let exact = row.idempotency_key == request.idempotency_key
        && row.leader_wallet == request.leader_wallet.to_string()
        && row.source_trade_id == request.source_trade_id.0
        && row.market_id == request.market_id
        && outcome_id == request.outcome_id
        && side == request.side
        && parse_shares(&row.quantity, "prepared fill quantity")? == request.quantity
        && Price::new(money(&row.fill_price, "prepared fill price")?)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?
            == request.fill_price
        && parse_collateral(&row.principal, "prepared fill principal")? == request.principal
        && parse_collateral(&row.fee, "prepared fill fee")? == request.fee
        && row.entry_unix == request.entry_unix
        && prepared_seq == request.prepared_receipt.sequence;
    if exact {
        Ok(())
    } else {
        Err(SupabaseStateError::Corrupt(
            "authority canonical fill row differs from Prepared".to_owned(),
        ))
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

/// Reconcile the active Start-bound paper protocol before any producer starts.
///
/// Completed Prepared/Final pairs are projected locally in order when SQLite is behind. The sole
/// unmatched Prepared, if present, is retried against its frozen authority request, projected, and
/// completed with one synchronized Final. Scanner validation rejects a second Prepared from
/// overtaking it, so an unknown authority result always blocks the remaining financial prefix.
pub async fn reconcile_active_financial_frames<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    paper_state: &PaperStateDb,
    paper_log_path: &std::path::Path,
    source_log_path: &std::path::Path,
    writer: &mut Writer,
) -> Result<usize, SupabaseStateError> {
    let era =
        paper_era(scan_paper_log(paper_log_path).map_err(|error| {
            SupabaseStateError::Corrupt(format!("scan active paper log: {error}"))
        })?);
    let (start_receipt, _) = era.start.as_ref().ok_or_else(|| {
        SupabaseStateError::Corrupt(
            "active financial reconciliation requires QualificationStarted".to_owned(),
        )
    })?;
    paper_state.seed_financial_start(*start_receipt)?;

    let local_last = paper_state.financial_last_prepared_seq()?;
    let prepared_sequences = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. }) => {
                Some(frame.receipt.sequence)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(local_last) = local_last
        && !prepared_sequences.contains(&local_last)
    {
        return Err(SupabaseStateError::Corrupt(format!(
            "local financial sequence {} is absent from the active paper prefix",
            local_last.0
        )));
    }

    let mut appended_finals = 0usize;
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            expected_authority,
            payload,
        }) = &frame.frame
        else {
            continue;
        };
        let existing_final = era
            .frames
            .iter()
            .find_map(|candidate| match &candidate.frame {
                PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                    prepared_receipt,
                    result,
                }) if prepared_receipt == &frame.receipt => {
                    Some((result.clone(), candidate.receipt))
                }
                _ => None,
            });
        if local_last.is_some_and(|last| frame.receipt.sequence < last) {
            let Some((result, final_receipt)) = &existing_final else {
                return Err(SupabaseStateError::Corrupt(
                    "local projection advanced beyond an unmatched Prepared".to_owned(),
                ));
            };
            terminalize_final_fill_decision(paper_state, payload, result, *final_receipt)?;
            continue;
        }
        if local_last == Some(frame.receipt.sequence)
            && let Some((result, final_receipt)) = &existing_final
        {
            terminalize_final_fill_decision(paper_state, payload, result, *final_receipt)?;
            continue;
        }

        let persisted_result = existing_final.as_ref().map(|(result, _)| result.clone());
        let result = match (payload, persisted_result) {
            (FinancialPayload::Fill { .. }, Some(result @ FinancialResult::Fill { .. })) => {
                apply_financial_result(
                    paper_state,
                    *start_receipt,
                    expected_authority,
                    frame.receipt,
                    payload,
                    &result,
                    source_log_path,
                )?;
                result
            }
            (
                FinancialPayload::Fill {
                    operation,
                    economic,
                },
                None,
            ) => {
                let request = PreparedFillRequest::from_prepared(
                    expected_authority.clone(),
                    frame.receipt,
                    operation,
                    economic,
                );
                let canonical = supabase.commit_prepared_fill(&request).await?;
                let result = FinancialResult::Fill { canonical };
                apply_financial_result(
                    paper_state,
                    *start_receipt,
                    expected_authority,
                    frame.receipt,
                    payload,
                    &result,
                    source_log_path,
                )?;
                result
            }
            (
                FinancialPayload::Resolution { .. },
                Some(result @ FinancialResult::Resolution { .. }),
            ) => {
                apply_financial_result(
                    paper_state,
                    *start_receipt,
                    expected_authority,
                    frame.receipt,
                    payload,
                    &result,
                    source_log_path,
                )?;
                result
            }
            (
                FinancialPayload::Resolution {
                    condition_id,
                    payout_by_outcome_index_json,
                    resolution_source_receipt,
                },
                None,
            ) => {
                let settled_at_unix = resolution_source_received_at(
                    source_log_path,
                    *resolution_source_receipt,
                    condition_id,
                    payout_by_outcome_index_json,
                )?;
                let request = PreparedResolutionRequest {
                    expected_authority: expected_authority.clone(),
                    prepared_receipt: frame.receipt,
                    condition: condition_id.clone(),
                    payout_by_outcome_index_json: payout_by_outcome_index_json.clone(),
                    settled_at_unix,
                };
                let canonical = supabase.apply_prepared_resolution(&request).await?;
                let result = FinancialResult::Resolution { canonical };
                apply_financial_result(
                    paper_state,
                    *start_receipt,
                    expected_authority,
                    frame.receipt,
                    payload,
                    &result,
                    source_log_path,
                )?;
                result
            }
            _ => {
                return Err(SupabaseStateError::Corrupt(
                    "financial Final kind differs from its Prepared".to_owned(),
                ));
            }
        };

        if let Some((_, final_receipt)) = existing_final {
            terminalize_final_fill_decision(paper_state, payload, &result, final_receipt)?;
            continue;
        }
        let now = time::OffsetDateTime::now_utc();
        let final_payload = serde_json::to_vec(&PaperLogRecord::FinancialFinal {
            prepared_receipt: frame.receipt,
            result: result.clone(),
        })?;
        let final_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION_V2,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: final_payload,
            })
            .map_err(|error| {
                SupabaseStateError::Corrupt(format!("append recovered FinancialFinal: {error}"))
            })?;
        terminalize_final_fill_decision(paper_state, payload, &result, final_receipt)?;
        appended_finals = appended_finals.saturating_add(1);
    }
    Ok(appended_finals)
}

pub(crate) fn terminalize_final_fill_decision(
    paper_state: &PaperStateDb,
    payload: &FinancialPayload,
    result: &FinancialResult,
    final_receipt: AppendReceipt,
) -> Result<(), SupabaseStateError> {
    let (FinancialPayload::Fill { operation, .. }, FinancialResult::Fill { canonical }) =
        (payload, result)
    else {
        return Ok(());
    };
    let row = paper_state
        .open_decision_pending()?
        .into_iter()
        .find(|row| row.source_trade_id == operation.source_trade_id);
    let Some(row) = row else {
        return Ok(());
    };
    let evidence = DecisionEvidenceAccumulator::from_pending_checkpoint(&row).map_err(|error| {
        SupabaseStateError::Corrupt(format!(
            "rebuild pending fill evidence {}: {error}",
            operation.source_trade_id
        ))
    })?;
    let terminal = render_pending_evidence(
        Some(&evidence),
        AuthorityEvidence::commit_fill_v2(&canonical.outcome, canonical.bankroll),
        TerminalDispositionEvidence::final_fill(final_receipt),
    )?
    .ok_or_else(|| {
        SupabaseStateError::Corrupt("fill terminal evidence unexpectedly absent".to_owned())
    })?;
    paper_state.close_decision_pending(
        &operation.source_trade_id,
        &terminal.0,
        "fill",
        terminal.1,
    )?;
    Ok(())
}

pub(crate) fn apply_financial_result(
    paper_state: &PaperStateDb,
    start: AppendReceipt,
    expected: &ExpectedAuthority,
    prepared_receipt: AppendReceipt,
    payload: &FinancialPayload,
    result: &FinancialResult,
    source_log_path: &std::path::Path,
) -> Result<(), SupabaseStateError> {
    match (payload, result) {
        (
            FinancialPayload::Fill {
                operation,
                economic,
            },
            FinancialResult::Fill { canonical },
        ) => {
            let request = PreparedFillRequest::from_prepared(
                expected.clone(),
                prepared_receipt,
                operation,
                economic,
            );
            if !matches!(canonical.outcome.as_str(), "applied" | "existing")
                || canonical.applied_prepared_seq != prepared_receipt.sequence
                || canonical.quantity != request.quantity
                || canonical.principal != request.principal
                || canonical.fee != request.fee
                || canonical.fill_price != request.fill_price
            {
                return Err(SupabaseStateError::Corrupt(
                    "fill Final canonical values differ from Prepared".to_owned(),
                ));
            }
            let record = FinancialFillRecord {
                idempotency_key: request.idempotency_key,
                market_id: MarketId(VenueMarketId(request.market_id)),
                outcome_id: OutcomeId(request.outcome_id),
                side: request.side,
                quantity: request.quantity,
                fill_price: request.fill_price,
                principal: request.principal,
                fee: request.fee,
            };
            paper_state.apply_financial_fill(
                start,
                expected.prior_completed_prepared_sequence,
                prepared_receipt.sequence,
                &record,
                canonical.bankroll,
            )?;
        }
        (
            FinancialPayload::Resolution {
                condition_id,
                payout_by_outcome_index_json,
                resolution_source_receipt,
            },
            FinancialResult::Resolution { canonical },
        ) => {
            if !matches!(canonical.outcome.as_str(), "applied" | "existing")
                || canonical.applied_prepared_seq != prepared_receipt.sequence
                || canonical.settled_at_unix
                    != resolution_source_received_at(
                        source_log_path,
                        *resolution_source_receipt,
                        condition_id,
                        payout_by_outcome_index_json,
                    )?
            {
                return Err(SupabaseStateError::Corrupt(
                    "resolution Final sequence or settlement time differs from evidence".to_owned(),
                ));
            }
            let condition = MarketId(VenueMarketId(condition_id.0.clone()));
            if paper_state.financial_last_prepared_seq()? != Some(prepared_receipt.sequence) {
                let payout = BinaryPayoutVector::from_canonical_json(payout_by_outcome_index_json)
                    .map_err(|error| {
                        SupabaseStateError::Corrupt(format!("resolution payout vector: {error}"))
                    })?;
                let snapshot = paper_state.financial_snapshot(canonical.settled_at_unix)?;
                let decimals = payout.decimals();
                let binary_payout =
                    BinaryPayout::new(decimals[0], decimals[1]).map_err(|error| {
                        SupabaseStateError::Corrupt(format!("resolution payout vector: {error}"))
                    })?;
                let positions = snapshot
                    .positions
                    .into_iter()
                    .filter(|position| position.market_id == condition)
                    .map(|position| {
                        let net_shares =
                            position.long.checked_sub(position.short).map_err(|_| {
                                SupabaseStateError::Corrupt(format!(
                                    "resolution position {}:{} is net short",
                                    position.market_id, position.outcome_id.0
                                ))
                            })?;
                        Ok((position.outcome_id.0, net_shares))
                    })
                    .collect::<Result<Vec<_>, SupabaseStateError>>()?;
                let derived =
                    aggregate_resolution_credit(&positions, &binary_payout).map_err(|error| {
                        SupabaseStateError::Corrupt(format!("resolution arithmetic: {error}"))
                    })?;
                if derived != canonical.credit {
                    return Err(SupabaseStateError::Corrupt(
                        "resolution authority credit differs from shared aggregate arithmetic"
                            .to_owned(),
                    ));
                }
            }
            paper_state.apply_financial_resolution(
                start,
                expected.prior_completed_prepared_sequence,
                prepared_receipt.sequence,
                &condition,
                payout_by_outcome_index_json,
                *resolution_source_receipt,
                canonical.settled_at_unix,
                canonical.credit,
                canonical.bankroll,
            )?;
        }
        _ => {
            return Err(SupabaseStateError::Corrupt(
                "financial result kind mismatch".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn resolution_source_received_at(
    source_log_path: &std::path::Path,
    receipt: AppendReceipt,
    condition: &PolymarketConditionId,
    payout_json: &str,
) -> Result<i64, SupabaseStateError> {
    let replay = Reader::replay(source_log_path).map_err(|error| {
        SupabaseStateError::Corrupt(format!("open source log for resolution evidence: {error}"))
    })?;
    for frame in replay {
        let (sequence, envelope) = frame.map_err(|error| {
            SupabaseStateError::Corrupt(format!("read source resolution evidence: {error}"))
        })?;
        if sequence == receipt.sequence {
            if envelope.this_hash != receipt.this_hash {
                return Err(SupabaseStateError::Corrupt(
                    "resolution source receipt hash differs from verified envelope".to_owned(),
                ));
            }
            if envelope.source_id.0 != "polymarket.clob.market" {
                return Err(SupabaseStateError::Corrupt(
                    "resolution receipt does not reference CLOB market evidence".to_owned(),
                ));
            }
            if envelope.schema_version != CLOB_RESOLUTION_SCHEMA_VERSION
                || envelope.parser_version != CLOB_RESOLUTION_PARSER_VERSION
            {
                return Err(SupabaseStateError::Corrupt(
                    "resolution receipt uses an unsupported CLOB schema or parser version"
                        .to_owned(),
                ));
            }
            let market = parse_clob_market(&envelope.payload).map_err(|error| {
                SupabaseStateError::Corrupt(format!(
                    "parse referenced CLOB resolution evidence: {error}"
                ))
            })?;
            if market.condition_id.as_deref() != Some(condition.0.as_str()) {
                return Err(SupabaseStateError::Corrupt(
                    "referenced CLOB resolution condition differs from Prepared".to_owned(),
                ));
            }
            let ClobPayoutResolution::Resolved(payout) = market.resolution_evidence().payout else {
                return Err(SupabaseStateError::Corrupt(
                    "referenced CLOB market is not resolved".to_owned(),
                ));
            };
            if payout.canonical_json() != payout_json {
                return Err(SupabaseStateError::Corrupt(
                    "referenced CLOB payout differs from Prepared".to_owned(),
                ));
            }
            return Ok(envelope.received_at.0.unix_timestamp());
        }
    }
    Err(SupabaseStateError::Corrupt(format!(
        "resolution source receipt {} is absent",
        receipt.sequence.0
    )))
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
    let era = paper_era(scan_paper_log(event_log_path).map_err(|error| {
        SupabaseStateError::Corrupt(format!(
            "scan paper log {}: {error}",
            event_log_path.display()
        ))
    })?);
    if era.start.is_some() {
        return Err(SupabaseStateError::Corrupt(
            "legacy Supabase frame walk is forbidden after QualificationStarted".to_owned(),
        ));
    }
    for frame in era.frames {
        let seq = frame.receipt.sequence;
        let seq_i = i64::try_from(seq.0).unwrap_or(i64::MAX);
        if seq_i <= new_wm {
            continue;
        }
        let PaperLogFrame::LegacyFill(fill) = frame.frame else {
            paper_state.set_supabase_applied_event_seq(seq)?;
            new_wm = seq_i;
            continue;
        };
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
    use std::sync::atomic::AtomicUsize;

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

    #[derive(Clone, Default)]
    struct PreparedResolutionAuthority {
        calls: Arc<AtomicUsize>,
    }

    impl SupabaseStateTrait for PreparedResolutionAuthority {
        async fn commit_fill_v2(
            &self,
            _row: &SupabaseFillRow,
        ) -> Result<FillV2Outcome, SupabaseStateError> {
            Err(SupabaseStateError::Corrupt(
                "legacy fill RPC is outside this fixture".to_owned(),
            ))
        }

        async fn apply_resolution_v2(
            &self,
            _market_id: &MarketId,
            _outcome_prices: &[Decimal],
            _settled_at_unix: i64,
        ) -> Result<ResolutionV2Outcome, SupabaseStateError> {
            Err(SupabaseStateError::Corrupt(
                "legacy resolution RPC is outside this fixture".to_owned(),
            ))
        }

        fn apply_prepared_resolution(
            &self,
            request: &PreparedResolutionRequest,
        ) -> impl Future<Output = Result<CanonicalResolutionResult, SupabaseStateError>> + Send
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let prepared = request.prepared_receipt.sequence;
            let settled_at_unix = request.settled_at_unix;
            async move {
                Ok(CanonicalResolutionResult {
                    outcome: "applied".to_owned(),
                    bankroll: Decimal::from(100u32),
                    applied_prepared_seq: prepared,
                    credit: CollateralAmount::ZERO,
                    settled_at_unix,
                })
            }
        }
    }

    fn append_test_record<T: serde::Serialize>(
        writer: &mut Writer,
        schema_version: u32,
        value: &T,
    ) -> AppendReceipt {
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("test".to_owned()),
                schema_version,
                parser_version: 1,
                observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(value).unwrap(),
            })
            .unwrap()
    }

    #[tokio::test]
    async fn unmatched_resolution_recovery_mutates_and_finalizes_once() {
        use crate::paper_recovery::{FinancialPayload, QualificationStarted, TailBinding};

        let dir = tempfile::tempdir().unwrap();
        let paper_path = dir.path().join("paper.log");
        let source_path = dir.path().join("source.log");
        let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        db.init_bankroll(Decimal::from(100u32)).unwrap();

        let mut source_writer = Writer::open(&source_path).unwrap();
        let source_receipt = source_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("polymarket.clob.market".to_owned()),
                schema_version: pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
                observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload: br#"{"condition_id":"condition","closed":true,
                    "is_50_50_outcome":false,
                    "tokens":[{"token_id":"yes","outcome":"Yes","price":1,"winner":true},
                              {"token_id":"no","outcome":"No","price":0,"winner":false}]}"#
                    .to_vec(),
            })
            .unwrap();
        drop(source_writer);

        let mut writer = Writer::open(&paper_path).unwrap();
        let tail = TailBinding {
            physical_tail: 8,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        let start = append_test_record(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION_V2,
            &PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
                starting_bankroll: CollateralAmount::from_decimal_exact(Decimal::from(100u32))
                    .unwrap(),
                paper_prefix: tail.clone(),
                source_prefix: tail.clone(),
                live_prefix: tail,
                artifact_blake3: "artifact".to_owned(),
                static_config_hash: "static".to_owned(),
                hot_config_hash: "hot".to_owned(),
                generation: "generation".to_owned(),
                activation_id: "activation".to_owned(),
                ranking_batch_id: 1,
                policy_hash: "policy".to_owned(),
                membership: Vec::new(),
                membership_proofs_hash: "proofs".to_owned(),
                schema_version: 1,
                parser_version: 1,
                financial_semantic_version: 1,
            })),
        );
        let prepared = append_test_record(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION_V2,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Resolution {
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                    resolution_source_receipt: source_receipt,
                },
            },
        );

        let authority = PreparedResolutionAuthority::default();
        assert_eq!(
            reconcile_active_financial_frames(
                &authority,
                &db,
                &paper_path,
                &source_path,
                &mut writer,
            )
            .await
            .unwrap(),
            1
        );
        drop(writer);
        assert_eq!(authority.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.financial_last_prepared_seq().unwrap(),
            Some(prepared.sequence)
        );

        let mut writer = Writer::open(&paper_path).unwrap();
        assert_eq!(
            reconcile_active_financial_frames(
                &authority,
                &db,
                &paper_path,
                &source_path,
                &mut writer,
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(authority.calls.load(Ordering::SeqCst), 1);
        let finals = scan_paper_log(&paper_path)
            .unwrap()
            .into_iter()
            .filter(|frame| {
                matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
                )
            })
            .count();
        assert_eq!(finals, 1);
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
        // The loss bound holds at any magnitude: a five-digit integer part keeps 23
        // fractional digits, so a nonzero 24th digit fails closed, while over-precision
        // beyond the 24th position is still dropped.
        assert_eq!(
            super::bankroll_money("3.12345678901234567890123456789", "bankroll")
                .unwrap()
                .to_string(),
            "3.123456789012345678901234567"
        );
        assert!(super::bankroll_money("12345.1234567890123456789012345", "bankroll").is_err());
        assert_eq!(
            super::bankroll_money("123456.12345678901234567890120000001", "bankroll")
                .unwrap()
                .to_string(),
            "123456.1234567890123456789012"
        );
        assert!(
            super::bankroll_money("123456789012345678901234567.10000000000001", "bankroll")
                .is_err()
        );
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
