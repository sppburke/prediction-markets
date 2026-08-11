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
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pe_core_types::{EventSeq, MarketId, OutcomeId, Side, SourceTradeId, VenueMarketId};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{
    FillRecord, LeaderPositionRow, PaperPositionRow, PaperStateDb, PaperStateError,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::supabase_reader::auth_token;
use crate::supabase_sink::{SupabaseFillRow, supabase_fill_from};

/// Errors from the authoritative Supabase boundary. On the trade path an
/// [`SupabaseStateError`] from the RPC is the fail-closed trigger (the caller skips the
/// trade; the event log holds the fill and replays on restart).
#[derive(Debug, thiserror::Error)]
pub enum SupabaseStateError {
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
}

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Parse a PostgREST scalar-RPC return (a JSON string or number) into a [`Decimal`] — never
/// via `f64`. `NULL` maps to [`SupabaseStateError::Null`].
fn decimal_from_rpc(
    v: &serde_json::Value,
    what: &'static str,
) -> Result<Decimal, SupabaseStateError> {
    match v {
        serde_json::Value::String(s) => {
            Decimal::from_str(s.trim()).map_err(|_| SupabaseStateError::Corrupt(s.clone()))
        }
        // The RPCs `RETURN text`, so a bare number is unexpected. Accept an exact integer
        // without ever touching `f64` (CLAUDE.md: no f64 for money); refuse a float rather
        // than round-trip it lossily.
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Decimal::from)
            .or_else(|| n.as_u64().map(Decimal::from))
            .ok_or_else(|| {
                SupabaseStateError::Corrupt(format!("{what}: non-integer numeric RPC return {n}"))
            }),
        serde_json::Value::Null => Err(SupabaseStateError::Null(what)),
        other => Err(SupabaseStateError::Corrupt(other.to_string())),
    }
}

/// Idempotent authoritative writes. Abstracted as a trait so scenario tests drive
/// [`commit_fill_authoritative`] / [`apply_resolution_authoritative`] with an in-memory fake.
pub trait SupabaseStateTrait: Send + Sync {
    /// Apply a fill via the authoritative `commit_fill` RPC; returns the new bankroll.
    fn commit_fill(
        &self,
        row: &SupabaseFillRow,
    ) -> impl Future<Output = Result<Decimal, SupabaseStateError>> + Send;
    /// Apply a resolution credit via the authoritative `apply_resolution` RPC; returns the
    /// resulting bankroll (unchanged when the market was already settled).
    fn apply_resolution(
        &self,
        market_id: &MarketId,
        outcome_prices: &[Decimal],
        credit: Decimal,
        settled_at_unix: i64,
    ) -> impl Future<Output = Result<Decimal, SupabaseStateError>> + Send;
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
    async fn post_rpc(
        &self,
        func: &'static str,
        body: &serde_json::Value,
    ) -> Result<Decimal, SupabaseStateError> {
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
        let value: serde_json::Value = resp.json().await.map_err(SupabaseStateError::Decode)?;
        decimal_from_rpc(&value, func)
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
            .map(|r| {
                Decimal::from_str(r.bankroll_str.trim())
                    .map_err(|_| SupabaseStateError::Corrupt(r.bankroll_str.clone()))
            })
            .transpose()
    }

    /// The authoritative net positions, for the boot pull into the local cache.
    pub async fn fetch_positions(&self) -> Result<Vec<PaperPositionRow>, SupabaseStateError> {
        let url = format!(
            "{}/rest/v1/paper_positions?select=market_id,outcome_id,long_contracts,short_contracts",
            self.base_url
        );
        let rows: Vec<PositionRow> = self.get_json(&url).await?;
        rows.into_iter()
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

impl SupabaseStateTrait for SupabaseStateClient {
    async fn commit_fill(&self, row: &SupabaseFillRow) -> Result<Decimal, SupabaseStateError> {
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
        self.post_rpc("commit_fill", &body).await
    }

    async fn apply_resolution(
        &self,
        market_id: &MarketId,
        outcome_prices: &[Decimal],
        credit: Decimal,
        settled_at_unix: i64,
    ) -> Result<Decimal, SupabaseStateError> {
        // `outcome_prices` as a jsonb array of text decimals — matches the sink's
        // `settled_markets.outcome_prices` shape (so `wallet_live_stats` reads agree).
        let prices: Vec<String> = outcome_prices.iter().map(|d| d.to_string()).collect();
        let body = serde_json::json!({
            "p_market_id": market_id.0.0,
            "p_outcome_prices": prices,
            "p_credit": credit.to_string(),
            "p_settled_at_unix": settled_at_unix,
        });
        self.post_rpc("apply_resolution", &body).await
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

/// Authoritative fill commit (issue #397): write the Supabase `commit_fill` RPC **first**
/// (fail-closed — the `?` propagates an RPC error so the caller skips the trade; the event
/// log already holds the fill and replays on restart), **then** mirror to local SQLite.
/// Returns the authoritative (RPC) bankroll. A local SQLite mirror failure is logged, not
/// propagated — the authoritative write already succeeded and SQLite self-heals on restart.
///
/// The RPC `.await` completes before `PaperStateDb::commit_fill` takes the SQLite mutex, so
/// the lock is never held across the await (no cross-await lock, no `Send` hazard).
pub async fn commit_fill_authoritative<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    paper_state: &PaperStateDb,
    source_trade_id: &SourceTradeId,
    leader: &LeaderPositionRow,
    record: &FillRecord,
    seq: EventSeq,
    sup_row: &SupabaseFillRow,
    flip: Option<pe_paper_state::DispatchFlip<'_>>,
) -> Result<Decimal, SupabaseStateError> {
    let new_bankroll = supabase.commit_fill(sup_row).await?;
    // #508 round-4: the local transaction (mirror + seen + dispatch flip) follows the
    // authoritative RPC; a failure here surfaces and retries IN-PROCESS (bounded) — the
    // dispatch flip must not silently wait for a restart. Restart recovery remains the
    // durable backstop.
    let mut local = Ok(());
    for attempt in 1u32..=3 {
        local = paper_state
            .commit_fill_with_flip(source_trade_id, leader, record, seq, flip)
            .map(|_| ());
        match &local {
            Ok(()) => break,
            Err(e) if attempt < 3 => {
                tracing::error!(
                    error = %e,
                    attempt,
                    "authoritative fill: local mirror/flip failed; retrying in-process"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(_) => {}
        }
    }
    if let Err(e) = local {
        warn!(
            error = %e,
            "authoritative fill committed to Supabase but local SQLite mirror/flip failed \
             after in-process retries (heals on restart/reprocess)"
        );
    }
    Ok(new_bankroll)
}

/// Authoritative resolution (issue #397): apply the `apply_resolution` RPC **first**
/// (fail-closed — on error return `Err` so the caller leaves the market unsettled for the
/// next tick to retry), **then** mirror to SQLite via the atomic
/// [`ResolutionStore::settle_and_credit`]. Returns the authoritative (RPC) bankroll.
pub async fn apply_resolution_authoritative<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    store: &mut ResolutionStore,
    market_id: &MarketId,
    outcome_prices: &[Decimal],
    credit: Decimal,
    settled_at_unix: i64,
) -> Result<Decimal, SupabaseStateError> {
    let bankroll = supabase
        .apply_resolution(market_id, outcome_prices, credit, settled_at_unix)
        .await?;
    if let Err(e) = store.settle_and_credit(
        market_id.clone(),
        outcome_prices.to_vec(),
        credit,
        settled_at_unix,
    ) {
        warn!(
            error = %e,
            market = %market_id,
            "authoritative resolution committed to Supabase but local SQLite mirror failed \
             (heals on restart/retry)"
        );
    }
    Ok(bankroll)
}

// ── Boot: catch-up then pull ────────────────────────────────────────────────────

/// Catch up Supabase from the local fills beyond `watermark` (issue #397 boot step 3):
/// replay each `list_fills()` row with `event_seq > watermark` through the idempotent
/// `commit_fill` RPC, advancing the watermark along the confirmed prefix and **halting at
/// the first failed apply** (the tail retries next boot). Returns `(new_watermark,
/// fully_caught_up)` — `fully_caught_up` is `false` when the loop broke early on a failed
/// apply, so the caller knows Supabase is incomplete (its bankroll is then *overstated* —
/// missing un-applied debits — and must not be pulled back into SQLite). Re-applying a fill
/// already in `paper_fills` is a no-op debit (the RPC's insert gate), so a reset watermark
/// replays safely.
pub async fn catch_up_supabase<S: SupabaseStateTrait + ?Sized>(
    supabase: &S,
    paper_state: &PaperStateDb,
    watermark: i64,
) -> Result<(i64, bool), SupabaseStateError> {
    let mut new_wm = watermark;
    let mut fully_caught_up = true;
    for row in paper_state
        .list_fills()?
        .into_iter()
        .filter(|f| f.event_seq > watermark)
    {
        match supabase_fill_from(&row) {
            Some(sup) => match supabase.commit_fill(&sup).await {
                Ok(_) => new_wm = row.event_seq,
                Err(e) => {
                    warn!(
                        error = %e,
                        event_seq = row.event_seq,
                        "supabase authoritative catch-up: commit_fill failed; halting at last confirmed prefix"
                    );
                    fully_caught_up = false;
                    break;
                }
            },
            // Non-`wf|` fill (no leader): cannot write `paper_fills.leader_wallet` (NOT NULL).
            // Advance past it so it never blocks the prefix — matches the sink reconcile skip.
            None => {
                warn!(
                    key = %row.idempotency_key,
                    "supabase authoritative catch-up: skipping non-winner-follow fill (no leader)"
                );
                new_wm = row.event_seq;
            }
        }
    }
    Ok((new_wm, fully_caught_up))
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
pub async fn supabase_authoritative_boot(
    client: &SupabaseStateClient,
    paper_state: &PaperStateDb,
) -> Result<(), SupabaseStateError> {
    // 3. Catch up Supabase from local fills beyond the dedicated watermark.
    let wm = i64::try_from(paper_state.last_supabase_applied_event_seq()?.0).unwrap_or(i64::MAX);
    let (new_wm, fully_caught_up) = catch_up_supabase(client, paper_state, wm).await?;
    if new_wm > wm {
        paper_state.set_supabase_applied_event_seq(EventSeq(u64::try_from(new_wm).unwrap_or(0)))?;
    }

    // If catch-up halted before completing, Supabase is missing some debits (its bankroll is
    // overstated), so the pull below must NOT overwrite the event-log-reconciled SQLite value.
    // Keep the local state; the next boot retries catch-up. Not fatal — the local cache is
    // self-consistent with the local fills, and the un-applied tail is durable in the event log.
    if !fully_caught_up {
        warn!(
            "supabase authoritative boot: catch-up halted before completing; skipping the \
             bankroll/positions pull (Supabase incomplete). Retaining local SQLite state; the \
             next boot completes catch-up."
        );
        return Ok(());
    }

    // 4. Pull authoritative bankroll + positions Supabase → local (catch-up complete, so the
    //    Supabase values are the authority).
    match client.fetch_bankroll().await? {
        Some(bankroll) => {
            paper_state.set_bankroll(bankroll)?;
            info!(bankroll = %bankroll, "supabase authoritative boot: pulled bankroll");
        }
        // Fail-closed: an authoritative boot with no `paper_bankroll` row cannot establish the
        // authority. Refuse to boot rather than warn-and-trade with a stale local balance (an
        // un-backfilled deploy: flag on without `--backfill-supabase`). Mirrors the existing
        // "hard-fail if Supabase empty" boot precedent for the watchlist.
        None => return Err(SupabaseStateError::Uninitialised("paper_bankroll")),
    }
    let positions = client.fetch_positions().await?;
    let n = positions.len();
    for pos in positions {
        paper_state.upsert_position(
            &pos.market_id,
            pos.outcome_id,
            pos.long_contracts,
            pos.short_contracts,
        )?;
    }
    info!(
        positions = n,
        "supabase authoritative boot: pulled positions"
    );
    Ok(())
}
