//! One-time SQLite → Supabase backfill for the authoritative cutover (issue #397).
//!
//! Run once with the service stopped (`pe-service --backfill-supabase <config>`) after the
//! schema is applied and before flipping `PE_SUPABASE_AUTHORITATIVE`. Pushes the
//! authoritative-but-unmirrored tables (`paper_bankroll` + `paper_positions`) from the
//! complete local SQLite, completes the pre-cutover `paper_fills` tail, and seeds the
//! catch-up watermark to the event-log head. Idempotent.

use anyhow::{Context, Result};
use pe_paper_state::PaperStateDb;
use tracing::info;

use crate::supabase_sink::{SinkWriter, SupabaseWriter, reconcile_fills};
use crate::supabase_state::SupabaseStateClient;

/// Per-table results from [`backfill_supabase`], printed for cutover verification.
pub struct BackfillCounts {
    pub bankroll_set: bool,
    pub positions: usize,
    pub fills_hwm: i64,
    pub watermark: i64,
}

/// Push `paper_bankroll` + `paper_positions` from the local SQLite into Supabase, run a final
/// `paper_fills` reconcile so the pre-cutover fill tail is complete (#397 SF-A), and seed the
/// catch-up watermark to the event-log head so the first authoritative boot re-applies zero
/// fills (#397 B-C). Idempotent. `paper_state` must already be reconciled from the event log
/// (so the local scalars are complete). Requires the service-role secret (RLS blocks anon
/// writes); the caller validates that.
pub async fn backfill_supabase(
    paper_state: &PaperStateDb,
    supabase_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<BackfillCounts> {
    let client =
        SupabaseStateClient::new(reqwest::Client::new(), supabase_url, anon_key, secret_key);

    // paper_bankroll — the singleton, from the complete local scalar.
    let bankroll_set = match paper_state.bankroll().context("read local bankroll")? {
        Some(b) => {
            client
                .upsert_bankroll(b)
                .await
                .context("upsert paper_bankroll")?;
            true
        }
        None => false,
    };

    // paper_positions — every net position row.
    let positions = paper_state
        .paper_positions()
        .context("read local positions")?;
    for pos in &positions {
        client
            .upsert_position(pos)
            .await
            .with_context(|| format!("upsert paper_positions {}", pos.market_id))?;
    }

    // Final paper_fills reconcile (#397 SF-A, hardened in #510): a FULL idempotent sweep
    // from `-1` — not the sink HWM — so fill seq 0 is included (the sink HWM seeds at 0 and
    // `> hwm` would skip it) and a stale high-epoch HWM surviving a paper reset (the reset
    // preserves `supabase_sink_hwm` while the fresh log restarts at seq 0) cannot mask the
    // current epoch. `upsert_fill` is merge-idempotent and the backfill is one-time, so the
    // full sweep is safe and rerunnable.
    let writer = SupabaseWriter::new(reqwest::Client::new(), supabase_url, anon_key, secret_key);
    let (new_hwm, complete) = reconcile_fills(&writer, paper_state, -1)
        .await
        .context("final fills reconcile")?;
    // #510: NEVER seed cursors on a partial sweep — that would permanently hide the omitted
    // fills behind the watermark. Abort (rerunnable; everything above is idempotent).
    anyhow::ensure!(
        complete,
        "fills reconcile halted before completing; rerun --backfill-supabase (no cursor was seeded)"
    );
    // Unconditional write: after a complete full sweep this IS the current-epoch HWM, and a
    // regression vs a stale pre-reset value is the correct outcome (#510).
    writer.write_hwm(new_hwm).await.context("write fills HWM")?;

    // Seed the catch-up watermark to the event-log head (#397 B-C) — but only when a fill
    // exists (#510 absent-vs-zero): on an empty DB `last_applied_event_seq()` conflates
    // absence with 0 and would persist a false `Some(0)` ("seq 0 confirmed"). The paper log
    // carries only fills, so `fills_count() > 0` ⟺ frames exist.
    let head = paper_state
        .last_applied_event_seq()
        .context("read event-log head")?;
    if paper_state.fills_count().context("count fills")? > 0 {
        paper_state
            .set_supabase_applied_event_seq(head)
            .context("seed supabase watermark")?;
    }

    let counts = BackfillCounts {
        bankroll_set,
        positions: positions.len(),
        fills_hwm: new_hwm,
        watermark: i64::try_from(head.0).unwrap_or(i64::MAX),
    };
    info!(
        bankroll_set = counts.bankroll_set,
        positions = counts.positions,
        fills_hwm = counts.fills_hwm,
        watermark = counts.watermark,
        "supabase backfill complete"
    );
    Ok(counts)
}
