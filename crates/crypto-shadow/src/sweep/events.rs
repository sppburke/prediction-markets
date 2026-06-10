//! The single streaming decode pass of the normative replay loop (issue #310):
//! `raw_ticks` in `id` (tape) order → materialized exchange [`SweepEvent`]s
//! (small), a per-frame [`BookIndex`], per-market **activation ids**, and
//! [`DecodeStats`] — logged, never silent.

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::db::{DbError, ShadowDb};
use crate::types::{BtcMarketMeta, ExchangeVenue, FeedSource};
use crate::{clob_ws, exchange_ws};

use super::book_index::{BOOK_INDEX_PER_TOKEN_CAP, BookIndex};

/// One decoded exchange trade tick with its tape position. The detector window
/// runs on `observed_at_ms` (exchange source clock); `received_ms` (node clock)
/// feeds `feed_to_book_lag_ms` — exactly as live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SweepEvent {
    pub venue: ExchangeVenue,
    pub price: Decimal,
    pub observed_at_ms: i64,
    pub received_ms: i64,
    pub tape_id: i64,
}

/// Tallies from the decode pass, stamped into the sweep output's tape-validity
/// block. Mirrors the live `DriveStats` philosophy: counted, surfaced once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DecodeStats {
    pub frames_total: u64,
    /// Chainlink frames are skipped — they never drive live observations.
    pub chainlink_skipped: u64,
    pub clob_book_updates_indexed: u64,
    /// Book updates for tokens outside the `markets` table (never queried).
    pub clob_unknown_token_updates: u64,
    pub clob_decode_errors: u64,
    pub exchange_ticks: u64,
    /// Exchange frames that are valid but not trades (acks, heartbeats).
    pub exchange_non_trade_frames: u64,
    pub exchange_decode_errors: u64,
    /// Book frames dropped after a token hit `book_index_per_token_cap`.
    pub book_entries_dropped_after_cap: u64,
    /// Tokens that hit the cap.
    pub capped_tokens: u64,
}

/// Everything the per-cell replay needs, decoded once.
pub(super) struct Tape {
    pub events: Vec<SweepEvent>,
    pub books: BookIndex,
    /// `condition_id -> tape id` of the first raw CLOB frame referencing either
    /// of the market's tokens — the replay's registration gate.
    pub activation_by_condition: HashMap<String, i64>,
    pub stats: DecodeStats,
}

/// Stream the tape once: decode exchange frames to events, fold CLOB book
/// frames into the per-frame index, record activation ids, tally stats.
pub(super) fn load_and_decode(db: &ShadowDb, markets: &[BtcMarketMeta]) -> Result<Tape, DbError> {
    let mut token_to_condition: HashMap<String, String> = HashMap::new();
    for m in markets {
        token_to_condition.insert(m.yes_token_id.clone(), m.condition_id.clone());
        token_to_condition.insert(m.no_token_id.clone(), m.condition_id.clone());
    }

    let mut events: Vec<SweepEvent> = Vec::new();
    let mut books = BookIndex::new(BOOK_INDEX_PER_TOKEN_CAP);
    let mut activation: HashMap<String, i64> = HashMap::new();
    let mut stats = DecodeStats::default();

    db.raw_ticks_ordered(|row| {
        stats.frames_total += 1;
        match row.source {
            FeedSource::Chainlink => {
                stats.chainlink_skipped += 1;
            }
            // A CLOB frame is at most one of: a book update or a trade print
            // (both share the channel) — attempt each decoder; a genuinely
            // malformed frame fails both but is ONE unprocessable frame, so the
            // tally increments at most once per frame (mirrors live `drive`).
            FeedSource::Clob => {
                let mut clob_decode_failed = false;
                match clob_ws::parse_clob_frame(&row.payload_json) {
                    Ok(updates) => {
                        for u in updates {
                            if let Some(condition) = token_to_condition.get(&u.token_id) {
                                activation.entry(condition.clone()).or_insert(row.id);
                                books.push(row.id, row.received_ms, &u);
                                stats.clob_book_updates_indexed += 1;
                            } else {
                                stats.clob_unknown_token_updates += 1;
                            }
                        }
                    }
                    Err(_) => clob_decode_failed = true,
                }
                match clob_ws::parse_clob_trade(&row.payload_json) {
                    Ok(Some(trade)) => {
                        // A trade print also "references" the token: it can be
                        // the activation frame (rare — snapshots usually land
                        // first after subscribe). The trade itself is unused in
                        // PR1 (scalp/MM are PR2/PR3).
                        if let Some(condition) = token_to_condition.get(&trade.token_id) {
                            activation.entry(condition.clone()).or_insert(row.id);
                        }
                    }
                    Ok(None) => {}
                    Err(_) => clob_decode_failed = true,
                }
                if clob_decode_failed {
                    stats.clob_decode_errors += 1;
                }
            }
            FeedSource::Bybit | FeedSource::Okx | FeedSource::Coinbase => {
                if let Some(venue) = row.source.exchange_venue() {
                    match exchange_ws::parse_trade_frame(venue, &row.payload_json) {
                        Ok(Some(tick)) => {
                            stats.exchange_ticks += 1;
                            events.push(SweepEvent {
                                venue,
                                price: tick.price,
                                observed_at_ms: tick.observed_at_ms,
                                received_ms: row.received_ms,
                                tape_id: row.id,
                            });
                        }
                        Ok(None) => stats.exchange_non_trade_frames += 1,
                        Err(_) => stats.exchange_decode_errors += 1,
                    }
                }
            }
        }
        Ok(())
    })?;

    stats.book_entries_dropped_after_cap = books.dropped_after_cap();
    stats.capped_tokens = books.capped_tokens();

    Ok(Tape {
        events,
        books,
        activation_by_condition: activation,
        stats,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

    use crate::types::BtcSeriesKind;

    fn market() -> BtcMarketMeta {
        BtcMarketMeta {
            condition_id: "0xc1".to_string(),
            yes_token_id: "y1".to_string(),
            no_token_id: "n1".to_string(),
            series: BtcSeriesKind::Five,
            range_start_ms: 1_000,
            range_end_ms: 301_000,
            tick: dec!(0.01),
        }
    }

    #[test]
    fn decode_pass_builds_events_books_and_activation_ids() {
        let dir = tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let frames = vec![
            // Tape 1: chainlink frame -> skipped, never decoded.
            (FeedSource::Chainlink, 1_000_i64, "{garbage".to_string()),
            // Tape 2: book frame for a known token -> activation + index.
            (
                FeedSource::Clob,
                1_200,
                r#"{"market":"0xc1","price_changes":[{"asset_id":"y1","best_bid":"0.48","best_ask":"0.52"}]}"#
                    .to_string(),
            ),
            // Tape 3: book frame for an unknown token -> counted, not indexed.
            (
                FeedSource::Clob,
                1_250,
                r#"{"market":"0xzz","price_changes":[{"asset_id":"zz","best_bid":"0.10","best_ask":"0.90"}]}"#
                    .to_string(),
            ),
            // Tape 4: exchange trade tick.
            (
                FeedSource::Bybit,
                1_300,
                r#"{"data":[{"p":"60000","T":1299}]}"#.to_string(),
            ),
            // Tape 5: exchange ack (valid, not a trade).
            (
                FeedSource::Bybit,
                1_310,
                r#"{"success":true,"op":"subscribe"}"#.to_string(),
            ),
            // Tape 6: malformed CLOB frame -> one decode error.
            (FeedSource::Clob, 1_320, "{not json".to_string()),
        ];
        db.insert_frame_batch(&frames, &[]).unwrap();

        let tape = load_and_decode(&db, &[market()]).unwrap();
        assert_eq!(tape.stats.frames_total, 6);
        assert_eq!(tape.stats.chainlink_skipped, 1);
        assert_eq!(tape.stats.clob_book_updates_indexed, 1);
        assert_eq!(tape.stats.clob_unknown_token_updates, 1);
        assert_eq!(tape.stats.clob_decode_errors, 1);
        assert_eq!(tape.stats.exchange_ticks, 1);
        assert_eq!(tape.stats.exchange_non_trade_frames, 1);
        assert_eq!(tape.stats.exchange_decode_errors, 0);

        assert_eq!(tape.events.len(), 1);
        let ev = &tape.events[0];
        assert_eq!(
            (
                ev.venue,
                ev.price,
                ev.observed_at_ms,
                ev.received_ms,
                ev.tape_id
            ),
            (ExchangeVenue::Bybit, dec!(60000), 1_299, 1_300, 4)
        );

        assert_eq!(tape.activation_by_condition.get("0xc1"), Some(&2));
        let (book, recv) = tape.books.book_at_tape("y1", 4).unwrap();
        assert_eq!(book.best_ask.map(|p| p.0), Some(dec!(0.52)));
        assert_eq!(recv, 1_200);
    }
}
