//! Per-frame per-token book index — the replay analogue of live book state.
//!
//! **Per-frame, not change-only** (issue #310, round-2 plan review): live
//! `on_book_update` wholesale-replaces the stored book *and its `received_ms`*
//! on every frame (`join.rs`), including unchanged top-of-book `price_change`s
//! (deeper-level moves) and reconnect re-snapshots — and
//! `feed_to_book_lag_ms = signal_received_ms − book_received_ms` is a persisted,
//! compared column. A change-only index would systematically diverge from live
//! on it. One entry per decoded [`BookUpdate`], sides stored **verbatim**
//! (`None` included — never filled from a previous entry).

use std::collections::HashMap;

use pe_core_types::Price;

use crate::types::BookUpdate;

/// Per-token cap on indexed entries. Canonical: `docs/_GLOSSARY.md`
/// `book_index_per_token_cap`. On overflow the token **stops indexing** — the
/// drop count is surfaced via [`BookIndex::dropped_after_cap`] and lookups past
/// the cap horizon return `None` (legs unscored, never silently wrong).
pub(super) const BOOK_INDEX_PER_TOKEN_CAP: usize = 1_000_000;

/// One indexed book frame: tape position, node-receive clock, verbatim sides.
#[derive(Debug, Clone, Copy, PartialEq)]
struct IndexedBook {
    tape_id: i64,
    received_ms: i64,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
}

#[derive(Debug, Default)]
struct TokenBooks {
    /// Entries in tape order (append-only; `tape_id` strictly increasing).
    entries: Vec<IndexedBook>,
    /// Set when the cap was hit; later frames for this token are dropped.
    capped: bool,
}

/// Index over every decoded CLOB book frame, keyed by token.
#[derive(Debug)]
pub(super) struct BookIndex {
    per_token: HashMap<String, TokenBooks>,
    per_token_cap: usize,
    dropped_after_cap: u64,
}

impl BookIndex {
    /// New index with the given per-token entry cap.
    pub(super) fn new(per_token_cap: usize) -> Self {
        Self {
            per_token: HashMap::new(),
            per_token_cap: per_token_cap.max(1),
            dropped_after_cap: 0,
        }
    }

    /// Record one decoded book frame for `token_id` at tape position `tape_id`,
    /// received at the node at `received_ms`. Sides are stored verbatim.
    pub(super) fn push(&mut self, tape_id: i64, received_ms: i64, update: &BookUpdate) {
        let token = self.per_token.entry(update.token_id.clone()).or_default();
        if token.capped {
            self.dropped_after_cap += 1;
            return;
        }
        token.entries.push(IndexedBook {
            tape_id,
            received_ms,
            best_bid: update.best_bid,
            best_ask: update.best_ask,
        });
        if token.entries.len() >= self.per_token_cap {
            token.capped = true;
        }
    }

    /// The latest book for `token_id` at tape position `tape_id` (entries with
    /// `entry.tape_id <= tape_id` — position, not time: ~1k frames/s makes
    /// same-millisecond ties routine), paired with its node-receive clock.
    /// `None` when no book frame precedes the position, or when the token
    /// stopped indexing at the cap and `tape_id` lies past the cap horizon.
    pub(super) fn book_at_tape(&self, token_id: &str, tape_id: i64) -> Option<(BookUpdate, i64)> {
        let token = self.per_token.get(token_id)?;
        let idx = token.entries.partition_point(|e| e.tape_id <= tape_id);
        if idx == 0 {
            return None;
        }
        let last = token.entries.get(idx - 1)?;
        if token.capped && idx == token.entries.len() && tape_id > last.tape_id {
            // Past the cap horizon: frames were dropped, the latest book is
            // unknown — unscored beats silently stale.
            return None;
        }
        Some((
            BookUpdate {
                token_id: token_id.to_string(),
                best_bid: last.best_bid,
                best_ask: last.best_ask,
                observed_at_ms: None,
            },
            last.received_ms,
        ))
    }

    /// Frames dropped after a token hit the cap (0 on healthy tapes).
    pub(super) fn dropped_after_cap(&self) -> u64 {
        self.dropped_after_cap
    }

    /// Number of tokens that hit the cap (0 on healthy tapes).
    pub(super) fn capped_tokens(&self) -> u64 {
        let n = self.per_token.values().filter(|t| t.capped).count();
        u64::try_from(n).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn update(token: &str, bid: Option<&str>, ask: Option<&str>) -> BookUpdate {
        BookUpdate {
            token_id: token.to_string(),
            best_bid: bid.map(|b| Price(b.parse().unwrap())),
            best_ask: ask.map(|a| Price(a.parse().unwrap())),
            observed_at_ms: None,
        }
    }

    #[test]
    fn lookup_resolves_latest_entry_at_or_before_tape_position() {
        let mut ix = BookIndex::new(BOOK_INDEX_PER_TOKEN_CAP);
        ix.push(5, 1_200, &update("y", Some("0.48"), Some("0.52")));
        ix.push(8, 1_300, &update("y", Some("0.48"), Some("0.52"))); // unchanged frame still re-stamps
        // Before any entry -> None.
        assert_eq!(ix.book_at_tape("y", 4), None);
        // At the first entry.
        let (b, recv) = ix.book_at_tape("y", 5).unwrap();
        assert_eq!((b.best_ask, recv), (Some(Price(dec!(0.52))), 1_200));
        // Between entries resolves the earlier one; after both, the later one —
        // the per-frame re-stamp is what reproduces live `feed_to_book_lag_ms`.
        assert_eq!(ix.book_at_tape("y", 7).unwrap().1, 1_200);
        assert_eq!(ix.book_at_tape("y", 9).unwrap().1, 1_300);
        // Unknown token -> None.
        assert_eq!(ix.book_at_tape("n", 9), None);
    }

    #[test]
    fn none_sides_are_stored_verbatim_never_filled() {
        let mut ix = BookIndex::new(BOOK_INDEX_PER_TOKEN_CAP);
        ix.push(1, 100, &update("y", Some("0.48"), Some("0.52")));
        ix.push(2, 200, &update("y", Some("0.49"), None)); // absent ask side
        let (b, recv) = ix.book_at_tape("y", 3).unwrap();
        assert_eq!(b.best_bid, Some(Price(dec!(0.49))));
        assert_eq!(b.best_ask, None, "never filled from the previous entry");
        assert_eq!(recv, 200);
    }

    #[test]
    fn cap_stops_indexing_and_lookups_past_horizon_are_none() {
        let mut ix = BookIndex::new(2);
        ix.push(1, 100, &update("y", Some("0.40"), Some("0.60")));
        ix.push(2, 200, &update("y", Some("0.41"), Some("0.59")));
        ix.push(3, 300, &update("y", Some("0.42"), Some("0.58"))); // dropped
        assert_eq!(ix.dropped_after_cap(), 1);
        assert_eq!(ix.capped_tokens(), 1);
        // Within the indexed horizon: exact.
        assert_eq!(ix.book_at_tape("y", 2).unwrap().1, 200);
        // Past the horizon: unknown, not stale.
        assert_eq!(ix.book_at_tape("y", 3), None);
    }
}
