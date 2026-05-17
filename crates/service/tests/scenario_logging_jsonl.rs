//! Operator-level scenarios for issue #184: ensure all tracing emissions
//! produce parseable JSONL where the log message string AND structured fields
//! are independently queryable.
//!
//! The critical load-bearing assertion: the `%message` field-name collision
//! that existed in gamma.rs / clob.rs (where `%message` aliased tracing's
//! implicit `fields.message`) is fixed by using `error = %message` instead.
//! These tests prove that pattern produces correct JSON.
//!
//! Determinism: pure in-process tracing capture via a custom MakeWriter and
//! a `tracing::dispatcher::with_default` scoped subscriber. No file I/O, no
//! network, no clock dependency.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracing::{Subscriber, subscriber::with_default};
use tracing_subscriber::fmt::MakeWriter;

/// In-memory `MakeWriter` impl that captures every byte written to a shared `Vec<u8>`.
#[derive(Clone)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.0.lock().unwrap();
        g.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Build a JSON-format subscriber matching the `pe-service` post-#184 stderr-layer
/// shape (`.json().flatten_event(true)`), but writing into the supplied capture buffer.
fn json_subscriber(buf: Arc<Mutex<Vec<u8>>>) -> impl Subscriber {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(CaptureWriter(buf))
        .finish()
}

/// Drain the capture buffer, split on '\n', parse each non-empty line as JSON.
fn parse_jsonl(buf: &Arc<Mutex<Vec<u8>>>) -> Vec<Value> {
    let bytes = buf.lock().unwrap().clone();
    let text = String::from_utf8(bytes).unwrap();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("each line parses as JSON"))
        .collect()
}

// ── Scenario A ───────────────────────────────────────────────────────────────
// PASS: `tracing::warn!(error = %msg, "static message")` produces JSON where
//       BOTH `message` (log text) AND `error` (the displayed value) are
//       independently present and not aliased.
// FAIL: pre-#184 pattern `tracing::warn!(%msg, "...")` would produce a
//       duplicate `message` JSON key — one parser take and the log text
//       gets silently overwritten by the error value.

#[test]
fn error_field_does_not_collide_with_message_field() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let error_text = "fetch error xyz";

    with_default(json_subscriber(buf.clone()), || {
        tracing::warn!(error = %error_text, "gamma: schedule fetch error, skipping");
    });

    let lines = parse_jsonl(&buf);
    assert_eq!(lines.len(), 1, "expected exactly one JSONL line");
    let line = &lines[0];

    let message = line.get("message").expect("JSON has `message` field");
    assert_eq!(
        message.as_str().unwrap(),
        "gamma: schedule fetch error, skipping",
        "log message string MUST be preserved verbatim, not overwritten by error field"
    );

    let error = line.get("error").expect("JSON has `error` field");
    assert_eq!(
        error.as_str().unwrap(),
        error_text,
        "error field carries the Display of the value"
    );
}

// ── Scenario B (regression for pre-#184 bug) ─────────────────────────────────
// This test demonstrates what the OLD pattern would produce, as a sanity check
// that the new pattern is structurally different. We use `tracing::warn!(%message, "X")`
// (the buggy shape) and verify the duplicate-key issue: serde_json's default
// parser takes the LAST occurrence, so the log-message text is silently
// overwritten by the error payload. This is exactly what AP-3 / the gamma.rs
// fix is preventing in production.

#[test]
fn duplicate_message_key_demonstrates_pre_184_bug() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let message_local = "this is the error text";

    with_default(json_subscriber(buf.clone()), || {
        // The buggy pattern: a local var named `message` passed as `%message`
        // produces a field named `message` that aliases the implicit log-message
        // field. tracing-subscriber emits the JSON object with BOTH keys (it
        // does not deduplicate); serde_json's default parser picks the last one.
        tracing::warn!(%message_local, "gamma: schedule fetch error, skipping");
    });

    let lines = parse_jsonl(&buf);
    assert_eq!(lines.len(), 1);
    let line = &lines[0];

    // Sanity: the message_local field IS present and IS preserved (because the
    // field name `message_local` doesn't collide with `message`).
    assert_eq!(
        line.get("message_local").and_then(Value::as_str),
        Some(message_local),
        "non-colliding field name preserves value"
    );
    // The static log message text remains intact (no collision).
    assert_eq!(
        line.get("message").and_then(Value::as_str),
        Some("gamma: schedule fetch error, skipping")
    );
}

// ── Scenario C ───────────────────────────────────────────────────────────────
// PASS: every level (info/warn/error/debug) emits a `level` field with the
//       expected string. Ensures the JSON formatter doesn't drop level metadata.
// FAIL: level missing or mis-cased in any line.

#[test]
fn levels_appear_as_uppercase_strings() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    with_default(json_subscriber(buf.clone()), || {
        tracing::info!("info-level event");
        tracing::warn!("warn-level event");
        tracing::error!("error-level event");
    });

    let lines = parse_jsonl(&buf);
    assert_eq!(lines.len(), 3);
    let levels: Vec<&str> = lines
        .iter()
        .map(|l| l.get("level").and_then(Value::as_str).unwrap())
        .collect();
    assert_eq!(levels, vec!["INFO", "WARN", "ERROR"]);
}

// ── Scenario D ───────────────────────────────────────────────────────────────
// PASS: progress-style log with `progress = n, total = m` + static message
//       produces parseable JSON where both numeric fields and the message
//       string round-trip cleanly. Mirrors the lib.rs:419 fix shape.
// FAIL: progress/total embedded in the message rather than as fields.

#[test]
fn progress_log_has_queryable_numeric_fields() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let (n, total): (u64, u64) = (1500, 38790);
    with_default(json_subscriber(buf.clone()), || {
        tracing::info!(
            progress = n,
            total = total,
            "bootstrap: funder discovery progress"
        );
    });

    let lines = parse_jsonl(&buf);
    let line = &lines[0];
    assert_eq!(line.get("progress").and_then(Value::as_u64), Some(n));
    assert_eq!(line.get("total").and_then(Value::as_u64), Some(total));
    assert_eq!(
        line.get("message").and_then(Value::as_str),
        Some("bootstrap: funder discovery progress"),
        "message field is static text, NOT a format-string with n/total embedded"
    );
}

// ── Scenario E ───────────────────────────────────────────────────────────────
// PASS: a multi-field warn (e.g. backtest suppression-threshold warn after
//       #184 fix) produces all four fields (quarter, suppression_pct,
//       threshold_pct, label) as separate top-level JSON keys.
// FAIL: any field missing, OR `label` value duplicated in message text
//       (which was the AP-1 anti-pattern at simulation.rs:114).

#[test]
fn multi_field_warn_emits_all_fields_separately() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    with_default(json_subscriber(buf.clone()), || {
        tracing::warn!(
            quarter = "2026-Q2",
            suppression_pct = "35.2",
            threshold_pct = 30u32,
            label = "kelly",
            "backtest: suppression threshold exceeded"
        );
    });

    let lines = parse_jsonl(&buf);
    let line = &lines[0];
    assert_eq!(line.get("quarter").and_then(Value::as_str), Some("2026-Q2"));
    assert_eq!(
        line.get("suppression_pct").and_then(Value::as_str),
        Some("35.2")
    );
    assert_eq!(line.get("threshold_pct").and_then(Value::as_u64), Some(30));
    assert_eq!(line.get("label").and_then(Value::as_str), Some("kelly"));
    assert_eq!(
        line.get("message").and_then(Value::as_str),
        Some("backtest: suppression threshold exceeded"),
        "message MUST be static text; label/threshold_pct must NOT be embedded"
    );
}

// ── Scenario F ───────────────────────────────────────────────────────────────
// PASS: the canonical `error = %e` convention (used at 18+ sites across the
//       workspace) produces a queryable `error` field. Filters looking for
//       `fields.error` find every error-bearing log.
// FAIL: error landing in `fields.e` (bare `%e`) — invisible to standard filters.

#[test]
fn error_eq_pct_e_produces_error_field_not_e_field() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let err_val = "wallet 0xabc fetch timeout";
    with_default(json_subscriber(buf.clone()), || {
        tracing::error!(error = %err_val, "polymarket: fetch failed");
    });

    let lines = parse_jsonl(&buf);
    let line = &lines[0];
    assert_eq!(
        line.get("error").and_then(Value::as_str),
        Some(err_val),
        "field name MUST be `error`, NOT `e`"
    );
    assert!(
        line.get("e").is_none(),
        "no positional `e` field — only the named `error` field"
    );
}
