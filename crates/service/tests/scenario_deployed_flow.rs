//! #588 F2: runtime admission, real stream/poller copying, expiry, restart, and sealed qualification.
#![cfg(feature = "scenario")]

#[path = "support/golden.rs"]
mod golden;

/// PASS: absent newcomer history is validated and published before fresh/corrected stream fills;
/// a delayed portfolio read expires and releases its staged target; restart and sealed CLI replay
/// preserve exact receipts, economics, membership and history and return Pass.
#[tokio::test]
async fn deployed_flow_replays_exactly_and_qualifies() {
    golden::deployed_flow_replays_exactly_and_qualifies().await;
}
