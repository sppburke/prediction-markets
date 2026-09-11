//! I16 golden future-stream source and economic replay scenario.

#![cfg(feature = "scenario")]

#[path = "support/golden.rs"]
mod golden;

#[tokio::test]
async fn golden_source_stream_replays_exact_economic_core() {
    golden::golden_source_stream_replays_exact_economic_core().await;
}
