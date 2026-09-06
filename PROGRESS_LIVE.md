S1: DONE crates/execution-core/src/economic.rs:153
B1: DONE crates/execution-core/src/live_executor.rs:964
B2-live: DONE crates/service/src/live_fanout.rs:190
RE1: DONE crates/execution-core/src/live_executor.rs:412
RB2-live: DONE crates/service/src/live_fanout.rs:1155
E1: DONE crates/service/src/main.rs:1059
E2: DONE crates/service/src/live_fanout.rs:813
RE2: DONE crates/service/src/live_fanout.rs:1762
RE3: DONE crates/service/src/live_fanout.rs:2053
RE4: DONE crates/venue-polymarket/src/receipt.rs:154
E3: DONE crates/service/src/live_fanout.rs:4360
E4: DONE crates/service/src/live_fanout.rs:4466
RE5/E5: DONE crates/service/src/live_fanout.rs:2098
E6: DONE crates/service/src/live_fanout.rs:3705
RE6: DONE crates/source-polymarket-public/src/reconciliation.rs:536
RE8: DONE crates/service/src/live_fanout.rs:481
RE9: DONE crates/service/src/live_venue_adapter.rs:483
B7: DONE crates/execution-core/src/live_journal.rs:181
RB8-live: DONE crates/service/src/live_fanout.rs:1532
RB7-live: DONE crates/service/src/live_venue_adapter.rs:1313
RE10: DONE crates/execution-core/src/live_journal.rs:1047
ROUND2-open-order-inventory: DONE crates/execution-core/src/live_journal.rs:879
ROUND2-S5-live-risk-audit: DONE crates/service/src/live_fanout.rs:1459
ROUND2-strategy-doc-lint: REPORTED outside LIVE ownership crates/strategy-winner-follow/src/lib.rs
ROUND2-format: DONE cargo fmt --all and cargo fmt --all --check
ROUND2-live-merge-fixes: DONE allocator-based Kelly adapter crates/service/src/live_fanout.rs:1398; pe-service now reaches only non-owned orchestrator compile errors
ROUND2-summary: DONE INTEGRATION_SUMMARY_LIVE2.md
L3-4: DONE raw Polygon evidence and source-log resolution evidence are revalidated in the strict reducer; different finalized-head duplicates now fail and empty preimages are rejected (crates/service/src/live_fanout.rs).
L3-5: DONE verified live-journal replay now owns account/open-order recovery inventory, including retained current Prepared audits and matched transaction hashes; Polygon finality no longer depends on a SQLite target row (crates/execution-core/src/live_journal.rs, crates/service/src/live_fanout.rs).
L3-3: DONE composer validates condition/binary outcome/token/buy identity against admission, and live validation binds the same identity to the signed order (crates/execution-core/src/economic.rs, crates/execution-core/src/live_executor.rs).
L3-7: DONE composer cross-checks venue/audit derived shares, spend, and VWAP and rejects signed minimum-shares times limit below principal (crates/execution-core/src/economic.rs).
L3-1: DONE each catch-up Daily mark is composed and replay-validated from only live-journal/source envelopes received before that cutoff; current cash/inventory is no longer reused for historical dates (crates/service/src/live_fanout.rs).
L3-6: DONE one receipt-recording historical-price adapter uses the shared client and `.with_max_retries(0)`; live marks consume it (crates/service/src/mark_prices.rs).
L3-2: DONE each live account synchronizes its four halt-cause edges through `RiskHaltChange`, preserves the absolute-loss latch, awaits replay, and blocks entry on any paper/live owner in the global active set; repeated/released edges are consumed once by replay state (crates/service/src/live_fanout.rs, crates/service/src/main.rs).
L3-summary: DONE INTEGRATION_SUMMARY_LIVE3.md
