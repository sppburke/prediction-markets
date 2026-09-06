C3/RC12: DONE crates/service/src/orchestrator_control.rs:34
C1: NOT DONE Prepared/authority/local/Final and EconomicPrepared::compose exist at crates/service/src/orchestrator.rs:2755, but Kelly still composes the preliminary Dollar plan because the advertised LEAF S4 allocator API is absent
C2: DONE crates/service/src/orchestrator.rs:501
RC1: DONE scripts/supabase_paper_state_schema.sql:91
RC2: DONE crates/service/src/main.rs:228
RC3: DONE crates/paper-state/src/lib.rs:3135
RC4: DONE scripts/supabase_paper_state_schema.sql:252
RC8: DONE crates/service/src/orchestrator.rs:448
RC7: DONE crates/service/src/main.rs:1846 and crates/service/src/main.rs:1982
RC10: DONE crates/service/src/main.rs:286
RC9: DONE crates/service/src/main.rs:486
RC5: DONE crates/service/src/paper_api.rs:61
RC11: DONE crates/service/src/paper_api.rs:107
RC6/C9: NOT DONE parallel legacy integer and Financial* paper-state families remain at crates/paper-state/src/lib.rs:478
RC13/C7/C11: NOT DONE dispatcher and executor files are deleted and CLOB naming is live at crates/service/src/config.rs:127, but financial_log_paths remains an Option era switch at crates/service/src/orchestrator.rs:343 and public legacy fill DTOs remain
D1: DONE crates/service/src/risk_inputs.rs:353
D2: DONE crates/service/src/main.rs:900
D3: DONE crates/service/src/orchestrator.rs:2696
D4: DONE crates/service/src/trade_poller.rs:62
D5: DONE crates/service/src/risk_inputs.rs:86
D6: NOT DONE partition/audit helpers exist at crates/service/src/config_poller.rs:45 and crates/service/src/risk_inputs.rs:291, but boot/poll do not apply them through synchronized release controls
D7: DONE crates/risk-engine/src/inputs.rs:65
D8: NOT DONE active builders are real, but the fabricated pre-Start builder remains at crates/service/src/orchestrator.rs:3545
D9: DONE crates/service/src/orchestrator.rs:1026
RD1: DONE crates/risk-engine/src/inputs.rs:44
RD2: DONE crates/service/src/risk_inputs.rs:353
RD3: NOT DONE copied timestamps are gone, but duplicate decision-context page_occurrences/observed_source_receipts remain at crates/service/src/bucket_commit.rs:47
RD4: DONE crates/service/src/trade_poller.rs:1090
RD5: DONE crates/service/src/risk_inputs.rs:823
F2: NOT DONE orchestrator publication variant carries entries, but structural maintenance/capacity still call LiveWatchlist::replace directly at crates/service/src/watchlist_maintenance.rs:422
F3: NOT DONE SealCheck still returns the temporary fail-closed error at crates/service/src/orchestrator.rs:1060 and config polling does not send it
F9: NOT DONE no automatic 30-day/90-close seal producer is wired
C4: DONE crates/service/src/orchestrator_control.rs:68
C5: NOT DONE DailyBoundary synchronizes PortfolioMark at crates/service/src/orchestrator.rs:898, but SealCheck does not append QualificationSealed
C6: DONE crates/service/src/main.rs:315
B3: DONE crates/service/src/main.rs:315
B5: DONE paper delegates budget inversion to plan_sized_buy at crates/service/src/orchestrator.rs:1675
B6: DONE scripts/paper_reset/activate_financial_era.sh:254
B8: DONE current Financial15 economics exclude retired keys at crates/service/src/runtime_config.rs:342; Legacy17 decode names intentionally remain
RB2-paper: NOT DONE paper calls plan_sized_buy at crates/service/src/orchestrator.rs:1675, but the harvested LEAF caller-supplied Kelly allocator shape is absent from this tree
RB8-paper: NOT DONE active paths are retired, but public legacy fill surfaces and the pre-Start fabricated risk builder remain
C8: NOT DONE golden_stream_v1 and the requested crash/runtime matrices are absent
C10: DONE crates/service/src/paper_api.rs:68 and site/lib/types.ts:45
C12: NOT DONE pg parity test was added at crates/service/tests/pg_parity.rs:1762 but PE_TEST_PG_URL is unavailable for execution
RC14: NOT DONE changed-predecessor coverage exists at crates/paper-state/src/lib.rs:6656; the HTTP, concurrency, terminal-evidence, and full resolution matrices are absent
RB7-paper: NOT DONE legacy journal/venue tests are outside PAPER ownership; no merged cross-path planner/signer/paper/live scenario exists
ROUND 2 2026-09-06T00:17Z — S5: DONE `RiskAudit` carries sorted/deduplicated `price_receipts` and `evaluated_at_unix_ms`; paper derives both from the strict mid read/evaluation clock and `core_hash` binding is tested in crates/execution-core/src/economic.rs. LIVE-owned constructors still need the same fields.
ROUND 2 2026-09-06T00:17Z — C1/RB2-paper: DONE in PAPERA scope: `Orchestrator::plan_impact_gate` supplies the LEAF Kelly allocator backed by `pe_kelly_sizer::size_contracts`; the final plan feeds proposal risk, one `EconomicPrepared::compose`, and `evaluate_at_price(economic.sizing.all_in_price, ...)`. Added the justified direct `pe-kelly-sizer` service dependency; Cargo.lock update pending metadata/build.
ROUND 2 2026-09-06T00:27Z — D8/RB8-paper: DONE for the active path. Removed the fabricated pre-Start `pre_start_risk_snapshot`; a fill without verified admission/book/continuation evidence fails closed before risk composition, and the unreachable schema-one fill append after the active Prepared/Final return was deleted. The schema-one reader remains for recovery compatibility.
ROUND 2 2026-09-06T00:31Z — D6: DONE. Boot partitions the incident row before economic config parsing, then (after replay and before opening the producer gate) reconstructs active halts and the latest completed-hour latency value, calls `audited_halt_release`, and awaits the orchestrator's synchronized `RiskHaltChange::Released` acknowledgement. Polling uses the same `RiskHaltReleaseHandle` before publishing a last-good config; the appended release makes the hash consume-once.
ROUND 2 2026-09-06T00:31Z — RD3: DONE on inspection of the merged implementation. Receipt-bearing page occurrences and websocket receipts are transient producer inputs and persist only at the top level of `DecisionContinuationV3`; `decision_inputs_json` contains no nested copies, and source-envelope `received_at` remains the latency clock owner.
ROUND 2 2026-09-06T00:24Z — RC6/C9: PARTIAL. The restart held-position guard now selects `financial_snapshot` and compares exact `ShareAmount`s whenever Start exists, so a fractional position cannot disappear. The parallel public legacy integer and `Financial*` DTO families remain and require a repository-wide consumer migration that did not fit this interruption slice.
ROUND 2 2026-09-06T00:29Z — verification/summary: DONE. Required gates were attempted in order and recorded in `INTEGRATION_SUMMARY_PAPERA2.md`. Formatting, metadata, shell/SQL guards, financial-era harness, diff check, and all 52 risk-engine tests pass. Workspace Rust and PostgreSQL scenario gates stop at LIVE-owned unadapted S4/S5 constructors/call sites before tests or database connection.
