# 34 — Paper-P&L generation reset runbook

**Purpose.** Start a new paper-P&L era without destroying the prior one. Issue #557 makes this a
generation activation: build an explicitly empty schema-v1 seed, migrate that seed in place, archive
the five Supabase paper tables under one `activation_id`, and switch the service to permanent
generation-qualified paths. The activation and rollback commands are in
[`35-PE-SERVICE-DEPLOY-RUNBOOK.md`](35-PE-SERVICE-DEPLOY-RUNBOOK.md).

## Authority and invariants

Paper state spans Supabase, the local SQLite main, `paper.log`, `live_journal.log`, and
`source_events.log`. A generation also binds the captured legacy history input. The migration records
canonical paths and verified log tails, so none of those bound files may be moved after preparation.
The new generation therefore lives permanently below
`/home/sean/prediction-markets/gen/<generation>/`.

The historical instruction to stop when the production main is schema v2 is superseded. **Reset the
seed, then migrate:** copy the production schema-v1 main with SQLite's online backup command, empty the
exact twelve-table schema-v1 allowlist, and let the reviewed binary migrate that empty copy. Never
manufacture a fresh schema-v2 database and never delete migration records from an installed main.

The seed owner is [`seed_v1_empty.sh`](../scripts/paper_reset/seed_v1_empty.sh). It is dry-run by
default. With `--execute` it requires:

- exactly the schema-v1 `a5f3a8f` user tables and no others;
- zero rows in every copied table after reset;
- `PRAGMA user_version = 1` and `PRAGMA integrity_check = ok`;
- a WAL checkpoint and initially empty paper, live-journal, and source logs (an empty event log is the
  5-byte header `EDGE\x01`, never a zero-byte file: the migration verifies the header before any writer runs); and
- the captured legacy-history file with both BLAKE3 and SHA-256 recorded.

The activation driver's `prepared` state then runs the staged binary with
`--exit-after-anchors`. Its postconditions are schema v2, zero fills and settlements, no authoritative
watermark, no sealed v1 cursor rows, delivery cursors equal to bracket cursors, empty paper/live logs,
and a non-empty migrated source log.

## Supabase archive and reset

[`archive_paper_state.sql`](../scripts/paper_reset/archive_paper_state.sql) owns the #474
archive-before-delete transaction. It receives `activation_id` and the fresh bankroll through psql
variables, takes the existing exclusive locks, adds `activation_id text` to all five archive tables if
needed, copies every live row with that id, verifies copied counts, clears the live tables, verifies
zero live counts, and inserts `paper_bankroll (id, bankroll_str)`. The pre-reset bankroll singleton is
the durable stamp that lets a resumed activation prove that a database commit occurred before its
manifest rename.

A rerun with the same `activation_id` validates the already-fresh live book and does not archive or
delete again. All five stamped counts must exactly equal the manifest's recorded pre-reset live census
before either the initial transition or crash reconciliation can adopt `reset`. Per-table counts for
that id are printed and recorded in the fixed activation manifest.
Do not run the SQL independently during a generation cutover; the locked driver coordinates both
stores.

For a read-only preview of the SQL owner:

```bash
bash scripts/paper_reset/reset_paper_state.sh \
  --activation-id <activation-id> --bankroll <fresh-bankroll>
```

## Activation sequence

The exact operational command, rehearsal gate, durable states, and crash recovery are documented in
docs/35. At a high level:

1. Build the empty v1 seed at permanent generation paths and migrate it during warm prepare while the
   old service continues trading.
2. Complete the mandatory isolated rehearsal and record AC10 evidence.
3. Freeze the final ranking batch and prove the canonical due-wallet rule.
4. Disable and stop `pe-service`, preserving every pre-T0 file and installed artifact by hash.
5. Archive/reset Supabase with the manifest's `activation_id`.
6. Adopt the generation config, complete environment, and binary; start once; verify the fresh book,
   bindings, producer/task health, projection, site, and materialized view.

The fixed `/home/sean/pe-activation.json` is the sole activation authority. Do not infer state from
terminal output or manually advance it.

## Verification

- The manifest is `verified` and names the intended generation, reviewed revision, artifact hashes,
  frozen ranking batch, recorded systemd invocation, and signed-in-site operator confirmation.
- `paper_fills`, `paper_positions`, `settled_markets`, and `fill_market_snapshots` are empty;
  `paper_bankroll` contains the fresh singleton.
- Local fills and settlements are zero, the generation bindings match every effective env-over-TOML
  path, and boot replayed no historical paper event.
- The boot used the recorded ranking batch and performed no walk, or exactly the owner-approved
  due-wallet subset recorded in the manifest.
- The watchlist projection has succeeded, invocation-fresh status was observed,
  `wallet_live_stats_mv` was refreshed, and the recorded operator confirmation says the signed-in site
  displays the fresh era.
- Forge's prior loop flag, unit enablement, and unit activity were restored independently.

## Rollback

Use `scripts/deploy/rollback_generation.sh --activation-id <id>`; do not hand-copy rows. The driver
records `rolling_back`, validates every archived config/environment/binary source against its manifest
hash before changing the database or installed files, disables and stops any non-adoptable running
service before touching the database, and restores exactly the five archive-table row sets stamped with that id using explicit
column lists in one transaction. It verifies the restored counts, refreshes the materialized view,
restores the hash-bound old config/environment/binary, starts the old generation, and records
`rolled_back`.

Rollback is resumable. A running exact old generation is adopted only when the archived artifacts and
restored database independently match. A forward rerun of the rolled-back activation id is refused.
Archive tables and pre-T0 copies remain retained; never drop them as cleanup.

## Rebuild and migration recovery notes

`--rebuild-state` remains refused in authoritative mode: frame-only reconstruction cannot know which
authority writes were refused or ambiguous. Restart the compatible service so boot converges from the
system of record.

The v1-to-v2 migration phases remain machine-owned: `boundary_recorded`,
`version_two_inputs_appending`, `side_state_built`, `activation_tails_recorded`, and `installed`. Before
any v2 append or active-state commit, the migration's dedicated `--rollback-paper-v1` path can preserve
the failed side and restore its immutable v1 input. After either boundary it refuses; resume with the
same v2-compatible binary. Do not move bound logs or edit the migration record.
