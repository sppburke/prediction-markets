# 35 — pe-service binary deploy runbook (VPS)

> This runbook deploys ordinary `pe-service`, including the #508 per-account live path that ships
> dark over the shared paper book. It does not deploy or configure the isolated Polymarket V2
> canary; see
> [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

**Purpose.** The repeatable procedure for building the `pe-service` release binary and
deploying it to the VPS with one restart and no stop-before-swap window.

## Historical issue #557 generation activation

The procedure in this section applies only when a deployment creates a new paper-state generation;
issue #545 keeps the verified generation and uses the separate route below. The AC10 rehearsal is a
**mandatory pre-deploy gate**. Do not enter the generation activation driver's `guarded` state
without its recorded PASS evidence.

### Mandatory isolated rehearsal

Run the rehearsal from the final reviewed head with the exact staged binary and a VPS copy of
production state. Rehearsal isolation means all local durable paths and the bind address resolve to a
dedicated directory/port, the complete staged environment contains no service-role credential, and
the Polymarket venue remains real. It is not a special application mode and it is not permission to
write production Supabase.

1. Copy the stopped/checkpointed production SQLite main without opening it read-write:

   ```bash
   mkdir -p /home/sean/pe-rehearsal-557
   sqlite3 -readonly /home/sean/prediction-markets/paper_state.db \
     ".backup '/home/sean/pe-rehearsal-557/paper_state.db'"
   ```

   Copy the three logs and legacy-history input into that directory without changing the production
   files. Record source/destination SHA-256 values.
2. Create a **complete** rehearsal environment. Set all of these explicitly; no inherited production
   path or bind is permitted:

   ```text
   PE_BIND=<dedicated-loopback-bind>
   PE_EVENT_LOG_PATH=/home/sean/pe-rehearsal-557/paper.log
   PE_SOURCE_EVENT_LOG_PATH=/home/sean/pe-rehearsal-557/source_events.log
   PE_JSONL_LOG_PATH=/home/sean/pe-rehearsal-557/paper.jsonl
   PE_STATUS_PATH=/home/sean/pe-rehearsal-557/status.json
   PE_PAPER_STATE_DB_PATH=/home/sean/pe-rehearsal-557/paper_state.db
   PE_LEGACY_WALLET_HISTORY_PATH=/home/sean/pe-rehearsal-557/wallet_market_history.json
   ```

   Preserve every other required production environment entry, except put the Supabase publishable
   (`anon`) key in `PE_SUPABASE_SECRET_KEY`. The service-role key must not be present in the rehearsal
   environment or process.
3. Run `scripts/deploy/rehearsal_preflight.sh <rehearsal-env>`. Its database matrix must prove:

   - `anon` cannot execute any of the six service write RPCs while `service_role` can;
   - `anon` has no DML grants on the three live tables;
   - the privileged database connection observes one canonical C-ordered `accounts` census in which
     account IDs are unique and every requested/effective mode is `off`; and
   - each other reachable sink has RLS enabled, is not anon-owned, anon cannot bypass RLS, and no
     anon/PUBLIC/authenticated write policy exists.

   Its representative HTTP canaries must all return 401/403, including
   `service_watchlist_replace_v1` with the exact argument shape, and the marker-row query must remain
   zero.
4. Print and independently compare env-over-TOML effective bind/state/log/history paths before start.
   Run the staged binary against the rehearsal config and real venue. Let producers run for at least
   `ANCHOR_REFRESH_SECS`; use the canonical value and rationale in `docs/_GLOSSARY.md` rather than
   copying it here.
5. AC10 passes only when its acceptance bounds are met for reader continuity and drops, fence
   inspection, a full poll/refresh round, refused projection attempts, ERROR count, VmHWM, and the full
   D11 convergence record (duration, wallets/minute, errors/deferred, oldest final anchor age, and final
   due-wallet count) at the concurrency being shipped. Use `FIXFWD_SPEC.md` AC10 as the checklist and
   record every value; do not restate its numeric thresholds in this runbook. A failed or incomplete item
   is a deployment stop, not a waiver.

Archive the exact config, complete environment with secrets redacted, staged binary hashes, privilege
matrix/canary output, status snapshots, logs, and measurements with the issue evidence.

### Warm prepare and cutover

Prerequisites on the VPS, checked by the driver before it touches anything: `python3 realpath readlink
sha256sum b3sum sqlite3 psql flock systemctl ss stat od` on `PATH` (`b3sum` is a user-local
`cargo install b3sum`; run the driver with `$HOME/.cargo/bin` on `PATH`), `SUPABASE_DB_URL` exported
in the driver's environment (delivered over stdin to the launcher, never on a command line or in a file
on the host), and the rehearsal environment template carrying the publishable key in BOTH
`PE_SUPABASE_ANON_KEY` and `PE_SUPABASE_SECRET_KEY`. The version-one seed's three fresh logs are the
5-byte event-log header `EDGE\x01`, never zero-byte files: the migration verifies each header with a
reader before any writer opens the file.

The driver holds `/home/sean/.pe-deploy.lock` for its whole run and writes only the fixed
`/home/sean/pe-activation.json` manifest as activation authority. Its state order is
`seed → prepared → prechecked → guarded → archived → reset → switched → started → verified`, with
`refusing` as the durable transient state for a post-start refusal. Use a unique `activation_id`; a
different id is refused while the manifest is non-terminal.
When creating a new `seed` manifest, the driver first queries all five archive tables and refuses an
id that already stamps any row, including an id from an earlier terminal activation. Existing-manifest
resumes do not repeat that new-id check. When a prior terminal manifest records reset metadata, its
durable fact also makes zero or partial archive-column presence a corruption during this new-id check;
the driver refuses before seed creation and leaves the terminal manifest unchanged.
The lock is a provisioned root-owned mode-`0644` file. The driver only opens it read-only and refuses
when it is absent; it never creates or touches the production lock.

Stage the exact release binary, the complete service and rehearsal environment files, and the full
`smoke-test/service.toml` from the reviewed merge commit. The driver changes only the bind and
generation path owners in that full TOML. Preview first:

```bash
SUPABASE_DB_URL=<session-pooler-url> scripts/deploy/activate_generation.sh --dry-run \
  --activation-id <id> \
  --generation-dir /home/sean/prediction-markets/gen/<generation> \
  --source-v1-main <production-v1-main-copy> \
  --legacy-history <captured-legacy-history> \
  --staged-binary <reviewed-pe-service> \
  --config-template <merge-commit-service.toml> \
  --rehearsal-env <complete-rehearsal-env> \
  --service-env <complete-production-env> \
  --merge-commit <reviewed-40-hex> \
  --bind <production-bind> --rehearsal-bind <dedicated-loopback-bind> \
  --bankroll <fresh-bankroll>
```

Before starting the driver, pause the ranking loop from an operator host that can reach Forge and
verify the recorded and live state:

```bash
scp scripts/deploy/forge_pause.sh forge:/tmp/forge_pause.sh
ssh forge 'bash /tmp/forge_pause.sh pause'
ssh forge 'bash /tmp/forge_pause.sh status'
```

The activation driver runs on the VPS, which cannot resolve or reach Forge. The Forge pause is
therefore an operator invariant; the driver verifies its consequence by requiring the ranking batch
to remain frozen. Run the same driver command without `--dry-run`. While the old service still trades,
`seed` creates the empty schema-v1 generation and `prepared` migrates it with
`--exit-after-anchors`. If a crash leaves a version-two main before the `prepared` manifest rename, the
driver re-enters that command so the binary completes or verifies the machine-owned `installed` phase
and activation-tail bindings; table counts alone never adopt it. At `prechecked`, the driver records the
latest ranking batch and evaluates the canonical due-wallet rule from `docs/_GLOSSARY.md`. A nonzero
result stops. Only an explicit owner decision may be supplied as the exact
`--approve-due-subset <count>`; it is durable in the manifest.
Immediately before `guarded` disables or stops the service at T0, the driver re-reads the latest batch
and refuses with the service untouched if it differs from the `prechecked` batch.

Before T0, old state paths are resolved exclusively from the installed `.env` and service TOML, whose
hashes are bound in the manifest. The driver never parses `ExecStart`, `EnvironmentFile`, or other unit
text. Instead it proves ownership from the running `MainPID`: `/proc/<pid>/exe` matches the installed
binary; `/proc/<pid>/cwd` equals the service root and owns normalization of the relative config argv; argv
is exactly `<installed-binary> <installed-config>`; and every variable the installed environment file
defines under the shared strict data parser is present with an equal value. Any
process variable not defined by that file must be one of exactly `CREDENTIALS_DIRECTORY`, `HOME`,
`INVOCATION_ID`, `JOURNAL_STREAM`, `LANG`, `LOGNAME`, `MEMORY_PRESSURE_WATCH`,
`MEMORY_PRESSURE_WRITE`, `PATH`, `SHELL`, `SYSTEMD_EXEC_PID`, `USER`, or the wrapper-owned
`PWD`, `SHLVL`, `OLDPWD`, or `_`. `CREDENTIALS_DIRECTORY` must equal
`/run/credentials/pe-service.service`; every other extra is refused. `LD_PRELOAD` and
`LD_LIBRARY_PATH`, and `LD_AUDIT` are refused even when the installed environment file defines the
same value.
Environment files are data, never shell. Write settings as plain `NAME=value` physical lines. Blank
lines and lines whose first non-whitespace character is `#` or `;` are ignored, but a backslash
anywhere in a physical line is refused. Assignment names have no leading whitespace, `=` has no
adjacent whitespace, and `export` is not accepted. Values are either unquoted with no leading or
trailing whitespace and no quote, or wholly single/double quoted with no backslash or matching quote
inside. There is no expansion or continuation; every rejected physical line reports its line number.
The wrapper-owned `PWD`, `SHLVL`,
`OLDPWD`, and `_` names are stripped from the expected set because they do not affect the binary;
`/proc/<pid>/cwd` proves the working directory separately.
One systemd snapshot supplies `ActiveState`, `MainPID`, `InvocationID`, and `ActiveEnterTimestamp` before
the `/proc` proof, and an identical second snapshot must follow it. The loaded manager state must also
report `NeedDaemonReload=no`. Staged templates are not an old-state authority. Immediately before each
rehearsal preflight and execution, the driver rechecks the staged rehearsal environment, config, and
binary against their manifest hashes. Staging runs under a restrictive umask, and secret-bearing
environment files are mode `0600` from their first open. `guarded` starts T0 by disabling and stopping
`pe-service`.
`archived` copies every pre-T0 artifact without deleting it. `reset` applies the activation-stamped
single Supabase transaction and requires its five stamped counts to equal the recorded pre-reset census.
The five archive tables may lack their `activation_id` columns only before the manifest carries reset
metadata. Once `state >= reset` or `archive_counts` is durable, a zero- or partial-column result is
corruption and rollback stops after making the service inactive and disabled.
`switched`
hash-verifies and atomically adopts config, complete environment, and binary, then prints and validates
their effective paths. The persistence invariant is exact: artifacts are replaced in place at the paths
the running process proves it uses, so any unit that started the current process starts the next one
identically. Unit-file edits are outside the driver's control; it neither parses nor reloads them, and
drift is caught by the next activation's pre-T0 proof or by the immediate post-start proof. Every unit
bit is read from the exact output of `systemctl is-active` and `systemctl is-enabled`: only
`active`/`inactive`/`failed` and `enabled`/`disabled`, respectively, are accepted; transitional,
runtime-only, static, or manager-error results stop the driver. `started`
adopts an already-running exact generation or runs `systemctl enable pe-service` followed by
`systemctl start pe-service`, repeats the complete
running-process proof against the adopted artifacts, and records its `InvocationID` plus
`ActiveEnterTimestamp`. Both the `started` and `verified` proofs require the unit to be active and
enabled. `verified` repeats that proof before the manifest advances,
then waits for the production bind and for `status.json.updated_at` to be newer than that recorded
invocation: the binary binds its listener and starts the status writer only after the boot walk of the
approved due subset, so the deadline is 120 seconds per approved wallet (at least 300 seconds), and the
proved invocation must remain the running one throughout the wait. It then
rechecks both `InvocationID` and `ActiveEnterTimestamp` before continuing. It requires the latest ranking
batch to equal the frozen manifest batch, and proves the bind and permanent paths, the boot census recorded in the manifest (wallets anchored by this boot vs. carried over; the count is not compared with the precheck's due count, which ages while the driver waits), producer/critical-task health, fresh Supabase book, successful watchlist projection,
and refreshed materialized view. If the new-process proof fails either while entering `started` or at the
top of `verified`, or if any later material verification fails (bind, readiness/status, invocation,
frozen batch, approved walk, prepared generation, materialized view, fresh Supabase book, or
projection), the driver first atomically records `state=refusing` and the timestamped refusal intent.
It then attempts both disable and stop even if either command fails, proves the exact inactive and
disabled states, appends that intent to `post_start_refusals`, removes the recorded invocation fields,
and atomically rewinds the manifest to `switched`. A run resumed in `refusing` repeats this quiesce and
rewind sequence before any forward work. Frozen-batch drift after T0 also records `forward_blocked`; that activation
cannot run forward again and must use `rollback_generation.sh`. After any repairable refusal, the
operator chooses either to re-run the same activation (which starts and verifies the generation again)
or to roll it back. The operator-held pause on Forge remains in place through either decision.

The final signed-in site check cannot be automated because the site uses Google single sign-on. The
driver therefore prompts the operator to inspect the fresh era and records `site_confirmed_by` and
`site_confirmed_at` in the manifest before advancing to `verified`. A non-interactive
invocation must include `--site-confirmed`; that flag is the operator's attestation that the signed-in
check was performed, not an automated site probe. This interactive attestation is the sole verification
exception that leaves the service running at `started`; supply the attestation on the next identical run
or choose rollback.

After the driver reaches `verified`, update the Forge checkout to the exact merge commit and restore its
recorded flag, enablement, and activity independently on Forge itself:

```bash
ssh forge 'cd ~/prediction-markets && scripts/deploy/forge_pause.sh restore'
ssh forge 'cd ~/prediction-markets && scripts/deploy/forge_pause.sh status'
```

Do not restore Forge before `verified`: the driver checks the frozen batch again during verification.

Re-run the identical command after interruption or reboot. Each external boundary is rechecked, and
the first incomplete durable state resumes. Never edit the manifest or substitute a repository
checkout for its staged artifact paths. Before live use, run the network-free recovery proof:

```bash
bash scripts/deploy/test_activate_generation.sh
```

### Generation rollback

Rollback is also locked and resumable:

```bash
SUPABASE_DB_URL=<session-pooler-url> \
  scripts/deploy/rollback_generation.sh --activation-id <id>
```

It accepts every state from `guarded` onward, including a durable `refusing` intent, and records
`rolling_back` first without discarding any refusal or forward-blocking audit fields. It then immediately
disables and stops the service and proves it inactive and disabled. Only then does it validate archived
config/environment/binary sources when `archive_artifacts` is durable; before that manifest fact exists,
the installed artifacts must still match the pre-T0 hashes recorded by the driver. It reads the durable
archive stamp only after those checks. The database restore and materialized-view
refresh run only when stamped archive rows for this activation exist, and restored counts are compared
only after that restore. It then restores artifacts when an archive was recorded, enables and starts the
old generation, and proves the unit active and enabled with the running executable hash equal to the old
binary before recording `rolled_back`. A forward rerun of that id is thereafter refused. Retain the
manifest, generation, and pre-T0 archive for audit.

## Issue #545 schema-two financial-era activation

This route changes financial semantics inside the already-verified #557 generation. It does not
create a generation, stage new state paths, invoke `seed_v1_empty.sh`, or replace
`/home/sean/pe-activation.json`. The latter is read-only generation/activation identity;
`/home/sean/pe-financial-era.json` (`kind: financial-era-v1`) is the sole financial-transition
manifest.

### Bounded final-head rehearsal

Run the final-head harness before financial activation. Pass the same reviewed `service.toml` and
complete environment paths that will be supplied to the financial-era driver's `--target-config`
and `--target-environment`; the required positional argument is the reviewed full Git object
identity:

```bash
scripts/deploy/rehearsal545.sh --dry-run \
  --target-config <reviewed-service.toml> \
  --target-environment <reviewed-production-env> \
  <reviewed-40-hex>
read -r SUPABASE_DB_URL < <mode-0600-session-pooler-url-file> && export SUPABASE_DB_URL
PE_REHEARSAL_ROOT=<fresh-root-for-this-attempt> PE_REHEARSAL_BIND=<loopback:port> \
  scripts/deploy/rehearsal545.sh \
  --target-config <reviewed-service.toml> \
  --target-environment <reviewed-production-env> \
  <reviewed-40-hex>
```

Load the database-admin URL from a mode-0600 file with `read -r … && export`, never as an inline
`NAME=value` prefix or `$(<file)` substitution: both print the value under shell tracing, which the
driver's own confidentiality tests forbid. Run each attempt under an explicitly assigned fresh
`PE_REHEARSAL_ROOT` with no `PE_REHEARSAL_COPY_DIR` or `PE_REHEARSAL_EVIDENCE_HASH_FILE` override
(an override is taken verbatim and may point anywhere): the harness writes fixed-name artifacts per
revision under the root and its evidence binds the result manifest by absolute path, so a fresh root
preserves every earlier attempt and never reuses a copy the reanchor probe has already mutated. Reuse a
root only to resume with an untouched copy; never delete an earlier attempt's root.

The no-target `--dry-run <reviewed-40-hex>` form used by CI is intentionally a path-independent
parser/syntax check. The reviewed target pair remains accepted in dry-run and is mandatory for an
actual rehearsal; if either target flag is present, both must be present.

The harness requires `/home/sean/pe-activation.json` to be `verified`, reads the active generation
from it, and checkpoints its SQLite database, all three framed logs, and the captured legacy-history
input. The rehearsal and the driver's `prepare` accept an installed schema-2 generation written at or
after #556 with a nonempty membership (#567): the rehearsal child upgrades only its private copy, and
in the canonical route the production file is written only inside Start, after the stop at
`prepared`. Any read-write open by the reviewed binary would upgrade the production file in place
and strand the installed binary's rollback, so never run the reviewed binary's `--report`,
`--backfill-supabase`, or an ordinary start against the production paper state before Start. Before
starting the child, the harness runs the reviewed binary's `--update-paper-migration-paths` on the
private copy, which verifies the recorded activation prefixes against the copied logs and rewrites
only the copy's recorded log paths (the installed migration record binds absolute paths, #570); the
production record is never touched. A new or reused checkpoint must have the exact six-entry `copied.sha256` inventory and pass
`sha256sum --strict -c` before use. `PE_REHEARSAL_BIND` is mandatory and must be a numeric loopback
address with a nonzero port different from the installed service's port; the harness passes it to
the child as `PE_BIND`, derives the readiness URL from it, and runs against real first-party venue
endpoints.
The #557 activation manifest is inherited-generation and installed-old authority only. The harness
requires its full production schema and exact six-artifact inventory, verifies the installed old
binary/config/environment against those #557 hashes and destinations, and reads the active generation
from it. The explicit #545 config and production environment are independent reviewed targets: their
paths need not equal any #557 staged path, and the harness hashes their bytes directly. If legacy
`PE_REHEARSAL_CONFIG` or `PE_REHEARSAL_ENV` is present, it must resolve to the corresponding explicit
target path; an arbitrary override is refused.

The reviewed binary and config are copied first into a private rehearsal artifact directory, with
the copied files installed mode `0500` and `0400`, respectively. Their copied bytes are hashed,
self-validated, evidenced, and executed; later replacement of either supplied target path cannot
change the rehearsal invocation. Immediately before execution, the harness re-hashes the private
binary, config, and sanitized environment against their recorded identities. Its hash-bound watch
log also records that the resolved `/proc/<service_pid>/exe` path equals the private binary path.
The reviewed production environment must carry a publishable/anon-class value in
`PE_SUPABASE_ANON_KEY` and a distinct secret/service-role-class value in
`PE_SUPABASE_SECRET_KEY`. Write the reviewed file using the strict plain `NAME=value` grammar above;
syntax whose systemd interpretation could differ, including any backslash or `export` prefix, is
refused with its physical line number. The harness deterministically creates its own mode-`0600`
sanitized derivative by replacing only those two assignments with the publishable value; each
assignment must occur exactly once. The shared strict data parser validates both the target and
derivative without executing either. Preflight requests only the URL and two Supabase service variables, so target-file
`SUPABASE_DB_URL` and `PG*` assignments never enter its shell. It passes only that derivative to
`rehearsal_preflight.sh` and to the service child. The preflight accepts a modern
`sb_publishable_*` key or a legacy JWT with `role=anon` and
rejects a modern secret key, service-role JWT, malformed key, or mismatched slots before any HTTP
request. It receives the database-admin URL through an fd-backed environment handoff in a separate
sanitized process, passes it only to sanitized `psql` children, and removes its marker rows on every
exit. The service then starts under `env -i`
with only the explicit `ServiceConfig` environment allowlist and fixed rehearsal overrides;
the production service-role credential, `SUPABASE_DB_URL`, `PGDATABASE`, `CREDENTIALS_DIRECTORY`,
and unrelated inherited variables cannot reach it.

Path and cadence overrides are environment variables, not flags:
`PE_REHEARSAL_ROOT`, `PE_ACTIVATION_MANIFEST`, `PE_REHEARSAL_RELEASE_ROOT`,
`PE_REHEARSAL_BINARY`, `PE_REHEARSAL_COPY_DIR`, `PE_REHEARSAL_BIND`, `PE_REHEARSAL_TIMEOUT_SECS`,
`PE_REHEARSAL_POLL_SECS`, and `PE_REHEARSAL_EVIDENCE_HASH_FILE`.

The four concurrent observers are the status-file poller, reader-drop classifier, fence/anchor
census, and write-refusal counter. A fifth, privileged account observer runs synchronously before
the child starts and again after it has exited. Each decision pass also queries the staged process's
real `/health/ready` endpoint at the dedicated loopback bind and requires HTTP success plus
`ready: true` with no reported issues (the empty `issues` field may be omitted); status freshness
and critical-task assertions remain independent requirements. Before the publishable-only child is
started, the descriptor-fed privileged preflight queries `accounts` once and emits its canonical
C-ordered count and SHA-256 receipt after proving unique account IDs and requested/effective modes
of `off` on every row. The child still receives no service-role credential. Consequently, its fresh
status must contain the authorization-denied `live` shape: `stale: true` and an empty `accounts`
list. Any other child shape fails the rehearsal because it is evidence that a privileged credential
reached the child; the child snapshot is not compared with the privileged census. After the child
has quiesced and written final status, the descriptor-fed privileged observer takes a second
canonical account census. PASS requires both censuses to be safe and identical in count and digest.
The run stops at the first complete same-invocation proof, the first unsafe observation, process
exit, or its bound. PASS requires the reviewed revision, a complete ordinary poll after start,
successful re-anchor, real readiness, healthy critical owners, identical safe
before/after privileged account censuses, the expected authorization-denied child snapshot, and no
credit loss, unexpected fence/error, or successful database write. For this rehearsal, an
unexpected fence is a `wallet_fences` row whose cause is outside the explicit incident-reviewed
allowlist in `rehearsal545.sh`; membership in `WalletFenceCause` alone does not make a newly
observed fence expected. Immediately before PASS, the
harness quiesces the child, synchronously rescans one exact complete service-log prefix, queries the
current anchor/reanchor/fence database observation, and takes the final privileged account census.
The result binds that prefix's byte length and SHA-256 plus the database and census values; unsafe
evidence arriving while readiness is in flight therefore fails the same invocation. The harness
prints `REHEARSAL545_PASS` or `REHEARSAL545_FAIL` and writes a `rehearsal545-evidence-v1` JSON file. That
file records PASS/FAIL, the SHA-256 of the result manifest, its absolute path, and the rehearsed
binary's revision, embedded BLAKE3 identity, file SHA-256, activation ID, canonical generation
directory, copied-state manifest SHA-256, exact passing readiness-body SHA-256, config SHA-256, and
both environment identities. `environment_sha256` is the reviewed production target;
`rehearsal_environment_sha256` is the generated publishable-only derivative. Preserve and review the
JSON file and its result manifest. The result manifest records each privileged census's count,
SHA-256, and safety result plus `account_census_before_after_identical`; the outer JSON's
`evidence_sha256` binds those fields as part of the exact result-manifest bytes. The financial driver
compares the production identity to `--target-environment`, binds both before entering `prepared`,
and revalidates them from disk before entering `guarded`. The copy manifest, result manifest, and
outer JSON are installed with the shared durable atomic-write primitive (file sync, rename, then
parent-directory sync).

The mandatory production rehearsal is the PostgREST-level authorization proof: it runs the real
service through the target project's PostgREST endpoint with the publishable key in both credential
slots and requires the child's denied account read to surface as `live.stale: true` with an empty
`live.accounts` list. CI does not install a PostgREST daemon solely for this proof because that would
add a new infrastructure dependency outside this service/schema change; instead, the PostgreSQL
scenario proves the real candidate-schema grants under `anon` and `service_role`, and the service
unit test proves that PostgREST 401/403 responses select the stale, empty boot snapshot. These CI
checks do not replace or waive the production rehearsal.

Before `QualificationStarted`, installed artifacts and `ConfigEra::Legacy17` stay active. Its two
superseded values are compatibility data and never enter corrected economics. The old 17-name
contract is verified before the guarded mutation. Only after the physical Start does the driver
install the financial authority and multi-account live schemas, seed the Start identity, migrate to
`ConfigEra::Financial15`, refresh `wallet_live_stats_mv`, adopt the reviewed files, and start the
service. The live-schema boundary converts both `live_positions` quantity columns from the deployed
`bigint` shape to `numeric` without changing existing whole-contract values. An optional
`risk_halt_release_hash` remains separate incident control. Do not apply the 17→15 boundary early.

Run the exact reviewed driver command on the production host:

```bash
read -r SUPABASE_DB_URL < <mode-0600-session-pooler-url-file> && export SUPABASE_DB_URL
scripts/paper_reset/activate_financial_era.sh \
  --target-binary <reviewed-pe-service> \
  --target-config <reviewed-service.toml> \
  --target-environment <reviewed-production-env> \
  --paper-log <active-generation-paper.log> \
  --source-log <active-generation-source_events.log> \
  --live-journal <active-generation-live_journal.log> \
  --paper-state <active-generation-paper_state.db> \
  --fresh-bankroll <amount> \
  --rehearsal-evidence <rehearsal-evidence-json> \
  --ranking-batch-id <id> \
  --membership-json <canonical-membership-array.json>
```

The driver requires the real #557 activation manifest to be `state: verified`. That manifest owns
only the inherited generation identity: `activation_id`, canonical `generation_dir`, `merge_commit`,
bankroll, source-v1-main evidence, legacy-history evidence, and the #557 artifact inventory. Before
creating the financial manifest, the driver proves the installed old binary, config, and environment
match the #557 artifact hashes and installed destinations. The reviewed #545 target is independent:
its binary self-reports its revision and BLAKE3 under `--verify-staged-identity`, and the driver binds
the binary/config/environment SHA-256 values to the rehearsal evidence. A #545 target is neither
path-equal nor revision-equal to the inherited #557 artifacts. Before it creates the financial
manifest or can approach Start, the driver also requires the reviewed production
`PE_SUPABASE_SECRET_KEY` to be a modern `sb_secret_*` key or a legacy JWT with `role=service_role`;
the validator reads that value through the data parser rather than argv. The publishable-only
rehearsal derivative is never adopted as production. Before the financial manifest is created, the
same parser refuses `LD_PRELOAD`, `LD_LIBRARY_PATH`, and `LD_AUDIT`. Every offline rollback-check,
prepare, and Start invocation receives target-file assignments only through the shared service
configuration allowlist; unrelated target assignments are not exported. After Start, each artifact
adoption hashes the copied temporary file and compares it with the manifest's reviewed digest before
the destination rename; drift refuses without replacing the installed file. The Start hot-config
identity is not an operator assertion:
while the service is inert, the driver exports the database rows that the 17→15 migration retains to
a mode-private temporary file. The database helper reads the credential-bearing URL from its named
environment variable and exports `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, and optional
`PGSSLMODE` from it for `psql`; the URL is never in argv. The Rust prepare owner parses the rows as
`ConfigEra::Financial15` and calls
`RuntimeConfig::canonical_hash`. The membership-proofs identity is likewise a Rust-owned BLAKE3 of
the exact manifest membership array plus each member's current complete-history, coverage, latest
position-anchor, and position-validation records. Missing or inconsistent evidence fails prepare.

There is no durable activation policy artifact in the current repository, so Start carries no
separate policy identity. `QualificationStarted` binds the reviewed artifact, effective static
configuration, Rust-derived hot configuration, generation and activation, ranking batch, fresh
bankroll, membership and its proofs, and schema, parser, and financial-semantic model versions.

The exact resumable forward order is `rehearsal PASS → prepared → guarded → started → verified`.
Creating `prepared` records the verified #557 identity and source/history evidence; the independently
reviewed target identities; durable paths; ranking/membership inputs; fresh bankroll; and every
rehearsal binding named above without changing service or financial state.
Before any stop intent, the `prepared → guarded` transition rereads the bound evidence JSON and result
manifest, verifies the recorded hash, requires PASS, and requires the activation/generation,
copied-state/readiness, reviewed config/environment, revision, embedded BLAKE3 identity, and binary
SHA-256 to remain exact, including both the reviewed production-environment hash and the sanitized
rehearsal-environment hash. A missing, changed, failed, or mismatched rehearsal is a typed
`REHEARSAL_REFUSAL` and leaves the service and financial state untouched. Identical reruns preserve
the original binding. After that gate, the driver records stop intent, stops the service once, proves
it inert, verifies the old 17-name contract, takes and verifies a complete SQLite online backup,
records the remote census and all three log identities, and invokes the staged network-free
`prepare` command. That command scans the logs and returns the exact Start payload and expected
synchronized receipt; it does not mutate them.

From `guarded`, the driver archives/resets the remote paper state, invokes the network-free local
reset/Start, and then records the post-Start database boundaries in this order:
`authority-schema-intent → authority-schema-installed → live-schema-intent →
live-schema-installed → authority-start-seeded → financial-config-migration-intent →
financial-config-migrated → wallet-live-stats-refresh-intent →
wallet-live-stats-refreshed`. It then adopts the reviewed files, starts the service once, records
`started`, and exits. A rerun from `started` performs the first invocation-fresh verification and
records `verified`.

`started → verified` checks the installed identities; guarded log-prefix continuity; exact local and
remote Start/fresh-financial state; applied hot hash; ranking and membership; public projection;
required producers and critical owners; and accounts off, unarmed, fresh, and free of dispatch work.
It also queries the proven installed invocation's numeric-loopback `/health/ready` endpoint exactly
once and requires HTTP success, `ready: true`, and no issues. Readiness has no wait loop: if either the
invocation-fresh status proof or that one endpoint response is not already complete, the command fails
immediately and the operator reruns the same command. There is no soak, dwell, repeated sample, or
site approval. The public materialized-view evidence at this stage is its durable pre-start refresh
receipt, not a row comparison against the live `wallet_live_stats` base view: after service start,
new financial writes may legitimately make that base view newer than the scheduled snapshot.

`--rollback-before-start` is the only rollback route. Rollback-check scans and validates an exact
manifest-bound complete Start before consulting mutable status or live posture. A complete Start
forces roll-forward even when the shell manifest or status is missing or stale. Only the no-Start path
requires the pre-Start posture and may repair a partial final frame; it restores only from the durable
activation archive and complete SQLite backup. After the remote archive restoration it records
`rollback-wallet-live-stats-refresh-intent → rollback-wallet-live-stats-refreshed` while the service
is inert, then starts the old service and records `rolled_back`. A no-mutation rollback skips the
remote restore and refresh. At or after Start, rollback is forbidden: preserve the append-only era and
recover with a compatible reader.

The driver invokes these early-dispatch service commands; they accept either `--name=value` or
`--name value` spellings:

```bash
pe-service <service.toml> --financial-era=prepare \
  --activation-manifest=/home/sean/pe-financial-era.json \
  --financial-config-rows=<driver-exported-financial15-rows.json>
pe-service <service.toml> --financial-era=start \
  --activation-manifest=/home/sean/pe-financial-era.json \
  --financial-config-rows=<driver-exported-financial15-rows.json>
pe-service <service.toml> --financial-era=rollback-check \
  --activation-manifest=/home/sean/pe-financial-era.json
```

`prepare` is read-only, `start` performs the idempotent local financial reset and synchronized Start,
and `rollback-check` reports whether a complete Start is already present. They dispatch before normal
client construction.

CI proves this cross-store boundary against PostgreSQL 16 with
[`test_legacy_to_financial_pg.sh`](../scripts/deploy/test_legacy_to_financial_pg.sh). The scenario
installs checked-in pre-545 schema fixtures pinned to commit `8ea29a9`, creates fresh local logs and
paper state, runs the production `pe-service --financial-era=prepare` and `start` owners via
`cargo run -p pe-service --all-features --bin pe-service --`, and passes the returned synchronized
Start receipt to `seed_financial_start` before applying the configuration migration. It retries every
transition boundary once, applies the live schema twice to the legacy `bigint` position shape, proves
whole quantities survive and fractional quantities round-trip, and requires the materialized and base
view counts to agree after forward refresh and rollback restoration. Both pull-request and post-merge
`main` CI therefore exercise the same legacy input.

The offline qualification command is separate from activation and constructs no network client:

```bash
pe-service --qualify \
  --paper-log <paper.log> --source-log <source_events.log> \
  --live-journal <live_journal.log> \
  --paper-state <paper_state.db> --seal-hash <qualification-seal-hash> \
  --output <qualification-report.json>
```

It emits canonical compact JSON plus one trailing newline and reports its BLAKE3 hash. Only a sealed
`Pass` under `_GLOSSARY.md` permits the one subsequent manual paper-to-live-tiny review.

### #565 open-continuation census before deployment

Before swapping the binary, quiesce the service and take a consistent copy of the active paper
state and source log using the SQLite `.backup` and `cp -p` mechanics in
[`rehearsal545.sh`](../scripts/deploy/rehearsal545.sh). Run only this network-free, read-only census
against that copy; never run the rehearsal itself for this gate, because it rebinds migration paths,
sets `reanchor_required`, and boots the service.

```bash
# the staged candidate executable, e.g. /tmp/pe-service.new.<sha12>; the installed pre-#565
# executable does not implement this flag
/tmp/pe-service.new.<sha12> --validate-open-continuations \
  --paper-state <quiesced-copy/paper_state.db> \
  --source-log <quiesced-copy/source_events.log>
```

Record stdout, stderr, and the exit status. Success prints `open_rows=N validated=N` and exits zero.
A validation failure exits nonzero with `open decision continuation <source_trade_id>: <cause>`;
it blocks the swap for diagnosis without repairing rows or fabricating dispositions. Normal boot
uses the same validator and logs `open decision continuations validated` with `open_rows=N` before
resuming any open row. A boot-time validation failure is a startup error on stderr/journal (live
fan-out, the HTTP server, and the status writer have not started yet); the row stays intact and the
unit's `Restart=on-failure` policy repeats the failed start until the row is diagnosed. The ordinary [rollback](#rollback) to a pre-#565 binary is available only
before the first synchronized schema-3 reconciliation page. After that boundary, preserve all state
and use a binary retaining schema-3 page and V4 continuation compatibility; never delete records or
rewrite rows to make an older reader accept them.

## Facts

| item | value |
|---|---|
| VPS | `82.22.32.225`, user `sean` (`ssh -i ~/.ssh/id_personal sean@82.22.32.225` — never root) |
| unit | systemd **system** unit `pe-service` (`/etc/systemd/system/pe-service.service` + drop-in `pe-service.service.d/age-identity.conf`); `WantedBy=multi-user.target`, `Restart=on-failure`, `RestartUSec=10s`, `KillSignal=2` (SIGINT — the binary's shutdown signal, so a restart drains buffered trades). Start/stop/restart need root: run [`scripts/vps_grant_pe_service_sudo.sh`](../scripts/vps_grant_pe_service_sudo.sh) once as root to grant the deploy user passwordless, command-scoped `systemctl` control of the pe-service units (verified with `sudo -n -l`); until then use `ssh -t … 'sudo systemctl restart pe-service'` |
| binary path | `ExecStart` runs `/home/sean/prediction-markets/target/release/pe-service smoke-test/service.toml` with `EnvironmentFile=/home/sean/prediction-markets/.env` and `WorkingDirectory=/home/sean/prediction-markets`; environment contents use data-file semantics and are never shell-sourced |
| backup convention | before the swap: `cp -p target/release/pe-service target/release/pe-service.bak-<prior-sha12>` (hash-named, once-only) |
| env | `.env` on the VPS (REST URL plus both credential identities: publishable/anon-class `PE_SUPABASE_ANON_KEY` and secret/service-role-class `PE_SUPABASE_SECRET_KEY`; **no** `SUPABASE_DB_URL` there). The #545 rehearsal derives its publishable-only file from this reviewed production target and never installs the derivative. `PE_SUPABASE_AUTHORITATIVE` defaults to `false` but must be `true` in the #545 production target. `PE_` booleans must be `true`/`false`, never `1`/`0` (figment rejects ints → restart loop). Websocket knobs live there too (`PE_POLYMARKET_ACTIVITY_WS_ENABLED`, `PE_SOURCE_EVENT_LOG_PATH`, `PE_COPY_LATENCY_BUDGET_SECS`) |
| build box | the VPS has no cargo — build on the dev box and `scp`. Release-like builds derive the full revision directly from the checked-out Git object and reject a dirty, unknown, or invalid checkout; no environment override is accepted. Dev/test builds use the explicit `dev-dirty` sentinel when needed. |
| logs | `journalctl -u pe-service -f` (Tier-1 prod check); JSONL sinks per `jsonl_log_path`; `status.json` in the working directory |

## Topology (#546)

One `pe-service` process on the VPS runs **three coequal activity-websocket readers** over the same
endpoint and **one coordinator** that owns the source event log; there is no per-reader unit, no Forge
activity reader, and no cross-host failover. A VPS reboot stops all three readers until the enabled
unit starts the process again. Forge's independent ranking loop (`pe-rank-loop`,
[`26-…RUNBOOK.md`](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md) "Continuous Forge supervisor",
[`deploy/systemd/README.md`](../deploy/systemd/README.md)) is not in the copy path: its reboot pauses
new ranking publication only.

## Procedure

Every step is bound to the embedded full Git revision plus exact binary bytes (#544). The binary
reports `revision=<40-hex> config_identity=runtime-applied`; `--verify-staged-identity` checks that
revision and the executable's BLAKE3 digest before staging. Continue to use sha256 for the existing
desired/staged/installed/running byte comparison (#514): **desired** = hash of the local release build;
**staged** = hash of `/tmp/pe-service.new.<desired-sha12>` on the VPS (hash-qualified so concurrent
agents cannot clobber each other's staged binary, #516); **installed** = hash of the `ExecStart`
binary; **running** = hash of `/proc/<MainPID>/exe`. A deploy holds the deploy lock from preflight
through verify-or-rollback — acquire it as the FIRST VPS action and keep the descriptor open for the
whole run: `exec 9</home/sean/.pe-deploy.lock && flock -n 9 || { echo 'another deploy holds the lock'; exit 1; }`
(a read-only descriptor: the lock file is root-owned mode 0644, so `9>` fails for `sean`; `flock` needs no write access).
The lock serializes concurrent deploy agents on the VPS; backfill/reset (dev-box `psql` paths) are
excluded instead by their own runbook precondition that the service is STOPPED while they run (docs/34).

Since #546 the binary is replaced **while the old process keeps running** — Linux keeps the old inode
open under it — and activated by **one** `systemctl restart`. Every step is resumable: the hash
comparisons decide what remains; never guess from memory.

1. **Build from a clean checkout at the exact reviewed SHA** and record both identities:

   ```bash
   revision=$(git -C /home/sean/git/prediction-markets rev-parse --verify 'HEAD^{commit}')
   git -C /home/sean/git/prediction-markets status --porcelain=v1 --untracked-files=normal
   cargo build --release -p pe-service
   target/release/pe-service --version
   artifact_blake3=$(b3sum target/release/pe-service | awk '{print $1}')
   target/release/pe-service --verify-staged-identity "$revision" "$artifact_blake3"
   sha256sum target/release/pe-service
   ```

   The status command must print nothing; `--version` and the verification output must name the
   same full `revision`. A dirty release checkout fails during the build rather than emitting an
   unchecked identity.
2. **Ship** hash-qualified:
   `scp -i ~/.ssh/id_personal target/release/pe-service sean@82.22.32.225:/tmp/pe-service.new.<desired-sha12>`
3. **Preflight on the VPS** (read-only; lock first):

   ```bash
   exec 9</home/sean/.pe-deploy.lock && flock -n 9 || { echo 'another deploy holds the lock'; exit 1; }
   cd /home/sean/prediction-markets
   systemctl is-enabled pe-service; systemctl is-active pe-service
   systemctl show pe-service -p MainPID -p InvocationID -p ExecMainStartTimestamp -p NRestarts -p ExecStart -p WorkingDirectory -p FragmentPath -p DropInPaths -p UnitFileState -p WantedBy -p Restart -p RestartUSec -p KillSignal
   pid=$(systemctl show pe-service -p MainPID --value)
   sha256sum target/release/pe-service "/proc/$pid/exe" /tmp/pe-service.new.<desired-sha12> .env smoke-test/service.toml
   chmod 0755 /tmp/pe-service.new.<desired-sha12>
   /tmp/pe-service.new.<desired-sha12> --version
   /tmp/pe-service.new.<desired-sha12> --verify-staged-identity '<reviewed-40-hex>' '<staged-blake3>'
   stat -c '%d %n' target/release /tmp     # equal device ids ⇒ the rename below is atomic
   jq '.source_health, {bankroll,open_positions,fills_total,last_event_seq,watchlist_size}' status.json
   stat -c '%s %Y' smoke-test/source_events.log
   ```

   Expected contract: `enabled`, `active`, `UnitFileState=enabled`, `WantedBy=multi-user.target`,
   `Restart=on-failure`, `RestartUSec=10s`, `KillSignal=2`, staged = desired, installed = running.
   The staged `--version` revision and verified BLAKE3 must match step 1 in addition to the sha256
   equality. If installed = running = desired already, the deploy is complete (a resumed run): go to step 6.
   If installed = desired but running is prior, go to step 5. Any unit-policy or ownership drift
   stops the deploy for a reviewed correction; only enablement drift may be repaired in place with
   `sudo systemctl enable pe-service` (no `--now`, no restart).
4. **Baseline + atomic swap** (the old process keeps running on its open inode):

   ```bash
   art=/home/sean/.pe-deploy-artifacts/<desired-sha12>; install -d -m 0700 "$art"; umask 077
   cp status.json "$art/status.before.json"
   systemctl show pe-service -p InvocationID -p MainPID -p ExecStart -p WorkingDirectory > "$art/unit.before"
   sqlite3 -readonly paper_state.db ".backup '$art/paper.before.db'"
   [ -f target/release/pe-service.bak-<prior-sha12> ] || cp -p target/release/pe-service target/release/pe-service.bak-<prior-sha12>
   sha256sum target/release/pe-service.bak-<prior-sha12>       # = prior (= installed = running)
   chmod 0755 /tmp/pe-service.new.<desired-sha12>
   mv -T --no-copy /tmp/pe-service.new.<desired-sha12> target/release/pe-service && sync -f target/release/pe-service
   sha256sum target/release/pe-service "/proc/$pid/exe"        # installed = desired; running still = prior
   ```

5. **Config deltas for this deploy** (see each PR's "Deployment impact": `.env` / TOML boot knobs,
   PATCH/INSERT of the live `service_config` rows the new binary reads — seed `on conflict do nothing`
   never updates an existing row), then **one activation**:
   `sudo systemctl restart pe-service` (passwordless after the grant script; otherwise `ssh -t …`). Immediately before it, re-read the installed and
   running hashes and `InvocationID`: if the running hash is already desired (the unit re-activated on
   its own after a crash), do not restart again.
6. **Verify**:

   ```bash
   systemctl show pe-service -p MainPID -p InvocationID -p ExecMainStartTimestamp -p NRestarts -p ExecStart -p WorkingDirectory
   pid=$(systemctl show pe-service -p MainPID --value); sha256sum "/proc/$pid/exe" .env smoke-test/service.toml
   journalctl -u pe-service -n 100
   target/release/pe-service --version
   jq '{revision,applied_config_hash,tasks,runtime_config,watchlist_projection,source_health}' status.json
   curl -s localhost:8080/health/ready
   ```

   A new `InvocationID`, PID, and start timestamp prove activation; `NRestarts` must not increase
   afterwards (no restart loop); running hash = desired; embedded and `status.json.revision` equal
   the reviewed revision; `status.json.applied_config_hash` equals
   `runtime_config.applied_hash`; every critical task is running with no sticky failure; and
   `ExecStart`, `WorkingDirectory`, and the
   `.env` / `service.toml` hashes equal step 3; clean boot (no config-parse error, watchlist seeded from
   `latest_ranking`, `service_config poll loop started`, no poll failures; since #542 an admitting
   swap or backfill is preceded by `hot-watchlist admission state prepared`).
   **With `polymarket_activity_ws_enabled=true` (#530/#546)**: two consecutive `status.json`
   publications, each paired immediately with `/health/ready`, must show all three
   `source_health.ws_readers` records, `ws_live_reader_count >= 2`, `ws_sink_poisoned=false`,
   `copy_admission_blocked=false`, and at least two readers' `normalized_activity_rows_total`
   advancing between the publications; readiness must carry none of `activity_ws_unavailable`,
   `activity_ws_redundancy_degraded`, `activity_ws_sink_poisoned`, `copy_admission_blocked`; the
   source log's size/mtime must advance (append-only file; no content inspection here). Repeat
   reader copies cannot create duplicate rows by construction — `seen_trades`, `fills` (keyed by the
   `wf|leader|source_trade_id|…` idempotency key), and `dispatch_seeds` (keyed by the dispatch id
   derived from it) are all primary-keyed on the trade's own identity, and acceptance scenario R6
   proves three copies produce one row each — so the post-deploy check is identifier-bound: for any
   watched `source_trade_id` observed after activation, expect `seen=1`, `fills<=1`, `seeds<=1`, and
   `no_copy<=1` with `fills+no_copy<=1`; otherwise record `not observed`.

   ```bash
   id=<source_trade_id>
   sqlite3 -readonly -header paper_state.db "select (select count(*) from seen_trades where source_trade_id='$id') as seen, (select count(*) from fills where idempotency_key like 'wf|%|$id|%') as fills, (select count(*) from dispatch_seeds where source_trade_id='$id') as seeds, (select count(*) from no_copy_dispositions where source_trade_id='$id') as no_copy;"
   ``` If the second publication cannot
   establish two live readers, roll back instead of waiting. Record the readers'
   `consecutive_reconnects` and drop cadence: a churn pattern is the evidence for any keepalive
   follow-up (the first-party client sends `ping` every 5 s; `pe-service` does not).

## #544 database, paper-boundary, and site activation lane

Before staging the service, apply the additive projection function/schema and then the guarded
configuration migration while version one still serves:

```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/supabase_schema.sql
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/migrate_service_config_544.sql
```

The migration allows only the exact hot and retired key sets, deletes only the reviewed retired
rows, and aborts before mutation while listing any unknown key. Reload/verify the PostgREST schema
cache, prove `service_watchlist_replace_v1(timestamptz,jsonb)` is executable only by
`service_role`, and prove the remaining `service_config` rows are the complete 17-key allowlist
with only `kelly_fraction_override` permitted absent. In particular, the mandatory
`price_impact_cap_bps` row must be present at the canonical seed and `per_trade_cap` must remain
`unlimited`.

The first v2 service activation resolves the configured source and paper event-log paths plus the
derived `live_journal.log`, then captures immutable version-one bindings for each: canonical path,
physical tail, last sequence, and last hash.
It then resumes the machine-owned paper migration phase across restarts, reloads the complete
Supabase bankroll/positions authority, runs the activity/position bracket, records final tails,
and installs only the synchronized v2 main before producers start. Configure
`PE_SUPABASE_AUTHORITATIVE=true` and point `PE_LEGACY_WALLET_HISTORY_PATH` at the captured legacy
input. Do not add a history sidecar service or any periodic position-reseed step.

Only before the first v2 append or active-state commit, preserve the failed side and restore the
immutable v1 main with:

```bash
PE_PAPER_V1_BACKUP_PATH="$PAPER_V1_BACKUP" \
PE_PAPER_FAILED_SIDE_PATH="$FAILED_PAPER_SIDE" \
pe-service --rollback-paper-v1
```

After either boundary, rollback refuses; restart the same v2-compatible binary to resume the
recorded roll-forward. A path/hash/phase/binary mismatch is a stop condition, not a reason to
reseed or delete migration state.

Activate the dashboard as its own `pe-site` lane. Build it from the reviewed commit with `npm ci`,
`npm test`, and `npm run build`; record the commit and staged artifact hash, and stage the
hash-qualified release path early if useful. Do not switch the unit's release path until the new
service has completed one successful `service_watchlist_replace_v1` transaction and the returned
token, count, and rows have been independently verified. Then activate that reviewed release,
restart only `pe-site`, and verify `systemctl is-active pe-site` plus the signed-in Live page. The
site must show a valid empty or nonempty watched set only when runtime-token → watchlist →
runtime-token and count agree; induce no database writes during this read check. Site rollback
restores the prior release path and restarts only `pe-site`; the additive RPC/schema remain
installed. If service rollback to the legacy publisher is still permitted, roll the site back
first—the token-guarded reader must never run against the legacy producer.

## #530 websocket rollback ordering

Disabling `polymarket_activity_ws_enabled` (or rolling back to a pre-#530 binary)
reverts observation to poll-only — proven byte-identical by scenario WS3. **Reverse
dependency order is mandatory**: while a Δ=2 ranking batch is latest, the flag stays
enabled so stale REST observations keep failing closed; first restore and publish a
Δ=20 ranking, verify the newest `ranking_batches.latency_shift_secs = 20` and that
pe-service applied that batch, and only then disable the flag or roll the binary
back. Disabling first would copy Δ=2-selected wallets at poll latency — the
padded-watchlist loss class.

## Historical #510 pre-Start restart semantics

This section applies only to the legacy era before `QualificationStarted`. Its successor-gated
Supabase catch-up cursor makes a healthy legacy restart's boot catch-up a zero-RPC no-op; a non-zero
replay count is the bounded, idempotent gap heal for an earlier legacy authority failure. The
one-time `--backfill-supabase` cutover tool is also legacy-only and is refused after Start.

In the financial era this cursor is neither steady-state authority nor a financial progress marker.
The Start-bound `paper_bankroll.last_prepared_seq`, together with synchronized
`FinancialPrepared`/`FinancialFinal` receipts, owns financial sequencing and restart recovery.

## #508 Phase A config cutover (A0–A4)

Every production update is a predicated compare-and-swap. Save each returned `value` and
`updated_at`; zero rows from A3, A4, or a reversal means another writer won, so stop and reconcile.
A1 may return zero because the row already exists; A1b is authoritative in either case.

```sql
-- A0: capture both predicate tokens before any mutation.
select key, value, updated_at
from service_config
where key in ('price_impact_cap_bps', 'per_trade_cap')
order by key;

-- A1: only if per_trade_cap is absent, install the old-binary-safe posture.
insert into service_config (key, value, value_type, description, updated_by)
select 'per_trade_cap', 'mode_default', 'text',
       'Per-trade cap cutover posture (#508)', '<operator>'
where not exists (select 1 from service_config where key = 'per_trade_cap')
returning key, value, updated_at;

-- A1b: re-select and retain the exact tokens the later UPDATE will predicate on.
select key, value, updated_at
from service_config
where key in ('price_impact_cap_bps', 'per_trade_cap')
order by key;

-- A3: after A2, use the A1b price-impact tokens; exactly one row must return.
update service_config
set value = '100', updated_by = '<operator>', updated_at = now()
where key = 'price_impact_cap_bps'
  and value = '<A1b-price-impact-value>'
  and updated_at = '<A1b-price-impact-updated-at>'
returning key, value, updated_at;

-- A4: last, use the A1b per-trade tokens; exactly one row must return.
update service_config
set value = 'unlimited', updated_by = '<operator>', updated_at = now()
where key = 'per_trade_cap'
  and value = '<A1b-per-trade-value>'
  and updated_at = '<A1b-per-trade-updated-at>'
returning key, value, updated_at;
```

A2 is the normal binary deploy/restart above. Verify clean boot and that the last-known-good shared
impact gate is applied before A3; apply A4 only after A3 is observed. Retain the old binary and
captured tokens through two valid orders. Rollback changes `per_trade_cap` first, then either restore
the gate row and restart the current binary, or restore the old binary and only then restore the old
gate-row value. Each reversal uses the current `value, updated_at` tokens and must return exactly one
row.

## #508 Phase D ordinary-live installation and arming

### D1 — ship dark

Install the service-scoped age identity at a root-owned path with mode `0600`. Install the committed
`pe-service` drop-in that maps it as `LoadCredential=pe-age-identity:<identity-path>`, then run
`systemctl daemon-reload` and verify the effective unit with `systemctl cat pe-service`. Deploy the
Phase-D binary in that same swap/restart operation. Its first boot records the arming fence, so no
earlier promotion review can arm an account.

### Arm one account

1. Rotate the account credentials through the panel; plaintext must never enter `.env`, Supabase,
   logs, or shell history.
2. Externally provision and verify pUSD allowance from the account wallet for both V2 exchange
   spenders. `pe-service` verifies allowance but never mutates it.
3. Record the custody wallet kind and address, then verify Conditional Tokens `isApprovedForAll`
   for both V2 collateral adapters. Approval mutation remains an external operator action.
4. For EOA custody, provision a small POL gas balance. EOA transport remains deferred pending the
   wallet-kind inventory; ordinary-live v1 uses Relayer transport only.
5. After the Phase-D first boot, record the promotion review and its evidence in the panel.
6. Request `live_tiny` in the panel. Requested mode is not authority; wait for the service's audited
   effective-mode transition.
7. Observe the account admission audit and `status.json` live block. Any failed fence keeps the
   account dark; do not bypass it with direct table writes.

## Rollback

The same swap, reversed, plus one restart — triggered by any missing reader record, fewer than two
live readers on the second bounded check, sink poison, a restart loop, an unexplained financial-state
change, or a hash mismatch.

**Compatibility prerequisite (#565):** reverse to a pre-#565 executable only while no synchronized
schema-3 reconciliation page has become durable in the source log (the first successor poll page
crosses that boundary, before any commitment or version-4 continuation exists). After it, preserve
all state and roll forward with an executable that retains schema-3 page and version-4 continuation
compatibility; see the [open-continuation census](#565-open-continuation-census-before-deployment). Never delete
records or rewrite rows to make an older reader accept them.

```bash
cp -p target/release/pe-service.bak-<prior-sha12> /tmp/pe-service.rollback.<prior-sha12>
mv -T --no-copy /tmp/pe-service.rollback.<prior-sha12> target/release/pe-service && sync -f target/release/pe-service
sha256sum target/release/pe-service                          # = prior
```

Revert any config deltas the old binary does not understand (unknown `service_config` keys are
ignored by old binaries — safe to leave), then `ssh -t … 'sudo systemctl restart pe-service'` and
verify per step 6. There is no schema, database, ranker, or host rollback.
