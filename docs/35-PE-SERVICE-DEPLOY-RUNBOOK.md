# 35 — pe-service binary deploy runbook (VPS)

> This runbook deploys ordinary `pe-service`, including the #508 per-account live path that ships
> dark over the shared paper book. It does not deploy or configure the isolated Polymarket V2
> canary; see
> [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

**Purpose.** The repeatable procedure for building the `pe-service` release binary and
deploying it to the VPS with one restart and no stop-before-swap window.

## Facts

| item | value |
|---|---|
| VPS | `82.22.32.225`, user `sean` (`ssh -i ~/.ssh/id_personal sean@82.22.32.225` — never root) |
| unit | systemd **system** unit `pe-service` (`/etc/systemd/system/pe-service.service` + drop-in `pe-service.service.d/age-identity.conf`); `WantedBy=multi-user.target`, `Restart=on-failure`, `RestartUSec=10s`, `KillSignal=2` (SIGINT — the binary's shutdown signal, so a restart drains buffered trades). Restart needs interactive sudo (`ssh -t … 'sudo systemctl restart pe-service'`); a coding agent cannot restart it non-interactively |
| binary path | `ExecStart` runs `/bin/bash -c 'set -a; source /home/sean/prediction-markets/.env; set +a; exec /home/sean/prediction-markets/target/release/pe-service smoke-test/service.toml'` with `WorkingDirectory=/home/sean/prediction-markets` (verified 2026-08-31) |
| backup convention | before the swap: `cp -p target/release/pe-service target/release/pe-service.bak-<prior-sha12>` (hash-named, once-only) |
| env | `.env` on the VPS (REST keys `PE_SUPABASE_URL`/`PE_SUPABASE_SECRET_KEY`; **no** `SUPABASE_DB_URL` there). `PE_` booleans must be `true`/`false`, never `1`/`0` (figment rejects ints → restart loop). Websocket knobs live there too (`PE_POLYMARKET_ACTIVITY_WS_ENABLED`, `PE_SOURCE_EVENT_LOG_PATH`, `PE_COPY_LATENCY_BUDGET_SECS`) |
| build box | the VPS has no cargo — build on the dev box and `scp`. Dev box: `cargo build --release -p pe-service` at the target main SHA (rustc 1.95.0) |
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

Every step is bound to sha256 identity (#514): **desired** = hash of the local release build;
**staged** = hash of `/tmp/pe-service.new.<desired-sha12>` on the VPS (hash-qualified so concurrent
agents cannot clobber each other's staged binary, #516); **installed** = hash of the `ExecStart`
binary; **running** = hash of `/proc/<MainPID>/exe`. A deploy holds the deploy lock from preflight
through verify-or-rollback — acquire it as the FIRST VPS action and keep the descriptor open for the
whole run: `exec 9>/home/sean/.pe-deploy.lock && flock -n 9 || { echo 'another deploy holds the lock'; exit 1; }`.
The lock serializes concurrent deploy agents on the VPS; backfill/reset (dev-box `psql` paths) are
excluded instead by their own runbook precondition that the service is STOPPED while they run (docs/34).

Since #546 the binary is replaced **while the old process keeps running** — Linux keeps the old inode
open under it — and activated by **one** `systemctl restart`. Every step is resumable: the hash
comparisons decide what remains; never guess from memory.

1. **Build at the exact main SHA** being deployed (record the git SHA and the desired hash):
   `git -C /home/sean/git/prediction-markets rev-parse HEAD && cargo build --release -p pe-service && sha256sum target/release/pe-service`
2. **Ship** hash-qualified:
   `scp -i ~/.ssh/id_personal target/release/pe-service sean@82.22.32.225:/tmp/pe-service.new.<desired-sha12>`
3. **Preflight on the VPS** (read-only; lock first):

   ```bash
   exec 9>/home/sean/.pe-deploy.lock && flock -n 9 || { echo 'another deploy holds the lock'; exit 1; }
   cd /home/sean/prediction-markets
   systemctl is-enabled pe-service; systemctl is-active pe-service
   systemctl show pe-service -p MainPID -p InvocationID -p ExecMainStartTimestamp -p NRestarts -p ExecStart -p WorkingDirectory -p FragmentPath -p DropInPaths -p UnitFileState -p WantedBy -p Restart -p RestartUSec -p KillSignal
   pid=$(systemctl show pe-service -p MainPID --value)
   sha256sum target/release/pe-service "/proc/$pid/exe" /tmp/pe-service.new.<desired-sha12> .env smoke-test/service.toml
   stat -c '%d %n' target/release /tmp     # equal device ids ⇒ the rename below is atomic
   jq '.source_health, {bankroll,open_positions,fills_total,last_event_seq,watchlist_size}' status.json
   stat -c '%s %Y' smoke-test/source_events.log
   ```

   Expected contract: `enabled`, `active`, `UnitFileState=enabled`, `WantedBy=multi-user.target`,
   `Restart=on-failure`, `RestartUSec=10s`, `KillSignal=2`, staged = desired, installed = running.
   If installed = running = desired already, the deploy is complete (a resumed run): go to step 6.
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
   never updates an existing row), then **one activation** (operator, interactive sudo):
   `ssh -t … 'sudo systemctl restart pe-service'`. Immediately before it, re-read the installed and
   running hashes and `InvocationID`: if the running hash is already desired (the unit re-activated on
   its own after a crash), do not restart again.
6. **Verify**:

   ```bash
   systemctl show pe-service -p MainPID -p InvocationID -p ExecMainStartTimestamp -p NRestarts -p ExecStart -p WorkingDirectory
   pid=$(systemctl show pe-service -p MainPID --value); sha256sum "/proc/$pid/exe" .env smoke-test/service.toml
   journalctl -u pe-service -n 100
   jq '.source_health' status.json; curl -s localhost:8080/health/ready
   ```

   A new `InvocationID`, PID, and start timestamp prove activation; `NRestarts` must not increase
   afterwards (no restart loop); running hash = desired; `ExecStart`, `WorkingDirectory`, and the
   `.env` / `service.toml` hashes equal step 3; clean boot (no config-parse error, watchlist seeded from
   `latest_ranking`, `service_config poll loop started`, no poll failures; since #542 an admitting
   swap or backfill is preceded by `hot-watchlist admission state prepared`).
   **With `polymarket_activity_ws_enabled=true` (#530/#546)**: two consecutive `status.json`
   publications, each paired immediately with `/health/ready`, must show all three
   `source_health.ws_readers` records, `ws_live_reader_count >= 2`, `ws_sink_poisoned=false`,
   `copy_admission_blocked=false`, and at least two readers' `normalized_activity_rows_total`
   advancing between the publications; readiness must carry none of `activity_ws_unavailable`,
   `activity_ws_redundancy_degraded`, `activity_ws_sink_poisoned`, `copy_admission_blocked`; the
   source log's size/mtime must advance (append-only file; no content inspection here). Then the
   duplicate invariant on paper state — both queries must return nothing, whatever unrelated activity
   happened meanwhile:

   ```bash
   sqlite3 -readonly paper_state.db "select source_trade_id, count(*) from fills group by 1 having count(*) > 1; select source_trade_id, count(*) from dispatch_seeds group by 1 having count(*) > 1;"
   ```

   For any watched `source_trade_id` observed after activation, tie its seen row, fill, seed, and
   targets to that identifier; otherwise record `not observed`. If the second publication cannot
   establish two live readers, roll back instead of waiting. Record the readers'
   `consecutive_reconnects` and drop cadence: a churn pattern is the evidence for any keepalive
   follow-up (the first-party client sends `ping` every 5 s; `pe-service` does not).

**Deployment one-time note (#530)**: delete the retired runtime row
`delete from service_config where key='trade_poll_interval_secs';` (boot-owned only now).

## #530 websocket rollback ordering

Disabling `polymarket_activity_ws_enabled` (or rolling back to a pre-#530 binary)
reverts observation to poll-only — proven byte-identical by scenario WS3. **Reverse
dependency order is mandatory**: while a Δ=2 ranking batch is latest, the flag stays
enabled so stale REST observations keep failing closed; first restore and publish a
Δ=20 ranking, verify the newest `ranking_batches.latency_shift_secs = 20` and that
pe-service applied that batch, and only then disable the flag or roll the binary
back. Disabling first would copy Δ=2-selected wallets at poll latency — the
padded-watchlist loss class.

## #510 restart semantics (authoritative mode)

Since #510 the authoritative catch-up watermark advances at runtime (successor-gated in
`commit_fill_authoritative`), so a healthy restart's boot catch-up is a **zero-RPC no-op** —
the boot log prints one summary line: `supabase authoritative boot: catch-up summary`
(`old_watermark` / `head` / `replayed`). Expect `replayed=0` on a healthy restart; a non-zero
count is the bounded gap-heal (an earlier RPC failure or halted boot froze the watermark) and
completes idempotently. Diagnose with
`sqlite3 paper_state.db "select key,value from meta where key like '%event_seq'"` —
`last_supabase_applied_event_seq` tracks `last_applied_event_seq` in steady state.
`--backfill-supabase` (one-time cutover tool) now performs a strict full fill sweep and
aborts before seeding any cursor if the sweep halts (rerun to resume; fully idempotent).

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
change, or a hash mismatch:

```bash
cp -p target/release/pe-service.bak-<prior-sha12> /tmp/pe-service.rollback.<prior-sha12>
mv -T --no-copy /tmp/pe-service.rollback.<prior-sha12> target/release/pe-service && sync -f target/release/pe-service
sha256sum target/release/pe-service                          # = prior
```

Revert any config deltas the old binary does not understand (unknown `service_config` keys are
ignored by old binaries — safe to leave), then `ssh -t … 'sudo systemctl restart pe-service'` and
verify per step 6. There is no schema, database, ranker, or host rollback.
