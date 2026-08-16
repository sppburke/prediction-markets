# 35 — pe-service binary deploy runbook (VPS)

> This runbook deploys ordinary `pe-service`, including the #508 per-account live path that ships
> dark over the shared paper book. It does not deploy or configure the isolated Polymarket V2
> canary; see
> [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

**Purpose.** The repeatable procedure for building the `pe-service` release binary and
deploying it to the VPS. Until this file, the procedure lived only in session memory —
fields marked *(verify)* are recorded from operator history and must be confirmed
against the live box on first use, then corrected here.

## Facts

| item | value |
|---|---|
| VPS | `82.22.32.225`, user `sean` (`ssh -i ~/.ssh/id_personal sean@82.22.32.225` — never root) |
| unit | systemd **system** unit `pe-service` — start/stop needs interactive sudo (`ssh -t … 'sudo systemctl <verb> pe-service'`); a coding agent cannot restart it non-interactively |
| binary path | `target/release/pe-service` under the service checkout *(verify via `systemctl cat pe-service` → `ExecStart`/`WorkingDirectory`)* |
| backup convention | before overwrite: `cp target/release/pe-service target/release/pe-service.bak-<old-sha>` (e.g. `pe-service.bak-a166f05`, `pe-service.bak-da17c16`) |
| env | `.env` on the VPS (REST keys `PE_SUPABASE_URL`/`PE_SUPABASE_SECRET_KEY`; **no** `SUPABASE_DB_URL` there). `PE_` booleans must be `true`/`false`, never `1`/`0` (figment rejects ints → restart loop) |
| build box | the VPS has no cargo — build on the dev box and `scp`, or build in a VPS-compatible container. Dev box: `cargo build --release -p pe-service` at the target main SHA (rustc 1.95.0) |
| logs | `journalctl -u pe-service -f` (Tier-1 prod check); JSONL sinks per `jsonl_log_path` |

## Procedure

Every step is bound to sha256 identity (#514): record **desired** = `sha256sum` of the
local release build before shipping; on the VPS, **staged** = the hash of
`/tmp/pe-service.new` and **installed** = the hash of the `ExecStart` binary.

1. **Build at the exact main SHA** being deployed (record the git SHA and the desired
   binary hash):
   `git -C /home/sean/git/prediction-markets rev-parse --short HEAD && cargo build --release -p pe-service && sha256sum target/release/pe-service`
2. **Ship**: `scp -i ~/.ssh/id_personal target/release/pe-service sean@82.22.32.225:/tmp/pe-service.new`
3. **Preflight on the VPS** (before stopping anything): require `staged = desired` — a
   mismatch or missing file means the scp is partial: re-run step 2. If
   `installed = desired` already, the swap is complete (a resumed run): skip to step 6.
4. **Stop** (operator, interactive sudo): `ssh -t … 'sudo systemctl stop pe-service'`
5. **Backup + swap** (on the VPS, service stopped; `<old-sha>` = the git short SHA the
   installed binary was built from, falling back to the first 12 hex of its sha256 when
   unknown — the sha-derived name makes the backup once-only, so a rerun never clobbers
   it):
   `[ -f <workdir>/target/release/pe-service.bak-<old-sha> ] || cp -p <workdir>/target/release/pe-service <workdir>/target/release/pe-service.bak-<old-sha>`
   `mv /tmp/pe-service.new <workdir>/target/release/pe-service && chmod +x <workdir>/target/release/pe-service`
   then require `installed = desired` before proceeding.
6. **Config deltas for this deploy** (see each PR's "Deployment impact"): update `.env` /
   TOML boot knobs (e.g. `PE_WATCHLIST_MEMBERSHIP_MODE=full_rerank`) and PATCH/INSERT the
   live `service_config` rows the new binary reads (seed `on conflict do nothing` never
   updates an existing row — changed defaults need a manual `PATCH`).
7. **Start**: `ssh -t … 'sudo systemctl start pe-service'` — a failed start rolls back
   from the step-5 backup (see Rollback).
8. **Verify**: `journalctl -u pe-service -n 100` — clean boot (no config-parse error /
   restart loop), watchlist seeded from `latest_ranking`, `service_config poll loop
   started`, no poll failures; then confirm behavior-specific log lines for the deploy
   (e.g. the first `full re-rank membership swap applied` after a ranking push).

An interrupted deploy is resumed by re-running from step 3: the hash comparisons decide
whether to re-ship, re-swap, or only start and verify — never guess from memory.

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
Phase-D binary in that same stop/swap/start operation. Its first boot records the arming fence, so no
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

Stop the service, `mv target/release/pe-service.bak-<old-sha>` back over the binary,
revert any config deltas the old binary does not understand (unknown `service_config`
keys are ignored by old binaries — safe to leave), start, verify boot per step 7.
