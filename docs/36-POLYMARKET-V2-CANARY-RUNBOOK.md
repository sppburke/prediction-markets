# 36 — Polymarket V2 Winner-Follow canary runbook

> This runbook installs the isolated canary **inactive**. It does not authorize wallet creation,
> funding, allowance changes, arming, or an order POST. Campaign and per-stage authorities are
> separate operational approvals bound to the exact artifacts described here.

## Boundary

`pe-service-live-canary` is independent of ordinary paper `pe-service`: it has a dedicated Unix
account, binary, state root, runtime root, event log, socket, resolver inventory, and systemd
credential set. It cannot select `live_tiny` or `promoted`; the Supabase anon credential supplies
only the ordinary read-only watchlist and cannot arm or alter financial policy.

The canonical financial limits and lifecycle are in
[`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md). The unit must remain disabled and
stopped at the end of an implementation deployment.

## Build and stage the exact revision

On the build host, from a clean checkout of the reviewed `main` commit:

```bash
git rev-parse HEAD
rustc --version
cargo build --locked --release -p pe-service --bin pe-service-live-canary
git diff --exit-code
```

Record the full commit in the deployment evidence. Copy the binary and
`deploy/systemd/pe-service-live-canary.service` to root-controlled staging paths on the target;
do not replace a running binary.

## Install without starting

The following is an operator procedure requiring root authority; it is not part of an ordinary
development cycle.

1. Create the locked service identity with no login shell or home:
   `useradd --system --no-create-home --shell /usr/sbin/nologin prediction-edge-canary`.
2. Install the reviewed binary at `/usr/local/bin/pe-service-live-canary`, owned by root and mode
   `0755`.
3. Install the unit at `/etc/systemd/system/pe-service-live-canary.service`, owned by root and mode
   `0644`. Run `systemd-analyze verify` and `systemctl daemon-reload`.
4. Create `/etc/prediction-edge-canary` owned by root and mode `0700`. Credential source files are
   root-owned mode `0600`. Use
   [`canary-boot-config.example.json`](../deploy/systemd/canary-boot-config.example.json) as the
   schema guide, replacing every placeholder. The five Polymarket credential files, deposit-wallet
   address, and Supabase URL/anon key are each single-value files named exactly as the unit's
   `LoadCredential=` entries. Never use a service-role Supabase key.
5. Run `systemctl is-enabled pe-service-live-canary`; `disabled` or `static` is expected. Do not
   enable or start it.

Installing credential source files does not authorize starting the daemon. Operational authority
must separately approve the exact implementation commit, binary/config/SDK/resolver hashes,
wallet, owner signer, spender, jurisdiction/account attestations, allowance, and expiry.

## Resolver installation

Validate each root-reviewed resolver card offline before copying it into the private state tree:

```bash
pe-service-live-canary resolver-card validate-install INPUT.json OUTPUT.json
```

Install validated cards as
`/var/lib/pe-service-live-canary/resolvers/<condition_id>.json`, owned by
`prediction-edge-canary:prediction-edge-canary`, directory mode `0700`, file mode `0600`. Record a
deterministic inventory hash over the sorted filenames and bytes; the campaign authority must match
the daemon's computed inventory hash exactly.

After the boot config and complete resolver inventory are installed, obtain the versioned identity
from the installed executable itself:

```bash
/usr/local/bin/pe-service-live-canary artifact-identity \
  /etc/prediction-edge-canary/canary-boot-config.json \
  /var/lib/pe-service-live-canary/resolvers
```

Use this JSON verbatim when preparing later authority: it identifies the actual executable and boot
config bytes with BLAKE3, the sorted resolver inventory with BLAKE3, the upstream SDK archive with
SHA-256, and the effective reviewed vendor tree with SHA-256. Do not substitute path names, Git
abbreviations, or an independently chosen hashing recipe.

## Inactive verification

Before any separately authorized start, verify:

```bash
systemctl is-enabled pe-service-live-canary
systemctl is-active pe-service-live-canary
systemctl cat pe-service-live-canary
stat -c '%U %G %a %n' /etc/prediction-edge-canary /usr/local/bin/pe-service-live-canary
```

After an authorized daemon start—but before arming—`status` must show `inactive`, no campaign, no
pending attempt, zero slots consumed, and an unlatched kill state. Verify `/var/lib` and `/run`
service directories are mode `0700`; `canary.log`, `status.json`, and `control.sock` are mode `0600`.

## Operator commands

All commands are thin clients to the resident daemon. Set `PE_CANARY_COMMAND_ID` to a stable unique
ID when retrying the same state-changing command; reuse of an ID with different command content is
rejected. Authority JSON files remain root-controlled and contain no private key or API secret.

```bash
pe-service-live-canary status
pe-service-live-canary arm-probes CAMPAIGN_AUTHORIZATION.json
pe-service-live-canary reconcile
pe-service-live-canary probe-buy PROBE_AUTHORIZATION.json
pe-service-live-canary review-probe CAMPAIGN_ID PROBE_ORDINAL
pe-service-live-canary advance-organic ORGANIC_STAGE_AUTHORIZATION.json
pe-service-live-canary kill
```

`arm-probes`, each `probe-buy`, and `advance-organic` require distinct later authority. A successful
pre-reservation skip consumes no slot. Any synced reservation consumes its slot and worst-case
commitment permanently. Never retry a timed-out or ambiguous POST; use `reconcile` and remain closed.

## Observation and shutdown

Inspect sanitized status and the service journal. Never copy the mode-0600 event log into a general
log system: raw authenticated responses can contain owner identifiers. SIGINT and SIGTERM latch
admission and request one bounded final reconciliation; systemd's 50-second stop timeout is only the
outer guard around the application's 45-second deadline.

## Rollback and recovery

1. Invoke `kill` and wait for its synced acknowledgment; then invoke `reconcile`.
2. Stop and mask the unit. Do not delete or truncate the event log, resolver cards, status snapshot,
   authority artifacts, or unresolved inventory evidence.
3. Preserve credentials only when authenticated no-admission reconciliation is required. Otherwise
   remove the credential source files through the approved secret-handling process.
4. Preserve filled inventory in `closed_observing` until settlement and any external redemption are
   authoritatively reconciled. Redemption is not performed by this binary.
5. Restore a prior reviewed V2 binary only if its authority and journal compatibility remain valid.
   Never restore or run the retired V1 path with live credentials.

Rollback does not recover consumed slots or commitment and never authorizes funding, allowance
mutation, another campaign, or ordinary live mode.
