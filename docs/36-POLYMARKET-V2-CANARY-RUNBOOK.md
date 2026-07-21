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

## Install non-secret artifacts without starting

The following is an operator procedure requiring root authority; it is not part of an ordinary
development cycle.

1. Create the locked service identity with no login shell or home:
   `useradd --system --no-create-home --shell /usr/sbin/nologin prediction-edge-canary`.
2. Install the reviewed binary at `/usr/local/bin/pe-service-live-canary`, owned by root and mode
   `0755`.
3. Install the unit at `/etc/systemd/system/pe-service-live-canary.service`, owned by root and mode
   `0644`. Run `systemd-analyze verify` and `systemctl daemon-reload`.
4. Create `/etc/prediction-edge-canary` owned by root and mode `0700`. Do not place the boot config,
   private key, API credentials, deposit-wallet address, or Supabase values during this non-secret
   installation phase.
5. Run `systemctl is-enabled pe-service-live-canary`; `disabled` or `static` is expected. Do not
   enable or start it.

Installation does not authorize wallet creation, secret placement, or starting the daemon.
Operational authority must separately approve the exact implementation commit,
binary/config/SDK/resolver hashes, wallet, owner signer, spender, jurisdiction/account attestations,
allowance, and expiry.

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

## Finalize artifact identity, then place secrets

After the final non-secret boot config and complete resolver inventory are installed, but before
placing the seven secret single-value sources, obtain the versioned identity from the installed
executable itself. Use
[`canary-boot-config.example.json`](../deploy/systemd/canary-boot-config.example.json) as the schema
guide, replacing every placeholder. `jurisdiction` is the reviewed egress country as an uppercase
ISO 3166-1 alpha-2 code (for example, `IE`), not a country name or datacenter label:

```bash
/usr/local/bin/pe-service-live-canary artifact-identity \
  /etc/prediction-edge-canary/canary-boot-config.json \
  /var/lib/pe-service-live-canary/resolvers
```

Use this JSON verbatim when preparing later authority: it identifies the actual executable and boot
config bytes with BLAKE3, the sorted resolver inventory with BLAKE3, the upstream SDK archive with
SHA-256, and the effective reviewed vendor tree with SHA-256. Do not substitute path names, Git
abbreviations, or an independently chosen hashing recipe. `artifact-identity` does not read or hash
private keys, API credentials, deposit-wallet files, or Supabase values.

At reconciliation, the daemon requires the same-egress geoblock response country to match this
authority-bound `jurisdiction`. Under the current official Polymarket contract, IE, JP, MT, and NL
are close-only on the frontend but unrestricted at the API, so `blocked: true` alone does not close
API admission for those four matching country codes. Other blocked countries remain closed;
missing, malformed, or mismatched location evidence closes admission. The authenticated account
`closed_only` response is a separate gate and closes admission whenever true.

Under separate credential-placement authority, install the five Polymarket single-value files
(private key, API key, API secret, API passphrase, and deposit-wallet address) and the Supabase URL
and anon key. Each source is named exactly as the remaining unit `LoadCredential=` entries,
root-owned, and mode `0600`. Never use a service-role Supabase key. Secret placement does not
authorize starting the daemon; the later daemon boot and campaign authority independently validate
the public wallet, owner signer, and spender bindings. Keep the unit stopped and disabled/static.

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

## Prepare authority hashes offline

These commands only read the named non-secret JSON file and print one lowercase BLAKE3 hash. They do
not read credentials, contact the daemon or network, inspect the event log, or authorize an action:

```bash
pe-service-live-canary authority-hash probe PROBE_AUTHORIZATION.json
pe-service-live-canary authority-hash reviewed-probes REVIEWED_PROBE_HASHES.json
```

For a probe, set the required `authority_hash` field to `""`, run the first command, replace the
field with the printed hash, run the command again, and require the same output. The declared field
is deliberately excluded from the canonical probe hash. The reviewed-probes input is exactly the
ordered JSON string array from sanitized state; changing the order changes the bundle hash. Hashes
are reviewed inputs for later authority artifacts, never authorization by themselves. Authority
files remain root-controlled and are not systemd credentials.

## Operator commands

The commands below are thin clients to the resident daemon. Before each `reconcile` or mutating
command, set and record a stable unique `PE_CANARY_COMMAND_ID`; reuse that ID only when retrying the
identical command. Reuse with different command content is rejected. `status`, `artifact-identity`,
resolver validation, and authority hashing do not use command receipts. Authority JSON files remain
root-controlled and contain no private key or API secret.

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

1. Invoke `kill` with its recorded command ID and wait for its synced acknowledgment; then invoke
   `reconcile` with a distinct recorded command ID.
2. Require authoritative evidence for every pending or ambiguous order, filled position,
   settlement, redemption, allowance, and cash balance. Do not delete or truncate the event log,
   resolver cards, status snapshot, authority artifacts, credentials needed for recovery, or
   unresolved inventory evidence.
3. If filled or unresolved inventory remains, keep the campaign in no-admission
   `closed_observing`. Under explicit recovery authority, leave the daemon running closed or perform
   only bounded recovery starts and authenticated reconciliation at reviewed checkpoints. Recovery
   never restores admission, slots, or commitment and never authorizes a POST.
4. Stop and mask the unit only after authoritative reconciliation proves there is no pending or
   ambiguous order, open debit, unresolved recovery, or filled inventory awaiting settlement or
   separately authorized external redemption. Then remove credential sources through the approved
   secret-handling process. Redemption is not performed by this binary.
5. Restore a prior reviewed V2 binary only if its authority and journal compatibility remain valid.
   Never restore or run the retired V1 path with live credentials.

Rollback does not recover consumed slots or commitment and never authorizes funding, allowance
mutation, another campaign, or ordinary live mode.
