# systemd units

`pe-rank-loop.service` is the user-mode unit that owns the forge ranking loop
supervisor (`scripts/rank_and_push_loop.sh`, issue #521). The separate
`pe-service-live-canary.service` is a system unit for the isolated, initially disabled
Polymarket V2 canary. It uses a dedicated unprivileged account, systemd credentials, and private
state/runtime directories; it never reads the ordinary service `.env`.

Do not enable or start the canary as part of installation. Follow
[`docs/36-POLYMARKET-V2-CANARY-RUNBOOK.md`](../../docs/36-POLYMARKET-V2-CANARY-RUNBOOK.md).

Ordinary `pe-service` live credential custody (#508) follows the same root-controlled source-file
precedent without sharing canary secrets: install the committed drop-in that maps the root-owned
mode-`0600` age identity as `LoadCredential=pe-age-identity:<identity-path>`. The per-account bundles
remain age-sealed in Supabase and are never environment variables. Install, verify, and arm only via
[`docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md`](../../docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md).

## pe-rank-loop

User-mode service that runs the continuous ranking loop supervisor. The
supervisor reads scripts and the `pe-bootstrap` binary from the working tree
each cycle, so the unit assumes the repository checkout at `~/prediction-markets`
with `target/release/pe-bootstrap` built. Lifecycle, rollout order, flag
semantics, and verification live in
[`docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md`](../../docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md)
("Continuous Forge supervisor").

## Install

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/pe-rank-loop.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemd-analyze --user --recursive-errors=yes verify ~/.config/systemd/user/pe-rank-loop.service
sudo loginctl enable-linger "$USER"   # one-time: keep the user manager alive across boots

# Arm the flag BEFORE starting — a missing or `stop` flag makes the supervisor
# exit cleanly at once, leaving the unit inactive.
cd ~/prediction-markets
loop_flag_tmp="data/eval-results/.rank_and_push.loop.$$"
printf 'run\n' > "$loop_flag_tmp"
mv "$loop_flag_tmp" data/eval-results/rank_and_push.loop

systemctl --user enable --now pe-rank-loop
```

## Verification

```bash
systemctl --user is-enabled pe-rank-loop
systemctl --user is-active pe-rank-loop
journalctl --user -u pe-rank-loop -n 100
```
