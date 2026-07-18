# Vendored provenance

- Crate: `polymarket_client_sdk_v2` 0.7.0
- Registry: crates.io
- Registry archive SHA-256: `ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`
- Upstream repository: <https://github.com/Polymarket/rs-clob-client-v2>
- Upstream commit: `222143d321eba97d5711a848265eb9aab3bc7ff4`
- License: MIT (`LICENSE`)

Repository-local delta is limited to the canary's auditable adapter seams:

- the SDK's Alloy dependency is pinned from `^1.6.3` to exact `=1.6.3` so the reviewed transitive
  protocol/signing graph cannot drift under a lockfile refresh;
- `auth` publicly re-exports the SDK's exact transitive `Signer` trait so the repository does not
  add a direct renamed Alloy dependency;
- the common request path supports a raw response observer before status handling/parsing;
- the added request-observation helpers retain the upstream feature gates so the crate's empty
  default feature set continues to compile;
- authenticated raw one-shot reads expose geoblock, closed-only mode, exact order-hash lookup, open
  orders, trades, and balance/allowance bytes without changing parsing or retry policy, and accept
  an optional workflow deadline at the exact built-request seam so an active timeout retains its
  request identity;
- `post_order_once_raw` authenticates and sends the caller-supplied serialized order exactly once,
  with no transaction-hash polling, version-mismatch retry, batching, or replacement;
- `cancel_order_once_raw` provides the same exact-response, no-retry boundary for defensive
  cancellation of an unexpected live FOK order; and
- `standard_v2_order_hash` exposes the SDK's existing V2 EIP-712 hash derivation for adapter-side
  reconstruction checks.

No local change alters signing, amount rounding, order building, protocol fields, or response
parsing. The exact delta is mechanically compared with the registry archive during release review.
