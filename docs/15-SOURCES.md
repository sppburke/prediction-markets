# 15 — Sources and Implementation References

> Re-verify per the staleness rule in [`21-RESEARCH-AND-SOURCE-DISCOVERY.md`](21-RESEARCH-AND-SOURCE-DISCOVERY.md).

## Per-class re-verification TTL

| Class | TTL | Notes |
|---|---:|---|
| Venue API reference (Polymarket / Kalshi) | 60 days | Official `docs.*` references |
| Venue help-center market rules | 14 days | Help-center pages change without versioning |
| Public source resolver pages | 60 days | NWS, BLS, Spotify chart pages |
| Public chain provider docs | 90 days | Polygon RPC, archive provider |
| Rust ecosystem docs | 180 days | tokio, axum, polars, etc. |
| AWS service docs | 90 days | ECS, IAM, Secrets Manager |
| Third-party research references | re-check before use | CrowdIntel, etc. |

`Last checked` and `Re-verify by` columns are added per link below as research passes complete.

## Kalshi

| Link | Last checked | Re-verify by |
|---|---|---|
| https://docs.kalshi.com/welcome | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/getting_started/quick_start_websockets | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/getting_started/quick_start_market_data | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/getting_started/orderbook_responses | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/api-reference/orders/get-order-queue-position | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/api-reference/order-groups/create-order-group | 2026-05-07 | 2026-07-06 |
| https://docs.kalshi.com/changelog | 2026-05-07 | 2026-07-06 |
| https://help.kalshi.com/en/articles/13823837-weather-markets | — | — |
| https://help.kalshi.com/en/articles/13823838-crypto-markets | — | — |
| https://help.kalshi.com/en/articles/13823839-spotify-markets | — | — |
| https://help.kalshi.com/en/articles/13823840-netflix-markets | — | — |
| https://help.kalshi.com/en/articles/13823841-top-app-markets | — | — |
| https://help.kalshi.com/en/articles/13823807-what-are-trading-hours | — | — |
| https://help.kalshi.com/en/articles/13823851-liquidity-incentive-program | — | — |
| https://help.kalshi.com/en/articles/13823820-combos | — | — |

## Polymarket

> **SUPERSEDED (2026-08-25, issue #530 re-verification): RTDS DOES carry attributed trades.**
> The 2026-06-03 conclusion below no longer holds for `wss://ws-live-data.polymarket.com`:
> subscribing `{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}`
> streams **every platform trade with `proxyWallet`** (the identity axis `latest_ranking`
> ranks), plus `conditionId`, `asset`, `outcome`/`outcomeIndex`, `price`, `size`, `side`,
> `timestamp` (seconds, as a string), `transactionHash`, and profile fields; `fee` is
> optional per trade. No authentication. Measured 2026-08-24: p50 0.80s / p95 1.32s /
> p99 1.41s trade-timestamp→receipt over 6,293 trades; a 14h soak found the stream live
> only ~113/840 minutes on ping-alive sockets (subscription lapses silently — 1,442
> thirty-second silences vs 16 hard disconnects). Read-only experiments on 2026-08-31 (#546)
> showed the failure is connection-local: one of three or four parallel connections stalled
> while the others kept delivering every watched row, re-sending the subscription did not
> revive a stalled connection, a fresh connection delivered its first trade 0.6 s after a
> 187 ms handshake, and the largest activity gap on a healthy connection was 4.775 s. So
> `pe-service` runs `activity_ws_reader_count` independent readers with a
> `activity_ws_normalized_activity_timeout_secs` normalized-row timeout and direct reconnect
> (`_GLOSSARY.md`); the always-on REST poll remains the whole-process backstop. This is an
> **officially listed real-time data endpoint** (`docs.polymarket.com/getting-started/api`,
> "Real-Time Data") **whose activity subscription and payload are published by the
> first-party client** (`github.com/Polymarket/real-time-data-client`, `src/client.ts`:
> `DEFAULT_HOST`, `{"action":"subscribe",…}`, and a 5 s literal `ping` keepalive that
> `pe-service` deliberately does not send — no evidence ties it to delivery), **without
> published completeness, uptime, ordering, continuity, or resume guarantees**: re-check the
> endpoint, subscription shape, and payload keys before each deploy that relies on it
> (`scripts/probe_activity_ws.py`). Owner: `source-polymarket-public::activity_ws`
> (transport/envelope/constants) and `pe-service::activity_ingest` (readers, liveness,
> fan-in); the service's `trade_parser` normalizes identically to the REST path. The CLOB
> market channel remains wallet-anonymous — the note below stands for THAT feed.
> Last checked: 2026-08-31.
>
> **Historical (2026-06-03, issue #282 Phase 2 verification — CLOB channel still true; RTDS part superseded above).**
> `wss://ws-subscriptions-clob.polymarket.com/ws/market` (`last_trade_price` events) does not include the **wallet address**; wallet-level trade identification is impossible from the frame alone.
>
> **Correction (2026-06-09, issue #300 live capture).** A live `last_trade_price` market-channel frame **does** carry `transaction_hash` — verified against 2,581 captured frames (100% present, one unique hash per print, zero collisions). The earlier note that it omits `transaction_hash` is superseded; only the wallet address is absent, so wallet-level identity still requires an on-chain tx lookup, but the print is uniquely keyable. Full frame shape: `{market, asset_id, price, size, side, timestamp, fee_rate_bps, event_type, transaction_hash}` — `market` is the condition_id (authoritative, present on every print). `pe-crypto-shadow` keys `clob_trades` on `transaction_hash` and attributes via `market`.
>
> **Heartbeat contract (2026-06-10, issue #317, `wss-overview`).** The CLOB market **and** user channels require an **application-level** heartbeat: the client sends the text message `PING` every ~10s and the server replies the text `PONG`; the troubleshooting section attributes "connection drops after about 10 seconds" to a missing heartbeat. This is **not** a WS protocol Ping frame — it is a literal text payload. `pe-crypto-shadow` sends `Message::Text("PING")` every `crypto_shadow_clob_ping_interval_secs` (10) and filters the `PONG` reply (case-insensitive — sports channel lowercases it) out of the frame stream. The same page documents dynamic-subscription ops `{"operation":"subscribe"|"unsubscribe"}`; the harness intentionally does not use `unsubscribe` (prune applies at the next reconnect — a filed follow-up). The heartbeat lives only on this overview page, which is why it was missed until #317 (the `market-channel` page, checked 2026-06-09, has no heartbeat section).
>
> **`/activity` indexing latency measured (2026-06-14).** The REST `data-api.polymarket.com/activity` feed (the wallet-attributed FALLBACK source since #530; the activity websocket above is primary) indexes a trade within **~1–4s of its `timestamp`** — proven lower bound p50 1.2s / p95 3.8s / p99 5.2s / max 23s, from 4,655 live trades (`scripts/measure_activity_latency.py`, jitter-free lower-bound method). End-to-end FALLBACK-path copy latency (indexing + the 30s production poll + order placement) ≈ **~20–35s**, so a **1-minute** TTR floor is reliably copyable for near-resolution first-bets. Full methodology + run record: `docs/29-ACTIVITY-LATENCY-MEASUREMENT.md`.
>
> **CLOB `/book` is public/no-auth (2026-06-16, issue #350 WS2 PR-G live re-confirm).** `GET https://clob.polymarket.com/book?token_id=<id>` returns HTTP **200 with no auth header**; a bogus token id returns **404** `{"error":"No orderbook exists for the requested token id"}`. Body shape: `asks`/`bids` are arrays of `{price, size}` where **both fields are strings** (Decimal-safe), plus scalar string meta (`market`, `asset_id`, `tick_size`, `min_order_size`, `last_trade_price`, `timestamp`, `hash`) and `neg_risk` (bool). Asks are returned **high→low price** (best/lowest ask is not at index 0). `crates/service/src/clob_book.rs` parses the ask side only (liquidity capture is buy-only) into `Decimal`, deriving `best_ask` as the minimum price.
>
> **CLOB `/markets?closed=true` does NOT populate `tokens[].winner` on old markets (2026-06-18, issue #369 PR1 live reconciliation).** Response: `{count, limit, next_cursor, data:[…]}`; each market carries snake_case `condition_id`, `closed`, `end_date_iso`, and `tokens:[{outcome, price, token_id, winner}]`; `next_cursor` terminator is `LTE=`. **Gotcha:** for old markets (≈2022–2023) the resolved winner is reflected only in the terminal token `price` (~1.0 winner / ~0.0 loser) — `tokens[].winner` is `false` on **every** token, so `clob.rs::winner_index` reports the market as voided. Measured against `source='polygon'` over 848,098 traded markets: 0 winner *contradictions* (99.976% agreement) but 202 CLOB-null-vs-polygon-winner, **all** in 2022–2023; by 2024 winner-flag coverage is complete (2024 0/11,674, 2025 2/125,388, 2026 10/709,318 null). Benign for issue #369 because the 1.19M `source='polygon'` rows are kept; CLOB only needs correctness for new markets. Also confirmed: 0 traded multi-outcome (>2-token) markets, so the positional `winner_index`↔`outcome_id` mapping risk is empirically binary-only.

> **Schema-one bootstrap history contract (#608), Last checked: 2026-09-12;
> re-verify by 2026-11-11.** The [official activity reference](https://docs.polymarket.com/api-reference/core/get-user-activity)
> confirms that omitted/zero `start` on DESC reads uses the most recent roughly
> three years; positive `start=1` reaches full history. Each request retains its
> bounded `end`. Stable DESC offset ordering is documented, page size is capped
> at 500, and offsets above 5,000 return 400. Bootstrap therefore completes every
> boundary second before stepping below it, using inclusive `start`/`end` as
> recorded by the reconciliation check below. A full terminal single-second
> page is incomplete, never a success. The walk freezes its upper bound and
> rechecks the previous maximum second before extending; its gap-free guarantee
> covers stable API-visible history, not arbitrary late indexing into older
> seconds (see the measured indexing latency above). This check used current
> official documentation; no fresh live-wallet completeness claim is made.

> **Data-API reconciliation contract re-verified live (2026-09-03, issues #544/#555/#557).**
> `/activity` accepts one comma-separated `type` parameter: the production request
> `TRADE,SPLIT,MERGE,REDEEM,CONVERSION` returned mixed position-changing activity in one response.
> Its documented maximum offset is 5,000. `/positions` accepts `limit=500`, maximum offset
> 10,000, and the deterministic `sizeThreshold=0&sortBy=TOKENS&sortDirection=ASC` request used by
> the reconciliation reader. Explicit `redeemable=false` and `redeemable=true` walks are disjoint
> partitions and must both complete; no omitted-filter inference substitutes for either partition.
> A live #557 incident-wallet check observed `/positions` `size` at four decimal places
> (`34.0795`), while the same wallet's incident `/activity?type=REDEEM` `size` carried five decimal places.
> Across the seven incident anchors, position balances carried at most four decimal places. This is
> an observed source precision, not a parser rounding rule: the reader retains the exact lexical
> decimal, and the canonical REDEEM residual bound and rationale live in `docs/_GLOSSARY.md`.
> Captured URLs, fetch times, byte lengths, and SHA-256 hashes are recorded in the issue-#544
> fixture `MANIFEST.json`. **`/activity` `start` is INCLUSIVE (2026-09-01, #544 activation
> rehearsal):** a request with `start=S` returns rows whose `timestamp` equals `S`, proven live
> when a saturated-window split re-received boundary-second rows. The reconciliation reader keeps
> its exclusive `(start, end]` window contract internally and puts `start + 1` on the wire
> (`crates/source-polymarket-public/src/reconciliation.rs`); `end` remains inclusive as observed.
> **`/activity` REDEEM rows may omit the outcome (2026-09-01, #544 activation rehearsal + 79-wallet
> sweep):** some redemptions carry `outcomeIndex: 999` with an empty `outcome` label, empty `side`,
> and empty `asset` — 191 of 299,175 rows across 17 of 79 live watchlist wallets, every one
> winner-priced (`usdcSize == size`). Combo ("A AND B") redemptions use the same sentinel with a
> zero-padded composite condition id. Batch redemption transactions carry sibling REDEEM legs for
> other markets, so the sentinel row is the complete record for its condition. Zero-size REDEEM legs
> and `outcomeIndex: 999` sentinel rows do not identify the burn scope: the venue's redemption call
> accepts arbitrary index sets, and the public feed carries no calldata. Combo rows stay raw-only;
> ordinary rows with unknowable burn scope require a positions re-anchor rather than any inferred
> ledger effect. **On multi-outcome markets, one asset id can appear with two different
> `outcomeIndex` values:** for wallet `0x180e62e6…` and condition `0xd21e5817…`, four rows carry
> index 1 and three carry index 0. Activity `outcomeIndex` is therefore not a reliable identity on
> those markets, and the bracket defers such wallets. The `/positions` endpoint, walked through both
> `redeemable` partitions with `sizeThreshold=0&includeArchived=true`, reports the wallet's true
> current balances and is the authority for absolute balances at an anchor (issue #555).

> **Data API v2 captured official contracts (2026-09-10, issue #588).** The
> [account activity feed](https://docs.polymarket.com/api-reference/feeds/list-account-activity)
> uses keyset pagination on `(block_timestamp, sequence_id)`.
> [Data freshness](https://docs.polymarket.com/api-reference/service/get-data-freshness)
> is served from a background-refreshed snapshot; `computed_at` and `age_seconds` expose the
> snapshot's age, and the endpoint returns `503` before its first refresh. The
> [migration guide](https://docs.polymarket.com/api-reference/data-api/migrating-from-v1)
> says the original routes remain available and this Data API migration is unrelated to CLOB V2.
> Neither the activity nor freshness page establishes immutable bucket closure or two-second
> execution. The service retains its existing `/activity` interface. Captured pages:
> `/home/sean/reports/issue-588-plan-2026-09-10/review/{list-account-activity,get-data-freshness,migrating-from-v1}.md`.

> **Gamma `/markets?clob_token_ids=` is the asset identity authority (2026-09-02, issue #555 addendum, live probes).** The venue's activity feed mis-stamps individual rows: on wallet `0x0857…` / market `0xba9968…` six TRADE rows carried asset `9361629…` as `outcomeIndex 0` ("Yes") and one row carried the same asset as `outcomeIndex 1` while still labeled "Yes". Gamma resolves the token unambiguously (`clobTokenIds[0]` = that asset on a binary market), so a token's `(conditionId, outcome index)` comes from the market's `clobTokenIds` array position, never from activity rows alone. Query form: repeat-key `clob_token_ids=A&clob_token_ids=B…` (comma-joined ids are a validation error, `{"type":"validation error","error":"invalid clob token ids"}`); the plain query returned nothing for the sampled closed tokens and `&closed=true` returned their markets, so the open variant is tried first and the closed variant for leftovers. Gamma carries no combo classification: `Ordinary|Combo` stays activity-derived and must be unanimous per asset, and a position-only asset (never in activity) is deferred, not guessed. Every fetched metadata page is recorded through the source log before its identities are used.

> **Gamma `/markets?condition_ids=` + CLOB `/markets?closed=true` — UA blocklist & repeat-key batching (2026-06-20, issue #382 Phase-0 live probe `scripts/probe_gamma_ua.py`).** The `&closed=true` 403 is triggered by the literal `Python-urllib/*` default User-Agent (an anti-bot blocklist), **not** by a missing browser UA: both endpoints return **200** for a headerless request (a bare `reqwest::Client` = the shipped Rust clients), an empty UA, a product UA (`prediction-edge/1.0`), and a browser UA — and **403 only** for `Python-urllib/3.11`. The shipped UA-less clients therefore do not 403, but this transport health did not prevent the 2026-06-24 through 2026-08-21 persisted-terminator cursor wedge from stopping repeat resolution ingestion. Repeat-key batching (`?condition_ids=A&condition_ids=B…&limit=500`) works for **both** the plain (open) and `&closed=true` Gamma variants — 50/50 and 100/100 returned, demux-by-`conditionId` clean, no cross-market leak; comma-separated joining returns 0 (repeat-key mandatory); observed batch cap ≥ 100 (kept at `gamma_batch_size`=50). This supersedes the stale `crates/bootstrap/src/gamma.rs:5-6` "batching fails silently" claim for the repeat-key form.

| Link | Last checked | Re-verify by |
|---|---|---|
| https://docs.polymarket.com/developers/CLOB/websocket/wss-overview | 2026-06-10 | 2026-08-09 |
| https://docs.polymarket.com/ | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/llms.txt | — | — |
| https://docs.polymarket.com/market-data/websocket/overview | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/market-data/websocket/market-channel | 2026-06-09 | 2026-09-01 |
| https://docs.polymarket.com/market-data/websocket/user-channel | 2026-06-03 | 2026-09-01 |
| https://docs.polymarket.com/market-data/websocket/sports | — | — |
| https://docs.polymarket.com/market-data/websocket/rtds | 2026-06-03 | 2026-09-01 |
| https://docs.polymarket.com/v2-migration | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/overview | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/orders/create | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/trading/fees | 2026-09-05 | 2026-11-04 |
| https://docs.polymarket.com/builders/fees | 2026-09-05 | 2026-11-04 |
| https://docs.polymarket.com/trading/deposit-wallets | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/wallets-auth | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/trading/positions/manage | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/trading/clients/l2 | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/concepts/order-lifecycle | — | — |
| https://docs.polymarket.com/api-reference/authentication | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/geoblock | 2026-07-20 | 2026-09-18 |
| https://docs.polymarket.com/api-reference/introduction | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/polymarket-101 | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/deposit | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/supported-assets | — | — |
| https://docs.polymarket.com/trading/bridge/status | — | — |
| https://docs.polymarket.com/resources/contracts | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/api-reference/relayer/submit-a-transaction | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/api-reference/relayer/get-relayer-address-and-nonce | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/api-reference/relayer/get-a-transaction-by-id | 2026-08-11 | 2026-10-10 |
| https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings | 2026-05-04 | 2026-07-03 |
| https://docs.polymarket.com/api-reference/core/get-user-activity | 2026-09-12 | 2026-11-11 |
| https://docs.polymarket.com/api-reference/feeds/list-account-activity | 2026-09-10 | 2026-11-09 |
| https://docs.polymarket.com/api-reference/service/get-data-freshness | 2026-09-10 | 2026-11-09 |
| https://docs.polymarket.com/api-reference/data-api/migrating-from-v1 | 2026-09-10 | 2026-11-09 |
| https://clob.polymarket.com/markets?closed=true | 2026-09-01 | 2026-10-31 |
| https://docs.polymarket.com/api-reference/markets/list-markets | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/events/list-events | 2026-07-28 | 2026-09-26 |
| https://docs.polymarket.com/api-reference/markets/get-market-by-id | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/markets/get-clob-market-info | 2026-09-05 | 2026-11-04 |
| https://docs.polymarket.com/api-reference/market-data/get-order-book | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/trade/get-user-orders | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/trade/get-trades | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user | 2026-09-03 | 2026-11-01 |
| https://docs.polymarket.com/concepts/resolution | 2026-09-01 | 2026-10-31 |
| https://docs.polygon.technology/pos/reference/rpc-endpoints | 2026-09-05 | 2026-12-04 |
| https://docs.polygon.technology/pos/concepts/finality/finality | 2026-09-05 | 2026-12-04 |
| https://polygon.publicnode.com (`eth_chainId`, `eth_getTransactionReceipt`, `eth_getBlockByNumber`) | 2026-09-05 | 2026-12-04 |
| https://docs.polymarket.com/api-reference/tags/get-tag-by-id | 2026-07-18 | 2026-09-16 |
| https://clob.polymarket.com/markets/{condition_id} | 2026-09-01 | 2026-10-31 |
| https://clob.polymarket.com/book?token_id={tokenId} | 2026-07-18 | 2026-09-16 |
| https://clob.polymarket.com/prices-history?market={tokenId} | 2026-09-01 | 2026-10-31 |
| https://clob.polymarket.com/prices-history?market={tokenId}&startTs={start}&endTs={cutoff}&fidelity=1 | 2026-09-05 | 2026-11-04 |
| https://docs.polymarket.com/api-reference/markets/get-prices-history | 2026-09-01 | 2026-10-31 |
| https://gamma-api.polymarket.com/markets?condition_ids={id}&include_tag=true | 2026-07-18 | 2026-09-16 |
| https://gamma-api.polymarket.com/markets?clob_token_ids={token}&closed=true | 2026-09-02 | 2026-11-01 |
| https://docs.polymarket.com/developers/clob/markets | 2026-05-12 | 2026-07-11 |

## Public sources

| Link | Last checked | Re-verify by |
|---|---|---|
| https://polygon.technology/ | 2026-05-07 | 2026-08-05 |
| https://www.weather.gov/ | — | — |
| https://aviationweather.gov/data/api/ | — | — |
| https://rapidrefresh.noaa.gov/hrrr/ | — | — |
| https://vlab.noaa.gov/web/mdl/nbm | — | — |
| https://earthquake.usgs.gov/earthquakes/feed/ | — | — |
| https://www.nhc.noaa.gov/ | — | — |
| https://firms.modaps.eosdis.nasa.gov/ | — | — |
| https://www.bls.gov/ | — | — |
| https://www.bea.gov/ | — | — |
| https://www.census.gov/ | — | — |
| https://www.federalreserve.gov/ | — | — |
| https://home.treasury.gov/ | — | — |
| https://www.sec.gov/edgar | — | — |

## Rust implementation references

| Link | Last checked | Re-verify by |
|---|---|---|
| https://doc.rust-lang.org/edition-guide/rust-2024/index.html | — | — |
| https://blog.rust-lang.org/2026/04/16/Rust-1.95.0/ | 2026-05-02 | 2026-11-02 |
| https://tokio.rs/ | — | — |
| https://docs.rs/tokio | — | — |
| https://docs.rs/axum/latest/axum/ | — | — |
| https://opentelemetry.io/docs/languages/rust/ | — | — |
| https://docs.rs/tracing-opentelemetry | — | — |
| https://docs.pola.rs/api/rust/dev/polars/ | — | — |
| https://docs.pola.rs/user-guide/concepts/streaming/ | — | — |
| https://datafusion.apache.org/ | — | — |
| https://huggingface.github.io/candle/ | — | — |
| https://burn.dev/ | — | — |
| https://onnxruntime.ai/docs/ | — | — |
| https://docs.rs/ort | — | — |
| https://rust-ml.github.io/linfa/ | — | — |
| https://docs.rs/serde | — | — |
| https://docs.rs/simd-json | — | — |
| https://docs.rs/rust_decimal/latest/rust_decimal/ | — | — |
| https://docs.rs/async-nats/latest/async_nats/jetstream/index.html | — | — |
| https://docs.nats.io/nats-concepts/jetstream | — | — |
| https://docs.rs/loom/latest/loom/ | — | — |
| https://docs.rs/shuttle/latest/shuttle/ | — | — |

## AWS / CI references

| Link | Last checked | Re-verify by |
|---|---|---|
| https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services | — | — |
| https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html | 2026-05-07 | 2026-08-05 |
| https://docs.aws.amazon.com/AmazonECR/latest/userguide/what-is-ecr.html | 2026-05-07 | 2026-08-05 |
| https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html | 2026-05-07 | 2026-08-05 |

## Research-only references

Treat as research inspiration; not a production decision input unless an authorized, replayable export/API is reviewed.

| Link | Last checked | Re-verify by |
|---|---|---|
| https://crowdintel.xyz/network | 2026-05-02 | 2026-07-01 |
| https://crowdintel.xyz/methodology | 2026-05-02 | 2026-07-01 |
| https://crowdintel.xyz/docs/copy-trading-polymarket | 2026-05-02 | 2026-07-01 |
| https://crowdintel.xyz/docs | 2026-05-02 | 2026-07-01 |

## Last research pass

- 2026-09-05: Re-verified the corrected Winner-Follow fee and live-finality contracts for #545.
  Compact CLOB `fd` is the sole runtime fee authority: accepted economics are taker-only,
  exponent one, with one aggregate `shares × rate × price × (1 − price)` calculation truncated
  once to five decimal collateral places. Gamma fee fields never classify or gate it. The signed
  repository builder code is zero, so separately observed nonzero builder fees are unreachable.
  Finalized `OrderFilled` logs, not authenticated trade settlement ratios, provide live principal,
  exact fractional quantity, and fee. Polygon chain 137 finalized-height and canonical block-hash
  evidence is required before a live Fill Final; pending or conflicting evidence remains a recorded
  reconciliation outcome. Primary references are the Polymarket fee/compact-market documentation,
  Polygon RPC/finality documentation, and CTF Exchange v2 source at commit `ccc0596`.
  Paper and live daily marks share the recorded CLOB `/prices-history` request with `fidelity=1`;
  the successful response is appended before the latest at-or-before-cutoff sample is classified.
  Ordinary-live fill finality reads `eth_chainId`, each distinct `eth_getTransactionReceipt`, the
  finalized block, and any required canonical receipt-height block through the configured Polygon
  JSON-RPC endpoint before decoding supported V2 `OrderFilled` logs.

- 2026-09-10: Re-verified the user-activity `end=<unix>` upper bound and fixed `limit=500` page
  for issue #589 across 52 read-only GETs (four wallets, 13 monthly anchors): every returned row
  carried the requested `proxyWallet`, every page respected `max(timestamp) <= end`, and pages
  anchored before a wallet's first activity returned an empty list rather than an error
  (`data/archive/research-2026-09-exclusion-review/recapture-summary.json`).

- 2026-09-03: Re-verified `/positions` size precision for issue #557 against an incident wallet.
  The current-position size was `34.0795`; the same wallet's incident REDEEM activity size was
  `33.32222`, and all seven incident anchor balances used no more than four decimal places. This
  supports the glossary's one-reported-quantum residual rationale but does not change exact-decimal
  parsing or assert a documented venue guarantee.

- 2026-09-01: Re-verified issue #544's user-activity, current-position, price-history, and CLOB
  resolution contracts. Live captures confirmed the comma-separated multi-type activity request,
  the fixed `limit=500` reconciliation page size, documented offset maxima (activity 5,000;
  positions 10,000), deterministic position sort, and disjoint explicit redeemable partitions.
  The fixture manifest records every captured URL, fetch timestamp, size, and SHA-256.

- 2026-08-11: Re-verified the #508 ordinary-live Polymarket contracts and transports. Polygon 137
  lists `CtfCollateralAdapter` `0xAdA100Db00Ca00073811820692005400218FcE1f`,
  `NegRiskCtfCollateralAdapter` `0xadA2005600Dec949baf300f4C6120000bDB6eAab`, Conditional Tokens
  `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`, pUSD
  `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`, and Deposit Wallet Factory
  `0x00000000000Fb5C9ADea0298D729A0CB3823Cc07`. Adapter redemption is amounts-free
  `redeemPositions(address,bytes32,bytes32,uint256[])` (selector `0x01b7037c`) with a zero
  `parentCollectionId` and `indexSets [1,2]`. Deposit Wallet transport uses
  `GET /v1/account/transactions/params?address=<signer>&type=WALLET`, `POST /submit`, then
  `GET /v1/account/transactions/<id>` through `STATE_CONFIRMED`; Proxy/Safe retains
  `/relay-payload` plus `/transaction?id=`. Auth is `RELAYER_API_KEY` plus
  `RELAYER_API_KEY_ADDRESS`, or a Builder key/secret/passphrase emitting `POLY_BUILDER_*` headers
  whose signature is padded URL-safe-Base64 HMAC-SHA256 over `timestamp + method + path + body`.
  Official order docs select the standard or
  NegRisk V2 exchange from token market context; the shipped vendored SDK re-reads `neg_risk` for
  the token at sign time (`third_party/polymarket_client_sdk_v2/src/clob/client.rs:1832`).

- 2026-07-28: Re-verified the official Gamma `GET /events` reference for issue #506. It continues
  to expose offset/limit pagination and embedded `markets[]` rows carrying `conditionId` and
  `clobTokenIds`, matching the existing `pe-bootstrap events` parser. This pass changes retry and
  recovery behavior only; it does not change the endpoint or parsed payload contract.

- 2026-07-20: Re-verified the official geoblock reference. It classifies IE, JP, MT, and NL as
  close-only on the frontend and explicitly says the API is not restricted for those jurisdictions.
  Its field description and examples still describe `blocked` as general order availability, so the
  boolean alone is insufficient to distinguish this documented frontend-only group. A live
  same-egress read from the intended Ireland VPS returned `blocked=true`, `country=IE`, and the VPS
  address, while the public CLOB time endpoint remained reachable. The canary therefore requires
  the returned country to match its authority-bound boot jurisdiction and derives API eligibility
  from the documented country class; malformed or mismatched evidence fails closed. This pass does
  not establish authenticated account `closed_only` state or authenticated order acceptance.

- 2026-07-18: The official Gamma `List markets` contract exposes `include_tag` and direct
  `Market.tags`. For active standard Geopolitics condition
  `0x6bd56627aa21311850825edb27e53434a0e17a4f782be0086bc07f71eee00d0d`, the public
  `condition_ids` response omitted nested `events[].tags`; adding `include_tag=true` returned direct
  tag ID `100265` / slug `geopolitics`. The same sample explicitly reported Gamma `negRisk=false`,
  `feesEnabled=false`, `feeSchedule=null`, and `secondsDelay=null`; the long CLOB market reported
  active/accepting, `neg_risk=false`, `seconds_delay=0`, matching two-token identity, minimum order
  size 5, and tick 0.01. Its compact CLOB response omitted `nr`, `fd`, and `itode` (and both base-fee
  fields), while the public book agreed on condition/token identity, `neg_risk=false`, minimum size,
  and tick with nonempty string-valued bid/ask levels. This historical pass supports requesting
  documented direct tags and independent market, delay, compact metadata, and book checks. Its
  Gamma-assisted absent-fee inference was retired by #545; compact CLOB fee evidence now stands on
  its own and fails closed when it cannot produce the accepted schedule.

- 2026-07-17: Re-verified the production CLOB V2 migration/host, order/FOK behavior, fees,
  builder fees, deposit-wallet `POLY_1271` identity, authentication, standard V2 contracts,
  Gamma tag/market mapping, CLOB long/short market metadata and book, geoblock, authenticated
  closed-only/balance/orders/trades reads, and Data positions. Current sampled
  `/clob-markets/{condition_id}` responses may omit `nr`, `fd`, and `itode`: the canary accepts an
  absent `nr` only when independent long-market and book evidence explicitly prove standard
  `neg_risk == false`. Its former acceptance of absent `fd` using Gamma and base-fee fields is
  historical and was retired by #545; corrected economics require the compact parser's own
  accepted zero/taker schedule. The pass accepts absent `itode` only under the pinned SDK
  0.7.0/parser-version omission rule while the long-form market explicitly reports
  `seconds_delay == 0`. Any present `nr == true`, nonzero `fd`/base fee, `itode == true`, positive
  delay, source disagreement, or schema drift fails closed. Re-verify these observations before
  operational authority; documentation examples show these fields but do not establish that every
  live response includes them.

- 2026-05-07: Checked Kalshi REST/WS API reference, changelog, and core market spec pages. No breaking changes relative to prior implementation assumptions.
- 2026-05-07: Checked Polygon RPC docs and AWS ECS/ECR/Secrets Manager welcome pages for structural changes. No breaking changes noted.
- 2026-05-02: Checked Polymarket official docs for public Data/Gamma/CLOB read endpoints, proxy wallets, signature type/funder behavior, and bridge deposit/pUSD collateral flow.
- 2026-05-02: Checked CrowdIntel public pages for funding-network methodology. Treat as research inspiration only unless an authorized replayable API/export exists.
- 2026-05-12: Verified CTF deploy block (4_023_686, Sep-03-2020) on PolygonScan and computed `TOPIC_CONDITION_RESOLUTION` keccak hash via `alloy::primitives::keccak256` of the canonical signature for issue #149 multi-source pipeline. Verified CLOB `/markets?closed=true` paginated listing endpoint exists; confirmed `next_cursor=LTE=` terminator convention from Polymarket CLOB documentation.
- 2026-06-18: Re-verified CLOB `/markets?closed=true` live (issue #369 PR1) — response shape `{count, limit, next_cursor, data:[{condition_id, closed, end_date_iso, tokens:[{outcome, price, token_id, winner}]}]}`, `LTE=` terminator. Found `tokens[].winner` is unset on old (≈2022–2023) markets (winner encoded only in terminal price); see the CLOB `/markets` correction note above. Reconciled 848,098 traded markets vs `source='polygon'`: 0 winner contradictions, benign 202 CLOB-null all pre-2024, 0 traded multi-outcome markets.
- 2026-06-18: Retired the Polygon JSON-RPC market-resolution scan (issue #369 PR2). CLOB `/markets?closed=true` is now the **sole** market-resolution source; the Polygon JSON-RPC / `eth_getLogs` CTF-`ConditionResolution` source entries and the CTF contract-page reference are removed (their constants/scan were deleted from `pe-bootstrap`). Existing `source='polygon'` rows are retained, so historical resolution accuracy is unchanged.
- 2026-06-20: Re-verified live (issue #382 Phase-0 probe `scripts/probe_gamma_ua.py`) that Gamma `/markets?condition_ids=` and CLOB `/markets?closed=true` 403 **only** on the `Python-urllib/*` default User-Agent — headerless / empty / product (`prediction-edge/1.0`) / browser UAs all return 200, so the shipped UA-less Rust clients are unaffected and CLOB-as-sole-resolution-source is not silently failing. Confirmed repeat-key `condition_ids=` batching works for **both** the plain and `&closed=true` Gamma variants (50/50, 100/100, clean demux-by-`conditionId`; comma-separated returns 0; cap ≥ 100). See the Polymarket-section note above; supersedes the stale `gamma.rs:5-6` "batching fails silently" claim.
- 2026-08-21: Re-verified the CLOB closed-list and per-condition market responses, including terminal pagination, `end_date_iso`, and `tokens[].winner`. The endpoint remained healthy; a persisted terminal cursor had prevented every later resolution walk since 2026-06-24.
