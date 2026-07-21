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

> **WebSocket design note (2026-06-03, issue #282 Phase 2 verification).**
> `wss://ws-live-data.polymarket.com` (RTDS) provides comments, crypto prices, and equity prices only — no trade data.
> `wss://ws-subscriptions-clob.polymarket.com/ws/market` (`last_trade_price` events) does not include the **wallet address**; wallet-level trade identification is impossible from the frame alone.
> No Polymarket WebSocket supports per-wallet trade subscriptions. The REST `/activity` poll remains the primary ingestion path (issue #282 Open risk #1 materialized). Phase 2 RTDS ingestion is deferred.
>
> **Correction (2026-06-09, issue #300 live capture).** A live `last_trade_price` market-channel frame **does** carry `transaction_hash` — verified against 2,581 captured frames (100% present, one unique hash per print, zero collisions). The earlier note that it omits `transaction_hash` is superseded; only the wallet address is absent, so wallet-level identity still requires an on-chain tx lookup, but the print is uniquely keyable. Full frame shape: `{market, asset_id, price, size, side, timestamp, fee_rate_bps, event_type, transaction_hash}` — `market` is the condition_id (authoritative, present on every print). `pe-crypto-shadow` keys `clob_trades` on `transaction_hash` and attributes via `market`.
>
> **Heartbeat contract (2026-06-10, issue #317, `wss-overview`).** The CLOB market **and** user channels require an **application-level** heartbeat: the client sends the text message `PING` every ~10s and the server replies the text `PONG`; the troubleshooting section attributes "connection drops after about 10 seconds" to a missing heartbeat. This is **not** a WS protocol Ping frame — it is a literal text payload. `pe-crypto-shadow` sends `Message::Text("PING")` every `crypto_shadow_clob_ping_interval_secs` (10) and filters the `PONG` reply (case-insensitive — sports channel lowercases it) out of the frame stream. The same page documents dynamic-subscription ops `{"operation":"subscribe"|"unsubscribe"}`; the harness intentionally does not use `unsubscribe` (prune applies at the next reconnect — a filed follow-up). The heartbeat lives only on this overview page, which is why it was missed until #317 (the `market-channel` page, checked 2026-06-09, has no heartbeat section).
>
> **`/activity` indexing latency measured (2026-06-14).** The REST `data-api.polymarket.com/activity` feed (the only *wallet-attributed* trade source) indexes a trade within **~1–4s of its `timestamp`** — proven lower bound p50 1.2s / p95 3.8s / p99 5.2s / max 23s, from 4,655 live trades (`scripts/measure_activity_latency.py`, jitter-free lower-bound method). End-to-end copy latency (indexing + a 5–10s poll + order placement) ≈ **~10–20s**, so a **1-minute** TTR floor is reliably copyable for near-resolution first-bets. Full methodology + run record: `docs/29-ACTIVITY-LATENCY-MEASUREMENT.md`.
>
> **CLOB `/book` is public/no-auth (2026-06-16, issue #350 WS2 PR-G live re-confirm).** `GET https://clob.polymarket.com/book?token_id=<id>` returns HTTP **200 with no auth header**; a bogus token id returns **404** `{"error":"No orderbook exists for the requested token id"}`. Body shape: `asks`/`bids` are arrays of `{price, size}` where **both fields are strings** (Decimal-safe), plus scalar string meta (`market`, `asset_id`, `tick_size`, `min_order_size`, `last_trade_price`, `timestamp`, `hash`) and `neg_risk` (bool). Asks are returned **high→low price** (best/lowest ask is not at index 0). `crates/service/src/clob_book.rs` parses the ask side only (liquidity capture is buy-only) into `Decimal`, deriving `best_ask` as the minimum price.
>
> **CLOB `/markets?closed=true` does NOT populate `tokens[].winner` on old markets (2026-06-18, issue #369 PR1 live reconciliation).** Response: `{count, limit, next_cursor, data:[…]}`; each market carries snake_case `condition_id`, `closed`, `end_date_iso`, and `tokens:[{outcome, price, token_id, winner}]`; `next_cursor` terminator is `LTE=`. **Gotcha:** for old markets (≈2022–2023) the resolved winner is reflected only in the terminal token `price` (~1.0 winner / ~0.0 loser) — `tokens[].winner` is `false` on **every** token, so `clob.rs::winner_index` reports the market as voided. Measured against `source='polygon'` over 848,098 traded markets: 0 winner *contradictions* (99.976% agreement) but 202 CLOB-null-vs-polygon-winner, **all** in 2022–2023; by 2024 winner-flag coverage is complete (2024 0/11,674, 2025 2/125,388, 2026 10/709,318 null). Benign for issue #369 because the 1.19M `source='polygon'` rows are kept; CLOB only needs correctness for new markets. Also confirmed: 0 traded multi-outcome (>2-token) markets, so the positional `winner_index`↔`outcome_id` mapping risk is empirically binary-only.

> **Gamma `/markets?condition_ids=` + CLOB `/markets?closed=true` — UA blocklist & repeat-key batching (2026-06-20, issue #382 Phase-0 live probe `scripts/probe_gamma_ua.py`).** The `&closed=true` 403 is triggered by the literal `Python-urllib/*` default User-Agent (an anti-bot blocklist), **not** by a missing browser UA: both endpoints return **200** for a headerless request (a bare `reqwest::Client` = the shipped Rust clients), an empty UA, a product UA (`prediction-edge/1.0`), and a browser UA — and **403 only** for `Python-urllib/3.11`. So `pe-bootstrap`'s UA-less CLOB closed walk and the paper-pnl resolution poller do **not** 403; resolution ingestion is healthy (resolves the issue #382 open-risk that CLOB-as-sole-resolution-source might be silently 403ing). Repeat-key batching (`?condition_ids=A&condition_ids=B…&limit=500`) works for **both** the plain (open) and `&closed=true` Gamma variants — 50/50 and 100/100 returned, demux-by-`conditionId` clean, no cross-market leak; comma-separated joining returns 0 (repeat-key mandatory); observed batch cap ≥ 100 (kept at `gamma_batch_size`=50). This supersedes the stale `crates/bootstrap/src/gamma.rs:5-6` "batching fails silently" claim for the repeat-key form.

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
| https://docs.polymarket.com/trading/orders/create | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/fees | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/builders/fees | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/deposit-wallets | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/trading/clients/l2 | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/concepts/order-lifecycle | — | — |
| https://docs.polymarket.com/api-reference/authentication | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/geoblock | 2026-07-20 | 2026-09-18 |
| https://docs.polymarket.com/api-reference/introduction | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/polymarket-101 | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/deposit | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/supported-assets | — | — |
| https://docs.polymarket.com/trading/bridge/status | — | — |
| https://docs.polymarket.com/resources/contracts | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings | 2026-05-04 | 2026-07-03 |
| https://docs.polymarket.com/api-reference/core/get-user-trade-activity | 2026-07-17 | 2026-09-15 |
| https://clob.polymarket.com/markets?closed=true | 2026-06-20 | 2026-08-19 |
| https://docs.polymarket.com/api-reference/markets/list-markets | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/markets/get-market-by-id | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/markets/get-clob-market-info | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/market-data/get-order-book | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/api-reference/trade/get-user-orders | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/trade/get-trades | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user | 2026-07-17 | 2026-09-15 |
| https://docs.polymarket.com/api-reference/tags/get-tag-by-id | 2026-07-18 | 2026-09-16 |
| https://clob.polymarket.com/markets/{condition_id} | 2026-07-18 | 2026-09-16 |
| https://clob.polymarket.com/book?token_id={tokenId} | 2026-07-18 | 2026-09-16 |
| https://clob.polymarket.com/prices-history?market={tokenId} | 2026-06-23 | 2026-08-22 |
| https://docs.polymarket.com/api-reference/markets/get-prices-history | 2026-06-23 | 2026-08-22 |
| https://gamma-api.polymarket.com/markets?condition_ids={id}&include_tag=true | 2026-07-18 | 2026-09-16 |
| https://docs.polymarket.com/developers/clob/markets | 2026-05-12 | 2026-07-11 |

## Public sources

| Link | Last checked | Re-verify by |
|---|---|---|
| https://polygon.technology/ | 2026-05-07 | 2026-08-05 |
| https://docs.etherscan.io/etherscan-v2/api-endpoints/accounts | 2026-05-04 | 2026-08-04 |
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
  and tick with nonempty string-valued bid/ask levels. This pass supports requesting documented
  direct tags and retaining the existing independent fail-closed long-market, fee, delay, compact
  metadata, and book checks; it does not establish authenticated account, geoblock, or closed-only
  state.

- 2026-07-17: Re-verified the production CLOB V2 migration/host, order/FOK behavior, fees,
  builder fees, deposit-wallet `POLY_1271` identity, authentication, standard V2 contracts,
  Gamma tag/market mapping, CLOB long/short market metadata and book, geoblock, authenticated
  closed-only/balance/orders/trades reads, and Data positions. Current sampled
  `/clob-markets/{condition_id}` responses may omit `nr`, `fd`, and `itode`: the canary accepts an
  absent `nr` only when independent long-market and book evidence explicitly prove standard
  `neg_risk == false`, accepts absent `fd` only when Gamma explicitly proves fees disabled and all
  available CLOB base-fee fields are zero, and accepts absent `itode` only under the pinned SDK
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
