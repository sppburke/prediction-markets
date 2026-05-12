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

| Link | Last checked | Re-verify by |
|---|---|---|
| https://docs.polymarket.com/ | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/llms.txt | — | — |
| https://docs.polymarket.com/market-data/websocket/overview | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/market-data/websocket/market-channel | — | — |
| https://docs.polymarket.com/market-data/websocket/user-channel | — | — |
| https://docs.polymarket.com/market-data/websocket/sports | — | — |
| https://docs.polymarket.com/market-data/websocket/rtds | — | — |
| https://docs.polymarket.com/trading/overview | — | — |
| https://docs.polymarket.com/trading/orders/create | — | — |
| https://docs.polymarket.com/concepts/order-lifecycle | — | — |
| https://docs.polymarket.com/api-reference/authentication | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/api-reference/introduction | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/polymarket-101 | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/deposit | 2026-05-02 | 2026-07-01 |
| https://docs.polymarket.com/trading/bridge/supported-assets | — | — |
| https://docs.polymarket.com/trading/bridge/status | — | — |
| https://docs.polymarket.com/developers/contracts | — | — |
| https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings | 2026-05-04 | 2026-07-03 |
| https://docs.polymarket.com/api-reference/core/get-user-trade-activity | 2026-05-09 | 2026-07-08 |
| https://clob.polymarket.com/markets?closed=true | 2026-05-12 | 2026-07-11 |
| https://docs.polymarket.com/developers/clob/markets | 2026-05-12 | 2026-07-11 |

## Public sources

| Link | Last checked | Re-verify by |
|---|---|---|
| https://polygon.technology/ | 2026-05-07 | 2026-08-05 |
| Polygon JSON-RPC / archive provider docs (selected by impl) | 2026-05-07 | 2026-08-05 |
| Polygon JSON-RPC `eth_getLogs` for CTF `ConditionResolution` events (issue #149) | 2026-05-12 | 2026-08-10 |
| https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 (CTF contract page) | 2026-05-12 | 2026-08-10 |
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

- 2026-05-07: Checked Kalshi REST/WS API reference, changelog, and core market spec pages. No breaking changes relative to prior implementation assumptions.
- 2026-05-07: Checked Polygon RPC docs and AWS ECS/ECR/Secrets Manager welcome pages for structural changes. No breaking changes noted.
- 2026-05-02: Checked Polymarket official docs for public Data/Gamma/CLOB read endpoints, proxy wallets, signature type/funder behavior, and bridge deposit/pUSD collateral flow.
- 2026-05-02: Checked CrowdIntel public pages for funding-network methodology. Treat as research inspiration only unless an authorized replayable API/export exists.
- 2026-05-12: Verified CTF deploy block (4_023_686, Sep-03-2020) on PolygonScan and computed `TOPIC_CONDITION_RESOLUTION` keccak hash via `alloy::primitives::keccak256` of the canonical signature for issue #149 multi-source pipeline. Verified CLOB `/markets?closed=true` paginated listing endpoint exists; confirmed `next_cursor=LTE=` terminator convention from Polymarket CLOB documentation.
