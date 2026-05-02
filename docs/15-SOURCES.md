# 15 — Sources and Implementation References

Verify official venue docs again before live trading.

## Kalshi

- API docs: https://docs.kalshi.com/welcome
- WebSocket quick start: https://docs.kalshi.com/getting_started/quick_start_websockets
- Market data quick start: https://docs.kalshi.com/getting_started/quick_start_market_data
- Orderbook responses: https://docs.kalshi.com/getting_started/orderbook_responses
- Queue position: https://docs.kalshi.com/api-reference/orders/get-order-queue-position
- Order groups: https://docs.kalshi.com/api-reference/order-groups/create-order-group
- Changelog: https://docs.kalshi.com/changelog
- Weather markets: https://help.kalshi.com/en/articles/13823837-weather-markets
- Crypto markets: https://help.kalshi.com/en/articles/13823838-crypto-markets
- Spotify markets: https://help.kalshi.com/en/articles/13823839-spotify-markets
- Netflix markets: https://help.kalshi.com/en/articles/13823840-netflix-markets
- Top App markets: https://help.kalshi.com/en/articles/13823841-top-app-markets
- Trading hours: https://help.kalshi.com/en/articles/13823807-what-are-trading-hours
- Liquidity incentives: https://help.kalshi.com/en/articles/13823851-liquidity-incentive-program
- Combos: https://help.kalshi.com/en/articles/13823820-combos

## Polymarket

- Docs home: https://docs.polymarket.com/
- Documentation index: https://docs.polymarket.com/llms.txt
- WebSocket overview: https://docs.polymarket.com/market-data/websocket/overview
- Market channel: https://docs.polymarket.com/market-data/websocket/market-channel
- User channel: https://docs.polymarket.com/market-data/websocket/user-channel
- Sports WebSocket: https://docs.polymarket.com/market-data/websocket/sports
- RTDS: https://docs.polymarket.com/market-data/websocket/rtds
- Trading overview: https://docs.polymarket.com/trading/overview
- Create order: https://docs.polymarket.com/trading/orders/create
- Order lifecycle: https://docs.polymarket.com/concepts/order-lifecycle
- Authentication: https://docs.polymarket.com/api-reference/authentication
- CLOB API introduction: https://docs.polymarket.com/api-reference/introduction
- Polymarket 101 / proxy wallets: https://docs.polymarket.com/polymarket-101
- Bridge deposit flow: https://docs.polymarket.com/trading/bridge/deposit
- Supported bridge assets: https://docs.polymarket.com/trading/bridge/supported-assets
- Bridge transaction status: https://docs.polymarket.com/trading/bridge/status
- Contracts: https://docs.polymarket.com/developers/contracts

## Public sources

- Polygon: https://polygon.technology/
- Polygon JSON-RPC / provider docs for archive access as selected by implementation.
- NOAA/NWS: https://www.weather.gov/
- Aviation Weather Center API: https://aviationweather.gov/data/api/
- NOAA HRRR: https://rapidrefresh.noaa.gov/hrrr/
- NOAA NBM: https://vlab.noaa.gov/web/mdl/nbm
- USGS earthquake feeds: https://earthquake.usgs.gov/earthquakes/feed/
- National Hurricane Center: https://www.nhc.noaa.gov/
- NASA FIRMS: https://firms.modaps.eosdis.nasa.gov/
- BLS: https://www.bls.gov/
- BEA: https://www.bea.gov/
- Census: https://www.census.gov/
- Federal Reserve: https://www.federalreserve.gov/
- Treasury: https://home.treasury.gov/
- SEC EDGAR: https://www.sec.gov/edgar

## Rust implementation references

- Rust 2024 Edition Guide: https://doc.rust-lang.org/edition-guide/rust-2024/index.html
- Rust 1.95.0 announcement: https://blog.rust-lang.org/2026/04/16/Rust-1.95.0/
- Tokio: https://tokio.rs/
- Tokio crate: https://docs.rs/tokio
- Axum: https://docs.rs/axum/latest/axum/
- OpenTelemetry Rust: https://opentelemetry.io/docs/languages/rust/
- tracing-opentelemetry: https://docs.rs/tracing-opentelemetry
- Polars Rust: https://docs.pola.rs/api/rust/dev/polars/
- Polars streaming: https://docs.pola.rs/user-guide/concepts/streaming/
- DataFusion: https://datafusion.apache.org/
- Candle: https://huggingface.github.io/candle/
- Burn: https://burn.dev/
- ONNX Runtime: https://onnxruntime.ai/docs/
- ort: https://docs.rs/ort
- Linfa: https://rust-ml.github.io/linfa/
- Serde: https://docs.rs/serde
- simd-json: https://docs.rs/simd-json
- rust_decimal: https://docs.rs/rust_decimal/latest/rust_decimal/
- async-nats JetStream: https://docs.rs/async-nats/latest/async_nats/jetstream/index.html
- NATS JetStream: https://docs.nats.io/nats-concepts/jetstream
- Loom: https://docs.rs/loom/latest/loom/
- Shuttle: https://docs.rs/shuttle/latest/shuttle/


## Winner-Follow and current implementation references

- Polymarket API overview: https://docs.polymarket.com/api-reference/introduction
- Polymarket leaderboard endpoint: https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings
- Polymarket user trades endpoint: https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
- Polymarket profile/current positions/activity endpoints: https://docs.polymarket.com/api-reference/introduction
- Polymarket authentication signature types and funder: https://docs.polymarket.com/api-reference/authentication
- Polymarket proxy-wallet overview: https://docs.polymarket.com/polymarket-101
- Polymarket deposit flow and pUSD collateral notes: https://docs.polymarket.com/trading/bridge/deposit
- Polymarket WebSocket overview: https://docs.polymarket.com/market-data/websocket/overview
- CrowdIntel network page, research only: https://crowdintel.xyz/network
- CrowdIntel methodology, research only: https://crowdintel.xyz/methodology
- CrowdIntel copy-trading guide, research only: https://crowdintel.xyz/docs/copy-trading-polymarket
- CrowdIntel docs index, research only: https://crowdintel.xyz/docs
- Kalshi API overview: https://docs.kalshi.com/welcome
- Kalshi public trades websocket: https://docs.kalshi.com/websockets/public-trades
- Kalshi historical trades: https://docs.kalshi.com/api-reference/historical/get-historical-trades
- Kalshi leaderboard help: https://help.kalshi.com/en/articles/13823809-leaderboard
- Rust 1.95.0 announcement: https://blog.rust-lang.org/2026/04/16/Rust-1.95.0/
- GitHub Actions OIDC with AWS: https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services
- AWS ECS: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html
- AWS ECR: https://docs.aws.amazon.com/AmazonECR/latest/userguide/what-is-ecr.html
- AWS Secrets Manager: https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html

## Last research pass

- 2026-05-02: Checked Polymarket official docs for public Data/Gamma/CLOB read endpoints, proxy wallets, signature type/funder behavior, and bridge deposit/pUSD collateral flow.
- 2026-05-02: Checked CrowdIntel public pages for funding-network methodology. Treat as research inspiration only unless an authorized replayable API/export exists.
