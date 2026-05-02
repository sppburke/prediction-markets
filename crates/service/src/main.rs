#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init failed: {e}"))?;

    let cfg = match env::args().nth(1).map(PathBuf::from) {
        Some(path) => pe_config::load(&path)
            .with_context(|| format!("loading config from {}", path.display()))?,
        None => pe_config::ServiceConfig::default(),
    };

    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready));

    let listener = tokio::net::TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("bind {}", cfg.bind))?;
    info!(bind = %cfg.bind, "pe-service listening");

    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

async fn live() -> &'static str {
    "ok"
}

async fn ready() -> &'static str {
    "ok"
}
