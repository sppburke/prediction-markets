use pe_source_onchain_polygon::EnumerationError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("dune: {message}")]
    Dune { message: String },
    #[error("dune execution {state}: {message}")]
    DuneExecutionFailed { state: String, message: String },
    #[error("dune timeout after {secs}s waiting for execution {execution_id}")]
    DuneTimeout { execution_id: String, secs: u64 },
    #[error("etherscan: {message}")]
    Etherscan { message: String },
    #[error("wallet enumeration: {0}")]
    Enumeration(#[from] EnumerationError),
    #[error("polymarket fetch for {wallet}: {message}")]
    Polymarket { wallet: String, message: String },
    #[error("trade parse for {wallet}: {message}")]
    TradeParse { wallet: String, message: String },
    #[error("cache: {message}")]
    Cache { message: String },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("missing required environment variable '{0}' — set it in .env or export it")]
    MissingEnv(String),
    #[error("config: {0}")]
    Config(Box<figment::Error>),
    #[error("parse: {message}")]
    Parse { message: String },
    #[error(
        "fetch incomplete: {failed_wallets} wallet(s) could not be fetched or cached; re-run to retry"
    )]
    PartialFetch { failed_wallets: usize },
    #[error("funder discovery: {failed_wallets} wallet(s) failed; partial cache state")]
    PartialFunderFetch { failed_wallets: usize },
    #[error("gamma: {message}")]
    Gamma { message: String },
    #[error("clob: {message}")]
    Clob { message: String },
    #[error("polygon_ctf: {message}")]
    PolygonCtf { message: String },
    #[error("url parse: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("internal error")]
    Internal,
}

impl From<figment::Error> for BootstrapError {
    fn from(e: figment::Error) -> Self {
        BootstrapError::Config(Box::new(e))
    }
}
