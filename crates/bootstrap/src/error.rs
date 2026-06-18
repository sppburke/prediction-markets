use thiserror::Error;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("etherscan: {message}")]
    Etherscan { message: String },
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
    #[error("gamma: {message}")]
    Gamma { message: String },
    #[error("clob: {message}")]
    Clob { message: String },
    #[error("leaderboard: {message}")]
    Leaderboard { message: String },
    #[error("radion: {message}")]
    Radion { message: String },
    #[error("datadash: {message}")]
    Datadash { message: String },
    #[error("funder: {message}")]
    Funder { message: String },
    #[error("url parse: {0}")]
    UrlParse(#[from] url::ParseError),
    /// Operator-misconfig / invariant violation surfaced from a runtime check
    /// (e.g. `wallet_from_block > to_block` at the start of the OnChain
    /// enumeration arm). Separate from `Parse` (which is for input parsing)
    /// and from `Config` (which wraps figment's loader errors).
    #[error("invalid: {message}")]
    Invalid { message: String },
    #[error("internal error")]
    Internal,
}

impl From<figment::Error> for BootstrapError {
    fn from(e: figment::Error) -> Self {
        BootstrapError::Config(Box::new(e))
    }
}
