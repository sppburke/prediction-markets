//! Error types for the Polymarket venue adapter.

use pe_venue_core::VenueError;
use thiserror::Error;

/// Errors from the Polymarket CLOB REST API layer.
#[derive(Debug, Error)]
pub enum PolymarketError {
    #[error("HTTP error {status}: {body}")]
    Http { status: u16, body: String },

    #[error("rate limited: retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u32 },

    #[error("serialisation error: {0}")]
    Serialise(#[from] serde_json::Error),

    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("signing error: {0}")]
    Signing(String),

    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}

impl From<PolymarketError> for VenueError {
    fn from(e: PolymarketError) -> Self {
        match e {
            PolymarketError::RateLimited { retry_after_secs } => {
                VenueError::RateLimited { retry_after_secs }
            }
            PolymarketError::Http {
                status: 401 | 403,
                body,
            } => VenueError::Auth { message: body },
            other => VenueError::Network {
                message: other.to_string(),
            },
        }
    }
}
