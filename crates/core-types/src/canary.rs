//! Shared canary domain vocabulary.

use serde::{Deserialize, Serialize};

/// Origin of a bounded canary request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryOrigin {
    OperatorProbe,
    Organic,
}
