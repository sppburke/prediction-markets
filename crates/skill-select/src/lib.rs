//! Skill-based wallet-selection pipeline (epic #209; feature set per #205).
//!
//! v1 selects copy-trade-worthy Polymarket wallets by historical skill on
//! history ≤ a cutoff and validates the selection on a held-out forward window.
//! This crate is built in phases (issue #212): the `wallet_features` persistence
//! layer ([`db::SkillCache`]) and the deterministic per-wallet feature batch
//! ([`features::extract_features`]). The sign-randomization skill test,
//! selection (DSR + BHq), and the forward-test harness land in later phases.

pub mod db;
pub mod error;
pub mod features;

pub use db::{SkillCache, WalletFeatures};
pub use error::SkillSelectError;
pub use features::{DeterministicFeatures, extract_features};
