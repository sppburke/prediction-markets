//! Skill-based wallet-selection pipeline (epic #209; feature set per #205).
//!
//! v1 selects copy-trade-worthy Polymarket wallets by historical skill on
//! history ≤ a cutoff and validates the selection on a held-out forward window.
//! This crate is built in phases (issue #212); this first slice is the
//! persistence layer: the `wallet_features` table and its [`db::SkillCache`]
//! accessor. Feature extraction, the sign-randomization skill test, selection,
//! and the forward-test harness land in later phases.

pub mod db;
pub mod error;

pub use db::{SkillCache, WalletFeatures};
pub use error::SkillSelectError;
