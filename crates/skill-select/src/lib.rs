//! Skill-based wallet-selection pipeline (epic #209; feature set per #205).
//!
//! v1 selects copy-trade-worthy Polymarket wallets by historical skill on
//! history ≤ a cutoff and validates the selection on a held-out forward window.
//! This crate is built in phases (issue #212): the `wallet_features` persistence
//! layer ([`db::SkillCache`]), the deterministic per-wallet feature batch
//! ([`features::extract_features`]), and the event-level sign-randomization
//! skill test ([`skill_test::sign_randomization_test`]). Selection (DSR + BHq)
//! and the forward-test harness land in later phases.

pub mod db;
pub mod error;
pub mod features;
pub mod skill_test;

pub use db::{SkillCache, WalletFeatures};
pub use error::SkillSelectError;
pub use features::{DeterministicFeatures, extract_features};
pub use skill_test::{SkillTestResult, sign_randomization_test};
