//! Skill-based wallet-selection pipeline (epic #209; feature set per #205).
//!
//! v1 selects copy-trade-worthy Polymarket wallets by historical skill on
//! history ≤ a cutoff and validates the selection on a held-out forward window.
//! This crate is built in phases (issue #212): the `wallet_features` persistence
//! layer ([`db::SkillCache`]), the deterministic per-wallet feature batch
//! ([`features::extract_features`]), and the event-level sign-randomization
//! skill test ([`skill_test::sign_randomization_test`]), selection
//! ([`selection::select_wallets`] — Benjamini–Hochberg FDR + Deflated-Sharpe
//! rank), and the Phase-A extraction orchestration ([`extract::run_extract`] —
//! the streaming per-wallet pass that writes `wallet_features`). The forward-test
//! harness and the binary land in later phases.

pub mod composite;
pub mod config;
pub mod db;
pub mod error;
pub mod extract;
pub mod features;
pub mod forward;
pub mod selection;
pub mod skill_test;

pub use composite::{CompositeResult, CompositeWeights, rank_by_composite};
pub use config::SkillConfig;
pub use db::{SkillCache, WalletFeatures};
pub use error::SkillSelectError;
pub use extract::{ExtractReport, run_extract};
pub use features::{DeterministicFeatures, extract_features};
pub use forward::{ForwardPnl, ForwardReport, forward_pnl_for_wallet, run_forward_test};
pub use selection::{SelectionInput, SelectionResult, select_wallets};
pub use skill_test::{SkillTestResult, sign_randomization_test};
