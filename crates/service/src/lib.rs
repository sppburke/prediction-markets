#![forbid(unsafe_code)]
//! Library target for `pe-service` — exposes internal modules for scenario tests.
//! Production code lives in `main.rs`.

pub mod config;
pub mod entry_gate;
pub mod health;
pub mod logging;
pub mod market_end_cache;
pub mod operator_graph_scheduler;
pub mod orchestrator;
pub mod paper_api;
pub mod paper_recovery;
pub mod position_seeder;
pub mod seed;
pub mod trade_parser;
pub mod trade_poller;
pub mod wallet_history;
