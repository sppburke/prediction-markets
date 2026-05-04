#![forbid(unsafe_code)]
//! Library target for `pe-service` — exposes internal modules for scenario tests.
//! Production code lives in `main.rs`.

pub mod health;
pub mod logging;
pub mod operator_graph_scheduler;
pub mod orchestrator;
pub mod trade_parser;
pub mod trade_poller;
