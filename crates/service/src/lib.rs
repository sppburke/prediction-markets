#![forbid(unsafe_code)]
//! Library target for `pe-service` — exposes internal modules for scenario tests.
//! Production code lives in `main.rs`.

pub mod clob_book;
pub mod config;
pub mod config_poller;
pub mod demotion_stat;
pub mod dispatch_recovery;
pub mod entry_gate;
pub mod health;
pub mod live_accounts;
pub mod live_canary;
pub mod live_credentials;
pub mod live_fanout;
pub mod live_mode;
pub mod live_projections;
pub mod live_venue_adapter;
pub mod live_watchlist;
pub mod logging;
pub mod market_end_cache;
pub mod mid_price_cache;
pub mod orchestrator;
pub mod orchestrator_control;
pub mod organic_canary;
pub mod paper_api;
pub mod paper_recovery;
pub mod position_seeder;
pub mod runtime_config;
pub mod snapshot_worker;
pub mod status_writer;
pub mod supabase_backfill;
pub mod supabase_reader;
pub mod supabase_refresh;
pub mod supabase_sink;
pub mod supabase_state;
pub mod trade_parser;
pub mod trade_poller;
pub mod wallet_history;
pub mod watchlist_capacity;
pub mod watchlist_maintenance;
