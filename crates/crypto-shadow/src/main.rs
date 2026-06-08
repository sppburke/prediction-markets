//! `pe-crypto-shadow` binary: `run` (live collect) / `report` (offline stats) /
//! `print-config`. Manual arg parsing (the workspace has no `clap` dep), exit
//! codes, JSON tracing — matching the `pe-bootstrap` convention.

use std::path::PathBuf;

use tracing_subscriber::EnvFilter;

use pe_crypto_shadow::{generate_report, load, run};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let first = args.get(1).map(String::as_str);
    let known = matches!(first, Some("run" | "report" | "print-config"));
    let Some(sub) = first.filter(|_| known) else {
        eprintln!("usage: pe-crypto-shadow <run|report|print-config> [--config <path>]");
        std::process::exit(2);
    };

    // Optional `--config <path>`.
    let rest = &args[2..];
    let mut config_path: Option<PathBuf> = None;
    let mut i = 0;
    while i < rest.len() {
        if rest[i] == "--config" && i + 1 < rest.len() {
            config_path = Some(PathBuf::from(&rest[i + 1]));
            i += 1;
        }
        i += 1;
    }

    let cfg = match load(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "config error");
            std::process::exit(1);
        }
    };

    let exit = match sub {
        "run" => match run(&cfg, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        {
            Ok(s) => {
                tracing::info!(
                    observations = s.observations,
                    raw_ticks = s.raw_ticks,
                    "run complete"
                );
                0
            }
            Err(e) => {
                tracing::error!(error = %e, "run failed");
                1
            }
        },
        "report" => match generate_report(&cfg) {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(e) => {
                tracing::error!(error = %e, "report failed");
                1
            }
        },
        "print-config" => match toml::to_string_pretty(&cfg) {
            Ok(s) => {
                print!("{s}");
                0
            }
            Err(e) => {
                tracing::error!(error = %e, "print-config failed");
                1
            }
        },
        _ => 2,
    };
    std::process::exit(exit);
}
