use pe_bootstrap::{BootstrapConfig, parse_seed_as_of_env, run, seed_historical_snapshots};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = match BootstrapConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("bootstrap: config error: {e}");
            std::process::exit(1);
        }
    };

    // Historical-seed mode: when PE_SEED_AS_OF_DATES is set, run the parameterized
    // Dune query at each as-of date and write a snapshot row-set into the cache.
    // This path is exclusive of the regular discover/fetch/build pipeline.
    let seed_env = std::env::var("PE_SEED_AS_OF_DATES").unwrap_or_default();
    if !seed_env.trim().is_empty() {
        let dates = match parse_seed_as_of_env(&seed_env) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("bootstrap: PE_SEED_AS_OF_DATES parse error: {e}");
                std::process::exit(1);
            }
        };
        match seed_historical_snapshots(&config, &dates).await {
            Ok(rows) => {
                tracing::info!(
                    snapshots = dates.len(),
                    rows,
                    "bootstrap: historical seed complete"
                );
                return;
            }
            Err(e) => {
                eprintln!("bootstrap: historical seed fatal: {e}");
                std::process::exit(1);
            }
        }
    }

    match run(&config).await {
        Ok(watchlist) => {
            tracing::info!(
                active = watchlist.active_count,
                output = %config.output_path.display(),
                "bootstrap: complete"
            );
        }
        Err(e) => {
            eprintln!("bootstrap: fatal: {e}");
            std::process::exit(1);
        }
    }
}
