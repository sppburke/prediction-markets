use pe_bootstrap::{BootstrapConfig, run};
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
