//! AC1: run immediately after finalize, before preparing a publication.

use pe_bootstrap::cache_migration::measure_classifier_v6_sample;
use pe_bootstrap::error::BootstrapError;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let path = match (arguments.next(), arguments.next()) {
        (Some(path), None) => PathBuf::from(path),
        _ => {
            return Err(BootstrapError::Invalid {
                message: "usage: classifier_v6_sample <finalized-cache>".to_owned(),
            }
            .into());
        }
    };
    println!(
        "{}",
        serde_json::to_string(&measure_classifier_v6_sample(&path)?)?
    );
    Ok(())
}
