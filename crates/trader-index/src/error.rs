/// Errors that can arise during ledger reconstruction.
#[derive(Debug, thiserror::Error)]
pub enum TraderIndexError {
    #[error("reconstruction quality out of range: {value}")]
    QualityOutOfRange { value: u8 },
}
