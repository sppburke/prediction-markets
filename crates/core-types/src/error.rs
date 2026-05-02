use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error("{field} value out of range")]
    OutOfRange { field: &'static str },

    #[error("parse error: {message}")]
    ParseError { message: String },

    #[error("expected at most {max_dp} decimal places, got {actual_dp}")]
    ScaleError { max_dp: u32, actual_dp: u32 },

    #[error("conversion would lose precision")]
    LossyRounding,

    #[error("conversion error: {message}")]
    ConvError { message: String },
}
