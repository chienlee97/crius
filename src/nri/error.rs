use thiserror::Error;

#[derive(Debug, Error)]
pub enum NriError {
    #[error("NRI is disabled")]
    Disabled,
    #[error("NRI transport error: {0}")]
    Transport(String),
    #[error("NRI plugin error: {0}")]
    Plugin(String),
    #[error("NRI invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, NriError>;
