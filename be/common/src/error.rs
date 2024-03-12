use std::result;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T, E = HexaError> = result::Result<T, E>;

#[derive(Debug)]
pub enum HexaError {
    DataFusionError(String),
}