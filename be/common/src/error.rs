use datafusion::error::DataFusionError;
use snafu::Snafu;
use std::result;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T, E = HexaError> = result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum HexaError {
    DataFusionError(String),
}

// Exposes a macro to create `DataFusionError::ArrowError` with optional backtrace
// #[macro_export]
// macro_rules! datafusion_err {
//     ($ERR:expr, $MESSAGE:expr) => {
//         let err_msg = format!(
//             "Error:{}: Detail Message: {}",
//             $ERR.get_back_trace(),
//             $MESSAGE
//         );
//         Err(HexaError::DataFusionError(err_msg))
//     };
// }
