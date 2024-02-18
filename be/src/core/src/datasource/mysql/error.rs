use datafusion::arrow::error::ArrowError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum MysqlError {
    #[snafu(display("sqlx connect mysql error!"))]
    SqlxConnect {
        source: sqlx::error::Error,
    },
    #[snafu(display("sqlx query mysql error! detail:{detail}"))]
    SqlxQuery {
        detail: &'static str,
        source: sqlx::error::Error,
    },
    #[snafu(display("arrow error! detail:{detail}"))]
    ArrowCreate {
        detail: &'static str,
        source: ArrowError,
    },
    #[snafu(display("not support mysql type: {datatype} !"))]
    UnSupportDataType {
        datatype: String,
    },
}

pub type Result<T, E = MysqlError> = std::result::Result<T, E>;