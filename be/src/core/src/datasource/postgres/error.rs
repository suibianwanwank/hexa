use datafusion::arrow::error::ArrowError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum PostgresError {
    #[snafu(display("sqlx connect postgres error!"))]
    SqlxConnect {
        source: sqlx::error::Error,
    },
    #[snafu(display("sqlx query postgres error! detail:{detail}"))]
    SqlxQuery {
        detail: &'static str,
        source: sqlx::error::Error,
    },
    #[snafu(display("arrow create error! detail:{detail}"))]
    ArrowCreate {
        detail: &'static str,
        source: ArrowError,
    },
    #[snafu(display("not support postgres type: {datatype} !"))]
    UnSupportDataType {
        datatype: String,
    },
}

pub type Result<T, E = PostgresError> = std::result::Result<T, E>;