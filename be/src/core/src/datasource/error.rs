use datafusion::arrow::error::ArrowError;
use snafu::Snafu;


#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum DatasourceError {
    #[snafu(display("got sqlx error! detail:{detail}"))]
    Sqlx {
        detail: &'static str,
        source: sqlx::error::Error,
    },
    #[snafu(display("type error! detail:{detail}"))]
    SourceType {
        detail: &'static str,
    },
    #[snafu(display("gen record batch error! detail:{detail}"))]
    Data {
        detail: &'static str,
        source: ArrowError,
    },
    #[snafu(display("gen record batch error! detail:{detail}"))]
    DataType {
        detail: String,
    },
    #[snafu(display("gen record batch error! detail:{detail}"))]
    BatchCreate {
        detail: &'static str,
        source: ArrowError
    },
}

pub type Result<T, E = DatasourceError> = std::result::Result<T, E>;
