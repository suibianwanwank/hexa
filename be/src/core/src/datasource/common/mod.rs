use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use snafu::Snafu;
use tokio::sync::mpsc::error::SendError;
use crate::datasource::common::meta::DatabaseItem;
use crate::datasource::dispatch::SourceType;
use crate::datasource::mysql::error::MysqlError;
use crate::datasource::postgres::error::PostgresError;

pub(crate) mod macros;

pub mod accessor;
pub mod utils;
pub mod meta;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DatasourceCommonError {
    #[snafu(display("Mysql Access error"))]
    MysqlAccessSource {
        source: MysqlError,
    },
    #[snafu(display("Postgres Access error"))]
    PostgresAccessSource {
        source: PostgresError,
    },
    #[snafu(display("Try Get sqlx Row"))]
    TryGetSqlxRow {
        source: sqlx::error::Error,
    },
    #[snafu(display("Not support arrow data type:{tp:?}"))]
    NotSupportArrowType {
        tp: DataType,
    },
    #[snafu(display("create {dt} array error,detail:{detail}"))]
    ArrayCreate {
        dt: DataType,
        detail: String,
    },
    #[snafu(display("Decimal can not to i128"))]
    DecimalI128Create {
    },
    #[snafu(display("gen record batch error!"))]
    RecordBatchCreate {
        source: ArrowError,
    },
    #[snafu(display("missing access source type:{tp:?}"))]
    MissingAccessType {
        tp: SourceType,
    },
    #[snafu(display("sqlx fetch all error!"))]
    SqlxFetchAll {
        source: sqlx::error::Error,
    },
    #[snafu(display("sqlx fetch one error!"))]
    SqlxFetchOne {
        source: sqlx::error::Error,
    },
    #[snafu(display("sqlx fetch one error!"))]
    SendDatabaseItemStream {
        source: SendError<DatabaseItem>,
    },
    #[snafu(display("Unexpected metadata collection result! detail:{detail}"))]
    UnexpectedMetaResult {
        detail: String,
    },
}

pub type Result<T, E = DatasourceCommonError> = std::result::Result<T, E>;