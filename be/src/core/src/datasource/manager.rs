use super::error;
use crate::datasource::connector::Connector;
use crate::datasource::error::{BatchCreateSnafu, DataSnafu, DatasourceError};
use crate::datasource::mysql::MysqlConnector;
use datafusion::arrow::array::{RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use snafu::ResultExt;
use sqlx::{Executor, FromRow};
use hexa_proto::protos::datafusion::{DataSourceConfig, SourceType};

pub struct DataSourceConnector {}
#[derive(Debug, FromRow)]
pub struct QueryContext{
    #[sqlx(try_from = "String")]
    sql: String,
    schema: SchemaRef,
    config: DataSourceConfig
}

impl QueryContext{

    pub fn new(sql: String, schema: SchemaRef, config: DataSourceConfig) -> Self{
        Self{
            sql,
            schema,
            config
        }
    }

    pub fn config(&self) -> DataSourceConfig{
        self.config.clone()
    }

    pub fn sql(&self) -> &str{
        self.sql.as_str()
    }

    pub fn schema(&self) -> SchemaRef{
        self.schema.clone()
    }
}

impl DataSourceConnector {
    pub async fn query(ctx: &QueryContext) -> error::Result<RecordBatch> {

        let result = match ctx.config.source_type() {
            SourceType::Mysql => MysqlConnector::execute(ctx).await,
            // SourceType::Postgresql => PostgresConnector::execute(ctx).await,
            _ => Err(DatasourceError::SourceType {
                detail: "not support type",
            }),
        }?;

        let rb = RecordBatch::try_new(ctx.schema(), result).context(BatchCreateSnafu {
            detail: "gen record batch",
        });

        rb
    }
}
