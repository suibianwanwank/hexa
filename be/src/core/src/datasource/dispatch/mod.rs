use crate::datasource::common::accessor::Accessor;
use crate::datasource::common::meta::{DatabaseItem, TableDetail};
use crate::datasource::common::DatasourceCommonError::MissingAccessType;
use crate::datasource::common::RecordBatchCreateSnafu;
use crate::datasource::common::Result;
// use crate::datasource::mysql::MysqlAccessor;
use crate::datasource::postgres::PostgresAccessor;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use snafu::ResultExt;
use sqlx::FromRow;
use std::collections::HashMap;
use std::pin::Pin;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use hexa_common::during_time_info;
use hexa_common::timer::TimerLoggerGuard;

pub struct QueryDispatcher {}

#[derive(Debug, Clone)]
pub struct DataSourceConfig {
    pub name: String,
    pub source_type: SourceType,
    pub host: String,
    pub port: i32,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    pub option: HashMap<String, String>,
}

impl DataSourceConfig {}

#[derive(Debug, Clone)]
pub enum SourceType {
    Mysql,
    Postgresql,
    Hive2,
    Iceberg,
}

#[derive(Debug, FromRow)]
pub struct QueryContext {
    sql: String,
    schema: SchemaRef,
    config: DataSourceConfig,
}

impl QueryContext {
    pub fn new(sql: String, schema: SchemaRef, config: DataSourceConfig) -> Self {
        Self {
            sql,
            schema,
            config,
        }
    }

    pub fn config(&self) -> DataSourceConfig {
        self.config.clone()
    }

    pub fn sql(&self) -> &str {
        self.sql.as_str()
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl QueryDispatcher {
    pub async fn query(
        &self,
        config: DataSourceConfig,
        schema: SchemaRef,
        sql: &str,
    ) -> Result<RecordBatch> {
        during_time_info!(format!("Finished to execute query,scan sql is: [{}]!\n", sql));
        let access = self.get_accessor(config).await?;

        let array = access.query_and_get_array(schema.clone(), sql).await?;

        RecordBatch::try_new(schema, array).context(RecordBatchCreateSnafu {})
    }

    pub async fn list_all_tables_and_schemas(
        &self,
        config: DataSourceConfig,
    ) -> Result<ReceiverStream<DatabaseItem>> {
        during_time_info!("Access list table and schemas finished");

        let accessor = self.get_accessor(config).await?;

        accessor.get_all_schema_and_table().await
    }

    pub async fn get_table_detail(
        &self,
        config: DataSourceConfig,
        schema: &str,
        table: &str,
    ) -> Result<TableDetail> {
        let accessor = self.get_accessor(config).await?;

        accessor.get_table_detail(schema, table).await
    }

    pub async fn get_accessor(&self, config: DataSourceConfig) -> Result<Pin<Box<dyn Accessor>>> {
        match config.source_type {
            // SourceType::Mysql => Ok(Box::pin(MysqlAccessor::new(
            //     config.name.clone(),
            //     config.host.clone(),
            //     Some(config.port as u16),
            //     config.username.clone(),
            //     Some(config.password.clone()),
            // ))),
            SourceType::Postgresql => Ok(Box::pin(PostgresAccessor::new(
                config.name.clone(),
                config.host.clone(),
                Some(config.port as u16),
                config.username.clone(),
                Some(config.password.clone()),
                config.database.clone(),
            ))),
            _ => Err(MissingAccessType {
                tp: config.source_type.clone(),
            }),
        }
    }
}