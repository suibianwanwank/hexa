use datafusion::arrow::array::ArrayRef;
use hexa_proto::protos::execute::{DataSourceConfig, SourceType};
use crate::datasource::connector::{Connector};
use crate::datasource::error::DatasourceError;
use crate::datasource::mysql::MysqlConnector;
use crate::datasource::postgres::PostgresConnector;
use super::error;

pub struct ConnectorManager {}


impl ConnectorManager {
    pub async fn submit_sql(sql: &str, config: DataSourceConfig) -> error::Result<Vec<Vec<ArrayRef>>> {
        let conn = match config.source_type() {
            SourceType::Mysql => {
                MysqlConnector::execute(config, sql).await
            }
            SourceType::Postgresql => {
                PostgresConnector::execute(config, sql).await
            }
            _ => Err(DatasourceError::Type {detail: "not support type" })
        };

        conn
    }
}