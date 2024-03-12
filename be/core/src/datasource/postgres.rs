use super::error::{Result, SqlxSnafu};
use crate::datasource::connector::{DataConverter, Connector};
use datafusion::arrow::array::ArrayRef;
use hexa_proto::protos::execute::DataSourceConfig;
use snafu::ResultExt;
use sqlx::postgres::{PgConnectOptions, PgRow};
use sqlx::{Connection, ConnectOptions, Executor, PgConnection, Row};
use std::fmt::Debug;

pub struct PostgresConnector {}

impl PostgresConnector {
    pub fn new() -> Self {
        PostgresConnector {}
    }
}

impl DataConverter for PgRow {
    fn convert_array(&self) -> Result<Vec<ArrayRef>> {
        let vec: Vec<ArrayRef> = Vec::new();
        Ok(vec)
    }
}

impl Connector for PostgresConnector {

    async fn execute(config: DataSourceConfig, sql: &str) -> Result<Vec<Vec<ArrayRef>>> {
        let options = PgConnectOptions::new()
            .host(config.host.as_str())
            .username(config.username.as_str())
            .password(config.password.as_str())
            .port(config.port as u16);

        let mut conn = options.connect().await.context(SqlxSnafu {
            detail: "Postgres connect fetch all",
        })?;

        let mut res = conn.fetch_all(sqlx::query(sql)).await.context(SqlxSnafu {
            detail: "mysql exec fetch all",
        })?;

        let mut array_vec: Vec<Vec<ArrayRef>> = Vec::with_capacity(res.len());

        for k in res {
            array_vec.push(k.convert_array()?);
        }

        Ok(array_vec)
    }
}

#[cfg(test)]
mod tests {}
