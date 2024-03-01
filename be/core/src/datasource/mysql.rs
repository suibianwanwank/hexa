use super::error::{Result, SqlxSnafu};
use crate::datasource::connector::{DataConverter, Connector};
use datafusion::arrow::array::ArrayRef;
use hexa_proto::protos::execute::DataSourceConfig;
use snafu::ResultExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlRow};
use sqlx::{Connection, ConnectOptions, Executor, MySqlConnection, Row};
use std::fmt::Debug;

pub struct MysqlConnector {}


impl DataConverter for MySqlRow {
    fn convert_array(&self) -> Result<Vec<ArrayRef>> {
        let vec: Vec<ArrayRef> = Vec::new();
        Ok(vec)
    }
}

impl Connector for MysqlConnector {
    fn get_connection(config: DataSourceConfig) -> Result<Box<MySqlConnection>> {
        todo!()
    }

    async fn execute(config: DataSourceConfig, sql: &str) -> Result<Vec<Vec<ArrayRef>>> {
        let options = MySqlConnectOptions::new()
            .host(config.host.as_str())
            .username(config.username.as_str())
            .password(config.password.as_str())
            .port(config.port as u16);


        let mut conn = options.connect().await.context(SqlxSnafu {
            detail: "mysql connect fetch all",
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
mod tests {


    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        // let a = MysqlConnect::submit("select * from tpcds.call_center limit 100").await;
        // for row in a.unwrap() {
        //     let t = row.columns();
        //     t.
        //
        //     let a = row.try_get(0).unwrap();
        //
        //
        //
        //
        //     let c = a.to_owned().decode();
        //
        //     println!("{}", c);

        // }
        Ok(())
    }
}
