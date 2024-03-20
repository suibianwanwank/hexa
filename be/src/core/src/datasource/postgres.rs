// use super::error::{Result, SqlxSnafu};
// use crate::datasource::connector::{Connector};
// use datafusion::arrow::array::ArrayRef;
// use hexa_proto::protos::execute::DataSourceConfig;
// use snafu::ResultExt;
// use sqlx::postgres::{PgConnectOptions, PgRow};
// use sqlx::{ConnectOptions, Executor, Row};
// use crate::datasource::manager::QueryContext;
//
// pub struct PostgresConnector {}
//
// impl PostgresConnector {
//     pub fn new() -> Self {
//         PostgresConnector {}
//     }
// }
//
//
//
// impl Connector for PostgresConnector {
//
//     async fn execute(ctx: QueryContext) -> Result<Vec<ArrayRef>> {
//         let config = ctx.config();
//         let sql = ctx.sql();
//
//         let options = PgConnectOptions::new()
//             .host(config.host.as_str())
//             .username(config.username.as_str())
//             .password(config.password.as_str())
//             .port(config.port as u16);
//
//         let mut conn = options.connect().await.context(SqlxSnafu {
//             detail: "Postgres connect fetch all",
//         })?;
//
//         let res = conn.fetch_all(sqlx::query(sql)).await.context(SqlxSnafu {
//             detail: "mysql exec fetch all",
//         })?;
//
//         let mut array_vec: Vec<Vec<ArrayRef>> = Vec::with_capacity(res.len());
//
//         for row in res {
//             let a = row.try_get(0).to_string();
//             array_vec.push(row.convert_array()?);
//         }
//
//         Ok(array_vec)
//     }
// }
//
// #[cfg(test)]
// mod tests {}
