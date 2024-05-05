// use std::collections::HashMap;
// use std::sync::Arc;
// use crate::datasource::common::accessor::{Accessor, SqlxAccessor};
// use crate::datasource::common::meta::{ColumnDetail, DatabaseItem, SchemaDetail, TableDetail};
// use crate::datasource::common::utils::{
//     get_bool_from_str, get_option_u32_from_row, get_str_from_row, make_date32_array_from_row,
//     make_decimal128_array_from_row, make_decimal128_type, make_i16_array_from_row,
//     make_i32_array_from_row, make_i64_array_from_row, make_i8_array_from_row,
//     make_u16_array_from_row, make_u32_array_from_row, make_u64_array_from_row,
//     make_u8_array_from_row, make_utf8_array_from_row,
// };
// use crate::datasource::common::DatasourceCommonError::NotSupportArrowType;
// use crate::datasource::common::{MysqlAccessSourceSnafu, SendDatabaseItemStreamSnafu};
// use crate::datasource::common::{SqlxFetchAllSnafu, SqlxFetchOneSnafu};
// use crate::datasource::mysql::error::MysqlError::UnSupportDataType;
// use crate::datasource::mysql::error::SqlxConnectSnafu;
// use hexa_common::timer::TimerLoggerGuard;
// use crate::{execute_sqlx_query_to_array, impl_row_to_array};
// use async_trait::async_trait;
// use datafusion::arrow::datatypes::DataType::*;
// use datafusion::arrow::datatypes::{DataType, SchemaRef};
// use datafusion_common::arrow::array::ArrayRef;
// use lazy_static::lazy_static;
// use error::Result;
// use snafu::ResultExt;
// use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
// use sqlx::{ConnectOptions, Database, MySql, MySqlConnection, Row};
// use sqlx::{Executor};
// use tokio::sync::{mpsc, Mutex};
// use tokio::sync::mpsc::Sender;
// use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
// use tonic::codegen::tokio_stream::StreamExt;
// use tracing::info;
// use hexa_common::during_time_info;
//
// pub mod error;
//
//
// /// Not the best design, may be cached all connection type
// lazy_static! {
//     static ref DB_POOL: Arc<Mutex<HashMap<String, MySqlConnection>>> = Arc::new(Mutex::new(HashMap::new()));
// }
//
// pub struct MysqlAccessor {
//     name: String,
//     host: String,
//     port: Option<u16>,
//     username: String,
//     password: Option<String>,
// }
//
// impl MysqlAccessor {
//     pub fn new(
//         name: String,
//         host: String,
//         port: Option<u16>,
//         username: String,
//         password: Option<String>,
//     ) -> Self {
//         MysqlAccessor {
//             name,
//             host,
//             port,
//             username,
//             password,
//         }
//     }
//
//     async fn inner_get_conn<'a>(&self) -> Result<&'a MySqlConnection> {
//         during_time_info!("Connect to database mysql!");
//
//         if let Some(conn) = DB_POOL.lock().await.get_mut(&self.name) {
//             return Ok(conn.clone());
//         }
//
//         let mut options = MySqlConnectOptions::new()
//             .host(self.host.as_str())
//             .username(self.username.as_str());
//
//         if let Some(port) = self.port {
//             options = options.port(port);
//         }
//
//         if let Some(password) = self.password.clone() {
//             options = options.password(password.as_str());
//         }
//
//         let conn = options.connect().await.context(SqlxConnectSnafu {})?;
//
//         Ok(&conn)
//     }
// }
//
// #[async_trait]
// impl SqlxAccessor for MysqlAccessor {
//     type Database = MySql;
//
//     async fn get_conn<'a>(&self) -> super::common::Result<&'a MySqlConnection> {
//         self.inner_get_conn()
//             .await
//             .context(MysqlAccessSourceSnafu {})
//     }
//
//     async fn row_to_array(
//         &self,
//         rows: &[<Self::Database as Database>::Row],
//         schema: SchemaRef,
//     ) -> crate::datasource::common::Result<Vec<ArrayRef>> {
//         during_time_info!("Convert row to array");
//         mysql_row_to_array(rows, schema)
//     }
// }
//
// fn mysql_type_to_arrow_type(
//     type_name: &str,
//     precision: Option<u32>,
//     scale: Option<u32>,
// ) -> Result<DataType> {
//     let p = precision.map(|a| Some(a as i64)).unwrap_or(None);
//     let s = scale.map(|a| Some(a as i64)).unwrap_or(None);
//     match type_name.to_uppercase().as_str() {
//         "TINYINT(1)" | "BOOLEAN" | "BOOL" => Ok(Boolean),
//         "TINYINT" => Ok(Int8),
//         "SMALLINT" => Ok(Int16),
//         "INT" => Ok(Int32),
//         "BIGINT" => Ok(Int64),
//         "TINYINT UNSIGNED" => Ok(UInt8),
//         "SMALLINT UNSIGNED" => Ok(UInt16),
//         "INT UNSIGNED" => Ok(UInt32),
//         "BIGINT UNSIGNED" => Ok(UInt64),
//         "FLOAT" => Ok(Float32),
//         "DOUBLE" => Ok(Float64),
//         "VARCHAR" | "CHAR" | "TEXT" => Ok(Utf8),
//         "VARBINARY" | "BINARY" | "BLOB" => Ok(Binary),
//         "DECIMAL" => Ok(make_decimal128_type(p, s, 38, 9)),
//         "DATE" => Ok(Date32),
//         _ => Err(UnSupportDataType {
//             datatype: type_name.into(),
//         }),
//     }
// }
//
// impl_row_to_array!(mysql_row_to_array, MySqlRow, [
//     { Int8,  make_i8_array_from_row },
//     { Int16, make_i16_array_from_row },
//     { Int32, make_i32_array_from_row },
//     { Int64, make_i64_array_from_row },
//     { UInt8, make_u8_array_from_row },
//     { UInt16, make_u16_array_from_row },
//     { UInt32, make_u32_array_from_row },
//     { UInt64, make_u64_array_from_row },
//     { Utf8, make_utf8_array_from_row },
//     { Date32, make_date32_array_from_row },
//     { Decimal128(_p,_c), make_decimal128_array_from_row}
// ]);
//
// #[async_trait]
// impl Accessor for MysqlAccessor {
//     async fn get_table_detail(
//         &self,
//         schema: &str,
//         table: &str,
//     ) -> crate::datasource::common::Result<TableDetail> {
//         let query = format!(
//             "select * from information_schema.columns where table_schema = '{}' and table_name = '{}'",
//             schema,
//             table
//         );
//         let cq = format!("SELECT COUNT(*) FROM {}.{} limit 1", schema, table);
//
//         let mut conn = self.get_conn().await?;
//
//         let rows = conn
//             .fetch_all(sqlx::query(query.as_str()))
//             .await
//             .context(SqlxFetchOneSnafu {})?;
//
//         let rc: i64 = conn
//             .fetch_one(sqlx::query(cq.as_str()))
//             .await
//             .context(SqlxFetchOneSnafu {})?
//             .get(0);
//
//         let columns = rows
//             .iter()
//             .map(|row| {
//                 let name = get_str_from_row(row, COLUMN_NAME_FIELD)?;
//                 let type_name = get_str_from_row(row, COLUMN_TYPE_FIELD)?;
//                 let precision = get_option_u32_from_row(row, PRECISION_FIELD)?;
//                 let scale = get_option_u32_from_row(row, SCALE_FIELD)?;
//                 let dt = mysql_type_to_arrow_type(type_name.as_str(), precision, scale)
//                     .context(MysqlAccessSourceSnafu {})?;
//                 let nullable = get_bool_from_str(
//                     get_str_from_row(row, IS_NULLABLE_FIELD)?.as_str(),
//                     "YES",
//                     "NO",
//                 )?;
//                 Ok(ColumnDetail::new(name, dt, nullable))
//             })
//             .collect::<crate::datasource::common::Result<Vec<_>>>()?;
//
//         Ok(TableDetail::new(
//             self.name.clone(),
//             schema.into(),
//             table.into(),
//             rc,
//             columns,
//         ))
//     }
//
//     async fn get_all_schema_and_table(
//         &self,
//     ) -> crate::datasource::common::Result<ReceiverStream<DatabaseItem>> {
//         let (tx, rx) = mpsc::channel(10000);
//
//         //TODO may we need a connection manager to optimize it
//         let conn1 = self.get_conn().await?;
//         let conn2 = self.get_conn().await?;
//         let catalog_name = self.name.clone();
//         tracing::log::info!("connect to pg data source Successful!");
//         tokio::spawn(async move {
//             get_database_item_and_send(tx, conn1, conn2, catalog_name)
//                 .await
//                 .unwrap()
//         });
//
//         Ok(ReceiverStream::new(rx))
//     }
//
//     async fn query_and_get_array(
//         &self,
//         schema: SchemaRef,
//         sql: &str,
//     ) -> crate::datasource::common::Result<Vec<ArrayRef>> {
//         execute_sqlx_query_to_array!(self, schema, sql)
//     }
// }
//
// async fn get_database_item_and_send(
//     tx: Sender<DatabaseItem>,
//     mut conn1: MySqlConnection,
//     mut conn2: MySqlConnection,
//     catalog_name: String,
// ) -> crate::datasource::common::Result<()> {
//     let mut database_rows = sqlx::query("SHOW DATABASES").fetch(&mut conn1);
//
//     while let Some(row) = database_rows
//         .try_next()
//         .await
//         .context(SqlxFetchAllSnafu {})?
//     {
//         // map the row into a user-defined domain type
//         let schema_name = get_str_from_row(&row, SCHEMA_NAME_FIELD)?;
//         info!(
//             "collect schema {} in connect :{}",
//             schema_name, catalog_name
//         );
//
//         // send schema info
//         let sd = SchemaDetail {
//             catalog_name: catalog_name.clone(),
//             schema_name: schema_name.clone(),
//         };
//
//         tx.send(DatabaseItem::Schema(sd))
//             .await
//             .context(SendDatabaseItemStreamSnafu {})?;
//
//         //do next
//
//         let sql = format!(
//             "select table_name from information_schema.tables where table_schema= '{}'",
//             schema_name.clone()
//         );
//
//         let mut table_rows = sqlx::query(&sql).fetch(&mut conn2);
//
//         while let Some(row) = table_rows.try_next().await.context(SqlxFetchAllSnafu {})? {
//             // send table info
//             let table_name: String = get_str_from_row(&row, TABLE_NAME_FIELD)?;
//             info!(
//                 "collect table {}.{} in connect :{}",
//                 schema_name, table_name, catalog_name
//             );
//
//             let td = TableDetail::new_without_columns(
//                 catalog_name.clone(),
//                 schema_name.clone(),
//                 table_name,
//             );
//
//             tx.send(DatabaseItem::Table(td))
//                 .await
//                 .context(SendDatabaseItemStreamSnafu {})?;
//         }
//     }
//     Ok(())
// }
//
// const SCHEMA_NAME_FIELD: &str = "Database";
//
// const TABLE_NAME_FIELD: &str = "TABLE_NAME";
//
// const COLUMN_NAME_FIELD: &str = "COLUMN_NAME";
//
// const IS_NULLABLE_FIELD: &str = "IS_NULLABLE";
//
// const COLUMN_TYPE_FIELD: &str = "DATA_TYPE";
//
// const PRECISION_FIELD: &str = "NUMERIC_PRECISION";
//
// const SCALE_FIELD: &str = "NUMERIC_SCALE";
