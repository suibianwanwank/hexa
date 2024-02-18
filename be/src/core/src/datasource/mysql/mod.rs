use crate::datasource::common::accessor::{Accessor, SqlxAccessor};
use crate::datasource::common::meta::{ColumnDetail, DatabaseItem, SchemaDetail, TableDetail};
use crate::datasource::common::utils::{
    make_i16_array_from_row, make_i32_array_from_row, make_i64_array_from_row,
    make_i8_array_from_row, make_u16_array_from_row, make_u32_array_from_row,
    make_u64_array_from_row, make_u8_array_from_row,
};
use crate::datasource::common::DatasourceCommonError::NotSupportArrowType;
use crate::datasource::common::{MysqlAccessSourceSnafu, SendDatabaseItemStreamSnafu};
use crate::datasource::common::{SqlxFetchAllSnafu, SqlxFetchOneSnafu};
use crate::datasource::mysql::error::MysqlError::UnSupportDataType;
use crate::datasource::mysql::error::SqlxConnectSnafu;
use crate::{execute_sqlx_query_to_array, impl_row_to_array};
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType::*;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::arrow::array::ArrayRef;
use error::Result;
use snafu::ResultExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlRow};
use sqlx::Executor;
use sqlx::{Column, ConnectOptions, Database, MySql, MySqlConnection, Row, TypeInfo};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::StreamExt;

pub mod error;

pub struct MysqlAccessor {
    name: String,
    host: String,
    port: Option<u16>,
    username: String,
    password: Option<String>,
}

impl MysqlAccessor {
    pub fn new(
        name: String,
        host: String,
        port: Option<u16>,
        username: String,
        password: Option<String>,
    ) -> Self {
        MysqlAccessor {
            name,
            host,
            port,
            username,
            password,
        }
    }

    async fn inner_get_conn(&self) -> Result<MySqlConnection> {
        let mut options = MySqlConnectOptions::new()
            .host(self.host.as_str())
            .username(self.username.as_str());

        if let Some(port) = self.port {
            options = options.port(port);
        }

        if let Some(password) = self.password.clone() {
            options = options.password(password.as_str());
        }

        let conn = options.connect().await.context(SqlxConnectSnafu {})?;

        Ok(conn)
    }
}

#[async_trait]
impl SqlxAccessor for MysqlAccessor {
    type Database = MySql;

    async fn get_conn(&self) -> super::common::Result<MySqlConnection> {
        self.inner_get_conn().await.context(MysqlAccessSourceSnafu {})
    }

    async fn row_to_array(
        &self,
        rows: &[<Self::Database as Database>::Row],
        schema: SchemaRef,
    ) -> crate::datasource::common::Result<Vec<ArrayRef>> {
        mysql_row_to_array(rows, schema)
    }
}

fn mysql_type_to_arrow_type(type_name: &str) -> Result<DataType> {
    match type_name {
        "TINYINT(1)" | "BOOLEAN" | "BOOL" => Ok(Boolean),
        "TINYINT" => Ok(Int8),
        "SMALLINT" => Ok(Int16),
        "INT" => Ok(Int32),
        "BIGINT" => Ok(Int64),
        "TINYINT UNSIGNED" => Ok(UInt8),
        "SMALLINT UNSIGNED" => Ok(UInt16),
        "INT UNSIGNED" => Ok(UInt32),
        "BIGINT UNSIGNED" => Ok(UInt64),
        "FLOAT" => Ok(Float32),
        "DOUBLE" => Ok(Float64),
        "VARCHAR" | "CHAR" | "TEXT" => Ok(Utf8),
        "VARBINARY" | "BINARY" | "BLOB" => Ok(Binary),
        _ => Err(UnSupportDataType {
            datatype: type_name.to_string(),
        }),
    }
}

impl_row_to_array!(mysql_row_to_array, MySqlRow, [
    { Int8,  make_i8_array_from_row },
    { Int16, make_i16_array_from_row },
    { Int32, make_i32_array_from_row },
    { Int64, make_i64_array_from_row },
    { UInt8, make_u8_array_from_row },
    { UInt16, make_u16_array_from_row },
    { UInt32, make_u32_array_from_row },
    { UInt64, make_u64_array_from_row }
]);

#[async_trait]
impl Accessor for MysqlAccessor {
    // type DatabaseItemStream = ReceiverStream<DatabaseItem>;
    async fn get_table_detail(
        &self,
        schema: &str,
        table: &str,
    ) -> crate::datasource::common::Result<TableDetail> {
        let query = format!("SELECT * FROM {}.{} limit 1", schema, table);
        let cq = format!("SELECT COUNT(*) FROM {}.{} limit 1", schema, table);

        let mut conn = self.get_conn().await?;

        let row = conn
            .fetch_one(sqlx::query(query.as_str()))
            .await
            .context(SqlxFetchOneSnafu {})?;

        let rc: i64 = conn
            .fetch_one(sqlx::query(cq.as_str()))
            .await
            .context(SqlxFetchOneSnafu {})?
            .get(0);

        let columns = row
            .columns()
            .iter()
            .map(|c| {
                let arrow_type =
                    mysql_type_to_arrow_type(c.type_info().name()).context(MysqlAccessSourceSnafu {})?;
                Ok(ColumnDetail::new(
                    c.name().into(),
                    arrow_type,
                    c.type_info().is_null(),
                ))
            })
            .collect::<crate::datasource::common::Result<Vec<_>>>()?;

        Ok(TableDetail::new(
            self.name.clone(),
            schema.into(),
            table.into(),
            rc,
            columns,
        ))
    }

    async fn get_all_schema_and_table(
        &self,
    ) -> crate::datasource::common::Result<ReceiverStream<DatabaseItem>> {
        let (tx, rx) = mpsc::channel(100000);

        let mut conn = self.get_conn().await?;

        let mut database_rows = sqlx::query("SHOW DATABASES").fetch(&mut conn);

        while let Some(row) = database_rows
            .try_next()
            .await
            .context(SqlxFetchAllSnafu {})?
        {
            // map the row into a user-defined domain type
            let schema_name: &str = row.try_get(0).context(SqlxFetchAllSnafu {})?;

            // send schema info
            let sd = SchemaDetail {
                catalog_name: self.name.clone(),
                schema_name: schema_name.into(),
            };

            let _ = tx
                .send(DatabaseItem::Schema(sd))
                .await
                .context(SendDatabaseItemStreamSnafu {})?;

            //do next

            let sql = format!(
                "select table_name from information_schema.tables where table_schema= '{}'",
                schema_name
            );

            let mut conn1 = self.get_conn().await?;
            let mut table_rows = sqlx::query(&sql).fetch(&mut conn1);

            while let Some(row) = table_rows.try_next().await.context(SqlxFetchAllSnafu {})? {
                // send table info
                let table_name: &str = row.try_get(0).context(SqlxFetchAllSnafu {})?;

                let td = TableDetail::new_without_columns(
                    self.name.clone(),
                    schema_name.into(),
                    table_name.to_string(),
                );

                let _ = tx
                    .send(DatabaseItem::Table(td))
                    .await
                    .context(SendDatabaseItemStreamSnafu {})?;
            }
        }

        Ok(ReceiverStream::new(rx))
    }

    async fn query_and_get_array(&self, schema: SchemaRef, sql: &str) -> crate::datasource::common::Result<Vec<ArrayRef>> {
        execute_sqlx_query_to_array!(self, schema, sql)
    }
}