pub(crate) mod error;

use crate::datasource::common::accessor::{Accessor, SqlxAccessor};
use crate::datasource::common::meta::{ColumnDetail, DatabaseItem, SchemaDetail, TableDetail};
use crate::datasource::common::utils::{
    get_bool_from_str, get_option_i32_from_row, get_str_from_row, make_date32_array_from_row,
    make_decimal128_array_from_row, make_decimal128_type, make_i16_array_from_row,
    make_i32_array_from_row, make_i64_array_from_row, make_i8_array_from_row,
    make_utf8_array_from_row,
};
use crate::datasource::common::DatasourceCommonError::NotSupportArrowType;
use crate::datasource::common::PostgresAccessSourceSnafu;
use crate::datasource::common::SqlxFetchAllSnafu;
use crate::datasource::postgres::error::PostgresError::UnSupportDataType;
use crate::datasource::postgres::error::SqlxConnectSnafu;
use crate::{execute_sqlx_query_to_array, impl_row_to_array};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType::*;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use error::Result;
use snafu::ResultExt;
use sqlx::postgres::{PgConnectOptions, PgRow};
use sqlx::{ConnectOptions, Database, Executor, PgConnection, Postgres, Row};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::log::info;

pub struct PostgresAccessor {
    name: String,
    host: String,
    port: Option<u16>,
    username: String,
    password: Option<String>,
    database: Option<String>,
}

impl PostgresAccessor {
    pub fn new(
        name: String,
        host: String,
        port: Option<u16>,
        username: String,
        password: Option<String>,
        database: Option<String>,
    ) -> Self {
        PostgresAccessor {
            name,
            host,
            port,
            username,
            password,
            database,
        }
    }

    async fn inner_get_conn(&self) -> Result<PgConnection> {
        let mut options = PgConnectOptions::new()
            .host(self.host.as_str())
            .username(self.username.as_str());

        if let Some(port) = self.port {
            options = options.port(port);
        }

        if let Some(password) = self.password.clone() {
            options = options.password(password.as_str());
        }

        if let Some(database) = self.database.clone() {
            options = options.database(database.as_str());
        }

        let conn = options.connect().await.context(SqlxConnectSnafu {})?;

        Ok(conn)
    }
}

#[async_trait]
impl SqlxAccessor for PostgresAccessor {
    type Database = Postgres;

    async fn get_conn(&self) -> super::common::Result<PgConnection> {
        self.inner_get_conn()
            .await
            .context(PostgresAccessSourceSnafu {})
    }

    async fn row_to_array(
        &self,
        rows: &[<Self::Database as Database>::Row],
        schema: SchemaRef,
    ) -> crate::datasource::common::Result<Vec<ArrayRef>> {
        pg_row_to_array(rows, schema)
    }
}

#[async_trait]
impl Accessor for PostgresAccessor {
    async fn get_table_detail(
        &self,
        schema: &str,
        table: &str,
    ) -> crate::datasource::common::Result<TableDetail> {
        // TODO query system table
        let query = format!(
            "select * from information_schema.columns where table_schema = '{}' and table_name = '{}'",
            schema,
            table
        );

        // let query = format!("SELECT * FROM {}.{} limit 1", schema, table);
        let cq = format!("SELECT COUNT(*) FROM {}.{}", schema, table);

        let mut conn = self.get_conn().await?;

        let rows = conn
            .fetch_all(sqlx::query(query.as_str()))
            .await
            .context(crate::datasource::common::SqlxFetchOneSnafu {})?;
        info!("Execute fetch all sql:{}", query);

        let rc: i64 = conn
            .fetch_one(sqlx::query(cq.as_str()))
            .await
            .context(crate::datasource::common::SqlxFetchOneSnafu {})?
            .get(0);

        let columns = rows
            .iter()
            .map(|row| {
                let name = get_str_from_row(row, COLUMN_NAME_FIELD)?;
                let type_name = get_str_from_row(row, COLUMN_TYPE_FIELD)?;
                let precision = get_option_i32_from_row(row, PRECISION_FIELD)?;
                let scale = get_option_i32_from_row(row, SCALE_FIELD)?;
                let dt = pg_type_to_arrow_type(type_name.as_str(), precision, scale)
                    .context(PostgresAccessSourceSnafu {})?;
                let nullable = get_bool_from_str(
                    get_str_from_row(row, IS_NULLABLE_FIELD)?.as_str(),
                    "YES",
                    "NO",
                )?;
                Ok(ColumnDetail::new(name, dt, nullable))
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
        let (tx, rx) = mpsc::channel(10000);

        //TODO may we need a connection manager to optimize it
        let conn1 = self.get_conn().await?;
        let conn2 = self.get_conn().await?;
        let catalog_name = self.name.clone();
        info!("connect to pg data source Successful!");
        tokio::spawn(async move { get_database_item_and_send(tx, conn1, conn2, catalog_name).await.unwrap() });

        Ok(ReceiverStream::new(rx))
    }

    async fn query_and_get_array(
        &self,
        schema: SchemaRef,
        sql: &str,
    ) -> crate::datasource::common::Result<Vec<ArrayRef>> {
        execute_sqlx_query_to_array!(self, schema, sql)
    }
}

async fn get_database_item_and_send(
    tx: Sender<DatabaseItem>,
    mut conn1: PgConnection,
    mut conn2: PgConnection,
    catalog_name: String,
) -> crate::datasource::common::Result<()> {
    info!("Start to collect pg");

    let mut database_rows =
        sqlx::query("SELECT * FROM information_schema.schemata").fetch(&mut conn1);

    while let Some(row) = database_rows
        .try_next()
        .await
        .context(SqlxFetchAllSnafu {})?
    {
        let schema_name = get_str_from_row(&row, TABLE_SCHEMA_FIELD)?;
        tracing::info!(
            "collect schema {} in connect :{}",
            &schema_name,
            catalog_name
        );

        let sd = SchemaDetail {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
        };

        let _ = tx
            .send(DatabaseItem::Schema(sd))
            .await
            .context(crate::datasource::common::SendDatabaseItemStreamSnafu {});

        //do next

        let sql = format!(
            "select table_name from information_schema.tables where table_schema= '{}'",
            &schema_name
        );

        let mut table_rows = sqlx::query(&sql).fetch(&mut conn2);

        while let Some(row) = table_rows.try_next().await.context(SqlxFetchAllSnafu {})? {
            // send table info
            let table_name = get_str_from_row(&row, TABLE_NAME_FIELD)?;
            tracing::info!(
                "collect table {}.{} in connect :{}",
                &schema_name,
                &table_name,
                catalog_name
            );

            let td = TableDetail::new_without_columns(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.to_string(),
            );

            tx.send(DatabaseItem::Table(td))
                .await
                .context(crate::datasource::common::SendDatabaseItemStreamSnafu {})?;
        }
    }

    Ok(())
}

impl_row_to_array!(pg_row_to_array, PgRow, [
    { Int8,  make_i8_array_from_row },
    { Int16, make_i16_array_from_row },
    { Int32, make_i32_array_from_row },
    { Int64, make_i64_array_from_row },
    { Utf8, make_utf8_array_from_row },
    { Date32, make_date32_array_from_row },
    { Decimal128(_p,_c), make_decimal128_array_from_row}
]);

fn pg_type_to_arrow_type(
    type_name: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> Result<DataType> {
    let p = precision.map(|a| Some(a as i64)).unwrap_or(None);
    let s = scale.map(|a| Some(a as i64)).unwrap_or(None);
    match type_name.to_uppercase().as_str() {
        "BOOL" => Ok(Boolean),
        "CHAR" | "CHARACTER" | "CHARACTER VARYING" => Ok(Utf8),
        "SMALLINT" | "SMALLSERIAL" | "INT2" => Ok(Int16),
        "INT" | "SERIAL" | "INT4" | "INTEGER" => Ok(Int32),
        "BIGINT" | "BIGSERIAL" | "INT8" => Ok(Int64),
        "REAL" | "FLOAT4" => Ok(Float32),
        "DATE" => Ok(Date32),
        // default setting
        "NUMERIC" => Ok(make_decimal128_type(p, s, 38, 9)),
        "DOUBLE PRECISION" | "FLOAT8" => Ok(Float64),
        "VARCHAR" | "TEXT" => Ok(Utf8),
        _ => Err(UnSupportDataType {
            datatype: type_name.to_string(),
        }),
    }
}

const TABLE_SCHEMA_FIELD: &str = "schema_name";

const TABLE_NAME_FIELD: &str = "table_name";

const COLUMN_NAME_FIELD: &str = "column_name";

const IS_NULLABLE_FIELD: &str = "is_nullable";

const COLUMN_TYPE_FIELD: &str = "data_type";

const PRECISION_FIELD: &str = "numeric_precision";

const SCALE_FIELD: &str = "numeric_scale";
