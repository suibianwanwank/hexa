use super::error::{Result, SqlxSnafu};
use crate::datasource::connector::{Connector, SqlxConnector};
use crate::datasource::manager::QueryContext;
use datafusion::arrow::array::{
    ArrayRef
};
use snafu::ResultExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlRow};
use sqlx::{Column, ConnectOptions, Executor, MySql, MySqlConnection, Row, TypeInfo};
use std::any::{Any};

pub struct MysqlConnector {}

impl Connector for MysqlConnector {
    async fn execute(ctx: &QueryContext) -> Result<Vec<ArrayRef>> {
        Self::sqlx_execute(ctx).await
    }
}

impl SqlxConnector for MysqlConnector {
    type ExecutorType = MySqlConnection;


    async fn get_conn(ctx: &QueryContext)
                      -> Result<Self::ExecutorType> {
        let config = ctx.config();

        let options = MySqlConnectOptions::new()
            .host(config.host.as_str())
            .username(config.username.as_str())
            .password(config.password.as_str())
            .port(config.port as u16);

        let mut conn = options.connect().await.context(SqlxSnafu {
            detail: "mysql connect fetch all",
        })?;


        Ok(conn)
    }

    async fn sqlx_execute(ctx: &QueryContext) -> Result<Vec<ArrayRef>> {
        let mut conn = Self::get_conn(ctx).await?;

        let res =
            conn.fetch_all(sqlx::query(ctx.sql()))
                .await
                .context(SqlxSnafu {
                    detail: "mysql exec fetch all",
                })?;

        let array = Self::convert_to_array::<MySqlRow>(&res, ctx.schema());
        array
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use snafu::ResultExt;
    use sqlx::mysql::MySqlConnectOptions;
    use hexa_proto::protos::datafusion::DataSourceConfig;
    use crate::datasource::connector::Connector;
    use crate::datasource::error;
    use crate::datasource::error::{BatchCreateSnafu, SqlxSnafu};
    use crate::datasource::manager::QueryContext;
    use crate::datasource::mysql::MysqlConnector;

    #[tokio::test]
    async fn test_sqlx_execute() -> error::Result<()> {

        let options = MySqlConnectOptions::new()
            .host("47.236.87.181")
            .username("root")
            .password("suibianwanwan")
            .port(3306);

        Ok(())
    }



    #[tokio::test]
    async fn test_mysql_execute() -> error::Result<()> {
        let sc = DataSourceConfig{
            source_type: 0,
            host: "47.236.87.181".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "suibianwanwan".to_string(),
            database: "".to_string(),
            option_parameters: Default::default(),
        };

        let field_a = Field::new("id", DataType::UInt16, false);
        let field_b = Field::new("age", DataType::Int64, false);
        let field_c = Field::new("runoob_author", DataType::Utf8, false);

        let schema = Schema::new(vec![field_a, field_b, field_c]);
        let sql = String::from("SELECT id,age,runoob_author FROM test.tbl");
        let ctx = QueryContext::new(sql, Arc::new(schema.clone()), sc);

        let res= MysqlConnector::execute(&ctx).await?;

        let rb = RecordBatch::try_new(Arc::new(schema), res).context(BatchCreateSnafu {
            detail: "mysql connect fetch all",
        })?;;

        let batches = vec![rb];


        let expected = [
            "+----+-----+---------------+",
            "| id | age | runoob_author |",
            "+----+-----+---------------+",
            "| 1  | 2   | sda           |",
            "| 2  | 30  | abc           |",
            "+----+-----+---------------+",
        ];

        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}
