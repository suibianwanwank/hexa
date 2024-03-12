use datafusion::arrow::array::ArrayRef;
use sqlx::{Connection, Executor};
use super::error::{Result};
use hexa_proto::protos::execute::DataSourceConfig;



pub trait Connector{
    async fn execute(config: DataSourceConfig, sql: &str) -> Result<Vec<Vec<ArrayRef>>>;
}

pub trait DataConverter {
    fn convert_array(&self) -> Result<Vec<ArrayRef>>;
}
