use datafusion::arrow::datatypes::SchemaRef;
use crate::datasource::dispatch::DataSourceConfig;

#[derive(Debug, Clone)]
pub struct SourceScanConfig {
    pub sql_list: Vec<String>,
    pub config: DataSourceConfig,
    pub source_schema: SchemaRef,
}