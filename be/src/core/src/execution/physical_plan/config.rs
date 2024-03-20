use datafusion::arrow::datatypes::SchemaRef;
use hexa_proto::protos::datafusion::DataSourceConfig;
#[derive(Debug, Clone)]
pub struct SourceScanConfig{
    pub sql: Vec<String>,
    pub config: DataSourceConfig,
    pub source_schema: SchemaRef,
}