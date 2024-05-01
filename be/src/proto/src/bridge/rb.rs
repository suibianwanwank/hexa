use crate::common::proto_error;
use crate::protobuf::list_tables_response::Info;
use crate::protobuf::{
    ArrowType, FieldInfo, ListTablesResponse, SchemaInfo, SourceScanConfig,
    TableInfo,
};
use datafusion_common::DataFusionError;
use hexa::datasource::common::meta::{DatabaseItem, TableDetail};
use hexa::datasource::dispatch::{DataSourceConfig, SourceType};

impl TryFrom<&crate::protobuf::SourceType> for SourceType {
    type Error = DataFusionError;

    fn try_from(source: &crate::protobuf::SourceType) -> Result<Self, Self::Error> {
        match source {
            crate::protobuf::SourceType::Mysql => Ok(SourceType::Mysql),
            crate::protobuf::SourceType::Hive2 => Ok(SourceType::Hive2),
            crate::protobuf::SourceType::Postgresql => Ok(SourceType::Postgresql),
            _ => Err(proto_error("can not parse source type")),
        }
    }
}

impl TryFrom<&TableDetail> for TableInfo {
    type Error = DataFusionError;

    fn try_from(table_detail: &TableDetail) -> Result<Self, Self::Error> {
        let fields = table_detail
            .columns
            .iter()
            .map(|col| {
                let dt = ArrowType::try_from(&col.column_type)
                    .map_err(|e| proto_error(format!("can not parse datatype, detail:{}", e)))?;

                Ok(FieldInfo {
                    field_name: col.column_name.clone(),
                    type_name: Some(dt),
                    nullable: col.nullable,
                })
            })
            .collect::<datafusion::error::Result<Vec<FieldInfo>>>()?;
        Ok(TableInfo {
            catalog_name: table_detail.catalog_name.clone(),
            schema_name: table_detail.schema_name.clone(),
            table_name: table_detail.table_name.clone(),
            row_count: table_detail.row_count,
            fields,
        })
    }
}

impl TryFrom<&SourceScanConfig> for DataSourceConfig {
    type Error = DataFusionError;

    fn try_from(config: &SourceScanConfig) -> Result<Self, Self::Error> {
        let t = crate::protobuf::SourceType::try_from(config.source_type).map_err(|_| {
            proto_error(format!(
                "Received a Scan message with unknown SourceType :{}",
                config.source_type
            ))
        })?;

        Ok(DataSourceConfig {
            name: config.name.clone(),
            source_type: SourceType::try_from(&t)?,
            host: config.host.clone(),
            port: config.port,
            username: config.username.clone(),
            password: config.password.clone(),
            database: Some(config.database.clone()),
            option: config.option_parameters.clone(),
        })
    }
}

impl TryFrom<&DatabaseItem> for ListTablesResponse {
    type Error = DataFusionError;

    fn try_from(item: &DatabaseItem) -> Result<Self, Self::Error> {
        let info = match item {
            DatabaseItem::Schema(schema) => {
                let schema_info = SchemaInfo {
                    catalog_name: schema.catalog_name.clone(),
                    schema_name: schema.schema_name.clone(),
                };
                Info::SchemaInfo(schema_info)
            }
            DatabaseItem::Table(table) => {
                let table_info = TableInfo {
                    catalog_name: table.catalog_name.clone(),
                    schema_name: table.schema_name.clone(),
                    table_name: table.table_name.clone(),
                    row_count: -1,
                    fields: vec![],
                };
                Info::TableInfo(table_info)
            }
        };

        Ok(ListTablesResponse { info: Some(info) })
    }
}
