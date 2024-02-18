use datafusion::arrow::datatypes::DataType;

#[derive(Clone, Debug)]
pub struct SchemaDetail {
    pub catalog_name: String,
    pub schema_name: String,
}

#[derive(Clone, Debug)]
pub struct TableDetail {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub row_count: i64,
    pub columns: Vec<ColumnDetail>,
}

impl TableDetail {
    pub fn new_without_columns(
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> Self {
        TableDetail {
            catalog_name,
            schema_name,
            table_name,
            row_count: -1,
            columns: Vec::new(),
        }
    }

    pub fn new(
        catalog_name: String,
        schema_name: String,
        table_name: String,
        row_count: i64,
        columns: Vec<ColumnDetail>,
    ) -> Self {
        TableDetail {
            catalog_name,
            schema_name,
            table_name,
            row_count,
            columns,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDetail {
    pub column_name: String,
    pub column_type: DataType,
    pub nullable: bool,
}

impl ColumnDetail {
    pub fn new(column_name: String, column_type: DataType, nullable: bool) -> Self {
        Self {
            column_name,
            column_type,
            nullable,
        }
    }
}

#[derive(Debug)]
pub enum DatabaseItem {
    Schema(SchemaDetail),
    Table(TableDetail),
}
