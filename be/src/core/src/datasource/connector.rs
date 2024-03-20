use super::error::{Result};
use crate::datasource::error;
use crate::datasource::manager::QueryContext;
use crate::datasource::utils::{build_int16_array, build_int32_array, build_int64_array, build_str_array, build_uint16_array, build_uint32_array, build_uint64_array, build_uint8_array};
use core::ops::AddAssign;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use hexa_common::gen_index_type;
use sqlx::{Database, Executor, Row};

pub trait Connector {
    async fn execute(ctx: &QueryContext) -> Result<Vec<ArrayRef>>;
}

gen_index_type!(RowIndex);

pub trait SqlxConnector {
    type ExecutorType;

    async fn get_conn(ctx: &QueryContext) -> Result<Self::ExecutorType>;

    async fn sqlx_execute(ctx: &QueryContext) -> Result<Vec<ArrayRef>>;

    fn convert_to_array<'a, T>(rows: &'a Vec<T>, schema: SchemaRef) -> Result<Vec<ArrayRef>>
    where
        T: Row,
        usize: sqlx::ColumnIndex<T>,
        u8: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        u16: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        u32: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        u64: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        i8: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        i16: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        i32: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        i64: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
        String: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
    {
        let count = rows.len();

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut res: Vec<ArrayRef> = Vec::new();

        let mut index: RowIndex = RowIndex::from(0);

        for field in schema.fields() {
            let array = match field.data_type() {
                DataType::Int16 => build_int16_array::<T>(&rows, index.0, count),
                DataType::Int32 => build_int32_array::<'a, T>(&rows, index.0, count),
                DataType::Int64 => build_int64_array::<T>(&rows, index.0, count),
                DataType::UInt8 => build_uint8_array::<T>(&rows, index.0, count),
                DataType::UInt16 => build_uint16_array::<'a, T>(&rows, index.0, count),
                DataType::UInt32 => build_uint32_array::<T>(&rows, index.0, count),
                DataType::UInt64 => build_uint64_array::<T>(&rows, index.0, count),
                DataType::Utf8 => build_str_array::<T>(&rows, index.0, count),
                _ => Err(error::DatasourceError::DataType {
                    detail: format!("Not support type, {}!", field.data_type()),
                }),
            };
            res.push(array?);
            index.incr();
        }

        Ok(res)
    }
}
