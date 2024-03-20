use crate::datasource::error;
use datafusion::arrow::array::{
    ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use sqlx::Row;
use std::sync::Arc;
use snafu::ResultExt;
use crate::datasource::error::DatasourceError::Sqlx;
use crate::datasource::error::SqlxSnafu;

pub fn build_int8_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, i8: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = Int8Array::builder(len);

    for row in rows {
        let i: i8 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(i);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_int16_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, i16: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = Int16Array::builder(len);

    for row in rows {
        let i: i16 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(i);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_int32_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, i32: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = Int32Array::builder(len);

    for row in rows {
        let i: i32 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(i);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_int64_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, i64: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = Int64Array::builder(len);

    for row in rows {
        let i: i64 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(i);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_uint8_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, u8: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = UInt8Array::builder(len);

    for row in rows {
        let u: u8 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(u);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_uint16_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, u16: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = UInt16Array::builder(len);

    for row in rows {
        let u: u16 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(u);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_uint32_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, u32: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = UInt32Array::builder(len);

    for row in rows {
        let u: u32 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(u);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}

pub fn build_uint64_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, u64: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut builder = UInt64Array::builder(len);

    for row in rows {
        let u: u64 = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        builder.append_value(u);
    }

    let data = builder.finish();

    Ok(Arc::new(data))
}


pub fn build_str_array<'a, C>(rows: &'a Vec<C>, index: usize, len: usize) -> error::Result<ArrayRef>
    where
        C: Row, usize: sqlx::ColumnIndex<C>, String: sqlx::Decode<'a, <C as Row>::Database> + sqlx::Type<<C as Row>::Database>,
{
    let mut value: Vec<String> = Vec::new();

    for row in rows {
        let s: String = row.try_get(index).context(SqlxSnafu {
            detail: "can't parse u16"
        })?;
        value.push(s);
    }

    let data = StringArray::from(value);

    Ok(Arc::new(data))
}
