use crate::datasource::common::DatasourceCommonError::ArrayCreate;
use crate::datasource::common::RecordBatchCreateSnafu;
use crate::{for_primitive_array_variants, impl_sqlx_rows_to_primitive_array_data};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDate;
use datafusion::arrow::array::{ArrayRef, Date32Builder, Decimal128Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::{Decimal128, Int8, Utf8};
use snafu::ResultExt;
use sqlx::Row;
use std::sync::Arc;

for_primitive_array_variants! {impl_sqlx_rows_to_primitive_array_data}

pub fn make_utf8_array_from_row<'a, 'b, T>(
    dt: DataType,
    rows: &'a [T],
    index: &'b str,
) -> crate::datasource::common::Result<ArrayRef>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // default capacity 1024
    let mut arr = StringBuilder::with_capacity(1024, rows.len());
    for row in rows.iter() {
        let s: String = row.try_get(index).map_err(|e| ArrayCreate {
            dt: dt.clone(),
            detail: e.to_string(),
        })?;
        arr.append_value(s);
    }

    Ok(Arc::new(arr.finish()))
}

pub fn make_utf8_array_from_row2<'a, 'b, T>(
    dt: DataType,
    rows: &'a [T],
    index: &'b str,
) -> crate::datasource::common::Result<ArrayRef>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // default capacity 1024
    let mut arr = StringBuilder::with_capacity(1024, rows.len());
    for row in rows.iter() {
        let s: String = row.try_get(index).map_err(|e| ArrayCreate {
            dt: dt.clone(),
            detail: e.to_string(),
        })?;
        arr.append_value(s);
    }
    Ok(Arc::new(arr.finish()))
}

pub fn make_date32_array_from_row<'a, 'b, T>(
    dt: DataType,
    rows: &'a [T],
    index: &'b str,
) -> crate::datasource::common::Result<ArrayRef>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    NaiveDate: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // todo capacity
    let mut arr = Date32Builder::with_capacity(rows.len());

    // unix epoch date
    let ed = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    for row in rows.iter() {
        let date: NaiveDate = row.try_get(index).map_err(|e| ArrayCreate {
            dt: dt.clone(),
            detail: e.to_string(),
        })?;

        arr.append_value(date.signed_duration_since(ed).num_days() as i32);
    }
    Ok(Arc::new(arr.finish()))
}

pub fn make_decimal128_array_from_row<'a, 'b, T>(
    dt: DataType,
    rows: &'a [T],
    index: &'b str,
) -> crate::datasource::common::Result<ArrayRef>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    BigDecimal: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // todo capacity

    match dt {
        Decimal128(p, s) => {
            let mut arr = Decimal128Builder::with_capacity(rows.len());

            for row in rows.iter() {
                let d: Option<BigDecimal> = row.try_get(index).map_err(|e| ArrayCreate {
                    dt: dt.clone(),
                    detail: e.to_string(),
                })?;

                match d {
                    None => {
                        arr.append_null();
                    }
                    Some(d) => {
                        let rd = d.with_scale(s as i64);

                        let (v, _) = rd.into_bigint_and_exponent();
                        let vi = match v.to_i128() {
                            None => Err(ArrayCreate {
                                dt: dt.clone(),
                                detail: "Decimal can not to i128".into(),
                            }),
                            Some(v) => Ok(v),
                        }?;
                        arr.append_value(vi);
                    }
                }
            }
            let arr = arr
                .finish()
                .with_precision_and_scale(p, s)
                .context(RecordBatchCreateSnafu {})?;
            Ok(Arc::new(arr))
        }
        _ => Err(ArrayCreate {
            dt: dt.clone(),
            detail: "Not match Arrow type decimal128".into(),
        }),
    }
}

pub fn get_str_from_row<'a, 'b, T>(row: &'a T, index: &'b str) -> crate::datasource::common::Result<String>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // todo capacity
    let s: String = row.try_get(index).map_err(|e| ArrayCreate {
        dt: Utf8,
        detail: e.to_string(),
    })?;
    Ok(s)
}

pub fn get_option_str_from_row<'a, 'b, T>(row: &'a T, index: &'b str) -> crate::datasource::common::Result<Option<String>>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // todo capacity
    let s: Option<String> = row.try_get(index).map_err(|e| ArrayCreate {
        dt: Utf8,
        detail: e.to_string(),
    })?;
    Ok(s)
}


pub fn get_option_i32_from_row<'a, 'b, T>(row: &'a T, index: &'b str) -> crate::datasource::common::Result<Option<i32>>
where
    T: Row,
    &'b str: sqlx::ColumnIndex<T>,
    i32: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
{
    // todo capacity
    let i: Option<i32> = row.try_get(index).map_err(|e| ArrayCreate {
        dt: Int8,
        detail: e.to_string(),
    })?;
    Ok(i)
}

pub fn make_decimal128_type(precision: Option<i32>, scale: Option<i32>, default_precision: u8, default_scale: i8) -> DataType {
    let p = match precision{
        None => {
            default_precision
        }
        Some(p) => {
            p as u8
        }
    };
    let s = match scale{
        None => {
            default_scale
        }
        Some(p) => {
            p as i8
        }
    };
    Decimal128(p, s)
}
