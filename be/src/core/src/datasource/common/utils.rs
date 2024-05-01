use crate::datasource::common::DatasourceCommonError::{ArrayCreate, DecimalI128Create, UnexpectedMetaResult};
use crate::datasource::common::RecordBatchCreateSnafu;
use crate::{for_primitive_array_variants, for_sqlx_type_variants, impl_get_type_from_sqlx_row, impl_sqlx_rows_to_primitive_array_data};
use crate::datasource::common::TryGetSqlxRowSnafu;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDate;
use datafusion::arrow::array::{ArrayRef, Date32Builder, Decimal128Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::{Decimal128};
use snafu::ResultExt;
use sqlx::Row;
use std::sync::Arc;

for_primitive_array_variants! {impl_sqlx_rows_to_primitive_array_data}

for_sqlx_type_variants! {impl_get_type_from_sqlx_row}

pub fn make_utf8_array_from_row<'a, 'b, T>(
    _dt: DataType,
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
        let s: String = row.try_get(index).context(TryGetSqlxRowSnafu{})?;
        arr.append_value(s.trim_end());
    }
    Ok(Arc::new(arr.finish()))
}

pub fn make_date32_array_from_row<'a, 'b, T>(
    _dt: DataType,
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
        let date: NaiveDate = row.try_get(index).context(TryGetSqlxRowSnafu{})?;

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
    match dt {
        Decimal128(p, s) => {
            let mut arr = Decimal128Builder::with_capacity(rows.len());

            for row in rows.iter() {
                let d: Option<BigDecimal> = row.try_get(index).context(TryGetSqlxRowSnafu{})?;

                if let Some(value) = d {
                    arr.append_value(make_decimal_to_array_i128(value, s as i32)?);
                } else {
                    arr.append_null();
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

fn make_decimal_to_array_i128(
    decimal: BigDecimal,
    scale: i32,
) -> crate::datasource::common::Result<i128> {
    // new with data type scale
    let d = decimal.with_scale(scale as i64);
    let (v, _) = d.into_bigint_and_exponent();
    match v.to_i128() {
        None => Err(DecimalI128Create {}),
        Some(v) => Ok(v),
    }
}

pub fn make_decimal128_type(
    precision: Option<i64>,
    scale: Option<i64>,
    default_precision: u8,
    default_scale: i8,
) -> DataType {
    let p = match precision {
        None => default_precision,
        Some(p) => p as u8,
    };
    let s = match scale {
        None => default_scale,
        Some(s) => s as i8,
    };
    Decimal128(p, s)
}

pub fn get_bool_from_str(
    actual_word: &str,
    expect_true: &str,
    expect_false: &str,
) -> crate::datasource::common::Result<bool> {
    if actual_word == expect_true {
        return Ok(true)
    }
    if actual_word == expect_false {
        return Ok(false)
    }
    Err(UnexpectedMetaResult { detail: format!("The nullable field of the PG can only be yes or no, but the result obtained is:{}", actual_word) })
}

