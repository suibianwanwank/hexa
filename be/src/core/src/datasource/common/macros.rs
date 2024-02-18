/// Macro for generating the match arms when converting a Vec<Row> in sqlx to a `ArrayRef`.
///
/// Arguments:
/// `builder`: The type of the array's builder that needs to be converted
/// `rows`: array that implements the row trait in sqlx
/// `col_idx`: the column index to row
#[macro_export]
macro_rules! make_array_data {
    ($builder:ty, $rows:expr, $col_idx:expr) => {{
        let mut arr = <$builder>::with_capacity($rows.len());
        for row in $rows.iter() {
            arr.append_value(row.try_get($col_idx).map_err(|e| ArrayCreate {
                detail: e.to_string(),
            })?);
        }
        Arc::new(arr.finish())
    }};
}

#[macro_export]
macro_rules! impl_sqlx_rows_to_primitive_array_data {
    ($( { $fName:ident, $rty:ty, $builder:ty } ),*) => {
        $(
            pub fn $fName<'a, 'b, T>(dt: DataType, rows: &'a [T], index: &'b str) -> $crate::datasource::common::Result<ArrayRef>
                where
                    T: sqlx::Row,
                    &'b str: sqlx::ColumnIndex<T>,
                    $rty: sqlx::Decode<'a, <T as Row>::Database> + sqlx::Type<<T as Row>::Database>,
            {
                let mut arr = <$builder>::with_capacity(rows.len());
                for row in rows.iter() {
                    arr.append_value(row.try_get(index).map_err(|e| ArrayCreate {
                        dt: dt.clone(),
                        detail: e.to_string(),
                    })?);
                }
                Ok(Arc::new(arr.finish()))
            }
        )*
    }
}


/// Macro for Executing a query and return RecordBatch results Connection
/// and type conversion via `SqlxAccessor`
///
/// Arguments:
/// `ac`: The expr implementation of the `SqlxAccessor` trait.
/// `schema`: the schema of query
/// `sql`: The sql to be executed
#[macro_export]
macro_rules! execute_sqlx_query_to_array {
    ($ac: expr, $schema: expr, $sql: expr) => {{
        let mut conn = $ac.get_conn().await?;

        let rows = conn
            .fetch_all(sqlx::query($sql))
            .await
            .context(SqlxFetchAllSnafu {})?;

        let array = $ac.row_to_array(&rows, $schema.clone()).await?;

        // TODO: May need fix plan schema
        Ok((array))
    }};
}

#[macro_export]
macro_rules! execute_sqlx_fetch_one {
    ($ac:expr, $sql: expr) => {{
        let mut conn = $ac.get_conn().await?;

        conn.fetch_one(sqlx::query($sql))
            .await
            .context(SqlxFetchAllSnafu {})?;
    }};
}

#[macro_export]
macro_rules! for_primitive_array_variants {
    ($macro:ident $(, $x:ident)*) => {
        $macro! {
            {make_i8_array_from_row, i8, Int8Builder},
            {make_i16_array_from_row, i16, Int16Builder},
            {make_i32_array_from_row, i32, Int32Builder},
            {make_i64_array_from_row, i64, Int64Builder},
            {make_u8_array_from_row, u8, UInt8Builder},
            {make_u16_array_from_row, u16, UInt16Builder},
            {make_u32_array_from_row, u32, UInt32Builder},
            {make_u64_array_from_row, u64, UInt64Builder}
        }
    };
}

/// The implementation of the sqlx row to array transformation.
///
/// Arguments:
/// `name`: function name to be generated.
/// `row`: Type of [`sqlx::Row`] associated with the [`sqlx::Database`]
/// The last parameter pass an array representing a list that means the datatype and the method of
/// converting [`sqlx::Row`]of sqlx to it.
#[macro_export]
macro_rules! impl_row_to_array {
    ($name:ident, $row:ty, [ $( { $tp:ident $( ( $($param:ident),+) )?, $mfn:ident } ),* ]) => {
        pub fn $name(rows: &[$row], schema: SchemaRef) -> $crate::datasource::common::Result<Vec<ArrayRef>>
        {
            let mut res: Vec<ArrayRef> = Vec::new();

            schema.fields.iter().try_for_each(|field| {
                let array: ArrayRef = match field.data_type() {
                    $(
                        $tp $(( $($param),+ ))? => {
                            $mfn::<$row>(field.data_type().clone(), rows, field.name())?
                        },
                    )*
                    _ => {
                        return Err(NotSupportArrowType {
                            tp: field.data_type().clone(),
                        })
                    }
                };
                res.push(array);

                Ok(())
            })?;

            Ok(res)
        }
    };
}
