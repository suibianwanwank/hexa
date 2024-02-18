use super::Result;
use crate::datasource::common::meta::{DatabaseItem, TableDetail};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use sqlx::Database;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

/// Accessor for connecting to sqlx data sources.
///
/// For the data sources provided in sqlx, we can simplify the repetitive code
/// by implementing this trait and by calling the [`crate::impl_row_to_array`], [`crate::execute_sqlx_query_to_array`] or more macros
///
/// Calling [`get_conn`] produces an [`Connection`] Associated type of [`Database`]
///
/// After implementing [`SqlxAccessor`], you can query the results and convert the result into a RecordBatch
/// by calling the [`execute_sqlx_query_to_array`] macro.
/// See the specific implementation of [`crate::datasource::postgres::PostgresAccessor`] for details
#[async_trait]
pub trait SqlxAccessor: Sync + Send {
    /// A database driver, for details, see sqlx [Database].
    type Database: Database;

    /// Get the Connection associated with the Database for accessing the database.
    async fn get_conn(&self) -> Result<<Self::Database as Database>::Connection>;

    /// Convert the queried sqlx row to an Array.
    async fn row_to_array(
        &self,
        rows: &[<Self::Database as Database>::Row],
        schema: SchemaRef,
    ) -> Result<Vec<ArrayRef>>;
}

/// Trait for connecting to data source.
///
/// Includes fn for metadata collection and data querying.
#[async_trait]
pub trait Accessor: Send + Sync + 'static {
    /// Get table details for a given path
    ///
    /// Currently, returns the number of rows and columns,
    /// columns correspond to the arrow type and nullable,Possible future expansion.
    ///
    /// Type inference is based primarily on the source connection to the rust data type
    /// and the value inside the corresponding arrow. Of course, it's not absolute,
    /// get at some point, there is a better alternative. Depends on actual realisation.
    async fn get_table_detail(&self, schema: &str, table: &str) -> Result<TableDetail>;

    /// Get all table and schema information in the connection.
    ///
    /// Note that the return is a stream that contains an enumeration of DataItem's, containing both schema and table information
    async fn get_all_schema_and_table(&self) -> Result<ReceiverStream<DatabaseItem>>;

    /// Execute the query and convert the result into vec of [`ArrayRef`].
    async fn query_and_get_array(&self, schema: SchemaRef, sql: &str)->Result<Vec<ArrayRef>>;
}
