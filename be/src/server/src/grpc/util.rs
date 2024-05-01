use datafusion::arrow::array::{RecordBatch};
use datafusion::arrow::util::pretty::{pretty_format_batches};
use hexa_proto::protobuf::exec_query_response::Data::QueryResult;
use hexa_proto::protobuf::ExecQueryResponse;
use tonic::Status;
use tracing::error;
pub(crate) fn format_all_batch_to_query_response(
    batch: &[RecordBatch]
) -> Result<ExecQueryResponse, Status> {
    let table = map_result_to_status_err(pretty_format_batches(batch))?;

    Ok(ExecQueryResponse {
        data: Some(QueryResult(table.to_string().into_bytes())),
    })
}

pub(crate) fn map_result_to_status_err<T, E>(r: Result<T, E>) -> Result<T, Status>
where
    snafu::Report<E>: std::fmt::Display,
{
    r.map_err(|e| {
        let es = snafu::Report::from_error(e).to_string();
        error!("{}", es);
        Status::internal(es)
    })
}
