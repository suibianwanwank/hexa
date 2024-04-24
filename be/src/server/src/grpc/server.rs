use crate::grpc::{
    DatasourceConfigTryFromSnafu, ExecuteStreamSnafu, ExecutionPlanTryIntoSnafu,
    GetTableDetailSnafu, ListTableAndSchemasSnafu, TableInfoTryFromSnafu,
};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use hexa::datasource::dispatch::{DataSourceConfig, QueryDispatcher};
use hexa_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use hexa_proto::protobuf::bridge_server::Bridge;
use hexa_proto::protobuf::{
    GetTableRequest, ListTablesRequest, ListTablesResponse, PhysicalPlanNode, RowDisplayResult,
    TableInfo,
};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{error, info, span, Level};

#[derive(Debug, Default, Copy, Clone)]
pub struct BeConnectBridgeService {}

/// Server-side implementation for communicating with Front end's grpc.
#[async_trait]
impl Bridge for BeConnectBridgeService {
    type executeQueryStream = ReceiverStream<Result<RowDisplayResult, Status>>;

    async fn execute_query(
        &self,
        request: Request<PhysicalPlanNode>,
    ) -> Result<Response<Self::executeQueryStream>, Status> {
        map_result_to_status_err(self.inner_execute_query(request).await)
    }

    type listTablesInCatalogStream = ReceiverStream<Result<ListTablesResponse, Status>>;

    async fn list_tables_in_catalog(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<Self::listTablesInCatalogStream>, Status> {
        map_result_to_status_err(self.inner_list_tables_in_catalog(request).await)
    }

    async fn get_table_detail(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<TableInfo>, Status> {
        map_result_to_status_err(self.inner_get_table_detail(request).await)
    }
}

impl BeConnectBridgeService {
    async fn inner_execute_query(
        &self,
        request: Request<PhysicalPlanNode>,
    ) -> super::Result<Response<ReceiverStream<Result<RowDisplayResult, Status>>>> {
        let ctx = SessionContext::new();

        let id = ctx.session_id();

        let span = span!(Level::INFO, "session", id);

        let _entered = span.enter();

        let plan = request
            .into_inner()
            .try_into_physical_plan(&ctx, &ctx.runtime_env(), &DefaultPhysicalExtensionCodec {})
            .context(ExecutionPlanTryIntoSnafu {})?;

        // Print execution plan
        let dp = DisplayableExecutionPlan::new(plan.as_ref());

        info!(
            "Receive Execution Plan from FE, start to execute plan: {}",
            dp.indent(true)
        );

        let rx = self.execute_task(plan).await?;

        Ok(Response::new(rx))
    }

    async fn inner_list_tables_in_catalog(
        &self,
        request: Request<ListTablesRequest>,
    ) -> super::Result<Response<ReceiverStream<Result<ListTablesResponse, Status>>>> {
        let qd = QueryDispatcher {};

        let conf = DataSourceConfig::try_from(&request.into_inner().config.unwrap())
            .context(DatasourceConfigTryFromSnafu {})?;

        let stream = qd
            .list_all_tables_and_schemas(conf)
            .await
            .context(ListTableAndSchemasSnafu {})?;

        let map_stream = stream_to_receiver(stream, |item| {
            map_result_to_status_err(ListTablesResponse::try_from(&item))
        })
        .await?;

        Ok(Response::new(map_stream))
    }

    async fn inner_get_table_detail(
        &self,
        request: Request<GetTableRequest>,
    ) -> super::Result<Response<TableInfo>> {
        let qd = QueryDispatcher {};

        let req = request.into_inner();

        let conf = DataSourceConfig::try_from(&req.config.unwrap())
            .context(DatasourceConfigTryFromSnafu {})?;

        let detail = qd
            .get_table_detail(conf, req.schema_name.as_str(), req.table_name.as_str())
            .await
            .context(GetTableDetailSnafu {})?;

        let info = TableInfo::try_from(&detail).context(TableInfoTryFromSnafu {})?;

        Ok(Response::new(info))
    }

    pub async fn execute_task(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> super::Result<ReceiverStream<Result<RowDisplayResult, Status>>> {
        let tc = Arc::new(TaskContext::default());

        let rbs = execute_stream(plan, tc).context(ExecuteStreamSnafu {})?;

        let rs = stream_to_receiver(rbs, |item| {
            Ok(RowDisplayResult::from(
                map_result_to_status_err(item_to_string(item))?.as_bytes(),
            ))
        })
        .await?;

        info!("Execution complete!");

        Ok(rs)
    }
}

fn item_to_string(rb: datafusion::error::Result<RecordBatch>) -> datafusion::error::Result<String> {
    let display = &[rb?];
    Ok(pretty_format_batches(display)?.to_string())
}

fn map_result_to_status_err<T, E>(r: Result<T, E>) -> Result<T, Status>
where
    snafu::Report<E>: std::fmt::Display,
{
    r.map_err(|e| {
        let es = snafu::Report::from_error(e).to_string();
        error!("{}", es);
        Status::internal(es)
    })
}

/// map stream to [`ReceiverStream`]
async fn stream_to_receiver<K, S, F, T>(
    mut stream: K,
    mut map_fn: F,
) -> super::Result<ReceiverStream<S>>
where
    F: FnMut(T) -> S + Send + 'static,
    T: Send + 'static,
    K: Stream<Item = T> + Unpin + Send + 'static,
    S: Send + 'static
{
    let (tx, rx) = mpsc::channel(1000000);

    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            match tx.send(map_fn(item)).await {
                Ok(_) => {}
                //TODO how to handler async error!
                Err(_e) => {
                    info!("Failed to send message");
                }
            };
        }
        info!("stream map finished!");
    });

    Ok(ReceiverStream::new(rx))
}
