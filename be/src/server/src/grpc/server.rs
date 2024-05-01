use crate::grpc::util::{format_all_batch_to_query_response, map_result_to_status_err};
use crate::grpc::{
    DatasourceConfigTryFromSnafu, ExecuteStreamSnafu, ExecutionPlanTryIntoSnafu,
    GetTableDetailSnafu, ListTableAndSchemasSnafu, TableInfoTryFromSnafu,
};
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use hexa::datasource::dispatch::{DataSourceConfig, QueryDispatcher};
use hexa_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use hexa_proto::protobuf::bridge_server::Bridge;
use hexa_proto::protobuf::{
    ExecQueryRequest, ExecQueryResponse, GetTableRequest, ListTablesRequest, ListTablesResponse,
    TableInfo,
};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{info, span, Level};

#[derive(Debug, Default, Copy, Clone)]
pub struct BeConnectBridgeService {}

/// Server-side implementation for communicating with Front end's grpc.
#[async_trait]
impl Bridge for BeConnectBridgeService {
    type executeQueryStream = ReceiverStream<Result<ExecQueryResponse, Status>>;

    async fn execute_query(
        &self,
        request: Request<ExecQueryRequest>,
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
        request: Request<ExecQueryRequest>,
    ) -> super::Result<Response<ReceiverStream<Result<ExecQueryResponse, Status>>>> {
        let req = request.into_inner();

        let node = req.node.unwrap();
        let job_id = req.header.unwrap().job_id;

        let runtime = Arc::new(RuntimeEnv::default());
        let session_state = SessionState::new_with_config_rt(SessionConfig::new(), runtime)
            .with_session_id(job_id.clone());
        let ctx = SessionContext::new_with_state(session_state);

        let span = span!(Level::INFO, "[Job_id]", job_id);

        let _entered = span.enter();

        // proto to datafusion plan
        let plan = node
            .try_into_physical_plan(&ctx, &ctx.runtime_env(), &DefaultPhysicalExtensionCodec {})
            .context(ExecutionPlanTryIntoSnafu {})?;

        // Print execution plan
        let dp = DisplayableExecutionPlan::new(plan.as_ref());

        info!(
            "Receive Execution Plan from FE, start to execute plan:\n {}",
            dp.indent(true)
        );

        // execute execution plan and return a stream
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

    async fn execute_task(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> super::Result<ReceiverStream<Result<ExecQueryResponse, Status>>> {
        let tc = Arc::new(TaskContext::default());

        // Attention! Here was expected to asynchronous return results, but found that
        // if you return the result after the format, because the stream is not parsed,
        // do not know the length of all the values in the table,
        // so wait until the future to return the record batch proto message, and then enable the async!


        // let mut rb_stream = execute_stream(plan, tc).context(ExecuteStreamSnafu {})?;
        //
        // let (tx, rx) = mpsc::channel(1000);
        //
        // //
        // tokio::spawn(async move {
        //     send_channel_message(
        //         &tx,
        //         format_columns_to_query_response("Query Result", &rb_stream.schema()),
        //     )
        //     .await;
        //     while let Some(item) = rb_stream.next().await {
        //         let resp = format_batch_to_query_response(&map_result_to_status_err(item)?);
        //         send_channel_message(&tx, resp).await;
        //     }
        //     info!("Finished to send execution response!");
        // });


        let (tx, rx) = mpsc::channel(1000);

        let rbs = collect(plan.clone(), tc).await.context(ExecuteStreamSnafu {})?;

        let msg = format_all_batch_to_query_response(&rbs);

        send_channel_message(&tx, msg).await;

        info!("Finished to send execution response!, return count:{}", rbs.len());

        Ok(ReceiverStream::new(rx))
    }
}

async fn send_channel_message<T>(tx: &Sender<T>, resp: T) {
    match tx.send(resp).await {
        Ok(_) => {}
        //TODO how to handler async error!
        Err(_e) => {
            info!("Failed to send message");
        }
    };
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
    S: Send + 'static,
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
