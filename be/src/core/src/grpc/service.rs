use datafusion::prelude::SessionContext;
use tonic::{Request, Response, Status};
use hexa_proto::protos::datafusion::PhysicalPlanNode;
use hexa_proto::protos::execute::back_end_service_server::BackEndService;
use hexa_proto::protos::execute::BytesResponse;

struct BackEndRpcService{
}

impl BackEndService for BackEndRpcService{
    async fn submit_task(&self, request: Request<PhysicalPlanNode>) -> Result<Response<BytesResponse>, Status> {

        let ctx = SessionContext::new();

        let node = request.into_inner();




        let plan = node.try_into_physical_plan(&ctx, &ctx.runtime_env());

        let ctx = SessionContext::new();



        Ok(())

    }
}

