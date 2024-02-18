use datafusion::error::DataFusionError;
use snafu::Snafu;
use hexa::datasource::common::DatasourceCommonError;

pub mod server;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum GrpcError {
    #[snafu(display("Failed to generate execution plan from grpc!"))]
    ExecutionPlanTryInto { source: DataFusionError },
    #[snafu(display("Failed to generate datasource config from grpc!"))]
    DatasourceConfigTryFrom { source: DataFusionError },
    #[snafu(display("Failed to generate table info to grpc!"))]
    TableInfoTryFrom { source: DataFusionError },
    #[snafu(display("Failed to execute stream!"))]
    ExecuteStream { source: DataFusionError },
    #[snafu(display("Failed to send message!, detail:{detail}"))]
    ChannelSend { detail: String },
    #[snafu(display("Failed to execute list table and schemas!"))]
    ListTableAndSchemas { source: DatasourceCommonError },
    #[snafu(display("Failed to execute get table detail!"))]
    GetTableDetail { source: DatasourceCommonError },
}

pub type Result<T, E = GrpcError> = std::result::Result<T, E>;
