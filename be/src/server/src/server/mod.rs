use crate::grpc::server::BeConnectBridgeService;
use crate::server::log::global_log_init;
use hexa_proto::protobuf::bridge_server::BridgeServer;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::runtime::Builder;

mod log;

pub trait Server {
    fn run(verbose: u8, bind: &str);
}

/// Back End's service startup
pub struct BeServer {}

impl Server for BeServer {
    fn run(verbose: u8, bind: &str) {
        global_log_init(verbose.into());

        let addr = SocketAddr::from_str(format!("0.0.0.0:{}", bind).as_str()).expect(
            "Configured port information Error!, please check if the port is configured correctly",
        );

        let runtime = Builder::new_multi_thread()
            .thread_stack_size(4 * 1024 * 1024)
            .enable_all()
            .build()
            .expect("Tokio runtime build error");

        runtime.block_on(async move { Self::start_grpc_server(addr).await });
    }
}

impl BeServer {
    async fn start_grpc_server(addr: SocketAddr) {
        let proto_server = BridgeServer::new(BeConnectBridgeService {});

        let build = tonic::transport::Server::builder()
            .add_service(proto_server)
            .serve(addr)
            .await;

        match build {
            Ok(_) => {}
            Err(e) => {
                panic!("Server failed to start due to an error: {:?}", e);
            }
        }
    }
}
