#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BytesResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortMergeJoinExecNode {
    #[prost(message, optional, tag = "1")]
    pub left: ::core::option::Option<super::datafusion::PhysicalPlanNode>,
    #[prost(message, optional, tag = "2")]
    pub right: ::core::option::Option<super::datafusion::PhysicalPlanNode>,
    #[prost(message, repeated, tag = "3")]
    pub on: ::prost::alloc::vec::Vec<super::datafusion::JoinOn>,
    #[prost(message, optional, tag = "4")]
    pub filter: ::core::option::Option<super::datafusion::JoinFilter>,
    #[prost(enumeration = "super::datafusion::JoinType", tag = "5")]
    pub join_type: i32,
    #[prost(bool, tag = "6")]
    pub null_equals_null: bool,
    #[prost(message, repeated, tag = "7")]
    pub left_sort_expr: ::prost::alloc::vec::Vec<
        super::datafusion::PhysicalSortExprNode,
    >,
    #[prost(message, repeated, tag = "8")]
    pub right_sort_expr: ::prost::alloc::vec::Vec<
        super::datafusion::PhysicalSortExprNode,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataSourceConfig {
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    #[prost(string, tag = "2")]
    pub host: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub port: i32,
    #[prost(string, tag = "4")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub database: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "7")]
    pub option_parameters: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SqlScanExecNode {
    #[prost(enumeration = "SourceType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub sql: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    Mysql = 0,
    Hive2 = 1,
    Postgresql = 2,
    Oracle = 3,
    Sqlserver = 4,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SourceType::Mysql => "MYSQL",
            SourceType::Hive2 => "HIVE2",
            SourceType::Postgresql => "POSTGRESQL",
            SourceType::Oracle => "ORACLE",
            SourceType::Sqlserver => "SQLSERVER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MYSQL" => Some(Self::Mysql),
            "HIVE2" => Some(Self::Hive2),
            "POSTGRESQL" => Some(Self::Postgresql),
            "ORACLE" => Some(Self::Oracle),
            "SQLSERVER" => Some(Self::Sqlserver),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod back_end_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct BackEndServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl BackEndServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> BackEndServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> BackEndServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            BackEndServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn submit_task(
            &mut self,
            request: impl tonic::IntoRequest<super::super::datafusion::PhysicalPlanNode>,
        ) -> Result<tonic::Response<super::BytesResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/execute.BackEndService/submitTask",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod back_end_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with BackEndServiceServer.
    #[async_trait]
    pub trait BackEndService: Send + Sync + 'static {
        async fn submit_task(
            &self,
            request: tonic::Request<super::super::datafusion::PhysicalPlanNode>,
        ) -> Result<tonic::Response<super::BytesResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct BackEndServiceServer<T: BackEndService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: BackEndService> BackEndServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for BackEndServiceServer<T>
    where
        T: BackEndService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/execute.BackEndService/submitTask" => {
                    #[allow(non_camel_case_types)]
                    struct submitTaskSvc<T: BackEndService>(pub Arc<T>);
                    impl<
                        T: BackEndService,
                    > tonic::server::UnaryService<
                        super::super::datafusion::PhysicalPlanNode,
                    > for submitTaskSvc<T> {
                        type Response = super::BytesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::datafusion::PhysicalPlanNode,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).submit_task(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = submitTaskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: BackEndService> Clone for BackEndServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: BackEndService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: BackEndService> tonic::server::NamedService for BackEndServiceServer<T> {
        const NAME: &'static str = "execute.BackEndService";
    }
}
