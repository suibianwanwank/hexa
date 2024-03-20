use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use datafusion::arrow::array::{RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::metrics;
use futures_core::Stream;
use hexa_common::gen_index_type;
use std::ops::{AddAssign, Index};
use std::task::ready;
use datafusion::common::{DataFusionError, exec_err};
use snafu::OptionExt;
use crate::datasource::manager::{DataSourceConnector, QueryContext};
use crate::execution::physical_plan::config::SourceScanConfig;

gen_index_type!(SqlIndex);

pub struct ScanStream {
    fut: StreamStateFut,
    schema: SchemaRef,
    elapsed_compute: metrics::Time,
}

type StreamStateFut =
    Pin<Box<dyn Future<Output = (ScanStreamState, Option<error::Result<RecordBatch>>)> + Send>>;

impl ScanStream {
    pub fn new(
        config: SourceScanConfig,
        elapsed_compute: metrics::Time,
    ) -> SendableRecordBatchStream {
        let state = Box::pin(ScanStreamState::new(config.clone()).poll_next());
        Box::pin(Self {
            fut: state,
            schema: config.source_schema.clone(),
            elapsed_compute,
        })
    }
}

impl RecordBatchStream for ScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ScanStream {
    type Item = error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> Poll<Option<Self::Item>> {
        let (state, rb) = ready!(self.fut.as_mut().poll(cx));
        match rb {
            None => Poll::Ready(None),
            Some(rb) => {
                self.fut = Box::pin(state.poll_next());
                Poll::Ready(Some(rb))
            }
        }
    }
}

struct ScanStreamState {
    config: SourceScanConfig,
    index: SqlIndex,
}

impl ScanStreamState {
    pub fn new(config: SourceScanConfig) -> Self {
        Self {
            config,
            index: 0.into(),
        }
    }

    async fn poll_next(mut self) -> (Self, Option<error::Result<RecordBatch>>) {

        if self.index.0 >= self.config.sql.len() {
            return (self, None);
        }

        let rb = self.read_data(self.index.into()).await;

        self.index.incr();

        (self, Some(rb))
    }

    async fn read_data(&mut self, index: usize) -> error::Result<RecordBatch> {
        let config = self.config.clone();


        let sql = config.sql.get(index).unwrap();
        let sql = (*sql).clone();

        let ctx = QueryContext::new(sql, config.source_schema, config.config);

        let rb= DataSourceConnector::query(&ctx).await.map_err(
            |e| DataFusionError::Execution(e.to_string())
        );

        rb
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::physical_plan::stream::ScanStream;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::{metrics};
    use datafusion::{assert_batches_eq, physical_plan};
    use std::sync::Arc;
    use hexa_proto::protos::datafusion::DataSourceConfig;
    use crate::execution::physical_plan::config::SourceScanConfig;

    #[tokio::test]
    async fn test_scan_stream() -> datafusion::common::Result<()> {
        let field_a = Field::new("a1", DataType::Int32, false);
        let field_b = Field::new("b2", DataType::Int32, false);
        let field_c = Field::new("c1", DataType::Int32, false);

        let schema = Schema::new(vec![field_a, field_b, field_c]);

        let config = SourceScanConfig{
            sql: Vec::new(),
            config: DataSourceConfig::default(),
            source_schema:Arc::new(schema)
        };

        let stream = ScanStream::new(
            config,
            metrics::Time::default(),
        );
        let batches = physical_plan::common::collect(stream).await?;

        let expected = [
            "+----+----+----+",
            "| a1 | b2 | c1 |",
            "+----+----+----+",
            "+----+----+----+",
        ];

        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}
