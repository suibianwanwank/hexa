use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::{Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, metrics};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_common::stats::Precision;
use crate::execution::physical_plan::config::SourceScanConfig;
use crate::execution::physical_plan::stream::ScanStream;

#[derive(Debug)]
pub struct SqlScanExec {
    basic_config: SourceScanConfig,
    statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
}

impl SqlScanExec{
    pub fn new(
        basic_config: SourceScanConfig,
        statistics: Statistics,
    ) -> Self {


        //todo
        let table_stats = Statistics {
            num_rows: Precision::Absent,
            // TODO correct byte size?
            total_byte_size: Precision::Absent,
            column_statistics: Default::default(),
        };

        Self {
            basic_config,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics: table_stats,
        }
    }
}

impl DisplayAs for SqlScanExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SqlScanExec: sql={:?}", self.basic_config.sql)
            }
        }
    }
}

impl ExecutionPlan for SqlScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.basic_config.source_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
        Ok(false)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.basic_config.source_schema.clone())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {

        let stream = ScanStream::new(self.basic_config.clone(), metrics::Time::default());

        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.statistics.clone())
    }
}

