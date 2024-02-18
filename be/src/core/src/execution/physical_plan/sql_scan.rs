use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics};
use datafusion::error::{Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, metrics};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_common::ColumnStatistics;
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
    ) -> Self {

        let size = basic_config.source_schema.fields.size();

        let mut  cs = Vec::with_capacity(size);


        for _i in 0..size{
            cs.push(ColumnStatistics::new_unknown())
        }

        let table_stats = Statistics {
            num_rows: Precision::Absent,
            // TODO correct byte size?
            total_byte_size: Precision::Absent,
            column_statistics: cs,
        };

        Self {
            basic_config,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics: table_stats,
        }
    }

    pub fn fix_scan_schema(&mut self, schema: SchemaRef){
        self.basic_config = SourceScanConfig {
            sql_list: self.basic_config.sql_list.clone(),
            config: self.basic_config.config.clone(),
            source_schema: schema,
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
                write!(f, "SqlScanExec: sql={:?}", self.basic_config.sql_list)
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {

        let stream = ScanStream::stream(&self.basic_config, metrics::Time::default());

        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.statistics.clone())
    }
}

