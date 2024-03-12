use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{plan_err, Statistics};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use hexa_proto::protos::execute::DataSourceConfig;
use crate::datasource::manager::ConnectorManager;

#[derive(Debug)]
pub struct SqlScanExec {
    sql: String,
    config: DataSourceConfig,
    schema: SchemaRef,
    statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for SqlScanExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SqlScanExec: sql={}", self.sql)
            }
        }
    }
}

impl ExecutionPlan for SqlScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // TODO optimize CrossJoin implementation to generate M * N partitions
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(false)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        //TODO
        EquivalenceProperties::new(self.schema.clone())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.statistics.clone())
    }
}

