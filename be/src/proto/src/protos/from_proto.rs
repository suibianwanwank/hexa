use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{AvroExec, CsvExec, FileScanConfig};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{logical_plan, Operator, ScalarUDF, UserDefinedLogicalNode};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::expressions::{
    create_aggregate_expr, in_list, BinaryExpr, CaseExpr, CastExpr, GetFieldAccessExpr,
    GetIndexedFieldExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal, NegativeExpr, NotExpr,
    TryCastExpr,
};
use datafusion::physical_expr::{functions, PhysicalExprRef, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::insert::FileSinkExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
    StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{create_window_expr, BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{
    udaf, AggregateExpr, ExecutionPlan, InputOrderMode, Partitioning, PhysicalExpr, WindowExpr,
};
use datafusion_common::{internal_err, not_impl_err, DataFusionError};
use crate::protos::datafusion::physical_aggregate_expr_node::AggregateFunction;
use crate::protos::datafusion::physical_expr_node::ExprType;
use crate::protos::datafusion::physical_plan_node::PhysicalPlanType;
use crate::protos::datafusion::repartition_exec_node::PartitionMethod;
use  crate::protos::datafusion::{
    csv_scan_exec_node, physical_expr_node, physical_get_indexed_field_expr_node,
    window_agg_exec_node, AggregateExecNode, AnalyzeExecNode, AvroScanExecNode,
    CoalesceBatchesExecNode, CoalescePartitionsExecNode, Column, CrossJoinExecNode,
    CsvScanExecNode, CsvSinkExecNode, EmptyExecNode, EmptyMessage, ExplainExecNode,
    FileScanExecConf, FilterExecNode, GlobalLimitExecNode, HashJoinExecNode, InterleaveExecNode,
    JoinOn, JoinSide, JoinType, JsonSink, JsonSinkExecNode, LocalLimitExecNode,
    NestedLoopJoinExecNode, ParquetSinkExecNode, PartiallySortedInputOrderMode, PhysicalExprNode,
    PhysicalExtensionNode, PhysicalHashRepartition, PhysicalPlanNode, PhysicalSortExprNode,
    PhysicalSortExprNodeCollection, PhysicalWindowExprNode, PlaceholderRowExecNode,
    ProjectionExecNode, RepartitionExecNode, ScalarFunction, SortExecNode, SortMergeJoinExecNode,
    SortPreservingMergeExecNode, StreamPartitionMode, SymmetricHashJoinExecNode, UnionExecNode,
    WindowAggExecNode,
};
use prost::bytes::BufMut;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[macro_export]
macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field.try_into()?)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}



impl AsExecutionPlan for PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        PhysicalPlanNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to decode physical plan: {e:?}"))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to encode physical plan: {e:?}"))
        })
    }

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{self:?}'"
            ))
        })?;
        match plan {
            PhysicalPlanType::Explain(explain) => Ok(Arc::new(ExplainExec::new(
                Arc::new(explain.schema.as_ref().unwrap().try_into()?),
                explain
                    .stringified_plans
                    .iter()
                    .map(|plan| plan.into())
                    .collect(),
                explain.verbose,
            ))),
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&projection.input, registry, runtime)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| {
                        Ok((
                            parse_physical_expr(expr, registry, input.schema().as_ref())?,
                            name.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&filter.input, registry, runtime)?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .map(|expr| parse_physical_expr(expr, registry, input.schema().as_ref()))
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing.".to_owned(),
                        )
                    })?;
                let filter_selectivity = filter.default_filter_selectivity.try_into();
                let filter = FilterExec::try_new(predicate, input)?;
                match filter_selectivity {
                    Ok(filter_selectivity) => Ok(Arc::new(
                        filter.with_default_selectivity(filter_selectivity)?,
                    )),
                    Err(_) => Err(DataFusionError::Internal(
                        "filter_selectivity in PhysicalPlanNode is invalid ".to_owned(),
                    )),
                }
            }
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                parse_protobuf_file_scan_config(scan.base_conf.as_ref().unwrap(), registry)?,
                scan.has_header,
                str_to_byte(&scan.delimiter, "delimiter")?,
                str_to_byte(&scan.quote, "quote")?,
                if let Some(csv_scan_exec_node::OptionalEscape::Escape(escape)) =
                    &scan.optional_escape
                {
                    Some(str_to_byte(escape, "escape")?)
                } else {
                    None
                },
                FileCompressionType::UNCOMPRESSED,
            ))),
            #[cfg(feature = "parquet")]
            PhysicalPlanType::ParquetScan(scan) => {
                let base_config =
                    parse_protobuf_file_scan_config(scan.base_conf.as_ref().unwrap(), registry)?;
                let predicate = scan
                    .predicate
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(expr, registry, base_config.file_schema.as_ref())
                    })
                    .transpose()?;
                Ok(Arc::new(ParquetExec::new(base_config, predicate, None)))
            }
            PhysicalPlanType::AvroScan(scan) => Ok(Arc::new(AvroExec::new(
                parse_protobuf_file_scan_config(scan.base_conf.as_ref().unwrap(), registry)?,
            ))),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&coalesce_batches.input, registry, runtime)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&merge.input, registry, runtime)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&repart.input, registry, runtime)?;
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| parse_physical_expr(e, registry, input.schema().as_ref()))
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(expr, hash_part.partition_count.try_into().unwrap()),
                        )?))
                    }
                    Some(PartitionMethod::RoundRobin(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::RoundRobinBatch(partition_count.try_into().unwrap()),
                        )?))
                    }
                    Some(PartitionMethod::Unknown(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::UnknownPartitioning(partition_count.try_into().unwrap()),
                        )?))
                    }
                    _ => internal_err!("Invalid partitioning scheme"),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&limit.input, registry, runtime)?;
                let fetch = if limit.fetch >= 0 {
                    Some(limit.fetch as usize)
                } else {
                    None
                };
                Ok(Arc::new(GlobalLimitExec::new(
                    input,
                    limit.skip as usize,
                    fetch,
                )))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&limit.input, registry, runtime)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.fetch as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&window_agg.input, registry, runtime)?;
                let input_schema = input.schema();

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .map(|window_expr| {
                        parse_physical_window_expr(window_expr, registry, input_schema.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_keys = window_agg
                    .partition_keys
                    .iter()
                    .map(|expr| parse_physical_expr(expr, registry, input.schema().as_ref()))
                    .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;

                if let Some(input_order_mode) = window_agg.input_order_mode.as_ref() {
                    let input_order_mode = match input_order_mode {
                        window_agg_exec_node::InputOrderMode::Linear(_) => InputOrderMode::Linear,
                        window_agg_exec_node::InputOrderMode::PartiallySorted(
                            PartiallySortedInputOrderMode { columns },
                        ) => InputOrderMode::PartiallySorted(
                            columns.iter().map(|c| *c as usize).collect(),
                        ),
                        window_agg_exec_node::InputOrderMode::Sorted(_) => InputOrderMode::Sorted,
                    };

                    Ok(Arc::new(BoundedWindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        partition_keys,
                        input_order_mode,
                    )?))
                } else {
                    Ok(Arc::new(WindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        partition_keys,
                    )?))
                }
            }
            PhysicalPlanType::Aggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&hash_agg.input, registry, runtime)?;
                let mode = AggregateMode::try_from(hash_agg.mode).map_err(|_| {
                    proto_error(format!(
                        "Received a AggregateNode message with unknown AggregateMode {}",
                        hash_agg.mode
                    ))
                })?;
                let agg_mode: AggregateMode = match mode {
                    AggregateMode::Partial => AggregateMode::Partial,
                    AggregateMode::Final => AggregateMode::Final,
                    AggregateMode::FinalPartitioned => AggregateMode::FinalPartitioned,
                    AggregateMode::Single => AggregateMode::Single,
                    AggregateMode::SinglePartitioned => AggregateMode::SinglePartitioned,
                };

                let num_expr = hash_agg.group_expr.len();

                let group_expr = hash_agg
                    .group_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                            .map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let null_expr = hash_agg
                    .null_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                            .map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let groups: Vec<Vec<bool>> = if !hash_agg.groups.is_empty() {
                    hash_agg
                        .groups
                        .chunks(num_expr)
                        .map(|g| g.to_vec())
                        .collect::<Vec<Vec<bool>>>()
                } else {
                    vec![]
                };

                let input_schema = hash_agg.input_schema.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "input_schema in AggregateNode is missing.".to_owned(),
                    )
                })?;
                let physical_schema: SchemaRef = SchemaRef::new(input_schema.try_into()?);

                let physical_filter_expr = hash_agg
                    .filter_expr
                    .iter()
                    .map(|expr| {
                        expr.expr
                            .as_ref()
                            .map(|e| parse_physical_expr(e, registry, &physical_schema))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let physical_aggr_expr: Vec<Arc<dyn AggregateExpr>> = hash_agg
                    .aggr_expr
                    .iter()
                    .zip(hash_agg.aggr_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        match expr_type {
                            ExprType::AggregateExpr(agg_node) => {
                                let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node.expr.iter()
                                    .map(|e| parse_physical_expr(e, registry, &physical_schema).unwrap()).collect();
                                let ordering_req: Vec<PhysicalSortExpr> = agg_node.ordering_req.iter()
                                    .map(|e| parse_physical_sort_expr(e, registry, &physical_schema).unwrap()).collect();
                                agg_node.aggregate_function.as_ref().map(|func| {
                                    match func {
                                        AggregateFunction::AggrFunction(i) => {
                                            let aggr_function = AggregateFunction::try_from(*i)
                                                .map_err(
                                                    |_| {
                                                        proto_error(format!(
                                                            "Received an unknown aggregate function: {i}"
                                                        ))
                                                    },
                                                )?;

                                            create_aggregate_expr(
                                                &aggr_function.into(),
                                                agg_node.distinct,
                                                input_phy_expr.as_slice(),
                                                &ordering_req,
                                                &physical_schema,
                                                name.to_string(),
                                            )
                                        }
                                        AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
                                            let agg_udf = registry.udaf(udaf_name)?;
                                            udaf::create_aggregate_expr(agg_udf.as_ref(), &input_phy_expr, &physical_schema, name)
                                        }
                                    }
                                }).transpose()?.ok_or_else(|| {
                                    proto_error("Invalid AggregateExpr, missing aggregate_function")
                                })
                            }
                            _ => internal_err!(
                                "Invalid aggregate expression for AggregateExec"
                            ),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggregateExec::try_new(
                    agg_mode,
                    PhysicalGroupBy::new(group_expr, null_expr, groups),
                    physical_aggr_expr,
                    physical_filter_expr,
                    input,
                    physical_schema,
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&hashjoin.left, registry, runtime)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&hashjoin.right, registry, runtime)?;
                let left_schema = left.schema();
                let right_schema = right.schema();
                let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left = parse_physical_expr(
                            &col.left.clone().unwrap(),
                            registry,
                            left_schema.as_ref(),
                        )?;
                        let right = parse_physical_expr(
                            &col.right.clone().unwrap(),
                            registry,
                            right_schema.as_ref(),
                        )?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                let join_type = JoinType::try_from(hashjoin.join_type).map_err(|_| {
                    proto_error(format!(
                        "Received a HashJoinNode message with unknown JoinType {}",
                        hashjoin.join_type
                    ))
                })?;
                let filter = hashjoin
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = f
                            .schema
                            .as_ref()
                            .ok_or_else(|| proto_error("Missing JoinFilter schema"))?
                            .try_into()?;

                        let expression = parse_physical_expr(
                            f.expression.as_ref().ok_or_else(|| {
                                proto_error("Unexpected empty filter expression")
                            })?,
                            registry, &schema,
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a HashJoinNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

                let partition_mode =
                    PartitionMode::try_from(hashjoin.partition_mode).map_err(|_| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown PartitionMode {}",
                            hashjoin.partition_mode
                        ))
                    })?;
                let partition_mode = match partition_mode {
                    PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                    PartitionMode::Partitioned => PartitionMode::Partitioned,
                    PartitionMode::Auto => PartitionMode::Auto,
                };
                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    &join_type.into(),
                    partition_mode,
                    hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::SymmetricHashJoin(sym_join) => {
                let left = into_physical_plan(&sym_join.left, registry, runtime)?;
                let right = into_physical_plan(&sym_join.right, registry, runtime)?;
                let left_schema = left.schema();
                let right_schema = right.schema();
                let on = sym_join
                    .on
                    .iter()
                    .map(|col| {
                        let left = parse_physical_expr(
                            &col.left.clone().unwrap(),
                            registry,
                            left_schema.as_ref(),
                        )?;
                        let right = parse_physical_expr(
                            &col.right.clone().unwrap(),
                            registry,
                            right_schema.as_ref(),
                        )?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                let join_type = JoinType::try_from(sym_join.join_type).map_err(|_| {
                    proto_error(format!(
                        "Received a SymmetricHashJoin message with unknown JoinType {}",
                        sym_join.join_type
                    ))
                })?;
                let filter = sym_join
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = f
                            .schema
                            .as_ref()
                            .ok_or_else(|| proto_error("Missing JoinFilter schema"))?
                            .try_into()?;

                        let expression = parse_physical_expr(
                            f.expression.as_ref().ok_or_else(|| {
                                proto_error("Unexpected empty filter expression")
                            })?,
                            registry, &schema,
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a HashJoinNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<_>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

                let left_sort_exprs =
                    parse_physical_sort_exprs(&sym_join.left_sort_exprs, registry, &left_schema)?;
                let left_sort_exprs = if left_sort_exprs.is_empty() {
                    None
                } else {
                    Some(left_sort_exprs)
                };

                let right_sort_exprs =
                    parse_physical_sort_exprs(&sym_join.right_sort_exprs, registry, &right_schema)?;
                let right_sort_exprs = if right_sort_exprs.is_empty() {
                    None
                } else {
                    Some(right_sort_exprs)
                };

                let partition_mode = StreamPartitionMode::try_from(sym_join.partition_mode)
                    .map_err(|_| {
                        proto_error(format!(
                            "Received a SymmetricHashJoin message with unknown PartitionMode {}",
                            sym_join.partition_mode
                        ))
                    })?;
                let partition_mode = match partition_mode {
                    StreamPartitionMode::SinglePartition => {
                        StreamJoinPartitionMode::SinglePartition
                    }
                    StreamPartitionMode::PartitionedExec => StreamJoinPartitionMode::Partitioned,
                };
                SymmetricHashJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    &join_type.into(),
                    sym_join.null_equals_null,
                    left_sort_exprs,
                    right_sort_exprs,
                    partition_mode,
                )
                .map(|e| Arc::new(e) as _)
            }
            PhysicalPlanType::Union(union) => {
                let mut inputs: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for input in &union.inputs {
                    inputs.push(input.try_into_physical_plan(registry, runtime)?);
                }
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::Interleave(interleave) => {
                let mut inputs: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for input in &interleave.inputs {
                    inputs.push(input.try_into_physical_plan(registry, runtime)?);
                }
                Ok(Arc::new(InterleaveExec::try_new(inputs)?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&crossjoin.left, registry, runtime)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&crossjoin.right, registry, runtime)?;
                Ok(Arc::new(CrossJoinExec::new(left, right)))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(schema)))
            }
            PhysicalPlanType::PlaceholderRow(placeholder) => {
                let schema = Arc::new(convert_required!(placeholder.schema)?);
                Ok(Arc::new(PlaceholderRowExec::new(schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&sort.input, registry, runtime)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {self:?}"
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: parse_physical_expr(expr, registry, input.schema().as_ref())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            internal_err!("physical_plan::from_proto() {self:?}")
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fetch = if sort.fetch < 0 {
                    None
                } else {
                    Some(sort.fetch as usize)
                };
                let new_sort = SortExec::new(exprs, input)
                    .with_fetch(fetch)
                    .with_preserve_partitioning(sort.preserve_partitioning);

                Ok(Arc::new(new_sort))
            }
            PhysicalPlanType::SortPreservingMerge(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&sort.input, registry, runtime)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {self:?}"
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: parse_physical_expr(expr, registry, input.schema().as_ref())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            internal_err!("physical_plan::from_proto() {self:?}")
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fetch = if sort.fetch < 0 {
                    None
                } else {
                    Some(sort.fetch as usize)
                };
                Ok(Arc::new(
                    SortPreservingMergeExec::new(exprs, input).with_fetch(fetch),
                ))
            }
            PhysicalPlanType::Extension(extension) => proto_error("Not support physical plan type"),
            PhysicalPlanType::NestedLoopJoin(join) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.left, registry, runtime)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.right, registry, runtime)?;
                let join_type = JoinType::try_from(join.join_type).map_err(|_| {
                    proto_error(format!(
                        "Received a NestedLoopJoinExecNode message with unknown JoinType {}",
                        join.join_type
                    ))
                })?;
                let filter = join
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = f
                            .schema
                            .as_ref()
                            .ok_or_else(|| proto_error("Missing JoinFilter schema"))?
                            .try_into()?;

                        let expression = parse_physical_expr(
                            f.expression.as_ref().ok_or_else(|| {
                                proto_error("Unexpected empty filter expression")
                            })?,
                            registry, &schema,
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a NestedLoopJoinExecNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

                Ok(Arc::new(NestedLoopJoinExec::try_new(
                    left,
                    right,
                    filter,
                    &join_type.into(),
                )?))
            }
            PhysicalPlanType::Analyze(analyze) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&analyze.input, registry, runtime)?;
                Ok(Arc::new(AnalyzeExec::new(
                    analyze.verbose,
                    analyze.show_statistics,
                    input,
                    Arc::new(convert_required!(analyze.schema)?),
                )))
            }
            PhysicalPlanType::JsonSink(sink) => {
                let input = into_physical_plan(&sink.input, registry, runtime)?;

                let data_sink: JsonSink = sink
                    .sink
                    .as_ref()
                    .ok_or_else(|| proto_error("Missing required field in protobuf"))?
                    .try_into()?;
                let sink_schema = convert_required!(sink.sink_schema)?;
                let sort_order = sink
                    .sort_order
                    .as_ref()
                    .map(|collection| {
                        collection
                            .physical_sort_expr_nodes
                            .iter()
                            .map(|proto| {
                                parse_physical_sort_expr(proto, registry, &sink_schema)
                                    .map(Into::into)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(Arc::new(FileSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    Arc::new(sink_schema),
                    sort_order,
                )))
            }
            PhysicalPlanType::CsvSink(sink) => {
                let input = into_physical_plan(&sink.input, registry, runtime)?;

                let data_sink: CsvSink = sink
                    .sink
                    .as_ref()
                    .ok_or_else(|| proto_error("Missing required field in protobuf"))?
                    .try_into()?;
                let sink_schema = convert_required!(sink.sink_schema)?;
                let sort_order = sink
                    .sort_order
                    .as_ref()
                    .map(|collection| {
                        collection
                            .physical_sort_expr_nodes
                            .iter()
                            .map(|proto| {
                                parse_physical_sort_expr(proto, registry, &sink_schema)
                                    .map(Into::into)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(Arc::new(FileSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    Arc::new(sink_schema),
                    sort_order,
                )))
            }
            PhysicalPlanType::ParquetSink(sink) => {
                let input = into_physical_plan(&sink.input, registry, runtime)?;

                let data_sink: ParquetSink = sink
                    .sink
                    .as_ref()
                    .ok_or_else(|| proto_error("Missing required field in protobuf"))?
                    .try_into()?;
                let sink_schema = convert_required!(sink.sink_schema)?;
                let sort_order = sink
                    .sort_order
                    .as_ref()
                    .map(|collection| {
                        collection
                            .physical_sort_expr_nodes
                            .iter()
                            .map(|proto| {
                                parse_physical_sort_expr(proto, registry, &sink_schema)
                                    .map(Into::into)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(Arc::new(FileSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    Arc::new(sink_schema),
                    sort_order,
                )))
            }
            PhysicalPlanType::SortMergeJoin(join) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.left, registry, runtime)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.right, registry, runtime)?;
                let left_schema = left.schema();
                let right_schema = right.schema();
                let join_type =
                    datafusion_common::JoinType::try_from(join.join_type).map_err(|_| {
                        proto_error(format!(
                            "Received a NestedLoopJoinExecNode message with unknown JoinType {}",
                            join.join_type
                        ))
                    })?;
                let filter = join
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = f
                            .schema
                            .as_ref()
                            .ok_or_else(|| proto_error("Missing JoinFilter schema"))?
                            .try_into()?;

                        let expression = parse_physical_expr(
                            f.expression.as_ref().ok_or_else(|| {
                                proto_error("Unexpected empty filter expression")
                            })?,
                            registry, &schema,
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a NestedLoopJoinExecNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;
                let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = join
                    .on
                    .iter()
                    .map(|col| {
                        let left = parse_physical_expr(
                            &col.left.clone().unwrap(),
                            registry,
                            left_schema.as_ref(),
                        )?;
                        let right = parse_physical_expr(
                            &col.right.clone().unwrap(),
                            registry,
                            right_schema.as_ref(),
                        )?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    Vec::new(),
                    join.null_equals_null,
                )
            }
            PhysicalPlanType::SqlScan(scan) => {
            }
        }
    }

    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self>
    where
        Self: Sized,
    {
        let plan_clone = plan.clone();
        let plan = plan.as_any();

        if let Some(exec) = plan.downcast_ref::<ExplainExec>() {
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Explain(ExplainExecNode {
                    schema: Some(exec.schema().as_ref().try_into()?),
                    stringified_plans: exec
                        .stringified_plans()
                        .iter()
                        .map(|plan| plan.into())
                        .collect(),
                    verbose: exec.verbose(),
                })),
            });
        }

        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>>>()?;
            let expr_name = exec.expr().iter().map(|expr| expr.1.clone()).collect();
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        expr_name,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<AnalyzeExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Analyze(Box::new(AnalyzeExecNode {
                    verbose: exec.verbose(),
                    show_statistics: exec.show_statistics(),
                    input: Some(Box::new(input)),
                    schema: Some(exec.schema().as_ref().try_into()?),
                }))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(FilterExecNode {
                    input: Some(Box::new(input)),
                    expr: Some(exec.predicate().clone().try_into()?),
                    default_filter_selectivity: exec.default_selectivity() as u32,
                }))),
            });
        }

        if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(limit.input().to_owned())?;

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GlobalLimit(Box::new(
                    GlobalLimitExecNode {
                        input: Some(Box::new(input)),
                        skip: limit.skip() as u32,
                        fetch: match limit.fetch() {
                            Some(n) => n as i64,
                            _ => -1, // no limit
                        },
                    },
                ))),
            });
        }

        if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(limit.input().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                    LocalLimitExecNode {
                        input: Some(Box::new(input)),
                        fetch: limit.fetch() as u32,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
            let left = PhysicalPlanNode::try_from_physical_plan(exec.left().to_owned())?;
            let right = PhysicalPlanNode::try_from_physical_plan(exec.right().to_owned())?;
            let on: Vec<JoinOn> = exec
                .on()
                .iter()
                .map(|tuple| {
                    let l = tuple.0.to_owned().try_into()?;
                    let r = tuple.1.to_owned().try_into()?;
                    Ok::<_, DataFusionError>(JoinOn {
                        left: Some(l),
                        right: Some(r),
                    })
                })
                .collect::<Result<_>>()?;
            let join_type: JoinType = exec.join_type().to_owned().into();
            let filter = exec
                .filter()
                .as_ref()
                .map(|f| {
                    let expression = f.expression().to_owned().try_into()?;
                    let column_indices = f
                        .column_indices()
                        .iter()
                        .map(|i| {
                            let side: JoinSide = i.side.to_owned().into();
                            ColumnIndex {
                                index: i.index as u32,
                                side: side.into(),
                            }
                        })
                        .collect();
                    let schema = f.schema().try_into()?;
                    Ok(hexa_proto::protos::datafusion::JoinFilter {
                        expression: Some(expression),
                        column_indices,
                        schema: Some(schema),
                    })
                })
                .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

            let partition_mode = match exec.partition_mode() {
                PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                PartitionMode::Partitioned => PartitionMode::Partitioned,
                PartitionMode::Auto => PartitionMode::Auto,
            };

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(HashJoinExecNode {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    on,
                    join_type: join_type.into(),
                    partition_mode: partition_mode.into(),
                    null_equals_null: exec.null_equals_null(),
                    filter,
                }))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<SymmetricHashJoinExec>() {
            let left = PhysicalPlanNode::try_from_physical_plan(exec.left().to_owned())?;
            let right = PhysicalPlanNode::try_from_physical_plan(exec.right().to_owned())?;
            let on = exec
                .on()
                .iter()
                .map(|tuple| {
                    let l = tuple.0.to_owned().try_into()?;
                    let r = tuple.1.to_owned().try_into()?;
                    Ok::<_, DataFusionError>(JoinOn {
                        left: Some(l),
                        right: Some(r),
                    })
                })
                .collect::<Result<_>>()?;
            let join_type: JoinType = exec.join_type().to_owned().into();
            let filter = exec
                .filter()
                .as_ref()
                .map(|f| {
                    let expression = f.expression().to_owned().try_into()?;
                    let column_indices = f
                        .column_indices()
                        .iter()
                        .map(|i| {
                            let side: JoinSide = i.side.to_owned().into();
                            ColumnIndex {
                                index: i.index as u32,
                                side: side.into(),
                            }
                        })
                        .collect();
                    let schema = f.schema().try_into()?;
                    Ok(hexa_proto::protos::datafusion::JoinFilter {
                        expression: Some(expression),
                        column_indices,
                        schema: Some(schema),
                    })
                })
                .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

            let partition_mode = match exec.partition_mode() {
                StreamJoinPartitionMode::SinglePartition => StreamPartitionMode::SinglePartition,
                StreamJoinPartitionMode::Partitioned => StreamPartitionMode::PartitionedExec,
            };

            let left_sort_exprs = exec
                .left_sort_exprs()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| {
                            Ok(PhysicalSortExprNode {
                                expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                                asc: !expr.options.descending,
                                nulls_first: expr.options.nulls_first,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or(vec![]);

            let right_sort_exprs = exec
                .right_sort_exprs()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| {
                            Ok(PhysicalSortExprNode {
                                expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                                asc: !expr.options.descending,
                                nulls_first: expr.options.nulls_first,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or(vec![]);

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SymmetricHashJoin(Box::new(
                    SymmetricHashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                        partition_mode: partition_mode.into(),
                        null_equals_null: exec.null_equals_null(),
                        left_sort_exprs,
                        right_sort_exprs,
                        filter,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CrossJoinExec>() {
            let left = PhysicalPlanNode::try_from_physical_plan(exec.left().to_owned())?;
            let right = PhysicalPlanNode::try_from_physical_plan(exec.right().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CrossJoin(Box::new(
                    CrossJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                    },
                ))),
            });
        }
        if let Some(exec) = plan.downcast_ref::<AggregateExec>() {
            let groups: Vec<bool> = exec
                .group_expr()
                .groups()
                .iter()
                .flatten()
                .copied()
                .collect();

            let group_names = exec
                .group_expr()
                .expr()
                .iter()
                .map(|expr| expr.1.to_owned())
                .collect();

            let filter = exec
                .filter_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            let agg = exec
                .aggr_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;
            let agg_names = exec
                .aggr_expr()
                .iter()
                .map(|expr| match expr.field() {
                    Ok(field) => Ok(field.name().clone()),
                    Err(e) => Err(e),
                })
                .collect::<Result<_>>()?;

            let agg_mode = match exec.mode() {
                AggregateMode::Partial => AggregateMode::Partial,
                AggregateMode::Final => AggregateMode::Final,
                AggregateMode::FinalPartitioned => AggregateMode::FinalPartitioned,
                AggregateMode::Single => AggregateMode::Single,
                AggregateMode::SinglePartitioned => AggregateMode::SinglePartitioned,
            };
            let input_schema = exec.input_schema();
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;

            let null_expr = exec
                .group_expr()
                .null_expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            let group_expr = exec
                .group_expr()
                .expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(
                    AggregateExecNode {
                        group_expr,
                        group_expr_name: group_names,
                        aggr_expr: agg,
                        filter_expr: filter,
                        aggr_expr_name: agg_names,
                        mode: agg_mode as i32,
                        input: Some(Box::new(input)),
                        input_schema: Some(input_schema.as_ref().try_into()?),
                        null_expr,
                        groups,
                    },
                ))),
            });
        }

        if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().try_into()?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Empty(EmptyExecNode {
                    schema: Some(schema),
                })),
            });
        }

        if let Some(empty) = plan.downcast_ref::<PlaceholderRowExec>() {
            let schema = empty.schema().as_ref().try_into()?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::PlaceholderRow(
                    PlaceholderRowExecNode {
                        schema: Some(schema),
                    },
                )),
            });
        }

        if let Some(coalesce_batches) = plan.downcast_ref::<CoalesceBatchesExec>() {
            let input =
                PhysicalPlanNode::try_from_physical_plan(coalesce_batches.input().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                    CoalesceBatchesExecNode {
                        input: Some(Box::new(input)),
                        target_batch_size: coalesce_batches.target_batch_size() as u32,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CsvExec>() {
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CsvScan(CsvScanExecNode {
                    base_conf: Some(exec.base_config().try_into()?),
                    has_header: exec.has_header(),
                    delimiter: byte_to_string(exec.delimiter(), "delimiter")?,
                    quote: byte_to_string(exec.quote(), "quote")?,
                    optional_escape: if let Some(escape) = exec.escape() {
                        Some(csv_scan_exec_node::OptionalEscape::Escape(byte_to_string(
                            escape, "escape",
                        )?))
                    } else {
                        None
                    },
                })),
            });
        }

        #[cfg(feature = "parquet")]
        if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            let predicate = exec
                .predicate()
                .map(|pred| pred.clone().try_into())
                .transpose()?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(ParquetScanExecNode {
                    base_conf: Some(exec.base_config().try_into()?),
                    predicate,
                })),
            });
        }

        if let Some(exec) = plan.downcast_ref::<AvroExec>() {
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::AvroScan(AvroScanExecNode {
                    base_conf: Some(exec.base_config().try_into()?),
                })),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Merge(Box::new(
                    CoalescePartitionsExecNode {
                        input: Some(Box::new(input)),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<RepartitionExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;

            let pb_partition_method = match exec.partitioning() {
                Partitioning::Hash(exprs, partition_count) => {
                    PartitionMethod::Hash(PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                Partitioning::RoundRobinBatch(partition_count) => {
                    PartitionMethod::RoundRobin(*partition_count as u64)
                }
                Partitioning::UnknownPartitioning(partition_count) => {
                    PartitionMethod::Unknown(*partition_count as u64)
                }
            };

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Repartition(Box::new(
                    RepartitionExecNode {
                        input: Some(Box::new(input)),
                        partition_method: Some(pb_partition_method),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<SortExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| {
                    let sort_expr = Box::new(PhysicalSortExprNode {
                        expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                        asc: !expr.options.descending,
                        nulls_first: expr.options.nulls_first,
                    });
                    Ok(PhysicalExprNode {
                        expr_type: Some(physical_expr_node::ExprType::Sort(sort_expr)),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(SortExecNode {
                    input: Some(Box::new(input)),
                    expr,
                    fetch: match exec.fetch() {
                        Some(n) => n as i64,
                        _ => -1,
                    },
                    preserve_partitioning: exec.preserve_partitioning(),
                }))),
            });
        }

        if let Some(union) = plan.downcast_ref::<UnionExec>() {
            let mut inputs: Vec<PhysicalPlanNode> = vec![];
            for input in union.inputs() {
                inputs.push(PhysicalPlanNode::try_from_physical_plan(input.to_owned())?);
            }
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Union(UnionExecNode { inputs })),
            });
        }

        if let Some(interleave) = plan.downcast_ref::<InterleaveExec>() {
            let mut inputs: Vec<PhysicalPlanNode> = vec![];
            for input in interleave.inputs() {
                inputs.push(PhysicalPlanNode::try_from_physical_plan(input.to_owned())?);
            }
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Interleave(InterleaveExecNode {
                    inputs,
                })),
            });
        }

        if let Some(exec) = plan.downcast_ref::<SortPreservingMergeExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| {
                    let sort_expr = Box::new(PhysicalSortExprNode {
                        expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                        asc: !expr.options.descending,
                        nulls_first: expr.options.nulls_first,
                    });
                    Ok(PhysicalExprNode {
                        expr_type: Some(physical_expr_node::ExprType::Sort(sort_expr)),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SortPreservingMerge(Box::new(
                    SortPreservingMergeExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        fetch: exec.fetch().map(|f| f as i64).unwrap_or(-1),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<NestedLoopJoinExec>() {
            let left = PhysicalPlanNode::try_from_physical_plan(exec.left().to_owned())?;
            let right = PhysicalPlanNode::try_from_physical_plan(exec.right().to_owned())?;

            let join_type: JoinType = exec.join_type().to_owned().into();
            let filter = exec
                .filter()
                .as_ref()
                .map(|f| {
                    let expression = f.expression().to_owned().try_into()?;
                    let column_indices = f
                        .column_indices()
                        .iter()
                        .map(|i| {
                            let side: JoinSide = i.side.to_owned().into();
                            ColumnIndex {
                                index: i.index as u32,
                                side: side.into(),
                            }
                        })
                        .collect();
                    let schema = f.schema().try_into()?;
                    Ok(hexa_proto::protos::datafusion::JoinFilter {
                        expression: Some(expression),
                        column_indices,
                        schema: Some(schema),
                    })
                })
                .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::NestedLoopJoin(Box::new(
                    NestedLoopJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        join_type: join_type.into(),
                        filter,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<WindowAggExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;

            let window_expr = exec
                .window_expr()
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<PhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<PhysicalExprNode>>>()?;

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Window(Box::new(WindowAggExecNode {
                    input: Some(Box::new(input)),
                    window_expr,
                    partition_keys,
                    input_order_mode: None,
                }))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<BoundedWindowAggExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;

            let window_expr = exec
                .window_expr()
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<PhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<PhysicalExprNode>>>()?;

            let input_order_mode = match &exec.input_order_mode {
                InputOrderMode::Linear => {
                    window_agg_exec_node::InputOrderMode::Linear(EmptyMessage {})
                }
                InputOrderMode::PartiallySorted(columns) => {
                    window_agg_exec_node::InputOrderMode::PartiallySorted(
                        PartiallySortedInputOrderMode {
                            columns: columns.iter().map(|c| *c as u64).collect(),
                        },
                    )
                }
                InputOrderMode::Sorted => {
                    window_agg_exec_node::InputOrderMode::Sorted(EmptyMessage {})
                }
            };

            return Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Window(Box::new(WindowAggExecNode {
                    input: Some(Box::new(input)),
                    window_expr,
                    partition_keys,
                    input_order_mode: Some(input_order_mode),
                }))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<FileSinkExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;
            let sort_order = match exec.sort_order() {
                Some(requirements) => {
                    let expr = requirements
                        .iter()
                        .map(|requirement| {
                            let expr: PhysicalSortExpr = requirement.to_owned().into();
                            let sort_expr = PhysicalSortExprNode {
                                expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                                asc: !expr.options.descending,
                                nulls_first: expr.options.nulls_first,
                            };
                            Ok(sort_expr)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Some(PhysicalSortExprNodeCollection {
                        physical_sort_expr_nodes: expr,
                    })
                }
                None => None,
            };

            if let Some(sink) = exec.sink().as_any().downcast_ref::<JsonSink>() {
                return Ok(PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::JsonSink(Box::new(
                        JsonSinkExecNode {
                            input: Some(Box::new(input)),
                            sink: Some(sink.try_into()?),
                            sink_schema: Some(exec.schema().as_ref().try_into()?),
                            sort_order,
                        },
                    ))),
                });
            }

            if let Some(sink) = exec.sink().as_any().downcast_ref::<CsvSink>() {
                return Ok(PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::CsvSink(Box::new(
                        CsvSinkExecNode {
                            input: Some(Box::new(input)),
                            sink: Some(sink.try_into()?),
                            sink_schema: Some(exec.schema().as_ref().try_into()?),
                            sort_order,
                        },
                    ))),
                });
            }

            if let Some(sink) = exec.sink().as_any().downcast_ref::<ParquetSink>() {
                return Ok(PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::ParquetSink(Box::new(
                        ParquetSinkExecNode {
                            input: Some(Box::new(input)),
                            sink: Some(sink.try_into()?),
                            sink_schema: Some(exec.schema().as_ref().try_into()?),
                            sort_order,
                        },
                    ))),
                });
            }

            // If unknown DataSink then let extension handle it
        }

        internal_err!(
            "Unsupported plan and extension codec failed"
        )
    }
}

fn into_physical_plan(
    node: &Option<Box<PhysicalPlanNode>>,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(field) = node {
        field.try_into_physical_plan(registry, runtime)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_expr(
    proto: &PhysicalExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::Literal(scalar) => Arc::new(Literal::new(scalar.try_into()?)),
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            parse_required_physical_expr(binary_expr.l.as_deref(), registry, "left", input_schema)?,
            from_proto_binary_op(&binary_expr.op)?,
            parse_required_physical_expr(
                binary_expr.r.as_deref(),
                registry,
                "right",
                input_schema,
            )?,
        )),
        ExprType::AggregateExpr(_) => {
            return not_impl_err!("Cannot convert aggregate expr node to physical expression");
        }
        ExprType::WindowExpr(_) => {
            return not_impl_err!("Cannot convert window expr node to physical expression");
        }
        ExprType::Sort(_) => {
            return not_impl_err!("Cannot convert sort expr node to physical expression");
        }
        ExprType::IsNullExpr(e) => Arc::new(IsNullExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::IsNotNullExpr(e) => Arc::new(IsNotNullExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::Negative(e) => Arc::new(NegativeExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::InList(e) => in_list(
            parse_required_physical_expr(e.expr.as_deref(), registry, "expr", input_schema)?,
            e.list
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            &e.negated,
            input_schema,
        )?,
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        parse_required_physical_expr(
                            e.when_expr.as_ref(),
                            registry,
                            "when_expr",
                            input_schema,
                        )?,
                        parse_required_physical_expr(
                            e.then_expr.as_ref(),
                            registry,
                            "then_expr",
                            input_schema,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            e.else_expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            parse_required_physical_expr(e.expr.as_deref(), registry, "expr", input_schema)?,
            convert_required!(e.arrow_type)?,
            None,
        )),
        ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
            parse_required_physical_expr(e.expr.as_deref(), registry, "expr", input_schema)?,
            convert_required!(e.arrow_type)?,
        )),
        ExprType::ScalarFunction(e) => {
            let scalar_function = ScalarFunction::try_from(e.fun).map_err(|_| {
                proto_error(format!("Received an unknown scalar function: {}", e.fun,))
            })?;

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            // TODO Do not create new the ExecutionProps
            let execution_props = ExecutionProps::new();

            functions::create_physical_expr(
                &(&scalar_function).into(),
                &args,
                input_schema,
                &execution_props,
            )?
        }
        ExprType::ScalarUdf(e) => {
            let udf = registry.udf(e.name.as_str())?;
            let signature = udf.signature();
            let scalar_fun = udf.fun().clone();

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            Arc::new(ScalarFunctionExpr::new(
                e.name.as_str(),
                scalar_fun,
                args,
                convert_required!(e.return_type)?,
                None,
                signature.type_signature.supports_zero_argument(),
            ))
        }
        ExprType::LikeExpr(like_expr) => Arc::new(LikeExpr::new(
            like_expr.negated,
            like_expr.case_insensitive,
            parse_required_physical_expr(
                like_expr.expr.as_deref(),
                registry,
                "expr",
                input_schema,
            )?,
            parse_required_physical_expr(
                like_expr.pattern.as_deref(),
                registry,
                "pattern",
                input_schema,
            )?,
        )),
        ExprType::GetIndexedFieldExpr(get_indexed_field_expr) => {
            let field = match &get_indexed_field_expr.field {
                Some(physical_get_indexed_field_expr_node::Field::NamedStructFieldExpr(
                    named_struct_field_expr,
                )) => GetFieldAccessExpr::NamedStructField {
                    name: convert_required!(named_struct_field_expr.name)?,
                },
                Some(physical_get_indexed_field_expr_node::Field::ListIndexExpr(
                    list_index_expr,
                )) => GetFieldAccessExpr::ListIndex {
                    key: parse_required_physical_expr(
                        list_index_expr.key.as_deref(),
                        registry,
                        "key",
                        input_schema,
                    )?,
                },
                Some(physical_get_indexed_field_expr_node::Field::ListRangeExpr(
                    list_range_expr,
                )) => GetFieldAccessExpr::ListRange {
                    start: parse_required_physical_expr(
                        list_range_expr.start.as_deref(),
                        registry,
                        "start",
                        input_schema,
                    )?,
                    stop: parse_required_physical_expr(
                        list_range_expr.stop.as_deref(),
                        registry,
                        "stop",
                        input_schema,
                    )?,
                    stride: parse_required_physical_expr(
                        list_range_expr.stride.as_deref(),
                        registry,
                        "stride",
                        input_schema,
                    )?,
                },
                None => return Err(proto_error("Field must not be None")),
            };

            Arc::new(GetIndexedFieldExpr::new(
                parse_required_physical_expr(
                    get_indexed_field_expr.arg.as_deref(),
                    registry,
                    "arg",
                    input_schema,
                )?,
                field,
            ))
        }
    };

    Ok(pexpr)
}

pub trait PhysicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()>;

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        not_impl_err!("PhysicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

fn parse_required_physical_expr(
    expr: Option<&PhysicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: &str,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.map(|e| parse_physical_expr(e, registry, input_schema))
        .transpose()?
        .ok_or_else(|| DataFusionError::Internal(format!("Missing required field {field:?}")))
}

pub fn proto_error<S: Into<String>>(message: S) -> DataFusionError {
    DataFusionError::Internal(message.into())
}

/// Parses a physical window expr from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical window exprression node.
/// * `name` - Name of the window expression.
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_window_expr(
    proto: &PhysicalWindowExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr = proto
        .args
        .iter()
        .map(|e| parse_physical_expr(e, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let partition_by = proto
        .partition_by
        .iter()
        .map(|p| parse_physical_expr(p, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let order_by = proto
        .order_by
        .iter()
        .map(|o| parse_physical_sort_expr(o, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let window_frame = proto
        .window_frame
        .as_ref()
        .map(|wf| wf.clone().try_into())
        .transpose()
        .map_err(|e| DataFusionError::Internal(format!("{e}")))?
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Missing required field 'window_frame' in protobuf".to_string(),
            )
        })?;

    create_window_expr(
        &convert_required!(proto.window_function)?,
        proto.name.clone(),
        &window_node_expr,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        input_schema,
    )
}

/// Parses a physical sort expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical sort expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_sort_expr(
    proto: &PhysicalSortExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<PhysicalSortExpr> {
    if let Some(expr) = &proto.expr {
        let expr = parse_physical_expr(expr.as_ref(), registry, input_schema)?;
        let options = SortOptions {
            descending: !proto.asc,
            nulls_first: proto.nulls_first,
        };
        Ok(PhysicalSortExpr { expr, options })
    } else {
        Err(proto_error("Unexpected empty physical expression"))
    }
}

pub trait AsExecutionPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self>
    where
        Self: Sized;
}

pub fn byte_to_string(b: u8, description: &str) -> Result<String> {
    let b = &[b];
    let b = std::str::from_utf8(b).map_err(|_| {
        DataFusionError::Internal(format!(
            "Invalid CSV {description}: can not represent {b:0x?} as utf8"
        ))
    })?;
    Ok(b.to_owned())
}

pub fn from_proto_binary_op(op: &str) -> Result<Operator> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
        "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
        "BitwiseAnd" => Ok(Operator::BitwiseAnd),
        "BitwiseOr" => Ok(Operator::BitwiseOr),
        "BitwiseXor" => Ok(Operator::BitwiseXor),
        "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
        "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
        "RegexIMatch" => Ok(Operator::RegexIMatch),
        "RegexMatch" => Ok(Operator::RegexMatch),
        "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
        "RegexNotMatch" => Ok(Operator::RegexNotMatch),
        "StringConcat" => Ok(Operator::StringConcat),
        "AtArrow" => Ok(Operator::AtArrow),
        "ArrowAt" => Ok(Operator::ArrowAt),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{other:?}'"
        ))),
    }
}

pub fn parse_protobuf_file_scan_config(
    proto: &FileScanExecConf,
    registry: &dyn FunctionRegistry,
) -> Result<FileScanConfig> {
    let schema: Arc<Schema> = Arc::new(convert_required!(proto.schema)?);
    let projection = proto
        .projection
        .iter()
        .map(|i| *i as usize)
        .collect::<Vec<_>>();
    let projection = if projection.is_empty() {
        None
    } else {
        Some(projection)
    };
    let statistics = convert_required!(proto.statistics)?;

    let file_groups: Vec<Vec<PartitionedFile>> = proto
        .file_groups
        .iter()
        .map(|f| f.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    let object_store_url = match proto.object_store_url.is_empty() {
        false => ObjectStoreUrl::parse(&proto.object_store_url)?,
        true => ObjectStoreUrl::local_filesystem(),
    };

    // Reacquire the partition column types from the schema before removing them below.
    let table_partition_cols = proto
        .table_partition_cols
        .iter()
        .map(|col| Ok(schema.field_with_name(col)?.clone()))
        .collect::<Result<Vec<_>>>()?;

    // Remove partition columns from the schema after recreating table_partition_cols
    // because the partition columns are not in the file. They are present to allow the
    // the partition column types to be reconstructed after serde.
    let file_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !table_partition_cols.contains(field))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let mut output_ordering = vec![];
    for node_collection in &proto.output_ordering {
        let sort_expr = node_collection
            .physical_sort_expr_nodes
            .iter()
            .map(|node| {
                let expr = node
                    .expr
                    .as_ref()
                    .map(|e| parse_physical_expr(e.as_ref(), registry, &schema))
                    .unwrap()?;
                Ok(PhysicalSortExpr {
                    expr,
                    options: SortOptions {
                        descending: !node.asc,
                        nulls_first: node.nulls_first,
                    },
                })
            })
            .collect::<Result<Vec<PhysicalSortExpr>>>()?;
        output_ordering.push(sort_expr);
    }

    Ok(FileScanConfig {
        object_store_url,
        file_schema,
        file_groups,
        statistics,
        projection,
        limit: proto.limit.as_ref().map(|sl| sl.limit as usize),
        table_partition_cols,
        output_ordering,
    })
}

pub fn str_to_byte(s: &String, description: &str) -> Result<u8> {
    if s.len() != 1 {
        return internal_err!("Invalid CSV {description}: expected single character, got {s}");
    }
    Ok(s.as_bytes()[0])
}

/// Parses a physical sort expressions from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with vector of physical sort expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_sort_exprs(
    proto: &[PhysicalSortExprNode],
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Vec<PhysicalSortExpr>> {
    proto
        .iter()
        .map(|sort_expr| {
            if let Some(expr) = &sort_expr.expr {
                let expr = parse_physical_expr(expr.as_ref(), registry, input_schema)?;
                let options = SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                };
                Ok(PhysicalSortExpr { expr, options })
            } else {
                Err(proto_error("Unexpected empty physical expression"))
            }
        })
        .collect::<Result<Vec<_>>>()
}
