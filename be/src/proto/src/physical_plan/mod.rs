// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonSink;
use datafusion::datasource::file_format::parquet::ParquetSink;
#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;
#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::datasource::physical_plan::{AvroExec, CsvExec};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::aggregates::{create_aggregate_expr, AggregateMode};
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::insert::FileSinkExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    CrossJoinExec, NestedLoopJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{
    udaf, AggregateExpr, ExecutionPlan, InputOrderMode, Partitioning, PhysicalExpr,
    WindowExpr,
};
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};
use prost::bytes::BufMut;
use prost::Message;

use crate::common::str_to_byte;
use crate::common::{byte_to_string, proto_error};
use crate::convert_required;
use crate::physical_plan::from_proto::{
    parse_physical_expr, parse_physical_sort_expr, parse_physical_sort_exprs,
    parse_protobuf_file_scan_config,
};
use crate::protobuf::physical_aggregate_expr_node::AggregateFunction;
use crate::protobuf::physical_expr_node::ExprType;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::repartition_exec_node::PartitionMethod;
use crate::protobuf::{
    self, window_agg_exec_node, PhysicalPlanNode, PhysicalSortExprNodeCollection,
};

use self::from_proto::parse_physical_window_expr;

pub mod from_proto;
pub mod to_proto;

impl AsExecutionPlan for PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
    {
        protobuf::PhysicalPlanNode::decode(buf).map_err(|e| {
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
        extension_codec: &dyn PhysicalExtensionCodec,
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &projection.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &filter.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                    })
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing."
                                .to_owned(),
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
                parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?,
                scan.has_header,
                str_to_byte(&scan.delimiter, "delimiter")?,
                str_to_byte(&scan.quote, "quote")?,
                if let Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(
                                escape,
                            )) = &scan.optional_escape
                {
                    Some(str_to_byte(escape, "escape")?)
                } else {
                    None
                },
                FileCompressionType::UNCOMPRESSED,
            ))),
            #[cfg(feature = "parquet")]
            PhysicalPlanType::ParquetScan(scan) => {
                let base_config = parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?;
                let predicate = scan
                    .predicate
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(
                            expr,
                            registry,
                            base_config.file_schema.as_ref(),
                        )
                    })
                    .transpose()?;
                Ok(Arc::new(ParquetExec::new(base_config, predicate, None)))
            }
            PhysicalPlanType::AvroScan(scan) => {
                Ok(Arc::new(AvroExec::new(parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?)))
            }
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &coalesce_batches.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&merge.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &repart.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| {
                                parse_physical_expr(e, registry, input.schema().as_ref())
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(
                                expr,
                                hash_part.partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::RoundRobin(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::RoundRobinBatch(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::Unknown(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::UnknownPartitioning(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    _ => internal_err!("Invalid partitioning scheme"),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&limit.input, registry, runtime, extension_codec)?;
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
                    into_physical_plan(&limit.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.fetch as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &window_agg.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let input_schema = input.schema();

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .map(|window_expr| {
                        parse_physical_window_expr(
                            window_expr,
                            registry,
                            input_schema.as_ref(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_keys = window_agg
                    .partition_keys
                    .iter()
                    .map(|expr| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                    })
                    .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;

                if let Some(input_order_mode) = window_agg.input_order_mode.as_ref() {
                    let input_order_mode = match input_order_mode {
                        window_agg_exec_node::InputOrderMode::Linear(_) => {
                            InputOrderMode::Linear
                        }
                        window_agg_exec_node::InputOrderMode::PartiallySorted(
                            protobuf::PartiallySortedInputOrderMode { columns },
                        ) => InputOrderMode::PartiallySorted(
                            columns.iter().map(|c| *c as usize).collect(),
                        ),
                        window_agg_exec_node::InputOrderMode::Sorted(_) => {
                            InputOrderMode::Sorted
                        }
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hash_agg.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let mode = protobuf::AggregateMode::try_from(hash_agg.mode).map_err(
                    |_| {
                        proto_error(format!(
                            "Received a AggregateNode message with unknown AggregateMode {}",
                            hash_agg.mode
                        ))
                    },
                )?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                    protobuf::AggregateMode::FinalPartitioned => {
                        AggregateMode::FinalPartitioned
                    }
                    protobuf::AggregateMode::Single => AggregateMode::Single,
                    protobuf::AggregateMode::SinglePartitioned => {
                        AggregateMode::SinglePartitioned
                    }
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
                                            let aggr_function = protobuf::AggregateFunction::try_from(*i)
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
                let left: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hashjoin.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hashjoin.right,
                    registry,
                    runtime,
                    extension_codec,
                )?;
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
                let join_type = protobuf::JoinType::try_from(hashjoin.join_type)
                    .map_err(|_| {
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
                                let side = protobuf::JoinSide::try_from(i.side)
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

                let partition_mode = protobuf::PartitionMode::try_from(
                    hashjoin.partition_mode,
                )
                    .map_err(|_| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown PartitionMode {}",
                            hashjoin.partition_mode
                        ))
                    })?;
                let partition_mode = match partition_mode {
                    protobuf::PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                    protobuf::PartitionMode::Partitioned => PartitionMode::Partitioned,
                    protobuf::PartitionMode::Auto => PartitionMode::Auto,
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
                let left = into_physical_plan(
                    &sym_join.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right = into_physical_plan(
                    &sym_join.right,
                    registry,
                    runtime,
                    extension_codec,
                )?;
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
                let join_type = protobuf::JoinType::try_from(sym_join.join_type)
                    .map_err(|_| {
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
                                let side = protobuf::JoinSide::try_from(i.side)
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

                let left_sort_exprs = parse_physical_sort_exprs(
                    &sym_join.left_sort_exprs,
                    registry,
                    &left_schema,
                )?;
                let left_sort_exprs = if left_sort_exprs.is_empty() {
                    None
                } else {
                    Some(left_sort_exprs)
                };

                let right_sort_exprs = parse_physical_sort_exprs(
                    &sym_join.right_sort_exprs,
                    registry,
                    &right_schema,
                )?;
                let right_sort_exprs = if right_sort_exprs.is_empty() {
                    None
                } else {
                    Some(right_sort_exprs)
                };

                let partition_mode =
                    protobuf::StreamPartitionMode::try_from(sym_join.partition_mode).map_err(|_| {
                        proto_error(format!(
                            "Received a SymmetricHashJoin message with unknown PartitionMode {}",
                            sym_join.partition_mode
                        ))
                    })?;
                let partition_mode = match partition_mode {
                    protobuf::StreamPartitionMode::SinglePartition => {
                        StreamJoinPartitionMode::SinglePartition
                    }
                    protobuf::StreamPartitionMode::PartitionedExec => {
                        StreamJoinPartitionMode::Partitioned
                    }
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
                    inputs.push(input.try_into_physical_plan(
                        registry,
                        runtime,
                        extension_codec,
                    )?);
                }
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::Interleave(interleave) => {
                let mut inputs: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for input in &interleave.inputs {
                    inputs.push(input.try_into_physical_plan(
                        registry,
                        runtime,
                        extension_codec,
                    )?);
                }
                Ok(Arc::new(InterleaveExec::try_new(inputs)?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &crossjoin.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &crossjoin.right,
                    registry,
                    runtime,
                    extension_codec,
                )?;
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
                    into_physical_plan(&sort.input, registry, runtime, extension_codec)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
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
                            internal_err!(
                                "physical_plan::from_proto() {self:?}"
                            )
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
                    into_physical_plan(&sort.input, registry, runtime, extension_codec)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
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
                            internal_err!(
                                "physical_plan::from_proto() {self:?}"
                            )
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
            PhysicalPlanType::Extension(extension) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = extension
                    .inputs
                    .iter()
                    .map(|i| i.try_into_physical_plan(registry, runtime, extension_codec))
                    .collect::<Result<_>>()?;

                let extension_node = extension_codec.try_decode(
                    extension.node.as_slice(),
                    &inputs,
                    registry,
                )?;

                Ok(extension_node)
            }
            PhysicalPlanType::NestedLoopJoin(join) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.left, registry, runtime, extension_codec)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.right, registry, runtime, extension_codec)?;
                let join_type =
                    protobuf::JoinType::try_from(join.join_type).map_err(|_| {
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
                                let side = protobuf::JoinSide::try_from(i.side)
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &analyze.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                Ok(Arc::new(AnalyzeExec::new(
                    analyze.verbose,
                    analyze.show_statistics,
                    input,
                    Arc::new(convert_required!(analyze.schema)?),
                )))
            }
            PhysicalPlanType::JsonSink(sink) => {
                let input =
                    into_physical_plan(&sink.input, registry, runtime, extension_codec)?;

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
                let input =
                    into_physical_plan(&sink.input, registry, runtime, extension_codec)?;

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
                let input =
                    into_physical_plan(&sink.input, registry, runtime, extension_codec)?;

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
        }
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
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>>;

}

pub trait PhysicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()>;
}

#[derive(Debug)]
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }
}

fn into_physical_plan(
    node: &Option<Box<PhysicalPlanNode>>,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(field) = node {
        field.try_into_physical_plan(registry, runtime, extension_codec)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}
