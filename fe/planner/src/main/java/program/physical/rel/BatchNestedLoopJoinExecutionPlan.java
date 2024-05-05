package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Set;

import static program.physical.rel.PhysicalPlanTransformUtil.transformJoinType;
import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToJoinFilter;

public class BatchNestedLoopJoinExecutionPlan
        extends EnumerableBatchNestedLoopJoin
        implements ExecutionPlan {


    public static BatchNestedLoopJoinExecutionPlan create(
            EnumerableBatchNestedLoopJoin enumerableBatchNestedLoopJoin) {
        return new BatchNestedLoopJoinExecutionPlan(enumerableBatchNestedLoopJoin.getCluster(),
                enumerableBatchNestedLoopJoin.getTraitSet(),
                enumerableBatchNestedLoopJoin.getLeft(),
                enumerableBatchNestedLoopJoin.getRight(),
                enumerableBatchNestedLoopJoin.getCondition(),
                enumerableBatchNestedLoopJoin.getVariablesSet(),
                null,
                enumerableBatchNestedLoopJoin.getJoinType());
    }

    protected BatchNestedLoopJoinExecutionPlan(RelOptCluster cluster,
                                               RelTraitSet traits,
                                               RelNode left,
                                               RelNode right,
                                               RexNode condition,
                                               Set<CorrelationId> variablesSet,
                                               ImmutableBitSet requiredColumns,
                                               JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, requiredColumns, joinType);
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.PhysicalPlanNode leftNode = ((ExecutionPlan) getLeft()).transformToDataFusionNode();
        proto.datafusion.PhysicalPlanNode rightNode = ((ExecutionPlan) getRight()).transformToDataFusionNode();
        proto.datafusion.JoinType joinType = transformJoinType(getJoinType());

        proto.datafusion.JoinFilter joinFilter =
                transformRexNodeToJoinFilter(condition, getRowType().getFieldList(), getLeft().getRowType().getFieldList().size());

        proto.datafusion.NestedLoopJoinExecNode.Builder builder = proto.datafusion.NestedLoopJoinExecNode.newBuilder()
                .setLeft(leftNode)
                .setRight(rightNode)
                .setJoinType(joinType)
                .setFilter(joinFilter);
        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setNestedLoopJoin(builder)
                .build();
    }

    @Override
    public EnumerableBatchNestedLoopJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new BatchNestedLoopJoinExecutionPlan(getCluster(), traitSet,
                left, right, condition, variablesSet, null, joinType);
    }
}
