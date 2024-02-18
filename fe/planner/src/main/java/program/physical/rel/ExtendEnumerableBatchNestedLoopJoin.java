package program.physical.rel;

import arrow.datafusion.protobuf.JoinFilter;
import arrow.datafusion.protobuf.JoinType;
import arrow.datafusion.protobuf.NestedLoopJoinExecNode;
import arrow.datafusion.protobuf.PhysicalPlanNode;
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

public class ExtendEnumerableBatchNestedLoopJoin
        extends EnumerableBatchNestedLoopJoin
        implements PhysicalPlan {


    public static ExtendEnumerableBatchNestedLoopJoin create(
            EnumerableBatchNestedLoopJoin enumerableBatchNestedLoopJoin) {
        return new ExtendEnumerableBatchNestedLoopJoin(enumerableBatchNestedLoopJoin.getCluster(),
                enumerableBatchNestedLoopJoin.getTraitSet(),
                enumerableBatchNestedLoopJoin.getLeft(),
                enumerableBatchNestedLoopJoin.getRight(),
                enumerableBatchNestedLoopJoin.getCondition(),
                enumerableBatchNestedLoopJoin.getVariablesSet(),
                null,
                enumerableBatchNestedLoopJoin.getJoinType());
    }

    protected ExtendEnumerableBatchNestedLoopJoin(RelOptCluster cluster,
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
    public PhysicalPlanNode transformToDataFusionNode() {
        PhysicalPlanNode leftNode = ((PhysicalPlan) getLeft()).transformToDataFusionNode();
        PhysicalPlanNode rightNode = ((PhysicalPlan) getRight()).transformToDataFusionNode();
        JoinType joinType = transformJoinType(getJoinType());

        JoinFilter joinFilter = transformRexNodeToJoinFilter(condition, getRowType().getFieldList(), getLeft().getRowType().getFieldList().size());

        NestedLoopJoinExecNode.Builder builder = NestedLoopJoinExecNode.newBuilder()
                .setLeft(leftNode)
                .setRight(rightNode)
                .setJoinType(joinType)
                .setFilter(joinFilter);
        return PhysicalPlanNode.newBuilder()
                .setNestedLoopJoin(builder)
                .build();
    }
}
