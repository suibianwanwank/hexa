package program.physical.rel;

import arrow.datafusion.protobuf.JoinFilter;
import arrow.datafusion.protobuf.JoinType;
import arrow.datafusion.protobuf.NestedLoopJoinExecNode;
import arrow.datafusion.protobuf.PhysicalPlanNode;
import org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;

import static program.physical.rel.PhysicalPlanTransformUtil.transformJoinType;
import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToJoinFilter;

public class ExtendEnumerableNestedLoopJoin
        extends EnumerableNestedLoopJoin
        implements PhysicalPlan {


    public static ExtendEnumerableNestedLoopJoin create(
            EnumerableNestedLoopJoin enumerableNestedLoopJoin) {
        return new ExtendEnumerableNestedLoopJoin(enumerableNestedLoopJoin.getCluster(),
                enumerableNestedLoopJoin.getTraitSet(),
                enumerableNestedLoopJoin.getLeft(),
                enumerableNestedLoopJoin.getRight(),
                enumerableNestedLoopJoin.getCondition(),
                enumerableNestedLoopJoin.getVariablesSet(),
                enumerableNestedLoopJoin.getJoinType());
    }

    protected ExtendEnumerableNestedLoopJoin(RelOptCluster cluster,
                                             RelTraitSet traits,
                                             RelNode left,
                                             RelNode right,
                                             RexNode condition,
                                             Set<CorrelationId> variablesSet,
                                             JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
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


    @Override
    public EnumerableNestedLoopJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new ExtendEnumerableNestedLoopJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }
}
