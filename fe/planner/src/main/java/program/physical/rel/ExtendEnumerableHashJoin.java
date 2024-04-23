package program.physical.rel;

import arrow.datafusion.protobuf.*;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

import static program.physical.rel.PhysicalPlanTransformUtil.*;

public class ExtendEnumerableHashJoin extends EnumerableHashJoin implements PhysicalPlan {


    public static ExtendEnumerableHashJoin create(EnumerableHashJoin enumerableHashJoin) {
        return new ExtendEnumerableHashJoin(enumerableHashJoin.getCluster(), enumerableHashJoin.getTraitSet(),
                enumerableHashJoin.getLeft(), enumerableHashJoin.getRight(), enumerableHashJoin.getCondition(),
                enumerableHashJoin.getVariablesSet(), enumerableHashJoin.getJoinType());
    }

    protected ExtendEnumerableHashJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                                       RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        PhysicalPlanNode.Builder builder = PhysicalPlanNode.newBuilder();

        PhysicalPlanNode leftNode = ((PhysicalPlan) getLeft()).transformToDataFusionNode();
        PhysicalPlanNode rightNode = ((PhysicalPlan) getRight()).transformToDataFusionNode();
        JoinType joinType = transformJoinType(getJoinType());

        RexNode condition = getCondition();

        JoinOn joinOn = transformJoinOn(condition, getLeft(), getRight());
        HashJoinExecNode.Builder hashJoin = HashJoinExecNode.newBuilder()
                .setLeft(leftNode)
                .setRight(rightNode)
                .setJoinType(joinType)
                .addOn(joinOn)
                .setNullEqualsNull(false);

        return builder.setHashJoin(hashJoin).build();
    }
}
