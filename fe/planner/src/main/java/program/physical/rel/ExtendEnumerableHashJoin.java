package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import proto.datafusion.HashJoinExecNode;

import java.util.List;
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
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {

        proto.datafusion.PhysicalPlanNode leftNode = ((PhysicalPlan) getLeft()).transformToDataFusionNode();
        proto.datafusion.PhysicalPlanNode rightNode = ((PhysicalPlan) getRight()).transformToDataFusionNode();
        proto.datafusion.JoinType joinType = transformJoinType(getJoinType());

        List<RexNode> conjunctions = RelOptUtil.conjunctions(getCondition());

        HashJoinExecNode.Builder hashJoin = HashJoinExecNode.newBuilder();

        for (RexNode conjunction : conjunctions) {
            proto.datafusion.JoinOn joinOn = transformJoinOn(conjunction, getLeft(), getRight());
            hashJoin.addOn(joinOn);
        }

        hashJoin.setLeft(leftNode)
                .setRight(rightNode)
                .setJoinType(joinType)
                .setNullEqualsNull(false);
        return proto.datafusion.PhysicalPlanNode.newBuilder().setHashJoin(hashJoin).build();
    }

    @Override
    public EnumerableHashJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new ExtendEnumerableHashJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }
}
