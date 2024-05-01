package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

import static program.physical.rel.PhysicalPlanTransformUtil.transformJoinOn;
import static program.physical.rel.PhysicalPlanTransformUtil.transformJoinType;

public class ExtendEnumerableMergeJoin
        extends EnumerableMergeJoin
        implements PhysicalPlan {

    public static ExtendEnumerableMergeJoin create(EnumerableMergeJoin enumerableMergeJoin) {
        return new ExtendEnumerableMergeJoin(enumerableMergeJoin.getCluster(),
                enumerableMergeJoin.getTraitSet(),
                enumerableMergeJoin.getLeft(),
                enumerableMergeJoin.getRight(),
                enumerableMergeJoin.getCondition(),
                enumerableMergeJoin.getVariablesSet(),
                enumerableMergeJoin.getJoinType());
    }

    protected ExtendEnumerableMergeJoin(RelOptCluster cluster, RelTraitSet traits,
                                        RelNode left, RelNode right, RexNode condition,
                                        Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
    }


    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.SortMergeJoinExecNode mergeJoinExecNode = proto.datafusion.SortMergeJoinExecNode.newBuilder()
                .setLeft(((PhysicalPlan) getLeft()).transformToDataFusionNode())
                .setRight(((PhysicalPlan) getRight()).transformToDataFusionNode())
                .setNullEqualsNull(false)
                .setJoinType(transformJoinType(getJoinType()))
                .addOn(transformJoinOn(getCondition(), getLeft(), getRight()))
                .build();
        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setExtension(proto.datafusion.PhysicalExtensionNode.newBuilder()
                        .setNode(mergeJoinExecNode.toByteString()))
                .build();
    }

    @Override
    public EnumerableMergeJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left,
                                    RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new ExtendEnumerableMergeJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }
}
