package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import proto.datafusion.LocalLimitExecNode;
import proto.datafusion.PhysicalPlanNode;

import java.util.List;

public class LimitExecutionPlan
        extends EnumerableLimit
        implements ExecutionPlan {

    /**
     * Creates an EnumerableLimit.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster
     * @param traitSet
     * @param input
     * @param offset
     * @param fetch
     */
    public LimitExecutionPlan(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                              @Nullable RexNode offset, @Nullable RexNode fetch) {
        super(cluster, traitSet, input, offset, fetch);
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        PhysicalPlanNode inputNode = ((ExecutionPlan) getInput()).transformToDataFusionNode();


        return PhysicalPlanNode.newBuilder()
                .setLocalLimit(LocalLimitExecNode.newBuilder()
                        .setInput(inputNode)
                        .setFetch(10)).build();
    }

    public static LimitExecutionPlan create(final RelNode input, @Nullable RexNode offset,
                                         @Nullable RexNode fetch) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.limit(mq, input))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.limit(mq, input));
        return new LimitExecutionPlan(cluster, traitSet, input, offset, fetch);
    }

    @Override public LimitExecutionPlan copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return new LimitExecutionPlan(
                getCluster(),
                traitSet,
                sole(newInputs),
                offset,
                fetch);
    }
}
