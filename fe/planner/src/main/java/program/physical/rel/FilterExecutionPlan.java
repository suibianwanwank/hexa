package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToExprNode;

public class FilterExecutionPlan
        extends EnumerableFilter
        implements ExecutionPlan {


    public static FilterExecutionPlan create(EnumerableFilter enumerableFilter) {
        return new FilterExecutionPlan(enumerableFilter.getCluster(),
                enumerableFilter.getTraitSet(), enumerableFilter.getInput(), enumerableFilter.getCondition());
    }

    /**
     * Creates an EnumerableFilter.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster
     * @param traitSet
     * @param child
     * @param condition
     */
    private FilterExecutionPlan(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.PhysicalExprNode exprNode = transformRexNodeToExprNode(getCondition());
        proto.datafusion.PhysicalPlanNode input = ((ExecutionPlan) getInput()).transformToDataFusionNode();
        proto.datafusion.FilterExecNode.Builder builder = proto.datafusion.FilterExecNode.newBuilder()
                .setExpr(exprNode)
                .setInput(input);
        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setFilter(builder)
                .build();
    }

    @Override
    public FilterExecutionPlan copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterExecutionPlan(getCluster(), traitSet, input, condition);
    }
}
