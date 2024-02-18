package program.physical.rel;

import arrow.datafusion.FilterExecNode;
import arrow.datafusion.PhysicalExprNode;
import arrow.datafusion.PhysicalPlanNode;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToExprNode;

public class ExtendEnumerableFilter
        extends EnumerableFilter
        implements PhysicalPlan {

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
    public ExtendEnumerableFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    @Override
    public PhysicalPlanNode transformToPP() {
        PhysicalExprNode exprNode = transformRexNodeToExprNode(getCondition());
        PhysicalPlanNode input = ((PhysicalPlan) getInput()).transformToPP();
        FilterExecNode.Builder builder = FilterExecNode.newBuilder()
                .setExpr(exprNode)
                .setInput(input);
        return PhysicalPlanNode.newBuilder()
                .setFilter(builder)
                .build();
    }
}