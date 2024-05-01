package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import proto.datafusion.PhysicalExprNode;
import proto.datafusion.PhysicalSortExprNode;

import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToExprNode;

public class ExtendEnumerableSort
        extends EnumerableSort
        implements PhysicalPlan {

    public static ExtendEnumerableSort create(EnumerableSort enumerableSort) {
        return new ExtendEnumerableSort(enumerableSort.getCluster(),
                enumerableSort.getTraitSet(),
                enumerableSort.getInput(),
                enumerableSort.getCollation(),
                enumerableSort.offset,
                enumerableSort.fetch);
    }

    /**
     * Creates an EnumerableSort.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster
     * @param traitSet
     * @param input
     * @param collation
     * @param offset
     * @param fetch
     */
    public ExtendEnumerableSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        long fetchValue = -1;
        if (fetch instanceof RexLiteral) {
            fetchValue = RexLiteral.intValue(fetch);
        }
        proto.datafusion.SortExecNode.Builder sortExecNode = proto.datafusion.SortExecNode.newBuilder()
                .setInput(((PhysicalPlan) getInput()).transformToDataFusionNode())
                .setFetch(fetchValue);
        for (RexNode sortExp : getSortExps()) {
            PhysicalSortExprNode.Builder expr = PhysicalSortExprNode.newBuilder()
                    .setAsc(true)
                    .setExpr(transformRexNodeToExprNode(sortExp))
                    .setNullsFirst(false);
            sortExecNode.addExpr(PhysicalExprNode.newBuilder().setSort(expr));
        }
        return proto.datafusion.PhysicalPlanNode.newBuilder().setSort(sortExecNode).build();
    }

    @Override
    public ExtendEnumerableSort copy(RelTraitSet traitSet, RelNode newInput,
                                     RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
        return new ExtendEnumerableSort(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
    }
}
