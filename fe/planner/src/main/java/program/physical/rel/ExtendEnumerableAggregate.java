package program.physical.rel;

import arrow.datafusion.AggregateExecNode;
import arrow.datafusion.PhysicalPlanNode;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static program.physical.rel.PhysicalPlanTransformUtil.transformAggFunction;
import static program.physical.rel.PhysicalPlanTransformUtil.transformRexNodeToExprNode;

public class ExtendEnumerableAggregate
        extends EnumerableAggregate
        implements PhysicalPlan {

    public ExtendEnumerableAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public PhysicalPlanNode transformToPP() {
        AggregateExecNode.Builder aggregateNode = AggregateExecNode.newBuilder();
        for (Integer bitSet : getGroupSet()) {
            RexInputRef inputRef = RexInputRef.of(bitSet, getRowType().getFieldList());
            aggregateNode.addGroupExpr(transformRexNodeToExprNode(inputRef));
        }
        for (AggregateCall call : getAggCallList()) {
            aggregateNode.addAggrExpr(transformAggFunction(call, getRowType().getFieldList()));
        }
        return PhysicalPlanNode.newBuilder().setAggregate(aggregateNode).build();
    }
}