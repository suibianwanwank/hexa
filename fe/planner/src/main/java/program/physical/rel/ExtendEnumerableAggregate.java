package program.physical.rel;

import arrow.datafusion.protobuf.*;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static program.physical.rel.PhysicalPlanTransformUtil.*;

public class ExtendEnumerableAggregate
        extends EnumerableAggregate
        implements PhysicalPlan {

    public static ExtendEnumerableAggregate create(EnumerableAggregate enumerableAggregate)
            throws InvalidRelException {
        return new ExtendEnumerableAggregate(enumerableAggregate.getCluster(),
                enumerableAggregate.getTraitSet(),
                enumerableAggregate.getInput(),
                enumerableAggregate.getGroupSet(),
                enumerableAggregate.getGroupSets(),
                enumerableAggregate.getAggCallList());
    }

    private ExtendEnumerableAggregate(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      RelNode input,
                                      ImmutableBitSet groupSet,
                                      @Nullable List<ImmutableBitSet> groupSets,
                                      List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        AggregateExecNode.Builder aggregateNode = AggregateExecNode.newBuilder();

        PhysicalPlanNode input = ((PhysicalPlan) getInput()).transformToDataFusionNode();
        aggregateNode.setInput(input);

        for (Integer bitSet : getGroupSet()) {
            RexInputRef inputRef = RexInputRef.of(bitSet, getRowType().getFieldList());
            aggregateNode.addGroupExpr(transformRexNodeToExprNode(inputRef));
        }
        for (AggregateCall call : getAggCallList()) {
            aggregateNode.addAggrExpr(transformAggFunction(call, getRowType().getFieldList()));
        }

        aggregateNode.setMode(AggregateMode.SINGLE);
        aggregateNode.setInputSchema(buildRelNodeSchema(getInput().getRowType().getFieldList()));
        return PhysicalPlanNode.newBuilder().setAggregate(aggregateNode).build();
    }

    @Override
    public EnumerableAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                                    @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new ExtendEnumerableAggregate(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }
}
