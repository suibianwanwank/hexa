package program.physical.rel;

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
import proto.datafusion.AggregateMode;
import proto.datafusion.MaybeFilter;
import proto.datafusion.PhysicalExprNode;

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
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.AggregateExecNode.Builder aggregateNode = proto.datafusion.AggregateExecNode.newBuilder();

        proto.datafusion.PhysicalPlanNode input = ((PhysicalPlan) getInput()).transformToDataFusionNode();
        aggregateNode.setInput(input);


        List<RelDataTypeField> inputFields = getInput().getRowType().getFieldList();
        for (Integer bitSet : getGroupSet()) {
            RexInputRef inputRef = RexInputRef.of(bitSet, getRowType().getFieldList());
            proto.datafusion.PhysicalColumn.Builder column = proto.datafusion.PhysicalColumn
                    .newBuilder()
                    .setIndex(inputRef.getIndex())
                    .setName(inputFields.get(inputRef.getIndex()).getName());
            aggregateNode.addGroupExpr(PhysicalExprNode.newBuilder().setColumn(column).build());
            aggregateNode.addGroupExprName(inputFields.get(inputRef.getIndex()).getName());
            aggregateNode.addGroups(false);
        }
        for (AggregateCall call : getAggCallList()) {
            aggregateNode.addAggrExpr(transformAggFunction(call, getInput().getRowType().getFieldList()));
            aggregateNode.addAggrExprName(call.getName());
            aggregateNode.addFilterExpr(MaybeFilter.newBuilder().build());
        }

        aggregateNode.setMode(AggregateMode.FINAL_PARTITIONED);
        aggregateNode.setInputSchema(buildRelNodeSchema(inputFields));

        return proto.datafusion.PhysicalPlanNode.newBuilder().setAggregate(aggregateNode).build();
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
