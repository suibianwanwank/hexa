package program.physical.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
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

import static program.physical.rel.ExecutionPlanToDataFusionPlanUtil.*;

public class AggregateExecutionPlan
        extends EnumerableAggregateBase
        implements ExecutionPlan, EnumerableRel {

    public AggregateExecutionPlan(RelOptCluster cluster,
                                  RelTraitSet traitSet,
                                  RelNode input,
                                  ImmutableBitSet groupSet,
                                  @Nullable List<ImmutableBitSet> groupSets,
                                  List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.AggregateExecNode.Builder aggregateNode = proto.datafusion.AggregateExecNode.newBuilder();

        proto.datafusion.PhysicalPlanNode input = ((ExecutionPlan) getInput()).transformToDataFusionNode();
        aggregateNode.setInput(input);


        List<RelDataTypeField> inputFields = getInput().getRowType().getFieldList();
        for (Integer bitSet : getGroupSet()) {
            RexInputRef inputRef = RexInputRef.of(bitSet, inputFields);
            proto.datafusion.PhysicalColumn.Builder column = proto.datafusion.PhysicalColumn
                    .newBuilder()
                    .setIndex(inputRef.getIndex())
                    .setName(inputFields.get(inputRef.getIndex()).getName());
            aggregateNode.addGroupExpr(PhysicalExprNode.newBuilder().setColumn(column).build());
            aggregateNode.addGroupExprName(inputFields.get(inputRef.getIndex()).getName());
            aggregateNode.addGroups(false);
        }

        List<RelDataTypeField> fields = getRowType().getFieldList();
        for (AggregateCall call : getAggCallList()) {
            aggregateNode.addAggrExpr(transformAggFunction(call, getInput().getRowType().getFieldList()));
            aggregateNode.addAggrExprName(call.getName() != null
                    ? call.getName()
                    : fields.get(fields.size() - getGroupCount()).getName());
            aggregateNode.addFilterExpr(MaybeFilter.newBuilder().build());
        }

        aggregateNode.setMode(AggregateMode.SINGLE);
        aggregateNode.setInputSchema(buildRelNodeSchema(inputFields));

        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setAggregate(aggregateNode)
                .build();
    }

    @Override
    public AggregateExecutionPlan copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                                       @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new AggregateExecutionPlan(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}
