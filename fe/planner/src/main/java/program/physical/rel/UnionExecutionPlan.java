package program.physical.rel;

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import proto.datafusion.PhysicalPlanNode;
import proto.datafusion.UnionExecNode;

import java.util.List;

public class UnionExecutionPlan
        extends EnumerableUnion
        implements ExecutionPlan {


    public static UnionExecutionPlan create(EnumerableUnion union){
        return new UnionExecutionPlan(union.getCluster(), union.getTraitSet(), union.getInputs(), union.all);
    }

    public UnionExecutionPlan(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        List<PhysicalPlanNode> inputs = Lists.newArrayList();
        for (RelNode input : getInputs()) {
            inputs.add(((ExecutionPlan) input).transformToDataFusionNode());
        }
        UnionExecNode.Builder union = UnionExecNode.newBuilder()
                .addAllInputs(inputs);
        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setUnion(union)
                .build();
    }

    @Override
    public EnumerableUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new UnionExecutionPlan(getCluster(), traitSet, inputs, all);
    }
}
