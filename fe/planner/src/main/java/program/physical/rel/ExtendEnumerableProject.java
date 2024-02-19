package program.physical.rel;

import arrow.datafusion.PhysicalPlanNode;
import arrow.datafusion.ProjectionExecNode;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ExtendEnumerableProject
        extends EnumerableProject
        implements PhysicalPlan {

    public static ExtendEnumerableProject create(EnumerableProject enumerableProject) {
        return new ExtendEnumerableProject(enumerableProject.getCluster(), enumerableProject.getTraitSet(),
                enumerableProject.getInput(), enumerableProject.getProjects(), enumerableProject.getRowType());
    }


    private ExtendEnumerableProject(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RelNode input,
                                    List<? extends RexNode> projects,
                                    RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        ProjectionExecNode.Builder builder = ProjectionExecNode.newBuilder();
        PhysicalPlanNode input = ((PhysicalPlan) getInput()).transformToDataFusionNode();
        builder.setInput(input);
        for (RexNode project : getProjects()) {
            builder.addExpr(PhysicalPlanTransformUtil.transformRexNodeToExprNode(project));
        }
        return PhysicalPlanNode.newBuilder()
                .setProjection(builder)
                .build();
    }

    @Override
    public ExtendEnumerableProject copy(RelTraitSet traitSet, RelNode input,
                                        List<RexNode> projects, RelDataType rowType) {
        return new ExtendEnumerableProject(getCluster(), traitSet, input,
                projects, rowType);
    }
}
