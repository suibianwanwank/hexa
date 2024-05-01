package program.physical.rel;

import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

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
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.ProjectionExecNode.Builder builder = proto.datafusion.ProjectionExecNode.newBuilder();
        proto.datafusion.PhysicalPlanNode input = ((PhysicalPlan) getInput()).transformToDataFusionNode();
        builder.setInput(input);

        for (RexNode project : getProjects()) {
            builder.addExpr(PhysicalPlanTransformUtil.transformRexNodeToExprNode(project));
        }

        for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
            builder.addExprName(relDataTypeField.getName());
        }

        return proto.datafusion.PhysicalPlanNode.newBuilder()
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
