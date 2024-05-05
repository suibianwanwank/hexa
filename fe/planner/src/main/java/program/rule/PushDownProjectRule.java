package program.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.immutables.value.Value;
import program.physical.rel.PushDownHintRel;

@Value.Enclosing
public class PushDownProjectRule extends RelRule<PushDownProjectRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected PushDownProjectRule(Config config) {
        super(config);
    }

    @Value.Immutable
    public interface Config
            extends RelRule.Config {
        Config DEFAULT = ImmutablePushDownProjectRule.Config.builder()
                .operandSupplier(b -> b.operand(Project.class)
                        .oneInput(b1 -> b1.operand(PushDownHintRel.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new PushDownProjectRule(this);
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        PushDownHintRel hintRel = call.rel(1);


        int projectSize = project.getRowType().getFieldList().size();
        int hintFieldSize = hintRel.getRowType().getFieldList().size();
        if (projectSize == hintFieldSize || !project.isMapping()) {
            return;
        }

        RelNode newProject = project.copy(project.getTraitSet(), hintRel.getInputs());
        PushDownHintRel pushDownHintRel =
                new PushDownHintRel(hintRel.getTableScan(), newProject.getCluster(), newProject.getTraitSet(), newProject);

        call.transformTo(pushDownHintRel);
    }
}
