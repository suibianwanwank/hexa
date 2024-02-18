package program.physical;

import context.QueryContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import program.program.RuleBasedProgram;
import program.util.PlannerCancelFlag;
import program.util.PlannerSetting;

import static java.util.Objects.requireNonNull;

public class PhysicalVolcanoProgram extends RuleBasedProgram {
    private VolcanoPlanner planner;

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        if (!(root instanceof PhysicalNode) || rules.isEmpty()) {
            return root;
        }

        if (planner == null) {
            PlannerSetting plannerSetting =
                    new PlannerSetting(PlannerCancelFlag.create(100000));
            planner = new PhysicalVolcanoPlanner(plannerSetting);
        }

        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }

        planner.setRoot(root);
        return planner.findBestExp();
    }

    public static class LogicalVolcanoProgramBuilder {
        private final PhysicalVolcanoProgram volcanoProgram = new PhysicalVolcanoProgram();

        public LogicalVolcanoProgramBuilder add(RelOptRule rule) {
            requireNonNull(rule);
            volcanoProgram.add(requireNonNull(rule));
            return this;
        }

        public LogicalVolcanoProgramBuilder add(RuleSet ruleSet) {
            requireNonNull(ruleSet);
            volcanoProgram.add(requireNonNull(ruleSet));
            return this;
        }

//        public LogicalVolcanoProgramBuilder setOutputTraits(RelTrait[] relTraits)
//        {
//            requireNonNull(relTraits);
//            volcanoProgram.setOutputTraits(requireNonNull(relTraits));
//            return this;
//        }

        public PhysicalVolcanoProgram build() {
            return volcanoProgram;
        }
    }
}
