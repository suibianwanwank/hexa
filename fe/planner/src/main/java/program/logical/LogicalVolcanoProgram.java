package program.logical;

import context.QueryContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import program.program.RuleBasedProgram;

import static java.util.Objects.requireNonNull;

public class LogicalVolcanoProgram extends RuleBasedProgram {

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        if (rules.isEmpty()) {
            return root;
        }

        LogicalVolcanoPlanner planner = (LogicalVolcanoPlanner) root.getCluster().getPlanner();



        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }


        RelTraitSet targetTraits = root.getCluster().traitSetOf(EnumerableConvention.INSTANCE);

        if (!root.getTraitSet().equals(targetTraits)) {
            root = planner.changeTraits(root, targetTraits);
        }

        planner.setRoot(root);
        return planner.findBestExp();
    }

    public static class LogicalVolcanoProgramBuilder {
        private final LogicalVolcanoProgram volcanoProgram = new LogicalVolcanoProgram();

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

        public LogicalVolcanoProgram build() {
            return volcanoProgram;
        }
    }
}
