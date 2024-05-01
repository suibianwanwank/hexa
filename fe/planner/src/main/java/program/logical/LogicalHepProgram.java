package program.logical;

import context.QueryContext;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import program.program.RuleBasedProgram;
import program.util.PlannerCancelFlag;
import program.util.PlannerSetting;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LogicalHepProgram extends RuleBasedProgram {

    private String name;

    private HepProgram hepProgram;

    private Integer matchLimit = Integer.MAX_VALUE;

    private HepMatchOrder matchOrder = HepMatchOrder.ARBITRARY;

    private RelTrait[] rootTraits;

    private final List<RelOptListener> optListenerList = new ArrayList<>();

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {

        if (hepProgram == null) {
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addMatchOrder(matchOrder);
            builder.addMatchLimit(matchLimit);
            builder.addRuleCollection(rules);
            hepProgram = builder.build();
        }

        PlannerSetting plannerSetting = new PlannerSetting(PlannerCancelFlag.create(100000));
        HepPlanner planner = new LogicalHepPlanner(hepProgram, plannerSetting);

        if (!optListenerList.isEmpty()) {
            for (RelOptListener listener : optListenerList) {
                planner.addListener(listener);
            }
        }

        if (!Objects.isNull(rootTraits) && rootTraits.length != 0) {
            RelTraitSet tarRelTraitSet = root.getTraitSet().plusAll(rootTraits);
            if (!root.getTraitSet().equals(tarRelTraitSet)) {
                planner.changeTraits(root, tarRelTraitSet);
            }
        }

        planner.setRoot(root);
        return planner.findBestExp();
    }


    public static class LogicalHepProgramBuilder {
        private final LogicalHepProgram hepRuleProgram = new LogicalHepProgram();

        public LogicalHepProgramBuilder setMatchLimit(Integer matchLimit) {
            requireNonNull(matchLimit);
            hepRuleProgram.matchLimit = matchLimit;
            return this;
        }

        public LogicalHepProgramBuilder setHepMatchOrder(HepMatchOrder hepMatchOrder) {
            requireNonNull(hepMatchOrder);
            hepRuleProgram.matchOrder = hepMatchOrder;
            return this;
        }

        public LogicalHepProgramBuilder setHepProgram(HepProgram hepProgram) {
            requireNonNull(hepProgram);
            hepRuleProgram.hepProgram = hepProgram;
            return this;
        }

        public LogicalHepProgramBuilder setRootTraits(RelTrait[] rootTraits) {
            requireNonNull(rootTraits);
            hepRuleProgram.rootTraits = rootTraits;
            return this;
        }

        public LogicalHepProgramBuilder add(RelOptRule rule) {
            requireNonNull(rule);
            hepRuleProgram.add(rule);
            return this;
        }

        public LogicalHepProgramBuilder add(RuleSet ruleSet) {
            requireNonNull(ruleSet);
            ruleSet.forEach(hepRuleProgram::add);
            return this;
        }

        public LogicalHepProgramBuilder setName(String name) {
            hepRuleProgram.name = name;
            return this;
        }

        public LogicalHepProgramBuilder addListeners(List<RelOptListener> listeners) {
            hepRuleProgram.optListenerList.addAll(listeners);
            return this;
        }

        public LogicalHepProgram build() {
            return hepRuleProgram;
        }
    }
}
