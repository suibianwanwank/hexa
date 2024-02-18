package program.program;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class RuleBasedProgram implements RuleOptimizeProgram {
    protected List<RelOptRule> rules = new ArrayList<>();

    public boolean add(RelOptRule rule) {
        requireNonNull(rule);
        return rules.add(rule);
    }

    public boolean add(RuleSet ruleSet) {
        requireNonNull(ruleSet);
        ruleSet.forEach(rules::add);
        return true;
    }

    public boolean remove(RelOptRule rule) {
        requireNonNull(rule);
        return rules.remove(rule);
    }

    public boolean remove(RuleSet ruleSet) {
        requireNonNull(ruleSet);
        ruleSet.forEach(rules::remove);
        return true;
    }

    public Boolean contains(RelOptRule rule) {
        requireNonNull(rule);
        return rules.contains(rule);
    }
}
