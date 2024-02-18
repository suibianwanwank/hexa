package program.program;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SequenceRuleBasedProgram
        implements RuleOptimizeProgram {
    private final List<RuleOptimizeProgram> ruleBasedProgramList = new ArrayList<>();

    @Override
    public RelNode optimize(RelNode relNode, QueryContext optimizeContext) {
        for (RuleOptimizeProgram ruleOptimizeProgram : ruleBasedProgramList) {
            relNode = ruleOptimizeProgram.optimize(relNode, optimizeContext);
        }
        return relNode;
    }

    public void addLast(RuleOptimizeProgram program) {
        requireNonNull(program);

        ruleBasedProgramList.add(program);
    }

    public static class SequenceRuleBasedProgramBuilder {
        private final SequenceRuleBasedProgram chainedProgram = new SequenceRuleBasedProgram();

        public SequenceRuleBasedProgramBuilder addLast(RuleOptimizeProgram program) {
            chainedProgram.addLast(program);
            return this;
        }

        public SequenceRuleBasedProgram build() {
            return chainedProgram;
        }
    }
}
