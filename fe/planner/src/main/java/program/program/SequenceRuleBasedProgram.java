package program.program;

import com.ccsu.profile.Phrase;
import com.ccsu.profile.ProfileUtil;
import com.google.common.base.Stopwatch;
import context.QueryContext;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class SequenceRuleBasedProgram
        implements RuleOptimizeProgram {

    private Phrase phrase;
    private final List<RuleOptimizeProgram> ruleBasedProgramList = new ArrayList<>();

    @Override
    public RelNode optimize(RelNode relNode, QueryContext optimizeContext) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (RuleOptimizeProgram ruleOptimizeProgram : ruleBasedProgramList) {
            relNode = ruleOptimizeProgram.optimize(relNode, optimizeContext);
        }

        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);

        if (phrase != null) {
            ProfileUtil.addPlanPhraseJobProfile(optimizeContext.getJobProfile(), phrase, elapsed, relNode);
        }
        return relNode;
    }

    public void addLast(RuleOptimizeProgram program) {
        requireNonNull(program);

        ruleBasedProgramList.add(program);
    }

    public static class SequenceRuleBasedProgramBuilder {
        private final SequenceRuleBasedProgram chainedProgram = new SequenceRuleBasedProgram();

        public SequenceRuleBasedProgramBuilder setPhrase(Phrase phrase) {
            chainedProgram.phrase = phrase;
            return this;
        }

        public SequenceRuleBasedProgramBuilder addLast(RuleOptimizeProgram program) {
            chainedProgram.addLast(program);
            return this;
        }

        public SequenceRuleBasedProgram build() {
            return chainedProgram;
        }
    }
}
