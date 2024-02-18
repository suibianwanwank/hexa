package program;

import org.apache.calcite.plan.hep.HepMatchOrder;
import program.logical.LogicalHepProgram;
import program.logical.LogicalVolcanoProgram;
import program.program.RuleOptimizeProgram;
import program.program.SequenceRuleBasedProgram;
import program.rule.LogicalCoreRules;

public class QueryPlannerProgram {

    private QueryPlannerProgram() {
    }

    private static final String EXPRESSION_REDUCE = "EXPRESSION_REDUCE";
    private static final String WINDOW_REWRITE = "WINDOW_REWRITE";

    public static final RuleOptimizeProgram PRE_SIMPLIFY_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .addLast(new LogicalHepProgram.LogicalHepProgramBuilder()
                            .setHepMatchOrder(HepMatchOrder.ARBITRARY)
                            .add(LogicalCoreRules.EXPRESSION_REDUCE)
                            .setName(EXPRESSION_REDUCE)
                            .build())
                    .addLast(new LogicalHepProgram.LogicalHepProgramBuilder()
                            .setHepMatchOrder(HepMatchOrder.ARBITRARY)
                            .add(LogicalCoreRules.WINDOW_REWRITE)
                            .setName(WINDOW_REWRITE)
                            .build())
                    .build();


    public static final RuleOptimizeProgram LOGICAL_OPTIMIZE_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .addLast(new LogicalVolcanoProgram.LogicalVolcanoProgramBuilder()
                            .add(LogicalCoreRules.LOGICAL_BASED_RULES)
                            .add(LogicalCoreRules.LOGICAL_TO_PHYSICAL_RULES)
                            .build())
                    .build();

    public static final RuleOptimizeProgram LOGICAL_TO_PHYSICAL_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .addLast(new LogicalVolcanoProgram.LogicalVolcanoProgramBuilder()
                            .add(LogicalCoreRules.LOGICAL_TO_PHYSICAL_RULES)
                            .build())
                    .build();
}
