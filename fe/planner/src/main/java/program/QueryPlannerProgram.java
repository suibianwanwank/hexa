package program;

import com.ccsu.profile.Phrase;
import org.apache.calcite.plan.hep.HepMatchOrder;
import program.logical.LogicalHepProgram;
import program.logical.LogicalVolcanoProgram;
import program.program.RuleOptimizeProgram;
import program.program.SequenceRuleBasedProgram;
import program.rule.*;
import program.rule.program.*;

public class QueryPlannerProgram {
    private static final String EXPRESSION_REDUCE = "EXPRESSION_REDUCE";
    private static final String WINDOW_REWRITE = "WINDOW_REWRITE";

    public static final RuleOptimizeProgram LOGICAL_PLAN_OPTIMIZE_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .setPhrase(Phrase.LOGICAL_PLAN_OPTIMIZE)
                    .addLast(new ArrowTypeDeriveProgram())
                    .addLast(new LogicalHepProgram.LogicalHepProgramBuilder()
                            .setHepMatchOrder(HepMatchOrder.ARBITRARY)
                            .add(LogicalCoreRules.EXPRESSION_REDUCE)
                            .add(LogicalCoreRules.JOIN_REMOVE)
                            .add(LogicalCoreRules.SUB_QUERY)
                            .build())
                    .addLast(new LogicalHepProgram.LogicalHepProgramBuilder()
                            .setHepMatchOrder(HepMatchOrder.ARBITRARY)
                            .add(LogicalCoreRules.WINDOW_REWRITE)
                            .setName(WINDOW_REWRITE)
                            .build())
                    .addLast(new DecorateProgram())
                    .build();

    /**
     * Program of Optimization rules for physical logical plan.
     * Optimize the logical plan and finally translate it into a physical execution plan through CBO.
     */
    public static final RuleOptimizeProgram LOGICAL_TO_PHYSICAL_PLAN_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .setPhrase(Phrase.LOGICAL_TO_PHYSICAL_PLAN)
                    .addLast(new TrimFieldsProgram())
                    .addLast(new LogicalVolcanoProgram.LogicalVolcanoProgramBuilder()
                            .add(LogicalCoreRules.LOGICAL_BASED_RULES)
                            .add(LogicalCoreRules.LOGICAL_TO_PHYSICAL_RULES)
                            .build())
                    .addLast(RenameProjectRule.INSTANCE)
                    .build();


    public static final RuleOptimizeProgram JDBC_PUSH_DOWN_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .addLast(AddPushDownHintRelProgram.DEFAULT)
                    .addLast(new LogicalHepProgram.LogicalHepProgramBuilder()
                            .setHepMatchOrder(HepMatchOrder.ARBITRARY)
                            .add(LogicalCoreRules.PUSH_DOWN_PROJECT_RULE)
                            .add(LogicalCoreRules.PUSH_DOWN_FILTER_RULE)
                            .build())
                    .addLast(RemovePushDownHintRelProgram.DEFAULT)
                    .build();

    /**
     *  Program of Optimization rules for physical execution plan.
     *
     * <p>Note that last rule must be {@link ExecutionPlanTransformRule}.</p>
     */
    public static final RuleOptimizeProgram PHYSICAL_OPTIMIZE_PROGRAM =
            new SequenceRuleBasedProgram.SequenceRuleBasedProgramBuilder()
                    .setPhrase(Phrase.PHYSICAL_PLAN_OPTIMIZE)
                    .addLast(JDBC_PUSH_DOWN_PROGRAM)
                    .addLast(RemoveSargProgram.DEFAULT)
                    .addLast(ExecutionPlanTransformRule.INSTANCE)
                    .build();
}
