package program.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class LogicalCoreRules {


    public static final RelOptRule PUSH_DOWN_FILTER_RULE = PushDownFilterRule.Config.DEFAULT.toRule();

    public static final RelOptRule PUSH_DOWN_PROJECT_RULE = PushDownProjectRule.Config.DEFAULT.toRule();

//    public static final RemoveJoinSingleValue REMOVE_JOIN_SINGLE_VALUE_RULE = RemoveJoinSingleValue.Config.DEFAULT.toRule();

    private LogicalCoreRules() {
    }

    public static final RuleSet EXPRESSION_REDUCE = RuleSets.ofList(ImmutableList.of(
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.CALC_REDUCE_EXPRESSIONS,
            CoreRules.UNION_PULL_UP_CONSTANTS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS));

    public static final RuleSet CONSTANT_REDUCTION_RULES = RuleSets.ofList(ImmutableList.of(
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.CALC_REDUCE_EXPRESSIONS,
            CoreRules.WINDOW_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_VALUES_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE,
            CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.AGGREGATE_VALUES));

    public static final RuleSet JOIN_REMOVE = RuleSets.ofList(ImmutableList.of(
            CoreRules.AGGREGATE_JOIN_REMOVE,
            CoreRules.AGGREGATE_JOIN_JOIN_REMOVE,
            CoreRules.PROJECT_JOIN_REMOVE,
            CoreRules.PROJECT_JOIN_JOIN_REMOVE));

    public static final RuleSet WINDOW_REWRITE = RuleSets.ofList(ImmutableList.of(
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_WINDOW_TRANSPOSE));

    public static final RuleSet LOGICAL_BASED_RULES = RuleSets.ofList(ImmutableList.of(
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
//            REMOVE_JOIN_SINGLE_VALUE_RULE,
            AbstractConverter.ExpandConversionRule.INSTANCE,
            PruneEmptyRules.UNION_INSTANCE,
            PruneEmptyRules.INTERSECT_INSTANCE,
            PruneEmptyRules.MINUS_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE,
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
            CoreRules.SORT_REMOVE_CONSTANT_KEYS,
            CoreRules.PROJECT_REMOVE,
            CoreRules.AGGREGATE_REMOVE,
            CoreRules.CALC_REMOVE,
            CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS));
    public static final RuleSet LOGICAL_TO_PHYSICAL_RULES = RuleSets.ofList(ImmutableList.of(
            AggregateExecutionPlanRule.DEFAULT_CONFIG.toRule(),
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            LimitExecutionPlanRule.Config.DEFAULT.toRule(),
            EnumerableRules.ENUMERABLE_COLLECT_RULE,
            EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
            EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE,
            EnumerableRules.ENUMERABLE_INTERSECT_RULE,
            EnumerableRules.ENUMERABLE_MINUS_RULE,
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
            EnumerableValuesRule.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            SourceScanExecutionRule.ENUMERABLE_SQL_SCAN_RULE,
            EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MATCH_RULE
    ));

    public static final RuleSet SUB_QUERY =
            RuleSets.ofList(ImmutableList.of(
                    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                    CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                    CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                    CoreRules.FILTER_CORRELATE));
}
