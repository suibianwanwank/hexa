package program.rule;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;
import program.physical.rel.LimitExecutionPlan;

/**
 * Rule to convert an {@link org.apache.calcite.rel.core.Sort} that has
 * {@code offset} or {@code fetch} set to an
 * {@link EnumerableLimit}
 * on top of a "pure" {@code Sort} that has no offset or fetch.
 *
 * @see EnumerableRules#ENUMERABLE_LIMIT_RULE
 */
@Value.Enclosing
public class LimitExecutionPlanRule
        extends RelRule<LimitExecutionPlanRule.Config> {
    /** Creates an EnumerableLimitRule. */
    protected LimitExecutionPlanRule(LimitExecutionPlanRule.Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        if (sort.offset == null && sort.fetch == null) {
            return;
        }
        RelNode input = sort.getInput();
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input =
                    sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);
        }
        call.transformTo(
                LimitExecutionPlan.create(
                        convert(call.getPlanner(), input,
                                input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
                        sort.offset,
                        sort.fetch));
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        LimitExecutionPlanRule.Config DEFAULT = ImmutableLimitExecutionPlanRule.Config
                .builder()
                .operandSupplier(b -> b.operand(Sort.class).anyInputs())
                .build();

        @Override default LimitExecutionPlanRule toRule() {
            return new LimitExecutionPlanRule(this);
        }
    }
}
