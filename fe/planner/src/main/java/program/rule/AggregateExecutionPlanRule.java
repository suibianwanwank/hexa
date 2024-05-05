package program.rule;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.checkerframework.checker.nullness.qual.Nullable;
import program.physical.rel.AggregateExecutionPlan;

class AggregateExecutionPlanRule extends ConverterRule {
    /** Default configuration. */
    static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "EnumerableAggregateRule")
            .withRuleFactory(AggregateExecutionPlanRule::new);

    /** Called from the Config. */
    protected AggregateExecutionPlanRule(Config config) {
        super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        final RelTraitSet traitSet = rel.getCluster()
                .traitSet().replace(EnumerableConvention.INSTANCE);
        try {
            return new AggregateExecutionPlan(
                    rel.getCluster(),
                    traitSet,
                    convert(agg.getInput(), traitSet),
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        } catch (InvalidRelException e) {
            return null;
        }
    }
}
