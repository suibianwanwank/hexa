package program.rule;

import com.facebook.airlift.log.Logger;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;
import program.physical.rel.PushDownHintRel;

@Value.Enclosing
public class PushDownFilterRule
        extends RelRule<PushDownFilterRule.Config> {

    private static final Logger LOGGER = Logger.get(PushDownFilterRule.class);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected PushDownFilterRule(Config config) {
        super(config);
    }

    @Value.Immutable
    public interface Config
            extends RelRule.Config {
        PushDownFilterRule.Config DEFAULT = ImmutablePushDownFilterRule.Config.builder()
                .operandSupplier(b -> b.operand(Filter.class)
                        .oneInput(b1 -> b1.operand(PushDownHintRel.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new PushDownFilterRule(this);
        }
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        PushDownHintRel hintRel = call.rel(1);

        RexNode condition = filter.getCondition();


        if (!deriveCouldFilterPushDown(condition)) {
            return;
        }
        RelNode newFilter = filter.copy(filter.getTraitSet(), hintRel.getInputs());
        PushDownHintRel pushDownHintRel =
                new PushDownHintRel(hintRel.getTableScan(), newFilter.getCluster(), newFilter.getTraitSet(), newFilter);

        call.transformTo(pushDownHintRel);
    }

    private boolean deriveCouldFilterPushDown(RexNode condition) {
        switch (condition.getKind()) {
            case AND:
            case OR:
            case EQUALS:
            case NOT_EQUALS:
            case IN:
            case NOT_IN:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case IS_NULL:
            case IS_NOT_NULL:
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case SEARCH:
            case LESS_THAN_OR_EQUAL: {
                if (condition instanceof RexCall) {
                    for (RexNode operand : ((RexCall) condition).getOperands()) {
                        if (!deriveCouldFilterPushDown(operand)) {
                            LOGGER.info("Can not push down operand:%s", operand);
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
            case INPUT_REF:
            case LITERAL:
                return true;
            default:
                return false;
        }
    }
}
