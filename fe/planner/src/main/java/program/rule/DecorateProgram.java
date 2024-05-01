package program.rule;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import program.program.RuleOptimizeProgram;

public class DecorateProgram implements RuleOptimizeProgram {
    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        final RelBuilder relBuilder =
                RelFactories.LOGICAL_BUILDER.create(root.getCluster(), null);
        return RelDecorrelator.decorrelateQuery(root, relBuilder);
    }
}
