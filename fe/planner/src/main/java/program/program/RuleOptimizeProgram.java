package program.program;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;

public interface RuleOptimizeProgram {
    RelNode optimize(RelNode root, QueryContext optimizeContext);
}
