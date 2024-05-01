package program.rule;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import program.program.RuleOptimizeProgram;
import program.rule.shuttle.DeriveArrowTypeShuttle;
import program.rule.shuttle.RelToRexShuttle;

public class ArrowTypeDeriveProgram implements RuleOptimizeProgram {

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        DeriveArrowTypeShuttle deriveArrowTypeShuttle = new DeriveArrowTypeShuttle(root.getCluster().getRexBuilder());

        return root.accept(new RelToRexShuttle(deriveArrowTypeShuttle));
    }
}
