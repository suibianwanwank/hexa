package program.rule.program;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import program.program.RuleOptimizeProgram;

public class RemoveSargProgram implements RuleOptimizeProgram {

    public static RemoveSargProgram DEFAULT = new RemoveSargProgram();

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        return new RelShuttleImpl(){
            @Override
            public RelNode visit(RelNode relNode) {
                if(relNode instanceof Filter){
                    Filter filter = (Filter) relNode;
                    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
                    RexNode newCondition = RexUtil.expandSearch(rexBuilder, null, filter.getCondition());

                    relNode = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
                }
                return super.visit(relNode);
            }
        }.visit(root);
    }
}
