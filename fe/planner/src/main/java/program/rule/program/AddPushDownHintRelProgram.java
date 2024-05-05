package program.rule.program;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import program.physical.rel.SourceScanExecutionPlan;
import program.physical.rel.PushDownHintRel;
import program.program.RuleOptimizeProgram;

public class AddPushDownHintRelProgram implements RuleOptimizeProgram {

    public static final AddPushDownHintRelProgram DEFAULT = new AddPushDownHintRelProgram();

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        return root.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                if (scan instanceof SourceScanExecutionPlan) {
                    return new PushDownHintRel(((SourceScanExecutionPlan) scan),
                            scan.getCluster(), scan.getTraitSet(), scan);
                }
                return super.visit(scan);
            }
        });
    }
}
