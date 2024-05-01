package program.rule.shuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexShuttle;

public class RelToRexShuttle extends RelShuttleImpl {

    private final RexShuttle shuttle;

    public RelToRexShuttle(RexShuttle shuttle) {
        this.shuttle = shuttle;
    }


    @Override
    public RelNode visit(LogicalValues values) {
        return super.visit(values.accept(shuttle));
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return super.visit(filter.accept(shuttle));
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return super.visit(project.accept(shuttle));
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return super.visit(union.accept(shuttle));
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return super.visit(correlate);
    }
}
