package program.rule;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import context.QueryContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import program.physical.rel.*;
import program.program.RuleOptimizeProgram;

public class ExecutionPlanTransformRule implements RuleOptimizeProgram {

    public static final ExecutionPlanTransformRule INSTANCE =
            new ExecutionPlanTransformRule();

    private ExecutionPlanTransformRule() {
    }

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        return root.accept(ExecutionPlanShuttle.INSTANCE);
    }

    static class ExecutionPlanShuttle extends RelShuttleImpl {

        public static final ExecutionPlanShuttle INSTANCE = new ExecutionPlanShuttle();

        @Override
        public RelNode visit(RelNode other) {
            try {
                if (other instanceof EnumerableFilter) {
                    other = FilterExecutionPlan.create((EnumerableFilter) other);
                } else if (other instanceof EnumerableProject) {
                    other = ProjectExecutionPlan.create((EnumerableProject) other);
                } else if (other instanceof EnumerableHashJoin) {
                    other = HashJoinExecutionPlan.create((EnumerableHashJoin) other);
                } else if (other instanceof EnumerableBatchNestedLoopJoin) {
                    other = BatchNestedLoopJoinExecutionPlan.create((EnumerableBatchNestedLoopJoin) other);
                } else if (other instanceof EnumerableMergeJoin) {
                    other = MergeJoinExecutionPlan.create((EnumerableMergeJoin) other);
                } else if (other instanceof EnumerableSort) {
                    other = SortExecutionPlan.create((EnumerableSort) other);
                } else if (other instanceof EnumerableNestedLoopJoin) {
                    other = NestedLoopJoinExecutionPlan.create((EnumerableNestedLoopJoin) other);
                } else if (other instanceof EnumerableUnion) {
                    other = UnionExecutionPlan.create((EnumerableUnion) other);
                }
                return super.visit(other);
            } catch (Exception e) {
                throw new CommonException(CommonErrorCode.PLAN_TRANSFORM_ERROR, e.getMessage());
            }
        }
    }
}
