package program.rule;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import context.QueryContext;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import program.physical.rel.ExtendEnumerableAggregate;
import program.physical.rel.ExtendEnumerableBatchNestedLoopJoin;
import program.physical.rel.ExtendEnumerableFilter;
import program.physical.rel.ExtendEnumerableMergeJoin;
import program.physical.rel.ExtendEnumerableProject;
import program.physical.rel.ExtendEnumerableSort;
import program.program.RuleOptimizeProgram;

public class ExtendPhysicalRelTransformRule implements RuleOptimizeProgram {

    public static final ExtendPhysicalRelTransformRule INSTANCE =
            new ExtendPhysicalRelTransformRule();

    private ExtendPhysicalRelTransformRule() {
    }

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        return root.accept(ExtendTransformerShuttle.INSTANCE);
    }

    static class ExtendTransformerShuttle extends RelShuttleImpl {

        public static final ExtendTransformerShuttle INSTANCE = new ExtendTransformerShuttle();

        @Override
        public RelNode visit(RelNode other) {
            try {
                if (other instanceof EnumerableAggregate) {
                    other = ExtendEnumerableAggregate.create((EnumerableAggregate) other);
                } else if (other instanceof EnumerableFilter) {
                    other = ExtendEnumerableFilter.create((EnumerableFilter) other);
                } else if (other instanceof EnumerableProject) {
                    other = ExtendEnumerableProject.create((EnumerableProject) other);
                } else if (other instanceof EnumerableBatchNestedLoopJoin) {
                    other = ExtendEnumerableBatchNestedLoopJoin.create((EnumerableBatchNestedLoopJoin) other);
                } else if (other instanceof EnumerableMergeJoin) {
                    other = ExtendEnumerableMergeJoin.create((EnumerableMergeJoin) other);
                } else if (other instanceof EnumerableSort) {
                    other = ExtendEnumerableSort.create((EnumerableSort) other);
                }
                return super.visit(other);
            } catch (Exception e) {
                throw new CommonException(CommonErrorCode.PLAN_TRANSFORM_ERROR, e.getMessage());
            }
        }
    }
}
