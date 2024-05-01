package program.rule;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import context.QueryContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import program.physical.rel.*;
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
                } else if (other instanceof EnumerableHashJoin) {
                    other = ExtendEnumerableHashJoin.create((EnumerableHashJoin) other);
                } else if (other instanceof EnumerableBatchNestedLoopJoin) {
                    other = ExtendEnumerableBatchNestedLoopJoin.create((EnumerableBatchNestedLoopJoin) other);
                } else if (other instanceof EnumerableMergeJoin) {
                    other = ExtendEnumerableMergeJoin.create((EnumerableMergeJoin) other);
                } else if (other instanceof EnumerableSort) {
                    other = ExtendEnumerableSort.create((EnumerableSort) other);
                } else if (other instanceof EnumerableNestedLoopJoin) {
                    other = ExtendEnumerableNestedLoopJoin.create((EnumerableNestedLoopJoin) other);
                } else if (other instanceof EnumerableUnion) {
                    other = ExtendEnumerableUnion.create((EnumerableUnion) other);
                }
                return super.visit(other);
            } catch (Exception e) {
                throw new CommonException(CommonErrorCode.PLAN_TRANSFORM_ERROR, e.getMessage());
            }
        }
    }
}
