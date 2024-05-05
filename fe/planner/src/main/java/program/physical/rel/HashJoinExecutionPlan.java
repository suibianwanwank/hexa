package program.physical.rel;

import com.ccsu.error.CommonException;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import program.util.RexUtils;
import proto.datafusion.HashJoinExecNode;
import proto.datafusion.JoinFilter;
import proto.datafusion.PhysicalExprNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.ccsu.error.CommonErrorCode.PLAN_TRANSFORM_ERROR;
import static program.physical.rel.PhysicalPlanTransformUtil.*;

public class HashJoinExecutionPlan extends EnumerableHashJoin implements ExecutionPlan {


    public static HashJoinExecutionPlan create(EnumerableHashJoin enumerableHashJoin) {
        return new HashJoinExecutionPlan(enumerableHashJoin.getCluster(), enumerableHashJoin.getTraitSet(),
                enumerableHashJoin.getLeft(), enumerableHashJoin.getRight(), enumerableHashJoin.getCondition(),
                enumerableHashJoin.getVariablesSet(), enumerableHashJoin.getJoinType());
    }

    protected HashJoinExecutionPlan(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                                    RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {

        proto.datafusion.PhysicalPlanNode leftNode = ((ExecutionPlan) getLeft()).transformToDataFusionNode();
        proto.datafusion.PhysicalPlanNode rightNode = ((ExecutionPlan) getRight()).transformToDataFusionNode();
        proto.datafusion.JoinType joinType = transformJoinType(getJoinType());

        List<RexNode> conjunctions = RelOptUtil.conjunctions(getCondition());

        HashJoinExecNode.Builder hashJoin = HashJoinExecNode.newBuilder();

        List<RexNode> rexNodeList = new ArrayList<>();
        for (RexNode conjunction : conjunctions) {
            if (checkHashJoinOn(conjunction)) {
                proto.datafusion.JoinOn joinOn = transformJoinOn(conjunction, getLeft(), getRight());
                hashJoin.addOn(joinOn);
                continue;
            }
            rexNodeList.add(conjunction);
        }


        RexNode filterNode = RexUtils.concatWithAnd(rexNodeList, getCluster().getRexBuilder());

        if (filterNode != null) {
            JoinFilter joinFilter = transformRexNodeToJoinFilter(filterNode, getRowType().getFieldList(),
                    getLeft().getRowType().getFieldList().size());
            hashJoin.setFilter(joinFilter);
        }


        hashJoin.setLeft(leftNode)
                .setRight(rightNode)
                .setJoinType(joinType)
                .setNullEqualsNull(false);
        return proto.datafusion.PhysicalPlanNode.newBuilder().setHashJoin(hashJoin).build();
    }

    private boolean checkHashJoinOn(RexNode node) {
        if (!node.isA(SqlKind.EQUALS)
                || !(node instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) node;

        if (call.getOperands().size() != 2) {
            return false;
        }

        return call.operands.get(0).isA(SqlKind.INPUT_REF)
                && call.operands.get(1).isA(SqlKind.INPUT_REF);
    }

    @Override
    public EnumerableHashJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new HashJoinExecutionPlan(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }
}
