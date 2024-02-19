package program.physical.rel;

import arrow.datafusion.AggregateFunction;
import arrow.datafusion.ArrowType;
import arrow.datafusion.Decimal;
import arrow.datafusion.EmptyMessage;
import arrow.datafusion.JoinFilter;
import arrow.datafusion.JoinOn;
import arrow.datafusion.JoinType;
import arrow.datafusion.PhysicalAggregateExprNode;
import arrow.datafusion.PhysicalCaseNode;
import arrow.datafusion.PhysicalCastNode;
import arrow.datafusion.PhysicalColumn;
import arrow.datafusion.PhysicalExprNode;
import arrow.datafusion.PhysicalLikeExprNode;
import arrow.datafusion.PhysicalScalarUdfNode;
import arrow.datafusion.PhysicalWhenThen;
import arrow.datafusion.ScalarValue;
import com.ccsu.error.CommonException;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;
import java.util.Objects;

import static com.ccsu.error.CommonErrorCode.PLAN_TRANSFORM_ERROR;

public class PhysicalPlanTransformUtil {

    public static final EmptyMessage EMPTY_MESSAGE = EmptyMessage.getDefaultInstance();

    private PhysicalPlanTransformUtil() {
    }

    public static JoinType transformJoinType(JoinRelType type) {
        switch (type) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT;
            case RIGHT:
                return JoinType.RIGHT;
            case FULL:
                return JoinType.FULL;
            case ANTI:
                return JoinType.LEFTANTI;
            case SEMI:
                return JoinType.LEFTSEMI;
            default:
                String errMsg = String.format("Join type:%s not support", type);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static JoinFilter transformRexNodeToJoinFilter(RexNode rexNode) {
        return JoinFilter.newBuilder()
                .setExpression(transformRexNodeToExprNode(rexNode))
                .build();
    }

    public static PhysicalExprNode transformRexNodeToExprNode(RexNode rexNode) {
        PhysicalExprNode.Builder builder = PhysicalExprNode.newBuilder();
        switch (rexNode.getKind()) {
            case LITERAL: {
                ScalarValue scalarValue = transformLiteral((RexLiteral) rexNode);
                return builder.setLiteral(scalarValue)
                        .build();
            }
            case INPUT_REF: {
                PhysicalColumn column = transformColumn((RexInputRef) rexNode);
                return builder.setColumn(column)
                        .build();
            }
            case CASE: {
                PhysicalCaseNode caseNode = transformCaseNode((RexCall) rexNode);
                return builder.setCase(caseNode)
                        .build();
            }
            case LIKE: {
                PhysicalLikeExprNode likeNode = transformLikeNode((RexCall) rexNode);
                return builder.setLikeExpr(likeNode)
                        .build();

            }
            case CAST: {
                PhysicalCastNode castNode = transformCastNode((RexCall) rexNode);
                return builder.setCast(castNode)
                        .build();
            }
            case OTHER: {
                PhysicalScalarUdfNode function = transformFunction(rexNode);
                return builder.setScalarUdf(function)
                        .build();
            }
            default:
                String errMsg = String.format("RexNode:%s can not be convert", rexNode);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static PhysicalExprNode transformAggFunction(AggregateCall call, List<RelDataTypeField> fields) {
        PhysicalAggregateExprNode.Builder builder = PhysicalAggregateExprNode.newBuilder();
        String aggName = call.getName();
        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction = AggregateFunction.valueOf(aggName);
            builder.setAggrFunction(aggregateFunction);
        } catch (Exception ignore) {
        }
        if (aggregateFunction == null) {
            builder.setUserDefinedAggrFunction(aggName);
        }
        for (Integer integer : call.getArgList()) {
            builder.addExpr(transformRexNodeToExprNode(RexInputRef.of(integer, fields)));
        }
        return PhysicalExprNode.newBuilder().setAggregateExpr(builder).build();
    }

    public static ScalarValue transformLiteral(RexLiteral literal) {
        ScalarValue.Builder builder = ScalarValue.newBuilder();
        Comparable comparable = Objects.requireNonNull(literal.getValue());
        switch (literal.getType().getSqlTypeName()) {
            case INTEGER:
                int i = Integer.parseInt((String) comparable);
                return builder.setInt16Value(i)
                        .build();
            case BIGINT:
                long l = Long.parseLong((String) comparable);
                return builder.setInt64Value(l)
                        .build();
            case VARCHAR:
                String s = (String) comparable;
                return builder.setLargeUtf8Value(s)
                        .build();
            default:
                String errMsg = String.format("SqlTypeName:%s can not be mapping", literal.getType().getSqlTypeName());
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static PhysicalColumn transformColumn(RexInputRef inputRef) {
        PhysicalColumn.Builder builder = PhysicalColumn.newBuilder();
        return builder.setIndex(inputRef.getIndex())
                .setName(inputRef.getName())
                .build();
    }

    public static PhysicalCaseNode transformCaseNode(RexCall call) {
        PhysicalCaseNode.Builder builder = PhysicalCaseNode.newBuilder();
        List<RexNode> operands = call.getOperands();
        for (int i = 0; i < operands.size() / 2; i += 2) {
            RexNode whenNode = operands.get(i);
            RexNode thenNode = operands.get(i + 1);
            PhysicalWhenThen whenThen = PhysicalWhenThen.newBuilder()
                    .setWhenExpr(transformRexNodeToExprNode(whenNode))
                    .setThenExpr(transformRexNodeToExprNode(thenNode))
                    .build();
            builder.addWhenThenExpr(whenThen);
        }
        builder.setElseExpr(transformRexNodeToExprNode(operands.get(operands.size() - 1)));
        return builder.build();
    }

    public static JoinOn transformJoinOn(RexNode rexNode) {
        if (!rexNode.isA(SqlKind.EQUALS)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
        RexCall call = (RexCall) rexNode;
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(0);
        if (!left.isA(SqlKind.INPUT_REF) || !right.isA(SqlKind.INPUT_REF)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
        RexInputRef leftInput = (RexInputRef) left;
        RexInputRef rightInput = (RexInputRef) right;
        return JoinOn.newBuilder()
                .setLeft(PhysicalColumn.newBuilder()
                        .setName(leftInput.getName())
                        .setIndex(leftInput.getIndex()))
                .setRight(PhysicalColumn.newBuilder()
                        .setName(rightInput.getName())
                        .setIndex(rightInput.getIndex()))
                .build();
    }

    public static PhysicalLikeExprNode transformLikeNode(RexCall call) {
        PhysicalLikeExprNode.Builder builder = PhysicalLikeExprNode.newBuilder();
        List<RexNode> operands = call.getOperands();
        RexNode expr = operands.get(0);
        RexNode pattern = operands.get(1);
        builder.setExpr(transformRexNodeToExprNode(expr));
        builder.setPattern(transformRexNodeToExprNode(pattern));
        return builder.setExpr(transformRexNodeToExprNode(expr))
                .setCaseInsensitive(true)
                .setPattern(transformRexNodeToExprNode(pattern))
                .build();
    }

    public static PhysicalCastNode transformCastNode(RexCall call) {
        List<RexNode> operands = call.getOperands();
        return PhysicalCastNode.newBuilder()
                .setExpr(transformRexNodeToExprNode(operands.get(0)))
                .setArrowType(transformRelTypeToArrowType(call.getType()))
                .build();
    }

    public static PhysicalScalarUdfNode transformFunction(RexNode node) {
        if (node instanceof RexCall) {
            SqlOperator operator = ((RexCall) node).getOperator();
            List<PhysicalExprNode> args = Lists.newArrayList();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                args.add(transformRexNodeToExprNode(operand));
            }
            return PhysicalScalarUdfNode.newBuilder()
                    .setName(operator.getName())
                    .addAllArgs(args)
                    .build();
        }
        String errMsg = String.format("RexNode:%s can not be convert", node);
        throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
    }

    public static ArrowType transformRelTypeToArrowType(RelDataType relDataType) {
        ArrowType.Builder builder = ArrowType.newBuilder();
        switch (relDataType.getSqlTypeName()) {
            case INTEGER:
                return builder.setINT32(EMPTY_MESSAGE)
                        .build();
            case DOUBLE:
                return builder.setFLOAT64(EMPTY_MESSAGE)
                        .build();
            case BIGINT:
                return builder.setINT64(EMPTY_MESSAGE)
                        .build();
            case DECIMAL:
                Decimal decimal = Decimal.newBuilder()
                        .setPrecision(relDataType.getPrecision())
                        .setScale(relDataType.getScale())
                        .build();
                return builder.setDECIMAL(decimal)
                        .build();
            default:
                String errMsg = String.format("RelDataType:%s can not be mapping", relDataType);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }
}
