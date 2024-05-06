package program.physical.rel;

import com.ccsu.error.CommonException;
import com.ccsu.meta.type.ArrowDataType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;
import proto.datafusion.InListNode;
import proto.datafusion.PhysicalExprNode;
import proto.datafusion.PhysicalInListNode;
import proto.datafusion.PhysicalIsNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.ccsu.error.CommonErrorCode.PLAN_TRANSFORM_ERROR;

public class PhysicalPlanTransformUtil {

    public static final proto.datafusion.EmptyMessage EMPTY_MESSAGE = proto.datafusion.EmptyMessage.getDefaultInstance();

    private PhysicalPlanTransformUtil() {
    }

    public static proto.datafusion.JoinType transformJoinType(JoinRelType type) {
        switch (type) {
            case INNER:
                return proto.datafusion.JoinType.INNER;
            case LEFT:
                return proto.datafusion.JoinType.LEFT;
            case RIGHT:
                return proto.datafusion.JoinType.RIGHT;
            case FULL:
                return proto.datafusion.JoinType.FULL;
            case ANTI:
                return proto.datafusion.JoinType.LEFTANTI;
            case SEMI:
                return proto.datafusion.JoinType.LEFTSEMI;
            default:
                String errMsg = String.format("Join type:%s not support", type);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static proto.datafusion.JoinFilter transformRexNodeToJoinFilter(RexNode rexNode, List<RelDataTypeField> fieldList, int leftSidePos) {

        proto.datafusion.Schema.Builder schema = proto.datafusion.Schema.newBuilder();

        List<proto.datafusion.ColumnIndex> columnIndexList = new ArrayList<>();

        for (RelDataTypeField relDataTypeField : fieldList) {
            proto.datafusion.ArrowType arrowType = transformRelTypeToArrowType(relDataTypeField.getType());
            proto.datafusion.Field.Builder field = proto.datafusion.Field.newBuilder().setArrowType(arrowType)
                    .setName(relDataTypeField.getName());
            schema.addColumns(field);

            if (columnIndexList.size() < leftSidePos) {
                proto.datafusion.ColumnIndex columnIndex = proto.datafusion.ColumnIndex.newBuilder()
                        .setSide(proto.datafusion.JoinSide.LEFT_SIDE)
                        .setIndex(columnIndexList.size()).build();
                columnIndexList.add(columnIndex);
                continue;
            }

            proto.datafusion.ColumnIndex columnIndex = proto.datafusion.ColumnIndex.newBuilder()
                    .setSide(proto.datafusion.JoinSide.RIGHT_SIDE)
                    .setIndex(columnIndexList.size() - leftSidePos).build();

            columnIndexList.add(columnIndex);
        }

        return proto.datafusion.JoinFilter.newBuilder()
                .setExpression(transformRexNodeToExprNode(rexNode))
                .addAllColumnIndices(columnIndexList)
                .setSchema(schema)
                .build();
    }

    public static proto.datafusion.PhysicalExprNode transformRexNodeToExprNode(RexNode rexNode) {
        proto.datafusion.PhysicalExprNode.Builder builder = proto.datafusion.PhysicalExprNode.newBuilder();
        switch (rexNode.getKind()) {
            case LITERAL: {
                proto.datafusion.ScalarValue scalarValue = transformLiteral((RexLiteral) rexNode);
                return builder.setLiteral(scalarValue)
                        .build();
            }
            case INPUT_REF: {
                proto.datafusion.PhysicalColumn column = transformColumn((RexInputRef) rexNode);
                return builder.setColumn(column)
                        .build();
            }
            case CASE: {
                proto.datafusion.PhysicalCaseNode caseNode = transformCaseNode((RexCall) rexNode);
                return builder.setCase(caseNode)
                        .build();
            }
            case LIKE: {
                proto.datafusion.PhysicalLikeExprNode likeNode = transformLikeNode((RexCall) rexNode);
                return builder.setLikeExpr(likeNode)
                        .build();

            }
            case CAST: {
                proto.datafusion.PhysicalCastNode castNode = transformCastNode((RexCall) rexNode);
                return builder.setCast(castNode)
                        .build();
            }
            case OR:
            case AND:
                return transformBinaryExpr((RexCall) rexNode);
            case IS_NOT_NULL: {
                PhysicalIsNotNull.Builder isNotNull = PhysicalIsNotNull.newBuilder()
                        .setExpr(transformRexNodeToExprNode(((RexCall) rexNode).getOperands().get(0)));
                return builder.setIsNotNullExpr(isNotNull).build();
            }
            case OTHER: {
                proto.datafusion.PhysicalScalarUdfNode function = transformFunction(rexNode);
                return builder.setScalarUdf(function)
                        .build();
            }
            default:
                if (rexNode.getKind().belongsTo(SqlKind.BINARY_COMPARISON)
                        || rexNode.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC)) {
                    return transformBinaryExpr((RexCall) rexNode);
                }
                String errMsg = String.format("RexNode:%s can not be convert", rexNode);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    /**
     * "And" => Ok(Operator::And),
     * "Or" => Ok(Operator::Or),
     * "Eq" => Ok(Operator::Eq),
     * "NotEq" => Ok(Operator::NotEq),
     * "LtEq" => Ok(Operator::LtEq),
     * "Lt" => Ok(Operator::Lt),
     * "Gt" => Ok(Operator::Gt),
     * "GtEq" => Ok(Operator::GtEq),
     * "Plus" => Ok(Operator::Plus),
     * "Minus" => Ok(Operator::Minus),
     * "Multiply" => Ok(Operator::Multiply),
     * "Divide" => Ok(Operator::Divide),
     * "Modulo" => Ok(Operator::Modulo),
     * "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
     * "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
     * "BitwiseAnd" => Ok(Operator::BitwiseAnd),
     * "BitwiseOr" => Ok(Operator::BitwiseOr),
     * "BitwiseXor" => Ok(Operator::BitwiseXor),
     * "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
     * "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
     * "RegexIMatch" => Ok(Operator::RegexIMatch),
     * "RegexMatch" => Ok(Operator::RegexMatch),
     * "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
     * "RegexNotMatch" => Ok(Operator::RegexNotMatch),
     * "StringConcat" => Ok(Operator::StringConcat),
     * "AtArrow" => Ok(Operator::AtArrow),
     * "ArrowAt" => Ok(Operator::ArrowAt),
     *
     * @param rexCall
     * @return
     */
    public static proto.datafusion.PhysicalExprNode transformBinaryExpr(RexCall rexCall) {
        proto.datafusion.PhysicalBinaryExprNode.Builder binary = proto.datafusion.PhysicalBinaryExprNode.newBuilder();

        proto.datafusion.PhysicalExprNode l = transformRexNodeToExprNode(rexCall.getOperands().get(0));
        proto.datafusion.PhysicalExprNode r = transformRexNodeToExprNode(rexCall.getOperands().get(1));

        binary.setL(l);
        binary.setR(r);

        if (!BINARY_OP_MAP.containsKey(rexCall.getKind())) {
            String errMsg = String.format("RexNode:%s can not be transformed", rexCall.getType());
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }

        binary.setOp(BINARY_OP_MAP.get(rexCall.getKind()));
        return proto.datafusion.PhysicalExprNode.newBuilder().setBinaryExpr(binary).build();
    }

    private final static Map<SqlKind, String> BINARY_OP_MAP = ImmutableMap.of(
            SqlKind.EQUALS, "Eq",
            SqlKind.GREATER_THAN, "Gt",
            SqlKind.GREATER_THAN_OR_EQUAL, "GtEq",
            SqlKind.LESS_THAN, "Lt",
            SqlKind.LESS_THAN_OR_EQUAL, "LtEq",
            SqlKind.NOT_EQUALS, "NotEq",
            SqlKind.AND, "And",
            SqlKind.OR, "Or",
            SqlKind.TIMES, "Multiply"
    );

    public static proto.datafusion.PhysicalExprNode transformAggFunction(AggregateCall call, List<RelDataTypeField> fields) {
        proto.datafusion.PhysicalAggregateExprNode.Builder builder = proto.datafusion.PhysicalAggregateExprNode.newBuilder();
        String aggName = call.getAggregation().getName();
        proto.datafusion.AggregateFunction aggregateFunction;
        try {
            aggregateFunction = proto.datafusion.AggregateFunction.valueOf(aggName);
            builder.setAggrFunction(aggregateFunction);
        } catch (Exception ignore) {
            builder.setUserDefinedAggrFunction(aggName);
        }

        if (call.getArgList().isEmpty()) {
            // like count(*), and datafusion not support expr = null
            for (int i = 0; i < fields.size(); i++) {
                builder.addExpr(transformRexNodeToExprNode(RexInputRef.of(i, fields)));
            }
        } else {
            for (Integer integer : call.getArgList()) {
                builder.addExpr(transformRexNodeToExprNode(RexInputRef.of(integer, fields)));
            }
        }
        return proto.datafusion.PhysicalExprNode.newBuilder().setAggregateExpr(builder).build();
    }

    public static proto.datafusion.ScalarValue transformLiteral(RexLiteral literal) {
        proto.datafusion.ScalarValue.Builder builder = proto.datafusion.ScalarValue.newBuilder();
        Comparable comparable = Objects.requireNonNull(literal.getValue());
        if (!(literal.getType() instanceof ArrowDataType)) {
            String value = literal.getValue2().toString();
            return builder.setUtf8Value(value)
                    .build();
        }
        ArrowDataType type = (ArrowDataType) literal.getType();
        switch (type.getArrowType()) {
            case INT8: {
                int i = Integer.parseInt((comparable).toString());
                return builder.setInt8Value(i)
                        .build();
            }
            case INT16: {
                int i = Integer.parseInt((comparable).toString());
                return builder.setInt16Value(i)
                        .build();
            }
            case INT32: {
                int i = Integer.parseInt((comparable).toString());
                return builder.setInt32Value(i)
                        .build();
            }
            case UTF8:
                return builder.setUtf8Value(literal.getValue2().toString())
                        .build();
            case BOOL:
                boolean b = Boolean.parseBoolean((comparable).toString());
                return builder.setBoolValue(b)
                        .build();
            default:
                String errMsg = String.format("SqlTypeName:%s can not be mapping", type.getSqlTypeName());
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static proto.datafusion.PhysicalColumn transformColumn(RexInputRef inputRef) {
        proto.datafusion.PhysicalColumn.Builder builder = proto.datafusion.PhysicalColumn.newBuilder();
        return builder.setIndex(inputRef.getIndex())
                .setName(inputRef.getName())
                .build();
    }

    public static proto.datafusion.PhysicalCaseNode transformCaseNode(RexCall call) {
        proto.datafusion.PhysicalCaseNode.Builder builder = proto.datafusion.PhysicalCaseNode.newBuilder();
        List<RexNode> operands = call.getOperands();
        for (int i = 0; i < operands.size() / 2; i += 2) {
            RexNode whenNode = operands.get(i);
            RexNode thenNode = operands.get(i + 1);
            proto.datafusion.PhysicalWhenThen whenThen = proto.datafusion.PhysicalWhenThen.newBuilder()
                    .setWhenExpr(transformRexNodeToExprNode(whenNode))
                    .setThenExpr(transformRexNodeToExprNode(thenNode))
                    .build();
            builder.addWhenThenExpr(whenThen);
        }
        builder.setElseExpr(transformRexNodeToExprNode(operands.get(operands.size() - 1)));
        return builder.build();
    }

    public static proto.datafusion.JoinOn transformJoinOn(RexNode rexNode, RelNode left, RelNode right) {
        if (!rexNode.isA(SqlKind.EQUALS)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }

        List<RelDataTypeField> leftFields = left.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = right.getRowType().getFieldList();

        RexCall call = (RexCall) rexNode;
        RexNode lOp = call.getOperands().get(0);
        RexNode rOp = call.getOperands().get(1);
        if (!lOp.isA(SqlKind.INPUT_REF) || !rOp.isA(SqlKind.INPUT_REF)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
        int leftIndex = ((RexInputRef) lOp).getIndex();
        int rightIndex = ((RexInputRef) rOp).getIndex() - leftFields.size();
        return proto.datafusion.JoinOn.newBuilder()
                .setLeft(proto.datafusion.PhysicalExprNode.newBuilder()
                        .setColumn(proto.datafusion.PhysicalColumn.newBuilder()
                                .setName(leftFields.get(leftIndex).getName())
                                .setIndex(leftIndex)))
                .setRight(proto.datafusion.PhysicalExprNode.newBuilder()
                        .setColumn(proto.datafusion.PhysicalColumn.newBuilder()
                                .setName(rightFields.get(rightIndex).getName())
                                .setIndex(rightIndex)))
                .build();
    }

    public static proto.datafusion.PhysicalLikeExprNode transformLikeNode(RexCall call) {
        proto.datafusion.PhysicalLikeExprNode.Builder builder = proto.datafusion.PhysicalLikeExprNode.newBuilder();
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

    public static proto.datafusion.PhysicalCastNode transformCastNode(RexCall call) {
        List<RexNode> operands = call.getOperands();
        return proto.datafusion.PhysicalCastNode.newBuilder()
                .setExpr(transformRexNodeToExprNode(operands.get(0)))
                .setArrowType(transformRelTypeToArrowType(call.getType()))
                .build();
    }

    public static proto.datafusion.PhysicalScalarUdfNode transformFunction(RexNode node) {
        if (node instanceof RexCall) {
            SqlOperator operator = ((RexCall) node).getOperator();
            List<proto.datafusion.PhysicalExprNode> args = Lists.newArrayList();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                args.add(transformRexNodeToExprNode(operand));
            }
            return proto.datafusion.PhysicalScalarUdfNode.newBuilder()
                    .setName(operator.getName())
                    .addAllArgs(args)
                    .build();
        }
        String errMsg = String.format("RexNode:%s can not be convert", node);
        throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
    }

    public static proto.datafusion.PhysicalScalarUdfNode transformAnd(RexNode node) {
        if (node instanceof RexCall) {
            SqlOperator operator = ((RexCall) node).getOperator();
            List<proto.datafusion.PhysicalExprNode> args = Lists.newArrayList();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                args.add(transformRexNodeToExprNode(operand));
            }
            return proto.datafusion.PhysicalScalarUdfNode.newBuilder()
                    .setName(operator.getName())
                    .addAllArgs(args)
                    .build();
        }
        String errMsg = String.format("RexNode:%s can not be convert", node);
        throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
    }

    public static proto.datafusion.ArrowType transformRelTypeToArrowType(RelDataType relDataType) {

        if (!(relDataType instanceof ArrowDataType)) {
            if (relDataType.getSqlTypeName() == SqlTypeName.INTEGER) {
                return proto.datafusion.ArrowType.newBuilder().setINT32(EMPTY_MESSAGE).build();
            }
            throw new CommonException(PLAN_TRANSFORM_ERROR, "Can not refer arrow type");
        }

        ArrowDataType arrowDataType = (ArrowDataType) relDataType;

        proto.datafusion.ArrowType.ArrowTypeEnumCase arrowTypeEnumCase = proto.datafusion.ArrowType.ArrowTypeEnumCase
                .valueOf(arrowDataType.getArrowType().name());

        switch (arrowTypeEnumCase) {
            case UINT8:
                return proto.datafusion.ArrowType.newBuilder().setUINT8(EMPTY_MESSAGE).build();
            case UINT16:
                return proto.datafusion.ArrowType.newBuilder().setUINT16(EMPTY_MESSAGE).build();
            case UINT32:
                return proto.datafusion.ArrowType.newBuilder().setUINT32(EMPTY_MESSAGE).build();
            case UINT64:
                return proto.datafusion.ArrowType.newBuilder().setUINT64(EMPTY_MESSAGE).build();
            case INT8:
                return proto.datafusion.ArrowType.newBuilder().setINT8(EMPTY_MESSAGE).build();
            case INT16:
                return proto.datafusion.ArrowType.newBuilder().setINT16(EMPTY_MESSAGE).build();
            case INT32:
                return proto.datafusion.ArrowType.newBuilder().setINT32(EMPTY_MESSAGE).build();
            case INT64:
                return proto.datafusion.ArrowType.newBuilder().setINT64(EMPTY_MESSAGE).build();
            case FLOAT16:
                return proto.datafusion.ArrowType.newBuilder().setFLOAT16(EMPTY_MESSAGE).build();
            case FLOAT32:
                return proto.datafusion.ArrowType.newBuilder().setFLOAT32(EMPTY_MESSAGE).build();
            case FLOAT64:
                return proto.datafusion.ArrowType.newBuilder().setFLOAT64(EMPTY_MESSAGE).build();
            case UTF8:
                return proto.datafusion.ArrowType.newBuilder().setUTF8(EMPTY_MESSAGE).build();
            case BOOL:
                return proto.datafusion.ArrowType.newBuilder().setBOOL(EMPTY_MESSAGE).build();
            case DATE64:
                return proto.datafusion.ArrowType.newBuilder().setDATE64(EMPTY_MESSAGE).build();
            case DATE32:
                return proto.datafusion.ArrowType.newBuilder().setDATE32(EMPTY_MESSAGE).build();
            case DECIMAL:
                proto.datafusion.Decimal.Builder builder = proto.datafusion.Decimal.newBuilder();
                if (arrowDataType.getPrecision() > 0) {
                    builder.setPrecision(arrowDataType.getPrecision());
                }
                if (arrowDataType.getScale() > 0) {
                    builder.setScale(arrowDataType.getScale());
                }
                return proto.datafusion.ArrowType.newBuilder().setDECIMAL(builder).build();
            default:
                String errMsg = String.format("RelDataType:%s can not be mapping", relDataType);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static proto.datafusion.Schema.Builder buildRelNodeSchema(List<RelDataTypeField> fieldList) {
        proto.datafusion.Schema.Builder schema = proto.datafusion.Schema.newBuilder();
        for (RelDataTypeField relDataTypeField : fieldList) {
            proto.datafusion.ArrowType arrowType = transformRelTypeToArrowType(relDataTypeField.getType());
            proto.datafusion.Field.Builder field = proto.datafusion.Field.newBuilder()
                    .setArrowType(arrowType)
                    .setNullable(relDataTypeField.getType().isNullable())
                    .setName(relDataTypeField.getName());
            schema.addColumns(field);
        }
        return schema;
    }
}
