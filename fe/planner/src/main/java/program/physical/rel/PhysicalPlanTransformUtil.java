package program.physical.rel;

import arrow.datafusion.protobuf.*;
import com.ccsu.error.CommonException;
import com.ccsu.meta.type.ArrowDataType;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public static JoinFilter transformRexNodeToJoinFilter(RexNode rexNode, List<RelDataTypeField> fieldList, int leftSidePos) {

        Schema.Builder schema = Schema.newBuilder();

        List<ColumnIndex> columnIndexList = new ArrayList<>();

        for (RelDataTypeField relDataTypeField : fieldList) {
            ArrowType arrowType = transformRelTypeToArrowType(relDataTypeField.getType());
            Field.Builder field = Field.newBuilder().setArrowType(arrowType)
                    .setName(relDataTypeField.getName());
            schema.addColumns(field);

            if (columnIndexList.size() < leftSidePos) {
                ColumnIndex columnIndex = ColumnIndex.newBuilder()
                        .setSide(JoinSide.LEFT_SIDE)
                        .setIndex(columnIndexList.size()).build();
                columnIndexList.add(columnIndex);
                continue;
            }

            ColumnIndex columnIndex = ColumnIndex.newBuilder()
                    .setSide(JoinSide.RIGHT_SIDE)
                    .setIndex(columnIndexList.size() - leftSidePos).build();

            columnIndexList.add(columnIndex);
        }

        return JoinFilter.newBuilder()
                .setExpression(transformRexNodeToExprNode(rexNode))
                .addAllColumnIndices(columnIndexList)
                .setSchema(schema)
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
            case OR:
            case AND:
                return transformBinaryExpr((RexCall) rexNode);
            case OTHER: {
                PhysicalScalarUdfNode function = transformFunction(rexNode);
                return builder.setScalarUdf(function)
                        .build();
            }


            default:
                if (rexNode.getKind().belongsTo(SqlKind.BINARY_COMPARISON)) {
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
    public static PhysicalExprNode transformBinaryExpr(RexCall rexCall) {
        PhysicalBinaryExprNode.Builder binary = PhysicalBinaryExprNode.newBuilder();

        PhysicalExprNode l = transformRexNodeToExprNode(rexCall.getOperands().get(0));
        PhysicalExprNode r = transformRexNodeToExprNode(rexCall.getOperands().get(1));

        binary.setL(l);
        binary.setR(r);

        if (!BINARY_OP_MAP.containsKey(rexCall.getKind())) {
            String errMsg = String.format("RexNode:%s can not be transformed", rexCall.getType());
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }

        binary.setOp(BINARY_OP_MAP.get(rexCall.getKind()));
        return PhysicalExprNode.newBuilder().setBinaryExpr(binary).build();
    }

    private final static Map<SqlKind, String> BINARY_OP_MAP = ImmutableMap.of(
            SqlKind.EQUALS, "Eq",
            SqlKind.GREATER_THAN, "Gt",
            SqlKind.GREATER_THAN_OR_EQUAL, "GtEq",
            SqlKind.LESS_THAN, "Lt",
            SqlKind.LESS_THAN_OR_EQUAL, "LtEq",
            SqlKind.NOT_EQUALS, "NotEq",
            SqlKind.AND, "And",
            SqlKind.OR, "Or"
    );

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
                String s = (String) comparable;
                return builder.setUtf8Value(s)
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

    public static JoinOn transformJoinOn(RexNode rexNode, RelNode left, RelNode right) {
        if (!rexNode.isA(SqlKind.EQUALS)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }

        List<RelDataTypeField> leftFields = left.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = left.getRowType().getFieldList();

        RexCall call = (RexCall) rexNode;
        RexNode lOp = call.getOperands().get(0);
        RexNode rOp = call.getOperands().get(1);
        if (!lOp.isA(SqlKind.INPUT_REF) || !rOp.isA(SqlKind.INPUT_REF)) {
            String errMsg = String.format("RexNode:%s can't transformed Join On", rexNode);
            throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
        int leftIndex = ((RexInputRef) lOp).getIndex();
        int rightIndex = ((RexInputRef) rOp).getIndex() - leftFields.size();
        return JoinOn.newBuilder()
                .setLeft(PhysicalExprNode.newBuilder().setColumn(PhysicalColumn.newBuilder()
                        .setName(leftFields.get(leftIndex).getName())
                        .setIndex(leftIndex)))
                .setRight(PhysicalExprNode.newBuilder().setColumn(PhysicalColumn.newBuilder()
                        .setName(rightFields.get(rightIndex).getName())
                        .setIndex(rightIndex)))
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

    public static PhysicalScalarUdfNode transformAnd(RexNode node) {
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

        ArrowDataType arrowDataType = (ArrowDataType) relDataType;

        ArrowType.ArrowTypeEnumCase arrowTypeEnumCase = ArrowType.ArrowTypeEnumCase.valueOf(arrowDataType.getArrowType().name());

        switch (arrowTypeEnumCase) {
            case UINT8:
                return ArrowType.newBuilder().setUINT8(EMPTY_MESSAGE).build();
            case UINT16:
                return ArrowType.newBuilder().setUINT16(EMPTY_MESSAGE).build();
            case UINT32:
                return ArrowType.newBuilder().setUINT32(EMPTY_MESSAGE).build();
            case UINT64:
                return ArrowType.newBuilder().setUINT64(EMPTY_MESSAGE).build();
            case INT8:
                return ArrowType.newBuilder().setINT8(EMPTY_MESSAGE).build();
            case INT16:
                return ArrowType.newBuilder().setINT16(EMPTY_MESSAGE).build();
            case INT32:
                return ArrowType.newBuilder().setINT32(EMPTY_MESSAGE).build();
            case INT64:
                return ArrowType.newBuilder().setINT64(EMPTY_MESSAGE).build();
            case FLOAT16:
                return ArrowType.newBuilder().setFLOAT16(EMPTY_MESSAGE).build();
            case FLOAT32:
                return ArrowType.newBuilder().setFLOAT32(EMPTY_MESSAGE).build();
            case FLOAT64:
                return ArrowType.newBuilder().setFLOAT64(EMPTY_MESSAGE).build();
            case UTF8:
                return ArrowType.newBuilder().setUTF8(EMPTY_MESSAGE).build();
            case BOOL:
                return ArrowType.newBuilder().setBOOL(EMPTY_MESSAGE).build();
            case DATE64:
                return ArrowType.newBuilder().setDATE64(EMPTY_MESSAGE).build();
            case DATE32:
                return ArrowType.newBuilder().setDATE32(EMPTY_MESSAGE).build();
            case DECIMAL:
                Decimal.Builder builder = Decimal.newBuilder();
                if (arrowDataType.getPrecision() > 0) {
                    builder.setPrecision(arrowDataType.getPrecision());
                }
                if (arrowDataType.getScale() > 0) {
                    builder.setScale(arrowDataType.getScale());
                }
                return ArrowType.newBuilder().setDECIMAL(builder).build();
            default:
                String errMsg = String.format("RelDataType:%s can not be mapping", relDataType);
                throw new CommonException(PLAN_TRANSFORM_ERROR, errMsg);
        }
    }

    public static Schema.Builder buildRelNodeSchema(List<RelDataTypeField> fieldList) {
        Schema.Builder schema = Schema.newBuilder();
        for (RelDataTypeField relDataTypeField : fieldList) {
            ArrowType arrowType = transformRelTypeToArrowType(relDataTypeField.getType());
            Field.Builder field = Field.newBuilder().setArrowType(arrowType)
                    .setNullable(relDataTypeField.getType().isNullable())
                    .setName(relDataTypeField.getName());
            schema.addColumns(field);
        }
        return schema;
    }
}
