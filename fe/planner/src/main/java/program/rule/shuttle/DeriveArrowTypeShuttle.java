package program.rule.shuttle;

import com.ccsu.meta.type.ArrowDataType;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class DeriveArrowTypeShuttle extends RexShuttle {

    private static final Map<SqlTypeName, ArrowTypeEnum> DEFAULT_MAP = new HashMap<>();

    static {
        DEFAULT_MAP.put(SqlTypeName.DECIMAL, ArrowTypeEnum.DECIMAL);
        DEFAULT_MAP.put(SqlTypeName.CHAR, ArrowTypeEnum.UTF8);
        DEFAULT_MAP.put(SqlTypeName.VARCHAR, ArrowTypeEnum.UTF8);
    }


    public DeriveArrowTypeShuttle(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
        typeStack = new Stack<>();
    }

    private final RexBuilder rexBuilder;

    private final Stack<RelDataType> typeStack;

    @Override
    public RexNode visitCall(RexCall call) {
        boolean isInStack = false;
        if (call.isA(SqlKind.COMPARISON)) {
            RexNode input1 = call.getOperands().get(0);
            RexNode input2 = call.getOperands().get(1);
            if (!(input2.getType() instanceof ArrowDataType)
                    && input1.getType() instanceof ArrowDataType) {
                typeStack.add(input1.getType());
                isInStack = true;
            } else if (!(input1.getType() instanceof ArrowDataType)
                    && input2.getType() instanceof ArrowDataType) {
                typeStack.add(input2.getType());
                isInStack = true;
            }
        }

        RexNode rexNode = super.visitCall(call);

        if (isInStack) {
            typeStack.pop();
        }
        return rexNode;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        if (literal.getType() instanceof ArrowDataType || typeStack.isEmpty()) {
            return super.visitLiteral(literal);
        }
        return rexBuilder.makeLiteral(literal.getValue2(), typeStack.peek());
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        RelToRexShuttle rexShuttle =
                new RelToRexShuttle(new DeriveArrowTypeShuttle(rexBuilder));
        RelNode newRel = subQuery.rel.accept(rexShuttle);
        return subQuery.clone(newRel);
    }
}
