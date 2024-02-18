package function;

import function.registry.FunctionRegistry;
import function.signature.FunctionTypeSignature;
import function.signature.SimpleFunctionTypeSignature;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class ExtendReturnTypeInference implements SqlReturnTypeInference {

    private final FunctionRegistry functionRegistry;

    public ExtendReturnTypeInference(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    @Override
    public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        //TODO transTo OdysseyFunction And GetType

        RelDataTypeFactory relDataTypeFactory = opBinding.getTypeFactory();
        String functionName = opBinding.getOperator().getName();
        List<FunctionTypeSignature> argumentTypes = new ArrayList<>();
        for (int i = 0; i < opBinding.getOperandCount(); i++) {
            RelDataType operandType = opBinding.getOperandType(i);
            argumentTypes.add(SimpleFunctionTypeSignature.of(operandType.getSqlTypeName()));
        }
        FunctionBodySignature signature = new FunctionBodySignature(functionName, argumentTypes);
        SqlTypeName returnType =
                functionRegistry.findRegisteredFunctionSignatureAndInferReturnType(signature);

        return relDataTypeFactory.createSqlType(returnType);
    }
}
