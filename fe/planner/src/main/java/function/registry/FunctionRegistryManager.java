package function.registry;

import com.google.common.collect.ImmutableList;
import function.signature.MultiFunctionTypeSignature;
import function.signature.SimpleFunctionTypeSignature;
import org.apache.calcite.sql.type.SqlTypeName;

public class FunctionRegistryManager {

    private FunctionRegistryManager() {
    }

    private static final String STRING_CONCAT_FUNCTION = "CONCAT";

    public static void registerSystemFunction(FunctionRegistry functionRegistry) {
        registerSystemStringFunction(functionRegistry);
        registerSystemMathFunction(functionRegistry);
        registerSystemDateFunction(functionRegistry);
        registerSystemWindowFunction(functionRegistry);
        registerSystemAggFunction(functionRegistry);

    }

    private static void registerSystemStringFunction(FunctionRegistry functionRegistry) {
        functionRegistry.registerFunction(STRING_CONCAT_FUNCTION,
                ImmutableList.of(SimpleFunctionTypeSignature.of(SqlTypeName.VARCHAR).supportVarArgs()),
                SimpleFunctionTypeSignature.of(SqlTypeName.VARCHAR)
        );
    }

    private static void registerSystemMathFunction(FunctionRegistry functionRegistry) {
        functionRegistry.registerFunction("SUM",
                ImmutableList.of(MultiFunctionTypeSignature.createAllNumericTypeSignature()),
                SimpleFunctionTypeSignature.of(SqlTypeName.VARCHAR),
                ImmutableList.of("PLUS, ADD"));
    }

    private static void registerSystemDateFunction(FunctionRegistry functionRegistry) {

    }

    private static void registerSystemWindowFunction(FunctionRegistry functionRegistry) {

    }

    private static void registerSystemAggFunction(FunctionRegistry functionRegistry) {

    }
}
