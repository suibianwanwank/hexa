package function.registry;

import function.FunctionBodySignature;
import function.signature.FunctionTypeSignature;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public interface FunctionRegistry {
    void registerFunction(String functionName, List<FunctionTypeSignature> paramList,
                          FunctionTypeSignature returnType);

    void registerFunction(String functionName,
                          List<FunctionTypeSignature> paramList,
                          FunctionTypeSignature returnType,
                          List<String> aliasList);

    SqlTypeName findRegisteredFunctionSignatureAndInferReturnType(FunctionBodySignature infer);
}
