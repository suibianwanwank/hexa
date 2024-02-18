package function.registry;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import function.FunctionBodySignature;
import function.signature.FunctionTypeSignature;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemFunctionRegistry implements FunctionRegistry {

    private final Map<String, String> functionAliasMap = new HashMap<>();

    private final Map<String, List<FunctionBodySignature>> functionSignatureMap = new HashMap<>();

    public SystemFunctionRegistry() {
        FunctionRegistryManager.registerSystemFunction(this);
    }

    @Override
    public void registerFunction(String functionName,
                                 List<FunctionTypeSignature> paramList,
                                 FunctionTypeSignature returnType) {
        if (!functionSignatureMap.containsKey(functionName)) {
            functionSignatureMap.put(functionName, new ArrayList<>());
        }
        functionSignatureMap.get(functionName).add(new FunctionBodySignature(functionName, paramList, returnType));
    }

    @Override
    public void registerFunction(String functionName,
                                 List<FunctionTypeSignature> paramList,
                                 FunctionTypeSignature returnType, List<String> aliasList) {
        registerFunction(functionName, paramList, returnType);
        for (String alias : aliasList) {
            functionAliasMap.put(alias, functionName);
        }
    }

    @Override
    public SqlTypeName findRegisteredFunctionSignatureAndInferReturnType(FunctionBodySignature infer) {
        List<FunctionBodySignature> functionBodySignatureList = functionSignatureMap.get(infer.getFunctionName());
        for (FunctionBodySignature functionBodySignature : functionBodySignatureList) {
            SqlTypeName functionReturnType = functionBodySignature.matchAndInferFunctionReturnType(infer);
            if (functionReturnType != null) {
                return functionReturnType;
            }
        }
        throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                String.format("function signature : %s not found, function return type infer failed", infer));
    }
}
