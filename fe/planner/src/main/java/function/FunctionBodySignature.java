package function;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import function.signature.FunctionTypeSignature;
import function.signature.ReferenceFunctionTypeSignature;
import function.signature.SimpleFunctionTypeSignature;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FunctionBodySignature {
    private String functionName;
    private final List<FunctionTypeSignature> parameTypeList;
    private FunctionTypeSignature returnType;

    public FunctionBodySignature(String functionName,
                                 List<FunctionTypeSignature> parameTypeList) {
        this.functionName = functionName;
        this.parameTypeList = parameTypeList;
    }

    public FunctionBodySignature(String functionName,
                                 List<FunctionTypeSignature> parameTypeList,
                                 FunctionTypeSignature returnType) {
        this.functionName = functionName;
        this.parameTypeList = parameTypeList;
        this.returnType = returnType;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<FunctionTypeSignature> getParameTypeList() {
        return parameTypeList;
    }

    public FunctionTypeSignature getReturnType() {
        return returnType;
    }

    public SqlTypeName matchAndInferFunctionReturnType(FunctionBodySignature functionSignature) {
        int matchedIndex = 0;
        List<Integer> paramMap = new ArrayList<>();
        List<FunctionTypeSignature> matchedFunctionParamList = functionSignature.getParameTypeList();
        for (FunctionTypeSignature paramTypeSignature : parameTypeList) {
            int placeholderCount =
                    !paramTypeSignature.isVarArgs() ? 1 : getVarArgsNumber(matchedFunctionParamList, matchedIndex);
            for (FunctionTypeSignature functionTypeSignature
                    : matchedFunctionParamList.subList(matchedIndex, placeholderCount)) {
                if (!paramTypeSignature.isSameFunctionType(functionTypeSignature, functionSignature)) {
                    return null;
                }
            }
            paramMap.add(matchedIndex);
            matchedIndex = matchedIndex + placeholderCount;
        }
        return inferFunctionReturnType(paramMap, matchedFunctionParamList);
    }

    private SqlTypeName inferFunctionReturnType(List<Integer> paramMap,
                                                List<FunctionTypeSignature> matchedFunctionParamList) {
        if (returnType instanceof SimpleFunctionTypeSignature) {
            return returnType.getFunctionType();
        }
        if (returnType instanceof ReferenceFunctionTypeSignature) {
            ReferenceFunctionTypeSignature referenceFunctionTypeSignature = (ReferenceFunctionTypeSignature) returnType;
            Integer referNum = paramMap.get(referenceFunctionTypeSignature.getReferenceParamIndex());
            FunctionTypeSignature inferredReturnType = matchedFunctionParamList.get(referNum);
            return referenceFunctionTypeSignature.isUseDerivedType()
                    ? inferredReturnType.getDerivedFunctionType() : inferredReturnType.getFunctionType();
        }
        throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                String.format("not support return type, returnType : %s", returnType));
    }

    private int getVarArgsNumber(List<FunctionTypeSignature> functionTypeSignatureList, int beginIndex) {
        SimpleFunctionTypeSignature baseFunctionTypeSignature =
                (SimpleFunctionTypeSignature) functionTypeSignatureList.get(beginIndex);
        int num = 1;
        for (int i = beginIndex + 1; i < functionTypeSignatureList.size(); i++) {
            SimpleFunctionTypeSignature simpleFunctionTypeSignature =
                    (SimpleFunctionTypeSignature) functionTypeSignatureList.get(i);
            if (simpleFunctionTypeSignature.getFunctionType() != baseFunctionTypeSignature.getFunctionType()) {
                break;
            }
            num++;
        }
        return num;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionBodySignature that = (FunctionBodySignature) o;
        return Objects.equals(functionName, that.functionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName);
    }
}
