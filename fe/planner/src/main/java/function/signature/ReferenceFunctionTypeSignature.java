package function.signature;

import function.FunctionBodySignature;
import org.apache.calcite.sql.type.SqlTypeName;
@Deprecated
public class ReferenceFunctionTypeSignature extends FunctionTypeSignature {
    private int referenceParamIndex;
    boolean isUseDerivedType;

    @Override
    public boolean isSameFunctionType(FunctionTypeSignature functionTypeSignature,
                                      FunctionBodySignature functionBodySignature) {
        return false;
    }

    @Override
    public SqlTypeName getFunctionType() {
        return null;
    }

    public int getReferenceParamIndex() {
        return referenceParamIndex;
    }

    public boolean isUseDerivedType() {
        return isUseDerivedType;
    }
}
