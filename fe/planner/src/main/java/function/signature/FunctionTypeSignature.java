package function.signature;

import function.FunctionBodySignature;
import org.apache.calcite.sql.type.SqlTypeName;
@Deprecated
public abstract class FunctionTypeSignature {

    protected boolean isVarArgs = false;

    public abstract boolean isSameFunctionType(FunctionTypeSignature functionTypeSignature,
                                               FunctionBodySignature functionBodySignature);

    public abstract SqlTypeName getFunctionType();

    public boolean isVarArgs() {
        return isVarArgs;
    }

    public SqlTypeName getDerivedFunctionType() {
        return getFunctionType();
    }

    public FunctionTypeSignature supportVarArgs() {
        this.isVarArgs = true;
        return this;
    }
}
