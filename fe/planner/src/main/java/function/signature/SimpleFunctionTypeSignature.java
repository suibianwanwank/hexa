package function.signature;

import function.FunctionBodySignature;
import org.apache.calcite.sql.type.SqlTypeName;
@Deprecated
public class SimpleFunctionTypeSignature extends FunctionTypeSignature {
    private final SqlTypeName sqlTypeName;

    public static SimpleFunctionTypeSignature of(SqlTypeName sqlTypeName) {
        return new SimpleFunctionTypeSignature(sqlTypeName);
    }

    private SimpleFunctionTypeSignature(SqlTypeName sqlTypeName) {
        this.sqlTypeName = sqlTypeName;
    }

    @Override
    public boolean isSameFunctionType(FunctionTypeSignature functionTypeSignature,
                                      FunctionBodySignature functionBodySignature) {
        return functionTypeSignature.getFunctionType() == sqlTypeName;
    }

    @Override
    public SqlTypeName getFunctionType() {
        return sqlTypeName;
    }
}
