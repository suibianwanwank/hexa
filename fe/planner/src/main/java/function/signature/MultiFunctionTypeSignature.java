package function.signature;

import com.google.common.collect.ImmutableList;
import function.FunctionBodySignature;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

@Deprecated
public class MultiFunctionTypeSignature extends FunctionTypeSignature {
    private final List<SqlTypeName> optionFunctionTypeList;

    public static MultiFunctionTypeSignature of(List<SqlTypeName> optionFunctionTypeList) {
        return new MultiFunctionTypeSignature(optionFunctionTypeList);
    }

    public static MultiFunctionTypeSignature createAllIntegerTypeSignature() {
        return new MultiFunctionTypeSignature(
                ImmutableList.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.SMALLINT));
    }

    public static MultiFunctionTypeSignature createAllNumericTypeSignature() {
        return new MultiFunctionTypeSignature(
                ImmutableList.of(SqlTypeName.INTEGER,
                        SqlTypeName.BIGINT,
                        SqlTypeName.SMALLINT,
                        SqlTypeName.DECIMAL,
                        SqlTypeName.FLOAT,
                        SqlTypeName.DOUBLE));
    }

    public static MultiFunctionTypeSignature createAllDecimalTypeSignature() {
        return new MultiFunctionTypeSignature(
                ImmutableList.of(SqlTypeName.DECIMAL,
                        SqlTypeName.FLOAT,
                        SqlTypeName.DOUBLE));
    }

    private MultiFunctionTypeSignature(List<SqlTypeName> optionFunctionTypeList) {
        this.optionFunctionTypeList = optionFunctionTypeList;
    }

    @Override
    public boolean isSameFunctionType(FunctionTypeSignature functionTypeSignature,
                                      FunctionBodySignature functionBodySignature) {
        SqlTypeName functionSqlType = functionTypeSignature.getFunctionType();
        return optionFunctionTypeList.contains(functionSqlType);
    }

    @Override
    public SqlTypeName getFunctionType() {
        return null;
    }


}
