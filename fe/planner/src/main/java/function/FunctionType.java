package function;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import org.apache.calcite.sql.type.SqlTypeName;

@Deprecated
public enum FunctionType {
    ARRAY,
    SMALLINT,
    BIGINT,
    INTEGER,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATETIME,
    TIMESTAMP,
    ANY;

    public static SqlTypeName parse(FunctionType functionType) {
        switch (functionType) {
            case ANY:
                return SqlTypeName.ANY;
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP;
            case FLOAT:
                return SqlTypeName.FLOAT;
            case BIGINT:
                return SqlTypeName.BIGINT;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case SMALLINT:
                return SqlTypeName.SMALLINT;
            case ARRAY:
                return SqlTypeName.ARRAY;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case INTEGER:
                return SqlTypeName.INTEGER;
            case DATETIME:
                return SqlTypeName.DATE;
            default:
                throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                        String.format("not support function type : %s ", functionType));
        }
    }

    public static FunctionType unParse(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case ANY:
                return FunctionType.ANY;
            case TIMESTAMP:
                return FunctionType.TIMESTAMP;
            case FLOAT:
                return FunctionType.FLOAT;
            case BIGINT:
                return FunctionType.BIGINT;
            case DECIMAL:
                return FunctionType.DECIMAL;
            case SMALLINT:
                return FunctionType.SMALLINT;
            case ARRAY:
                return FunctionType.ARRAY;
            case DOUBLE:
                return FunctionType.DOUBLE;
            case INTEGER:
                return FunctionType.INTEGER;
            case DATE:
                return FunctionType.DATETIME;
            default:
                throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                        String.format("not support function type : %s ", sqlTypeName));
        }
    }
}
