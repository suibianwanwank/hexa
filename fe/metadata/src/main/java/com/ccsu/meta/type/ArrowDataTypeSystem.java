package com.ccsu.meta.type;

import com.ccsu.meta.type.arrow.ArrowType;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class ArrowDataTypeSystem extends RelDataTypeSystemImpl {

    public static final int MAX_NUMERIC_PRECISION = 38;

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (typeFactory instanceof ArrowTypeFactory) {
            SqlTypeName sqlTypeName = argumentType.getSqlTypeName();
            ArrowTypeFactory arrowTypeFactory = (ArrowTypeFactory) typeFactory;
            switch (sqlTypeName) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE: {
                    RelDataType deriveType;
                    if (argumentType instanceof ArrowDataType) {
                        ArrowDataType arrowDataType = (ArrowDataType) argumentType;
                        deriveType = arrowTypeFactory.createArrowType(arrowDataType.getArrowType(), argumentType.getSqlTypeName());
                    } else {
                        deriveType = arrowTypeFactory.createSqlType(argumentType.getSqlTypeName());
                    }

                    return typeFactory.createTypeWithNullability(
                            deriveType, argumentType.isNullable());
                }
//                    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), argumentType.isNullable());
                case DECIMAL:
                    return typeFactory.createTypeWithNullability(((ArrowTypeFactory) typeFactory)
                                    .createArrowType(ArrowTypeEnum.DECIMAL,
                                            SqlTypeName.DECIMAL, MAX_NUMERIC_PRECISION, argumentType.getScale()),
                            argumentType.isNullable());
            }
            return argumentType;
        }
        return super.deriveSumType(typeFactory, argumentType);
    }
}
