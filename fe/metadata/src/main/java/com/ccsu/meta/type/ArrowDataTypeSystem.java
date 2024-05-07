package com.ccsu.meta.type;

import com.ccsu.meta.type.arrow.ArrowType;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ArrowDataTypeSystem extends RelDataTypeSystemImpl {

    public static final int MAX_NUMERIC_PRECISION = 38;
    public static final int MAX_NUMERIC_SCALE = 38;

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
                                            SqlTypeName.DECIMAL, argumentType.getPrecision() + 10, argumentType.getScale()),
                            argumentType.isNullable());
            }
            return argumentType;
        }
        return super.deriveSumType(typeFactory, argumentType);
    }

    @Override
    public @Nullable RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
        if (SqlTypeUtil.isExactNumeric(type1)
                && SqlTypeUtil.isExactNumeric(type2)) {
            if (SqlTypeUtil.isDecimal(type1)
                    || SqlTypeUtil.isDecimal(type2)) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = RelDataTypeFactoryImpl.isJavaType(type1)
                        ? typeFactory.decimalOf(type1)
                        : type1;
                type2 = RelDataTypeFactoryImpl.isJavaType(type2)
                        ? typeFactory.decimalOf(type2)
                        : type2;
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                int scale = s1 + s2;
                scale = Math.min(scale, getMaxPrecision(SqlTypeName.DECIMAL));
                int precision = p1 + p2 + 1;
                precision = Math.min(precision, getMaxScale(SqlTypeName.DECIMAL));

                RelDataType ret;
                ret = ((ArrowTypeFactory) typeFactory).createArrowType(ArrowTypeEnum.DECIMAL, SqlTypeName.DECIMAL, precision, scale);

                return ret;
            }
        }

        return null;
    }

    @Override
    public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType.getSqlTypeName() == SqlTypeName.DECIMAL) {
            return ((ArrowTypeFactory) typeFactory).createArrowType(ArrowTypeEnum.DECIMAL,
                    SqlTypeName.DECIMAL, argumentType.getPrecision() + 4, argumentType.getScale() + 4);
        }
        return super.deriveAvgAggType(typeFactory, argumentType);
    }

    @Override
    public int getMaxScale(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
            return MAX_NUMERIC_SCALE;
        }
        return super.getMaxScale(typeName);
    }

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
            return MAX_NUMERIC_PRECISION;
        }
        return super.getMaxPrecision(typeName);
    }

    @Override
    public int getMaxNumericPrecision() {
        return super.getMaxNumericPrecision();
    }
}
