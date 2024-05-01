package com.ccsu.meta.type;

import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.SerializableCharset;
import org.apache.calcite.util.Util;

import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;

public class ArrowTypeFactory extends SqlTypeFactoryImpl {

    public ArrowTypeFactory() {
        super(RelDataTypeSystem.DEFAULT);
    }

    public RelDataType createArrowType(ArrowTypeEnum arrowTypeEnum, SqlTypeName typeName) {
        if (typeName.allowsPrec()) {
            return createArrowType(arrowTypeEnum, typeName, typeSystem.getDefaultPrecision(typeName));
        }
        assertBasic(typeName);
        RelDataType newType = new ArrowDataType(arrowTypeEnum, typeSystem, typeName);
        return canonize(newType);
    }

    public RelDataType createArrowType(ArrowTypeEnum arrowTypeEnum, SqlTypeName typeName, int precision) {
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        if (typeName.allowsScale()) {
            return createArrowType(arrowTypeEnum, typeName, precision, typeName.getDefaultScale());
        }
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        // Does not check precision when typeName is SqlTypeName#NULL.
        RelDataType newType = precision == RelDataType.PRECISION_NOT_SPECIFIED
                ? new ArrowDataType(arrowTypeEnum, typeSystem, typeName)
                : new ArrowDataType(arrowTypeEnum, typeSystem, typeName, precision);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    public RelDataType createArrowType(ArrowTypeEnum arrowTypeEnum, SqlTypeName typeName, int precision, int scale) {
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        RelDataType newType =
                new ArrowDataType(arrowTypeEnum, typeSystem, typeName, precision, scale);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }


    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        if (typeName.allowsPrec()) {
            return createSqlType(typeName, typeSystem.getDefaultPrecision(typeName));
        }
        assertBasic(typeName);

        if (typeName == SqlTypeName.BOOLEAN) {
            RelDataType newType = new ArrowDataType(ArrowTypeEnum.BOOL, typeSystem, typeName);
            return canonize(newType);
        }
        if (typeName == SqlTypeName.VARCHAR || typeName == SqlTypeName.CHAR) {
            RelDataType newType = new ArrowDataType(ArrowTypeEnum.UTF8, typeSystem, typeName);
            return canonize(newType);
        }
        RelDataType newType = new BasicSqlType(typeSystem, typeName);
        return canonize(newType);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        if (typeName.allowsScale()) {
            return createSqlType(typeName, precision, typeName.getDefaultScale());
        }

        if (typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR) {
            RelDataType newType = new ArrowDataType(ArrowTypeEnum.UTF8, typeSystem, typeName, precision);
            newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
            return canonize(newType);
        }
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        // Does not check precision when typeName is SqlTypeName#NULL.
        RelDataType newType = precision == RelDataType.PRECISION_NOT_SPECIFIED
                ? new BasicSqlType(typeSystem, typeName)
                : new BasicSqlType(typeSystem, typeName, precision);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        RelDataType newType =
                new BasicSqlType(typeSystem, typeName, precision, scale);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override
    public RelDataType createUnknownType() {
        return createSqlType(SqlTypeName.UNKNOWN);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof ArrowDataType) {
            return ((ArrowDataType) type).createWithNullability(nullable);
        }
        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType copyType(RelDataType type) {
        return super.copyType(type);
    }

    private static void assertBasic(SqlTypeName typeName) {
        assert typeName != null;
        assert typeName != SqlTypeName.MULTISET
                : "use createMultisetType() instead";
        assert typeName != SqlTypeName.ARRAY
                : "use createArrayType() instead";
        assert typeName != SqlTypeName.MAP
                : "use createMapType() instead";
        assert typeName != SqlTypeName.ROW
                : "use createStructType() instead";
        assert !SqlTypeName.INTERVAL_TYPES.contains(typeName)
                : "use createSqlIntervalType() instead";
    }

    @Override
    public RelDataType createTypeWithCharsetAndCollation(RelDataType type, Charset charset, SqlCollation collation) {
        assert SqlTypeUtil.inCharFamily(type) : type;
        requireNonNull(charset, "charset");
        requireNonNull(collation, "collation");
        RelDataType newType;
        if (type instanceof ArrowDataType) {
            ArrowDataType arrowDataType = (ArrowDataType) type;
            newType = createWithCharsetAndCollation(arrowDataType, charset, collation);
            return canonize(newType);
        }
        return super.createTypeWithCharsetAndCollation(type, charset, collation);
    }

    ArrowDataType createWithCharsetAndCollation(ArrowDataType arrowDataType, Charset charset,
                                                SqlCollation collation) {
        Preconditions.checkArgument(SqlTypeUtil.inCharFamily(arrowDataType));
        return new ArrowDataType(arrowDataType.getArrowType(), arrowDataType.getTypeSystem(),
                arrowDataType.getSqlTypeName(), arrowDataType.isNullable(), charset, collation);
    }
}
