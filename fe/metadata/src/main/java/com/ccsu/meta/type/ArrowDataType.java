package com.ccsu.meta.type;

import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;

public class ArrowDataType extends BasicSqlType {

    private ArrowTypeEnum arrowType;

    private Charset charset;

    private SqlCollation collection;

    public ArrowDataType(ArrowTypeEnum arrowType, RelDataTypeSystem typeSystem, SqlTypeName typeName) {
        super(typeSystem, typeName, false);
        this.arrowType = arrowType;
    }

    protected ArrowDataType(ArrowTypeEnum arrowType, RelDataTypeSystem typeSystem, SqlTypeName typeName,
                            boolean nullable) {
        super(typeSystem, typeName, nullable);
        this.arrowType = arrowType;
    }

    /**
     * Constructs a type with precision/length but no scale.
     *
     * @param typeSystem Type system
     * @param typeName   Type name
     * @param precision  Precision (called length for some types)
     */
    public ArrowDataType(ArrowTypeEnum arrowType, RelDataTypeSystem typeSystem, SqlTypeName typeName,
                         int precision) {
        super(typeSystem, typeName, precision);
        this.arrowType = arrowType;
    }

    /**
     * Constructs a type with precision/length and scale.
     *
     * @param typeSystem Type system
     * @param typeName   Type name
     * @param precision  Precision (called length for some types)
     * @param scale      Scale
     */
    public ArrowDataType(ArrowTypeEnum arrowType, RelDataTypeSystem typeSystem, SqlTypeName typeName,
                         int precision, int scale) {
        super(typeSystem, typeName, precision, scale);
        this.arrowType = arrowType;
    }


    public ArrowDataType(ArrowTypeEnum arrowType, RelDataTypeSystem typeSystem,
                         SqlTypeName typeName, boolean isNullable, Charset charset, SqlCollation collection) {
        super(typeSystem, typeName);
        this.arrowType = arrowType;
        this.isNullable = isNullable;
        this.charset = charset;
        this.collection = collection;
    }

    public ArrowTypeEnum getArrowType() {
        return arrowType;
    }

    ArrowDataType createWithNullability(boolean nullable) {
        if (nullable == this.isNullable) {
            return this;
        }
        ArrowDataType arrowDataType;
        if (!getSqlTypeName().allowsPrec()) {
            arrowDataType = new ArrowDataType(this.arrowType, this.typeSystem, this.typeName);
        } else if (!getSqlTypeName().allowsScale()) {
            arrowDataType = new ArrowDataType(this.arrowType, this.typeSystem, this.typeName,
                    this.getPrecision());
        } else {
            arrowDataType = new ArrowDataType(this.arrowType, this.typeSystem, this.typeName,
                    this.getPrecision(), this.getScale());
        }

        arrowDataType.isNullable = nullable;
        return arrowDataType;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(arrowType);
        super.generateTypeString(sb, withDetail);
    }

    public RelDataTypeSystem getTypeSystem() {
        return typeSystem;
    }

    @Override
    public @Nullable Charset getCharset() {
        return charset;
    }

    @Override
    public @Nullable SqlCollation getCollation() {
        return collection;
    }
}
