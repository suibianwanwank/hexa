package com.ccsu.meta.type;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.meta.data.ColumnInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MysqlTypeConverter implements TypeConverter {
    private static final String MYSQL_JSON_URL = "";
    private static final Map<String, String> TYPE_MAPPING = new HashMap<>();
    private String jsonUrl;
    private final RelDataTypeFactory typeFactory;

//    static {
////        for (MetaDataType.TypeMap mapping :
// FormatUtil.fromJsonFile(MYSQL_JSON_URL, MetaDataType.class).getMappings()) {
////            TYPE_MAPPING.put(mapping.getSource().getName(), mapping.getOdysseyType().getName());
////        }
//    }

    public MysqlTypeConverter(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    @Override
    public RelDataType convertType(List<ColumnInfo> columns) {
        if (columns == null) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, "Can't collect table column");
        }
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnInfo column : columns) {
            String columnName = column.getColumnName();
            //TODO fixme
//            RelDataType sqlType = typeFactory.createSqlType(getSqlTypeName(TYPE_MAPPING.get(column.getColumnType())));
            RelDataType sqlType = typeFactory.createSqlType(getSqlTypeName(column.getColumnType()));
            builder.add(columnName, sqlType).nullable(column.isNullable());
        }
        return builder.build();
    }

    public SqlTypeName getSqlTypeName(String typeName) {
        SqlTypeName sqlTypeName = SqlTypeName.get(typeName.toUpperCase(Locale.ROOT));
        if (sqlTypeName == null) {
            // TODO fix err msg
            throw new CommonException(CommonErrorCode.METADATA_ERROR, "Type Mapping error" + typeName);
        }
        return sqlTypeName;
    }
}
