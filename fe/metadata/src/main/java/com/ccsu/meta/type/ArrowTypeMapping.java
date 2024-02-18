package com.ccsu.meta.type;

import com.ccsu.pojo.DatasourceType;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.meta.data.ColumnInfo;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowTypeMapping {

    private final ArrowTypeFactory typeFactory;

    private static final Map<ArrowTypeEnum, SqlTypeName> MAPPING = new HashMap<>();

    static {
        MAPPING.put(ArrowTypeEnum.INT8, SqlTypeName.SMALLINT);
        MAPPING.put(ArrowTypeEnum.INT16, SqlTypeName.INTEGER);
        MAPPING.put(ArrowTypeEnum.INT32, SqlTypeName.INTEGER);
        MAPPING.put(ArrowTypeEnum.INT64, SqlTypeName.BIGINT);
        MAPPING.put(ArrowTypeEnum.UINT8, SqlTypeName.SMALLINT);
        MAPPING.put(ArrowTypeEnum.UINT16, SqlTypeName.INTEGER);
        MAPPING.put(ArrowTypeEnum.UINT32, SqlTypeName.BIGINT);
        MAPPING.put(ArrowTypeEnum.UINT64, SqlTypeName.BIGINT);
        MAPPING.put(ArrowTypeEnum.DATE64, SqlTypeName.DATE);
        MAPPING.put(ArrowTypeEnum.DATE32, SqlTypeName.DATE);
        MAPPING.put(ArrowTypeEnum.DECIMAL, SqlTypeName.DECIMAL);
        MAPPING.put(ArrowTypeEnum.UTF8, SqlTypeName.VARCHAR);
    }

    public ArrowTypeMapping(ArrowTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    public RelDataType convertToArrowDataType(DatasourceType datasourceType, List<ColumnInfo> columnInfos) {
        if (columnInfos == null) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, "Can't collect table column");
        }
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnInfo columnInfo : columnInfos) {
            String columnName = columnInfo.getColumnName();
            SqlTypeName calciteTypeName = MAPPING.get(columnInfo.getColumnType());

            if (calciteTypeName == null) {
                throw new CommonException(CommonErrorCode.METADATA_ERROR,
                        "ComposeDataType is null, can not gen Arrow type:" + columnInfo.getColumnType());
            }

            RelDataType relDataType = createRelDataType(calciteTypeName, columnInfo.getColumnType(), columnInfo);

            builder.add(columnName, relDataType).nullable(columnInfo.isNullable());
        }
        return builder.build();
    }

    private RelDataType createRelDataType(SqlTypeName calciteTypeName, ArrowTypeEnum arrowTypeName, ColumnInfo columnInfo) {
        if (columnInfo.getPrecision() > 0 && columnInfo.getScale() > 0) {
            return typeFactory.createArrowType(arrowTypeName, calciteTypeName, columnInfo.getPrecision(), columnInfo.getScale());
        }
        if (columnInfo.getPrecision() > 0) {
            return typeFactory.createArrowType(arrowTypeName, calciteTypeName, columnInfo.getScale());
        }
        return typeFactory.createArrowType(arrowTypeName, calciteTypeName);
    }
}
