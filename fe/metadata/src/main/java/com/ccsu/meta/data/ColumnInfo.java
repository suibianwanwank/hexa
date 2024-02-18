package com.ccsu.meta.data;

import arrow.datafusion.protobuf.ArrowType;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@NoArgsConstructor
@Builder
public class ColumnInfo {

    private String columnName;

    private ArrowTypeEnum columnType;

    private boolean nullable;

    private int precision;

    private int scale;

    public ColumnInfo(String columnName,
                      ArrowTypeEnum columnType,
                      boolean nullable) {
        this(columnName, columnType, nullable, -1);
    }

    public ColumnInfo(String columnName,
                      ArrowTypeEnum columnType,
                      boolean nullable,
                      int precision) {
        this(columnName, columnType, nullable, precision, -1);
    }

    public ColumnInfo(String columnName,
                      ArrowTypeEnum columnType,
                      boolean nullable,
                      int precision,
                      int scale) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    public String showDataType() {
        if (precision >= 0 && scale >= 0) {
            return String.format("%s(%s,%s)", columnType, precision, scale);
        }
        if (precision >= 0) {
            return String.format("%s(%s)", columnType, precision);
        }
        return columnType.name();
    }
}
