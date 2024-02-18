package com.ccsu.meta.data;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;


@Getter
@Builder
public class ColumnInfo {

    private String columnName;

    private String columnType;

    private boolean nullable;

    private boolean isIndexKey;

    private String comment;

    private boolean hidden;

    private boolean isPartition;

    private long position;

    private Map<String, String> properties;

    private int precision;

    private int scale;

    public ColumnInfo(String columnName,
                      String columnType,
                      boolean nullable,
                      boolean isIndexKey,
                      String comment,
                      boolean hidden,
                      boolean isPartition,
                      long position,
                      Map<String, String> properties,
                      int precision,
                      int scale) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.nullable = nullable;
        this.isIndexKey = isIndexKey;
        this.comment = comment;
        this.hidden = hidden;
        this.isPartition = isPartition;
        this.position = position;
        this.properties = properties;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnInfo() {
    }
}
