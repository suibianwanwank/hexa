package com.ccsu.meta.data;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@Data
@NoArgsConstructor
public class TableInfo {

    private String catalogName;

    private String schemaName;

    private String tableName;

    private List<ColumnInfo> columns;

    private Long rowCount;

    private Long createTime;

    private Long updateTime;

    public TableInfo(String catalogName, String schemaName,String tableName) {
        this.catalogName =catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public TableInfo(String catalogName,
                     String schemaName,
                     String tableName,
                     List<ColumnInfo> columns,
                     Long rowCount) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
        this.rowCount = rowCount;
    }

    public static @Nullable TableInfo removeSystemCatalogName(String catalogName,
                                                              @Nullable TableInfo tableInfo) {
        if (tableInfo == null) {
            return null;
        }
        return new TableInfo(catalogName, tableInfo.getSchemaName(),
                tableInfo.getTableName(), tableInfo.getColumns(), tableInfo.getRowCount());
    }
}

