package com.ccsu.datasource.api.pojo;

import lombok.Getter;

@Getter
public class MetadataEntity {
    private String catalogName;
    private String schemaName;
    private String databaseName;
    private String partitionName;
    private String tableName;
    private ScopeType scopeType;

    public static MetadataEntity buildCatalog(String catalogName,
                                              String databaseName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.CATALOG;
        metadataEntity.catalogName = catalogName;
        metadataEntity.databaseName = databaseName;
        return metadataEntity;
    }

    public static MetadataEntity buildSchema(String catalogName,
                                             String databaseName,
                                             String schemaName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.SCHEMA;
        metadataEntity.catalogName = catalogName;
        metadataEntity.databaseName = databaseName;
        metadataEntity.schemaName = schemaName;
        return metadataEntity;
    }

    public static MetadataEntity buildTable(String catalogName,
                                            String databaseName,
                                            String schemaName,
                                            String tableName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.TABLE;
        metadataEntity.catalogName = catalogName;
        metadataEntity.databaseName = databaseName;
        metadataEntity.schemaName = schemaName;
        metadataEntity.tableName = tableName;
        return metadataEntity;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(scopeType);
        if (catalogName != null) {
            builder.append("catalogName:").append(catalogName).append(",");
        }
        if (databaseName != null) {
            builder.append("catalogName:").append(databaseName).append(",");
        }
        if (schemaName != null) {
            builder.append("catalogName:").append(schemaName).append(",");
        }
        if (tableName != null) {
            builder.append("catalogName:").append(tableName).append(",");
        }
        return builder.toString();
    }
}
