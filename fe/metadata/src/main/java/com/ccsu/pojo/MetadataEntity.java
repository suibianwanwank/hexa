package com.ccsu.pojo;

import lombok.Getter;

@Getter
public class MetadataEntity {
    private String catalogName;
    private String schemaName;
    private String databaseName;
    private String tableName;
    private ScopeType scopeType;

    public static MetadataEntity buildCatalogWithDatabase(String catalogName,
                                                          String databaseName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.CATALOG;
        metadataEntity.catalogName = catalogName;
        metadataEntity.databaseName = databaseName;
        return metadataEntity;
    }

    public static MetadataEntity buildCatalogWithoutDatabase(String catalogName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.CATALOG;
        metadataEntity.catalogName = catalogName;
        return metadataEntity;
    }

    public static MetadataEntity buildSchemaWithDatabase(String catalogName,
                                                         String databaseName,
                                                         String schemaName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.SCHEMA;
        metadataEntity.catalogName = catalogName;
        metadataEntity.databaseName = databaseName;
        metadataEntity.schemaName = schemaName;
        return metadataEntity;
    }

    public static MetadataEntity buildSchemaWithoutDatabase(String catalogName, String schemaName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.SCHEMA;
        metadataEntity.catalogName = catalogName;
        metadataEntity.schemaName = schemaName;
        return metadataEntity;
    }

    public static MetadataEntity buildTableWithDatabase(String catalogName,
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

    public static MetadataEntity buildTableWithoutDatabase(String catalogName,
                                                           String schemaName,
                                                           String tableName) {
        MetadataEntity metadataEntity = new MetadataEntity();
        metadataEntity.scopeType = ScopeType.TABLE;
        metadataEntity.catalogName = catalogName;
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
