package com.ccsu.meta.service;

import com.ccsu.MetadataService;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.MetaPath.PathType;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.meta.ExtendTranslateTable;
import com.ccsu.meta.CommonTranslateTable;
import com.ccsu.meta.type.TypeConverter;

import java.util.List;

import static java.util.Objects.requireNonNull;


public class QueryMetadataManagerImpl implements QueryMetadataManager {

    private final MetadataService metadataService;

    private final TypeConverter typeConverter;

    private final String clusterId;

    public QueryMetadataManagerImpl(MetadataService metadataService, TypeConverter typeConverter, String clusterId) {
        this.metadataService = requireNonNull(metadataService, "metadataService is null");
        this.typeConverter = typeConverter;
        this.clusterId = clusterId;
    }

    @Override
    public boolean existsCatalog(String catalogName) {
        MetaPath path = MetaPath.buildCatalogPath(catalogName);
        MetaIdentifier identifier = new MetaIdentifier(clusterId, path);
        CatalogInfo catalog = metadataService.getCatalog(identifier);
        return catalog != null;
    }

    @Override
    public ExtendTranslateTable getTable(List<String> path, boolean caseSensitive) {
        MetaPath tablePath =  new MetaPath(path, PathType.TABLE);
        MetaIdentifier identifier = new MetaIdentifier(clusterId, tablePath);
        TableInfo table = metadataService.getTable(identifier);
        if (table == null) {
            return null;
        }
        return new CommonTranslateTable(table, typeConverter);
    }

    @Override
    public boolean existSchema(List<String> path, boolean caseSensitive) {
        if (path.size() == 1) {
            // catalog
            MetaPath catalogPath = new MetaPath(path, PathType.CATALOG);
            MetaIdentifier identifier = new MetaIdentifier(clusterId, catalogPath);
            CatalogInfo catalog = metadataService.getCatalog(identifier);
            return catalog != null;
        }
        MetaPath schemaPath = new MetaPath(path, PathType.SCHEMA);
        MetaIdentifier identifier = new MetaIdentifier(clusterId, schemaPath);
        SchemaInfo schema = metadataService.getSchema(identifier);
        return schema != null;
    }
}

