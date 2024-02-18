package com.ccsu;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;

import java.util.List;

public interface MetadataService {
    CatalogInfo getCatalog(MetaIdentifier identifier);

    DatasourceConfig getSourceConfigByCatalogName(MetaIdentifier identifier);

    List<CatalogInfo> getAllCatalog(String clusterId);

    SchemaInfo getSchema(MetaIdentifier identifier);

    TableInfo getTable(MetaIdentifier identifier);

    TableInfo getAndRefreshTable(MetaIdentifier identifier);

    MetadataServiceImpl.TableInfoWithConfigResponse getTableAndSourceConfig(MetaIdentifier identifier);

    List<SchemaInfo> getAllSchemas(MetaIdentifier identifier);

    List<TableInfo> getAllTable(MetaIdentifier identifier);

    void registerCatalog(MetaIdentifier identifier, DatasourceConfig config);

    void updateCatalog(MetaPath key, CatalogInfo catalog);

    void addOrUpdateSchema(MetaPath key, SchemaInfo schema);

    void addOrUpdateTable(MetaPath key, TableInfo table);
}
