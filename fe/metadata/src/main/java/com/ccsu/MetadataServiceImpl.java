package com.ccsu;

import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.schedule.MetadataScheduleService;
import com.ccsu.store.api.*;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

import static com.ccsu.MetadataStoreConfig.*;
import static com.ccsu.meta.utils.MetadataUtil.generateSystemPath;

public class MetadataServiceImpl implements MetadataService {

    private final MetadataStoreHolder metadataStoreHolder;

    private final MetadataScheduleService metadataScheduleService;

    private final DataIndexStore<String, DatasourceConfig> catalogConfigStore;

    @Inject
    public MetadataServiceImpl(MetadataStoreHolder metadataStoreHolder,
                               MetadataScheduleService metadataScheduleService,
                               StoreManager storeManager) {
        this.metadataStoreHolder = metadataStoreHolder;
        this.metadataScheduleService = metadataScheduleService;
        this.catalogConfigStore = storeManager.getOrCreateDataIndexStore(CATALOG_CONFIG_CONFIG);
    }

    @Override
    public CatalogInfo getCatalog(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        return metadataStoreHolder.getCatalog(generateSystemPath(identifier.getPath(), datasourceConfig));
    }

    @Override
    public DatasourceConfig getSourceConfigByCatalogName(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        return entity.getValue();
    }

    @Override
    public List<CatalogInfo> getAllCatalog(String clusterId) {
        SearchTypes.SearchQuery term = FindByCondition.term(CLUSTER_KEY.getIndexFieldName(), clusterId);
        Iterable<? extends EntityWithTag<String, DatasourceConfig>> entityWithTags = catalogConfigStore.find(ImmutableFindByCondition.builder().condition(term).build());

        List<CatalogInfo> catalogInfos = new ArrayList<>();
        for (EntityWithTag<String, DatasourceConfig> entityWithTag : entityWithTags) {
            String catalogName = entityWithTag.getKey().split("#")[1];
            catalogInfos.add(new CatalogInfo(catalogName, entityWithTag.getValue().getSourceType()));
        }

        return catalogInfos;
    }

    @Override
    public SchemaInfo getSchema(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        return metadataStoreHolder.getSchema(generateSystemPath(identifier.getPath(), datasourceConfig));
    }

    @Override
    public TableInfo getTable(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        MetaPath systemPath = generateSystemPath(identifier.getPath(), datasourceConfig);

        TableInfo table = metadataStoreHolder.getTable(systemPath);

        if (table == null) {
            return null;
        }

        if (table.getColumns() == null) {
            // TODO need cache table not exist
            metadataScheduleService.registerTableDetailCollectEvent(systemPath, datasourceConfig, true);
            table = metadataStoreHolder.getTable(systemPath);
        }

        return TableInfo.removeSystemCatalogName(identifier.getPath().getCatalogName(), table);
    }

    @Override
    public TableInfo getAndRefreshTable(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        MetaPath systemPath = generateSystemPath(identifier.getPath(), datasourceConfig);

        TableInfo table = metadataStoreHolder.getTable(systemPath);

        if (table == null) {
            return null;
        }

        // TODO need cache table not exist
        metadataScheduleService.registerTableDetailCollectEvent(systemPath, datasourceConfig, true);

        table = metadataStoreHolder.getTable(systemPath);

        return TableInfo.removeSystemCatalogName(identifier.getPath().getCatalogName(), table);
    }

    @Override
    public TableInfoWithConfigResponse getTableAndSourceConfig(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        MetaPath systemPath = generateSystemPath(identifier.getPath(), datasourceConfig);

        TableInfo table = metadataStoreHolder.getTable(systemPath);

        if (table == null) {
            return null;
        }

        if (table.getColumns() == null) {
            // TODO need cache table not exist
            metadataScheduleService.registerTableDetailCollectEvent(systemPath, datasourceConfig, true);
            table = metadataStoreHolder.getTable(systemPath);
        }

        TableInfo tableInfo = TableInfo.removeSystemCatalogName(identifier.getPath().getCatalogName(), table);
        return new TableInfoWithConfigResponse(tableInfo, datasourceConfig);
    }

    @Override
    public List<SchemaInfo> getAllSchemas(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));

        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        MetaPath systemPath = generateSystemPath(identifier.getPath(), datasourceConfig);

        return metadataStoreHolder.getSchemasByCatalogName(systemPath.getCatalogName());
    }

    @Override
    public List<TableInfo> getAllTable(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigStore.get(generateCatalogKey(identifier));

        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        MetaPath systemPath = generateSystemPath(identifier.getPath(), datasourceConfig);

        return metadataStoreHolder.getTablesByCatalogName(systemPath.getCatalogName(), identifier.getPath().getSchemaName());
    }

    @Override
    public void updateCatalog(MetaPath key, CatalogInfo catalog) {
        metadataStoreHolder.addOrUpdateCatalog(key, catalog);
    }

    @Override
    public void addOrUpdateSchema(MetaPath key, SchemaInfo schema) {
        metadataStoreHolder.addOrUpdateSchema(key, schema);
    }

    @Override
    public void addOrUpdateTable(MetaPath key, TableInfo table) {
        metadataStoreHolder.addOrUpdateTable(key, table);
    }

    @Override
    public void registerCatalog(MetaIdentifier identifier, DatasourceConfig config) {
        String catalogKey = generateCatalogKey(identifier);
        catalogConfigStore.put(catalogKey, config);
        MetaPath systemPath = generateSystemPath(identifier.getPath(), config);
        metadataStoreHolder.addOrUpdateCatalog(systemPath, new CatalogInfo(config.getConfigUniqueKey(), config.getSourceType()));

        metadataScheduleService.registerTableInCatalogCollectEvent(systemPath, config, false);
    }

    private String generateCatalogKey(MetaIdentifier identifier) {
        return String.format("%s#%s", identifier.getClusterId(), identifier.getPath().getPath().get(0));
    }

    @Getter
    @AllArgsConstructor
    public static class TableInfoWithConfigResponse {
        private TableInfo tableInfo;
        private DatasourceConfig datasourceConfig;
    }
}
