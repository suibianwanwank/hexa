package com.ccsu;

import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.StoreManager;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.google.inject.Inject;

import java.util.List;

import static com.ccsu.MetadataStoreConfig.CATALOG_CONFIG_CONFIG;
import static com.ccsu.meta.utils.MetadataUtil.generateSystemPath;

public class MetadataServiceImpl implements MetadataService {

    private final MetadataStoreHolder metadataStoreHolder;

    private final DataStore<String, DatasourceConfig> catalogConfigMap;

    @Inject
    public MetadataServiceImpl(MetadataStoreHolder metadataStoreHolder,
                                                StoreManager storeManager) {
        this.metadataStoreHolder = metadataStoreHolder;
        this.catalogConfigMap = storeManager.getOrCreateDataStore(CATALOG_CONFIG_CONFIG);
    }

    @Override
    public CatalogInfo getCatalog(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigMap.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        return metadataStoreHolder.getCatalog(generateSystemPath(identifier.getPath(), datasourceConfig));
    }

    @Override
    public SchemaInfo getSchema(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigMap.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        return metadataStoreHolder.getSchema(generateSystemPath(identifier.getPath(), datasourceConfig));
    }

    @Override
    public TableInfo getTable(MetaIdentifier identifier) {
        EntityWithTag<String, DatasourceConfig> entity = catalogConfigMap.get(generateCatalogKey(identifier));
        if (entity == null) {
            return null;
        }
        DatasourceConfig datasourceConfig = entity.getValue();
        TableInfo table = metadataStoreHolder.getTable(generateSystemPath(identifier.getPath(), datasourceConfig));
        return TableInfo.removeSystemCatalogName(identifier.getPath().getCatalogName(), table);
    }

    @Override
    public List<SchemaInfo> getAllSchemas(MetaIdentifier identifier) {
        // TODO need index store
        return null;
    }

    @Override
    public List<TableInfo> getAllTable(MetaIdentifier identifier) {
        return null;
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
        catalogConfigMap.put(catalogKey, config);
        MetaPath systemPath = generateSystemPath(identifier.getPath(), config);
        metadataStoreHolder.addOrUpdateCatalog(systemPath, new CatalogInfo(catalogKey));
    }

    private String generateCatalogKey(MetaIdentifier identifier) {
        return String.format("%s#%s", identifier.getPath().getPath().get(0), identifier.getClusterId());
    }
}
