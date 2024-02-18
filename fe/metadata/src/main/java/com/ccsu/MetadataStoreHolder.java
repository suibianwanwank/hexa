package com.ccsu;

import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.store.api.*;
import com.ccsu.store.config.CatalogStoreConfig;
import com.ccsu.store.config.SchemaStoreConfig;
import com.ccsu.store.config.TableStoreConfig;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.ccsu.store.config.TableStoreConfig.CATALOG_AND_SCHEMA_INDEX;
import static com.ccsu.store.config.TableStoreConfig.CATALOG_INDEX;

public class MetadataStoreHolder {
    private static final long MAXIMUM_CACHE_SIZE = 10_000L;

    private static final long DEFAULT_TABLE_EXPIRY_TIME = 30 * 60 * 1000;

    private final DataIndexStore<MetaPath, CatalogInfo> catalogStore;

    private final DataIndexStore<MetaPath, SchemaInfo> schemaStore;

    private final DataIndexStore<MetaPath, TableInfo> tableStore;

    private final Cache<MetaPath, TableInfo> tableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(Duration.ofMillis(DEFAULT_TABLE_EXPIRY_TIME))
                    .maximumSize(MAXIMUM_CACHE_SIZE)
                    .build();

    private final Cache<MetaPath, CatalogInfo> catalogCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(Duration.ofMillis(DEFAULT_TABLE_EXPIRY_TIME))
                    .maximumSize(MAXIMUM_CACHE_SIZE)
                    .build();

    private final Cache<MetaPath, SchemaInfo> schemaCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(Duration.ofMillis(DEFAULT_TABLE_EXPIRY_TIME))
                    .maximumSize(MAXIMUM_CACHE_SIZE)
                    .build();

    @Inject
    public MetadataStoreHolder(StoreManager storeManager) {
        this.catalogStore = storeManager.getOrCreateDataIndexStore(new CatalogStoreConfig());
        this.schemaStore = storeManager.getOrCreateDataIndexStore(new SchemaStoreConfig());
        this.tableStore = storeManager.getOrCreateDataIndexStore(new TableStoreConfig());
    }

    public void addOrUpdateCatalog(MetaPath key, CatalogInfo catalog) {
        catalogStore.put(key, catalog);
        catalogCache.invalidate(key);
    }

    public void addOrUpdateSchema(MetaPath key, SchemaInfo value) {
        schemaStore.put(key, value);
        schemaCache.invalidate(key);
    }

    public void addOrUpdateTable(MetaPath key, TableInfo table) {
        tableStore.put(key, table);
        tableCache.invalidate(key);
    }

    public void addOrUpdateTableExceptColumns(MetaPath key, TableInfo table) {
        EntityWithTag<MetaPath, TableInfo> entity = tableStore.get(key);

        if (entity != null
                && entity.getValue().getColumns() != null
                && !entity.getValue().getColumns().isEmpty()) {
            table.setColumns(entity.getValue().getColumns());
        }
        tableStore.put(key, table);
        tableCache.invalidate(key);
    }

    @Nullable
    public CatalogInfo getCatalog(MetaPath key) {
        CatalogInfo catalog = catalogCache.getIfPresent(key);
        if (catalog != null) {
            return catalog;
        }
        Entity<MetaPath, CatalogInfo> entity = catalogStore.get(key);
        if (entity == null) {
            return null;
        }
        catalogCache.put(key, entity.getValue());
        return entity.getValue();
    }

    @Nullable
    public SchemaInfo getSchema(MetaPath key) {
        SchemaInfo schema = schemaCache.getIfPresent(key);
        if (schema != null) {
            return schema;
        }
        Entity<MetaPath, SchemaInfo> entity = schemaStore.get(key);
        if (entity == null) {
            return null;
        }
        schemaCache.put(key, entity.getValue());
        return entity.getValue();
    }

    @Nullable
    public TableInfo getTable(MetaPath key) {
        TableInfo table = tableCache.getIfPresent(key);
        if (table != null) {
            return table;
        }
        Entity<MetaPath, TableInfo> entity = tableStore.get(key);
        if (entity == null) {
            return null;
        }
        tableCache.put(key, entity.getValue());
        return entity.getValue();
    }

    public List<SchemaInfo> getSchemasByCatalogName(String catalogName) {
        SearchTypes.SearchQuery term = FindByCondition.term(SchemaStoreConfig.CATALOG_INDEX.getIndexFieldName(), catalogName);
        ImmutableFindByCondition condition = ImmutableFindByCondition.builder()
                .condition(term).build();

        Iterable<? extends EntityWithTag<MetaPath, SchemaInfo>> entityWithTags = schemaStore.find(condition);
        if (entityWithTags == null) {
            return null;
        }
        List<SchemaInfo> schemaInfoList = new ArrayList<>();
        for (EntityWithTag<MetaPath, SchemaInfo> entityWithTag : entityWithTags) {
            schemaInfoList.add(entityWithTag.getValue());
        }
        return schemaInfoList;
    }

    public List<TableInfo> getTablesByCatalogName(String catalogName, String schemaName) {
        SearchTypes.SearchQuery term = FindByCondition.term(CATALOG_AND_SCHEMA_INDEX.getIndexFieldName(), String.format("%s.%s", catalogName, schemaName));
        ImmutableFindByCondition condition = ImmutableFindByCondition.builder()
                .condition(term).build();

        Iterable<? extends EntityWithTag<MetaPath, TableInfo>> entityWithTags = tableStore.find(condition);
        if (entityWithTags == null) {
            return null;
        }
        List<TableInfo> tableInfoList = new ArrayList<>();
        for (EntityWithTag<MetaPath, TableInfo> entityWithTag : entityWithTags) {
            tableInfoList.add(entityWithTag.getValue());
        }
        return tableInfoList;
    }
}
