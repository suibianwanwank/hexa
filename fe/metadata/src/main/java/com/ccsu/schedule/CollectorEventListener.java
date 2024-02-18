package com.ccsu.schedule;

import com.ccsu.MetadataStoreHolder;
import com.ccsu.datasource.api.ConnectionManager;
import com.ccsu.datasource.api.pojo.FieldInfo;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.event.Event;
import com.ccsu.event.EventListener;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.ColumnInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;

public class CollectorEventListener implements EventListener<CollectInfo> {

    private final MetadataStoreHolder metaDataStoreHolder;

    @Inject
    public CollectorEventListener(MetadataStoreHolder metaDataStoreHolder) {
        this.metaDataStoreHolder = metaDataStoreHolder;
    }

    @Override
    public void onEvent(Event<CollectInfo> event) throws CommonException {
        CollectInfo info = event.getData();
        MetadataEntity scope = info.getCollectScope();
        switch (info.getCollectType()) {
            case COLLECT_CATALOG: {
                List<MetadataEntity> schemaEntities = ConnectionManager.listSchemas(info.getRequest(), scope);
                for (MetadataEntity schemaEntity : schemaEntities) {
                    MetaPath path =
                            MetaPath.buildSchemaPath(schemaEntity.getCatalogName(), schemaEntity.getSchemaName());
                    metaDataStoreHolder.addOrUpdateSchema(path, new SchemaInfo(schemaEntity.getSchemaName()));
                }
            }
            case COLLECT_TABLE_NAME: {
                List<MetadataEntity> tableEntities = ConnectionManager.getTableNames(info.getRequest(), scope);
                for (MetadataEntity tableEntity : tableEntities) {
                    String tableName = tableEntity.getTableName();
                    MetaPath path =
                            MetaPath.buildTablePath(tableEntity.getCatalogName(),
                                    tableEntity.getSchemaName(), tableName);
                    metaDataStoreHolder.addOrUpdateTable(path, new TableInfo(tableName));
                }
            }
            case COLLECT_TABLE_INFO: {
                com.ccsu.datasource.api.pojo.TableInfo tableInfo =
                        ConnectionManager.extractTableInfo(info.getRequest(), scope);
                MetaPath path =
                        MetaPath.buildTablePath(scope.getCatalogName(),
                                scope.getSchemaName(), scope.getTableName());
                metaDataStoreHolder.addOrUpdateTable(path, convertTableInfoToMetaTable(tableInfo, scope));
            }
            default: {
                throw new CommonException(CommonErrorCode.META_COLLECT_ERROR,
                        String.format("Not support collect type: %s", info.getCollectType()));
            }
        }
    }

    private TableInfo convertTableInfoToMetaTable(com.ccsu.datasource.api.pojo.TableInfo tableInfo,
                                                  MetadataEntity scope) {
        List<ColumnInfo> columnList = Lists.newArrayList();
        for (FieldInfo field : tableInfo.getFields()) {
            ColumnInfo column = ColumnInfo.builder()
                    .columnName(field.getColumnName())
                    .columnType(field.getTypeName())
                    .comment(field.getComment())
                    .nullable(field.getNullable() > 1)
                    .isIndexKey(field.getIsIndexKey()).build();
            columnList.add(column);
        }

        return new TableInfo(scope.getCatalogName(),
                scope.getSchemaName(),
                scope.getTableName(),
                columnList,
                tableInfo.getTableRows());
    }
}
