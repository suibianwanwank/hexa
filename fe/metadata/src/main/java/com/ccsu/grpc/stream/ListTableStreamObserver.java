package com.ccsu.grpc.stream;

import arrow.datafusion.protobuf.ListTablesResponse;
import arrow.datafusion.protobuf.SchemaInfo;
import arrow.datafusion.protobuf.TableInfo;
import com.ccsu.MetadataStoreHolder;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaPath;
import com.facebook.airlift.log.Logger;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;

public class ListTableStreamObserver implements StreamObserver<ListTablesResponse> {

    private static final Logger LOGGER = Logger.get(ListTableStreamObserver.class);

    private final DatasourceConfig datasourceConfig;

    private final MetadataStoreHolder metaDataStoreHolder;

    private final SettableFuture<Boolean> future;


    public ListTableStreamObserver(DatasourceConfig datasourceConfig, MetadataStoreHolder metaDataStoreHolder, SettableFuture<Boolean> future) {
        this.datasourceConfig = datasourceConfig;
        this.metaDataStoreHolder = metaDataStoreHolder;
        this.future = future;
    }

    @Override
    public void onNext(ListTablesResponse listTablesResponse) {
        if (listTablesResponse.getInfoCase() == ListTablesResponse.InfoCase.SCHEMAINFO) {
            SchemaInfo schemaInfo = listTablesResponse.getSchemaInfo();

            MetaPath metaPath = MetaPath.buildSchemaPath(datasourceConfig.getConfigUniqueKey(), schemaInfo.getSchemaName());

            com.ccsu.meta.data.SchemaInfo info = new com.ccsu.meta.data.SchemaInfo(schemaInfo.getSchemaName());

            LOGGER.info("SchemaInfo: %s", info);
            metaDataStoreHolder.addOrUpdateSchema(metaPath, info);

            return;
        }

        TableInfo tableInfo = listTablesResponse.getTableInfo();

        MetaPath metaPath = MetaPath.buildTablePath(datasourceConfig.getConfigUniqueKey(), tableInfo.getSchemaName(), tableInfo.getTableName());

        com.ccsu.meta.data.TableInfo info = new com.ccsu.meta.data.TableInfo(tableInfo.getCatalogName(), tableInfo.getSchemaName(), tableInfo.getTableName());

        LOGGER.info("TableInfo: %s", info);
        metaDataStoreHolder.addOrUpdateTableExceptColumns(metaPath, info);
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("failed to collect list tables in catalog: %s", throwable.getMessage());
        future.setException(throwable);
    }

    @Override
    public void onCompleted() {
        LOGGER.info("Finished to collect list tables in catalog");
        future.set(true);
    }
}