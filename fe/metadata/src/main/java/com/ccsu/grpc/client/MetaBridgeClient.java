package com.ccsu.grpc.client;

import arrow.datafusion.protobuf.*;
import com.ccsu.MetadataStoreHolder;
import com.ccsu.client.GrpcProvider;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.grpc.stream.ListTableStreamObserver;
import com.ccsu.meta.data.ColumnInfo;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.system.BackEndConfig;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.stream.Collectors;

import static com.ccsu.pojo.DatasourceType.transformToProtoSourceType;

public class MetaBridgeClient {

    private final GrpcProvider grpcProvider;

    private final MetadataStoreHolder metadataStoreHolder;

    private final BackEndConfig backEndConfig;

    @Inject
    public MetaBridgeClient(GrpcProvider grpcProvider, MetadataStoreHolder metadataStoreHolder, BackEndConfig backEndConfig) {
        this.grpcProvider = grpcProvider;
        this.metadataStoreHolder = metadataStoreHolder;
        this.backEndConfig = backEndConfig;
    }


    public void syncCollectAllItemInCatalog(DatasourceConfig config) {
        ManagedChannel channel = grpcProvider.getOrCreateChannel(backEndConfig);
        BridgeGrpc.BridgeStub stub = BridgeGrpc.newStub(channel);

        SettableFuture<Boolean> future = SettableFuture.create();

        SourceScanConfig.Builder scanConfig = SourceScanConfig.newBuilder()
                .setName(config.getConfigUniqueKey())
                .setHost(config.getHost())
                .setPort(Integer.parseInt(config.getPort()))
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSourceType(transformToProtoSourceType(config.getSourceType()));

        if (config.getDatabase() != null) {
            scanConfig.setDatabase(config.getDatabase());
        }

        ListTablesRequest request = ListTablesRequest.newBuilder().setConfig(scanConfig).build();

        stub.listTablesInCatalog(request, new ListTableStreamObserver(config, metadataStoreHolder, future));

        try {
            future.get();
        } catch (Exception e) {
            throw new CommonException(CommonErrorCode.GRPC_ERROR, e.getMessage());
        }
    }

    public void syncCollectTableDetail(DatasourceConfig config, String schemaName, String tableName) {
        ManagedChannel channel = grpcProvider.getOrCreateChannel(backEndConfig);


        BridgeGrpc.BridgeBlockingStub blockingStub = BridgeGrpc.newBlockingStub(channel);


        SourceScanConfig.Builder scanConfig = SourceScanConfig.newBuilder()
                .setName(config.getConfigUniqueKey())
                .setHost(config.getHost())
                .setPort(Integer.parseInt(config.getPort()))
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSourceType(transformToProtoSourceType(config.getSourceType()));

        if (config.getDatabase() != null) {
            scanConfig.setDatabase(config.getDatabase());
        }


        GetTableRequest request = GetTableRequest.newBuilder().setConfig(scanConfig)
                .setSchemaName(schemaName).setTableName(tableName).build();

        TableInfo tableInfo = blockingStub.getTableDetail(request);

        MetaPath metaPath = MetaPath.buildTablePath(config.getConfigUniqueKey(), schemaName, tableName);


        List<ColumnInfo> infoList = tableInfo.getFieldsList().stream()
                .map(this::transformFieldInfoToColumnInfo)
                .collect(Collectors.toList());

        com.ccsu.meta.data.TableInfo info = new com.ccsu.meta.data.TableInfo(config.getConfigUniqueKey(), schemaName, tableName
                , infoList, tableInfo.getRowCount());

        metadataStoreHolder.addOrUpdateTable(metaPath, info);
    }

    private ColumnInfo transformFieldInfoToColumnInfo(FieldInfo fieldInfo) {
        ArrowType.ArrowTypeEnumCase typeEnumCase = fieldInfo.getTypeName().getArrowTypeEnumCase();
        switch (typeEnumCase) {
            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT16:
            case FLOAT32:
            case FLOAT64:
            case UTF8:
            case DATE64:
            case DATE32:
                return new ColumnInfo(fieldInfo.getFieldName(),
                        ArrowTypeEnum.valueOf(typeEnumCase.name()),
                        fieldInfo.getNullable());
            case DECIMAL:
                Decimal decimal = fieldInfo.getTypeName().getDECIMAL();
                return new ColumnInfo(fieldInfo.getFieldName(),
                        ArrowTypeEnum.valueOf(typeEnumCase.name()),
                        fieldInfo.getNullable(), decimal.getPrecision(), decimal.getScale());
            default:
                throw new CommonException(CommonErrorCode.NOT_SUPPORT_ARROW_TYPE_ERROR, "Not support type: " + typeEnumCase.name());
        }
    }
}
