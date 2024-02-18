package com.ccsu.datasource;

import com.ccsu.MetadataStoreHolder;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.DatasourceType;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.inject.Inject;

public class MetadataRegister {

    private final MetadataStoreHolder metaDataStoreHolder;


    @Inject
    public MetadataRegister(MetadataStoreHolder metadataStoreHolder) {
        this.metaDataStoreHolder = metadataStoreHolder;
    }

    public void addMetaDataCollector(DatasourceConfig datasourceConfig) {
        DatasourceType sourceType = datasourceConfig.getSourceType();
        try {
            switch (sourceType) {
                case MYSQL: {
                    break;
                }
                default: {
                    throw new CommonException(CommonErrorCode.METADATA_ERROR,
                            String.format("暂时不支持的数据类型: %s", sourceType));
                }
            }
        } catch (Exception e) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, "连接数据源异常：" + e.getMessage());
        }
    }


}
