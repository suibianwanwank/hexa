package com.ccsu.schedule;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.pojo.MetadataEntity;
import lombok.Getter;

@Getter
public class CollectInfo {

    private final MetaScheduleType collectType;

    private final DatasourceConfig datasourceConfig;

    private final MetadataEntity collectScope;

    public CollectInfo(MetaScheduleType collectType, DatasourceConfig datasourceConfig, MetadataEntity collectScope) {
        this.collectType = collectType;
        this.datasourceConfig = datasourceConfig;
        this.collectScope = collectScope;
    }
}
