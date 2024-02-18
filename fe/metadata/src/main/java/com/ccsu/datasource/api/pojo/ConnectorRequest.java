package com.ccsu.datasource.api.pojo;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;

@Getter
public class ConnectorRequest {

    private final DatasourceConfig datasourceConfig;

    private final Map<String, Object> paramaterMap = Maps.newHashMap();

    private final boolean isExistDatabase;

    public ConnectorRequest(DatasourceConfig datasourceConfig, boolean isExistDatabase) {
        this.datasourceConfig = datasourceConfig;
        this.isExistDatabase = isExistDatabase;
    }
}
