package com.ccsu.schedule;

import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import lombok.Getter;

@Getter
public class CollectInfo {

    private MetaScheduleType collectType;

    private ConnectorRequest request;

    private MetadataEntity collectScope;
}
