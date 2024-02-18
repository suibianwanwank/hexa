package com.ccsu.meta.data;

import lombok.Getter;

@Getter
public class MetaIdentifier {

    private final String clusterId;

    private final MetaPath path;

    public MetaIdentifier(String clusterId, MetaPath path) {
        this.clusterId = clusterId;
        this.path = path;
    }

}
