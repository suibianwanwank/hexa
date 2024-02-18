package com.ccsu.meta.data;

import lombok.Getter;


@Getter
public class CatalogInfo {

    private final String catalogName;

    private Long createTime;

    private Long updateTime;

    public CatalogInfo(String catalogName) {
        this.catalogName = catalogName;
    }
}
