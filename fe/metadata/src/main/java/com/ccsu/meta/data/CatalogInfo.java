package com.ccsu.meta.data;

import com.ccsu.pojo.DatasourceType;
import lombok.Data;
import lombok.Getter;


@Data
public class CatalogInfo {

    private String catalogName;

    private DatasourceType datasourceType;

    private Long createTime;

    private Long updateTime;

    public CatalogInfo() {
    }

    public CatalogInfo(String catalogName, DatasourceType datasourceType) {
        this.catalogName = catalogName;
        this.datasourceType = datasourceType;
    }
}
