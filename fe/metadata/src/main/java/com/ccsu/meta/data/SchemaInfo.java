package com.ccsu.meta.data;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class SchemaInfo {

    private String schemaName;

    private Long createTime;

    private Long updateTime;

    public SchemaInfo(String schemaName) {
        this.schemaName = schemaName;
    }

    public SchemaInfo(String schemaName, Long createTime, Long updateTime) {
        this.schemaName = schemaName;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }
}
