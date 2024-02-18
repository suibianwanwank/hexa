/**
 * Aloudata.com Inc.
 * Copyright (c) 2021-2022 All Rights Reserved.
 */

package com.ccsu.datasource.api.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * TableInfo
 *
 * @version : TableInfo.java, v 0.1 2023-08-12 15:36
 */
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableInfo extends BaseInfo {
    private static final long serialVersionUID = 5655588138453609940L;

    private List<FieldInfo> fields;

    private List<FieldInfo> partitionFields;

    private Map<String, String> parameters;

    private String tableCat;

    private String tableSchema;

    private String tableName;

    private String tableType;

    private String remarks;

    private String typeCat;

    private String typeSchema;

    private String typeName;

    private String selfReferencingColName;

    private String refGeneration;

    private Long tableRows = 0L;

    private List<PartitionInfo> partitionInfos;

    private Long size = 0L;
}

