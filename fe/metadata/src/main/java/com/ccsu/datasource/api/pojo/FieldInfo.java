/**
 * Aloudata.com Inc.
 * Copyright (c) 2021-2022 All Rights Reserved.
 */

package com.ccsu.datasource.api.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FieldInfo extends BaseInfo {
    private static final long serialVersionUID = 9218001386926942374L;

    private String comment;

    private String name;

    private boolean partitionKey;

    private String sourceType;

    private Integer size;

    private String defaultValue;

    private Boolean isSortKey;

    private Boolean isIndexKey;

    private Boolean isUniqueKey;

    private String tableCat;

    private String tableSchema;

    private String tableName;

    private String columnName;

    private Integer dataType;

    private String typeName;

    private Integer columnSize;

    private Integer bufferLength;

    private Integer decimalDigits;

    private Integer numPrecRadix;

    private Integer nullable;

    private String remarks;

    private String columnDef;

    private Integer sqlDataType;

    private Integer sqlDatetimeSub;

    private Integer charOctetLength;

    private Integer ordinalPosition;

    private String isNullable;

    private String scopeCatalog;

    private String scopeSchema;

    private String scopeTable;

    private Short sourceDataType;

    private String isAutoincrement;

    private String isGeneratedcolumn;

}
