/**
 * Aloudata.com Inc.
 * Copyright (c) 2021-2022 All Rights Reserved.
 */

package com.ccsu.datasource.api.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * PartitionInfo
 *
 * @version : PartitionInfo.java, v 0.1 2023-08-12 15:23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartitionInfo extends BaseInfo {
    private static final long serialVersionUID = -515501846587835142L;
    private long lastModifiedTime;
    private long rowCount;
    private long size;
    private Map<String, String> externMessage;
}
