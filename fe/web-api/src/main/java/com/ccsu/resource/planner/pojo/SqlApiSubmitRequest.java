package com.ccsu.resource.planner.pojo;

import lombok.Data;

@Data
public class SqlApiSubmitRequest {
    String clusterId;
    String sql;
}
