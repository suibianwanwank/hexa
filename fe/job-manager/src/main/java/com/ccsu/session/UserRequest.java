package com.ccsu.session;

import lombok.Data;

@Data
public class UserRequest {
    private String clusterId;
    private String sessionId;
}
