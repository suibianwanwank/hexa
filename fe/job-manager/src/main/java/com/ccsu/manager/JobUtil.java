package com.ccsu.manager;

import java.util.UUID;

public class JobUtil {
    private JobUtil() {
    }

    public static String generateSqlJobId() {
        return UUID.randomUUID().toString();
    }
}
