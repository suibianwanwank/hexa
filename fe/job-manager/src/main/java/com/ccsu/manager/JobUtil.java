package com.ccsu.manager;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class JobUtil {
    private JobUtil() {
    }

    public static String generateSqlJobId() {
        return UUID.randomUUID().toString();
    }

    public static void syncCompletableFuture(CompletableFuture future){
        try{
            future.get();
        } catch (Exception e){
            throw new CommonException(CommonErrorCode.SUBMIT_JOB_ERROR, e.getMessage());
        }
    }
}
