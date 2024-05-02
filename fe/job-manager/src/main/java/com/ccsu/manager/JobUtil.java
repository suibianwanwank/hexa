package com.ccsu.manager;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.system.BackEndConfig;
import com.google.common.util.concurrent.SettableFuture;
import handler.Void;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class JobUtil {
    private JobUtil() {
    }

    public static String generateSqlJobId() {
        return UUID.randomUUID().toString();
    }

    public static void syncCompletableFuture(CompletableFuture future) {
        try {
            future.get();
        } catch (Exception e) {
            throw new CommonException(CommonErrorCode.SUBMIT_JOB_ERROR, e.getMessage());
        }
    }

    public static void syncSettableFuture(SettableFuture future, BackEndConfig backEndConfig) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new CommonException(CommonErrorCode.JOB_FUTURE_GET,
                    String.format("Send execute query grpc error, grpc address: %s, detail :%s", backEndConfig, e.getMessage()));
        }
    }

    public static void finishVoidFuture(SettableFuture<Void> future) {
        future.set(Void.DEFAULT);
    }
}
