package com.ccsu.manager;

import com.ccsu.Result;
import com.ccsu.session.UserRequest;

import java.util.concurrent.CompletableFuture;


public interface JobManager {
    Result cancelSqlJob();
    CompletableFuture<Result> submitSqlJob(UserRequest userRequest, String jobId);
}
