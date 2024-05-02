package com.ccsu.observer;

import com.facebook.airlift.log.Logger;
import observer.SqlJobObserver;

public class LoggerRecordObserver
        implements SqlJobObserver {

    private static final Logger LOGGER = Logger.get(LoggerRecordObserver.class);

    private final String jobId;

    public static LoggerRecordObserver newLoggerObserver(String jobId) {
        return new LoggerRecordObserver(jobId);
    }

    private LoggerRecordObserver(String jobId) {
        this.jobId = jobId;
    }


    @Override
    public void onCompleted() {
        LOGGER.info(String.format("SqlJob:[%s] execution successfully!", jobId));
    }

    @Override
    public void onError(Throwable cause) {
        String errorMessage = String.format("SqlJob:[%s] execution failure. error msg:%s", jobId, cause.getMessage());
        LOGGER.error(cause, errorMessage);
    }

    @Override
    public void onCancel(String cancelCause) {

    }

    @Override
    public void onDataArrived(String data) {
        LOGGER.info(String.format("SqlJob:[%s] receive data!", jobId));
    }
}
