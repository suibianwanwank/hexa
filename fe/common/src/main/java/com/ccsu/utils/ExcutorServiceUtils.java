package com.ccsu.utils;

import com.facebook.airlift.log.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExcutorServiceUtils {
    private static final int SERVICE_TIMEOUT = 3;

    private static final Logger LOGGER = Logger.get(ExcutorServiceUtils.class);

    private ExcutorServiceUtils() {
    }

    public static void close(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(SERVICE_TIMEOUT, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(SERVICE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOGGER.error("ExecutorService termination failure.");
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted exception occurred while awaiting termination.");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
