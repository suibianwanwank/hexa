package com.ccsu.observer;

import com.ccsu.Result;

public interface SqlJobObserver {

    /**
     * This is called when the sql job execution completes.
     *
     * @param result Result of sql job execution.
     */
    void onCompleted(Result result);

    /**
     * This method is called when an exception occurs in the sql job.
     *
     * @param cause The exception that occurred.
     */
    void onError(Throwable cause);

    /**
     * This is called when the sql job cancel.
     *
     * @param cancelCause cancel reason.
     */
    void onCancel(String cancelCause);
}
