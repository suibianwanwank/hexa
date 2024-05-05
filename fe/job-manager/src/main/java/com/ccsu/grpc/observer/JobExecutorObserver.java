package com.ccsu.grpc.observer;

import com.google.common.util.concurrent.SettableFuture;
import handler.Void;
import observer.SqlJobObserver;

public class JobExecutorObserver implements SqlJobObserver {

    private final SettableFuture<Void> settableFuture;

    public JobExecutorObserver(SettableFuture<Void> settableFuture) {
        this.settableFuture = settableFuture;
    }


    @Override
    public void onCompleted(JobContext context) {
        settableFuture.set(Void.DEFAULT);
    }

    @Override
    public void onError(Throwable cause) {
        settableFuture.setException(cause);
    }

    @Override
    public void onCancel(String cancelCause) {
    }

    @Override
    public void onDataArrived(String data) {

    }
}
