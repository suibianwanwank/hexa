package com.ccsu.observer;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import io.grpc.stub.StreamObserver;
import job.CliDisplayResponse;
import observer.SqlJobObserver;

public class CliJobObserver implements SqlJobObserver {

    private final StreamObserver<CliDisplayResponse> responseStream;

    public CliJobObserver(StreamObserver<CliDisplayResponse> responseStream) {
        this.responseStream = responseStream;
    }

    @Override
    public void onCompleted() {
        responseStream.onCompleted();
    }

    @Override
    public void onError(Throwable cause) {
        responseStream.onError(cause);
    }

    @Override
    public void onCancel(String cancelCause) {
        responseStream.onError(new CommonException(CommonErrorCode.GRPC_ERROR, "Job has been cancel"));
    }

    @Override
    public void onDataArrived(String data) {
        responseStream.onNext(CliDisplayResponse.newBuilder().setContent(data).build());
    }
}
