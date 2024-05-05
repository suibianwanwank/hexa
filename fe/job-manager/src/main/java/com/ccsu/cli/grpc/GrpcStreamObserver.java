package com.ccsu.cli.grpc;

import io.grpc.stub.StreamObserver;
import proto.cli.CliDisplayResponse;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import io.grpc.Status;
import observer.SqlJobObserver;

public class GrpcStreamObserver implements SqlJobObserver {
    private final StreamObserver<CliDisplayResponse> streamObserver;


    public GrpcStreamObserver(StreamObserver<CliDisplayResponse> streamObserver) {
        this.streamObserver = streamObserver;
    }


    @Override
    public void onCompleted(JobContext context) {
        streamObserver.onNext(CliDisplayResponse.newBuilder()
                .setContent(String.format("Query Completed, elapsed time: %sms.  [JobId]: '%s'",
                        context.getJobProfile().getElapsedTime(), context.getJobId()))
                .build());
        streamObserver.onCompleted();
    }

    @Override
    public void onError(Throwable cause) {
        Status status = Status.INTERNAL
                        .withDescription(cause.getMessage())
                        .withCause(cause);
        streamObserver.onError(status.asException());
    }

    @Override
    public void onCancel(String cancelCause) {
        streamObserver.onError(
                new CommonException(CommonErrorCode.GRPC_ERROR, "Task has been cancel")
        );
    }

    @Override
    public void onDataArrived(String data) {
        streamObserver.onNext(CliDisplayResponse.newBuilder()
                .setContent(data)
                .build());
    }
}
