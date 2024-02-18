package com.ccsu.grpc.observer.grpc;

import arrow.datafusion.protobuf.RowDisplayResult;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import observer.SqlJobObserver;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class SqlQueryGrpcObserver implements StreamObserver<RowDisplayResult> {

    private final List<SqlJobObserver> observers;

    public SqlQueryGrpcObserver(List<SqlJobObserver> observers) {
        this.observers = observers;
    }

    @Override
    public void onNext(RowDisplayResult rowDisplayResult) {
        String row = rowDisplayResult.getData().toString(StandardCharsets.UTF_8);
        for (SqlJobObserver observer : observers) {
            observer.onDataArrived(row);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        for (SqlJobObserver observer : observers) {
            observer.onError(throwable);
        }
    }

    @Override
    public void onCompleted() {
        for (SqlJobObserver observer : observers) {
            observer.onCompleted();
        }
    }
}
