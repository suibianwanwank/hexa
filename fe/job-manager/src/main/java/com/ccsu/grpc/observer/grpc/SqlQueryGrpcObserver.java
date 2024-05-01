package com.ccsu.grpc.observer.grpc;

import io.grpc.stub.StreamObserver;
import observer.SqlJobObserver;

import java.util.List;

public class SqlQueryGrpcObserver implements StreamObserver<proto.execute.ExecQueryResponse> {

    private final List<SqlJobObserver> observers;

    public SqlQueryGrpcObserver(List<SqlJobObserver> observers) {
        this.observers = observers;
    }

    @Override
    public void onNext(proto.execute.ExecQueryResponse execQueryResponse) {
        String row = execQueryResponse.getQueryResult().toStringUtf8();
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
