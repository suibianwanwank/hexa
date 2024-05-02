package com.ccsu.grpc.observer.grpc;

import com.ccsu.manager.JobUtil;
import com.google.common.util.concurrent.SettableFuture;
import handler.Void;
import io.grpc.stub.StreamObserver;
import observer.SqlJobObserver;

import java.util.List;

public class SqlQueryGrpcObserver implements StreamObserver<proto.execute.ExecQueryResponse> {

    private final List<SqlJobObserver> observers;

    private final SettableFuture<Void> future;

    public SqlQueryGrpcObserver(SettableFuture<Void> future, List<SqlJobObserver> observers) {
        this.future = future;
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
        future.setException(throwable);
    }

    @Override
    public void onCompleted() {
        JobUtil.finishVoidFuture(future);
    }
}
