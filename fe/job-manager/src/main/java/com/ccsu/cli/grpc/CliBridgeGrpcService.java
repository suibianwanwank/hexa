package com.ccsu.cli.grpc;

import com.ccsu.manager.JobManager;
import com.ccsu.session.UserRequest;
import io.grpc.stub.StreamObserver;
import observer.SqlJobObserver;
import proto.cli.CliBridgeGrpc.CliBridgeImplBase;
import proto.cli.sqlJobRequest;
import proto.cli.CliDisplayResponse;
import java.util.ArrayList;
import java.util.List;

public class CliBridgeGrpcService extends CliBridgeImplBase {

    //TODO If support cluster feature，remove it
    private static final String DEFAULT_CLUSTER = "suibianwanwan33";
    private final JobManager jobManager;


    public CliBridgeGrpcService(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void submitSqlJob(sqlJobRequest request,
                             StreamObserver<CliDisplayResponse> responseObserver) {
        String sql = request.getSql();
         List<SqlJobObserver> observerList = new ArrayList<>();
        observerList.add(new GrpcStreamObserver(responseObserver));
        jobManager.submitSqlJob(new UserRequest(DEFAULT_CLUSTER), sql, observerList);
    }
}
