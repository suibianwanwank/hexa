package com.ccsu.manager;

import arrow.datafusion.BackEndServiceGrpc;
import arrow.datafusion.BytesResponse;
import com.ccsu.system.EndPoint;
import com.ccsu.Result;
import com.ccsu.client.GrpcProvider;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import com.ccsu.session.UserRequest;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Stopwatch;
import context.QueryContext;
import handler.QueryPlanHandler;
import handler.QueryPlanResult;
import io.grpc.ManagedChannel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.concurrent.atomic.AtomicReference;

public class SqlJobExecutor {

    private static final Logger LOGGER = Logger.get(SqlJobExecutor.class);
    private final QueryContext queryContext;
    private final UserRequest userRequest;
    private AtomicReference<Result> result;
    private AtomicReference<Boolean> canceled;
    private GrpcProvider grpcProvider;
    private EndPoint endPoint;

    public SqlJobExecutor(QueryContext queryContext, UserRequest userRequest, EndPoint endPoint) {
        this.queryContext = queryContext;
        this.userRequest = userRequest;
        this.endPoint = endPoint;
    }

    public Result run() {
//        notifyObserversOnPhaseCompleted(STARTING);
        try {
            return executeSql();
        } catch (Throwable t) {
//            ExceptionUtils.checkInterrupted(t);


            String errorMessage = String.format("SqlJob:[%s] execution failure.", queryContext.getSqlJobId());
            LOGGER.error(t, errorMessage, t.getMessage());

//            if (!sqlJobState.get().isFinished()) {
//                notifyObserversOnError(t);
//            }
            return result.get();
        }
    }

    private Result executeSql() {
//        notifyObserversOnPhaseCompleted(PLANNING);

        Stopwatch stopwatch = queryContext.getStopwatch().reset().start();
        SqlParser sqlParser = new CalciteSqlParser();
        SqlNode sqlNode = sqlParser.parse(queryContext.getSql());

        if (sqlNode.isA(SqlKind.QUERY)) {

            QueryPlanResult queryPlanResult =
                    new QueryPlanHandler().handle(sqlNode, queryContext);


            ManagedChannel channel = grpcProvider.getOrCreateChannel(endPoint);

            BackEndServiceGrpc.BackEndServiceBlockingStub blockingStub = BackEndServiceGrpc.newBlockingStub(channel);

            BytesResponse bytesResponse = blockingStub.submitTask(queryPlanResult.getResult());

            return null;
        }


        //TODO DDL Or Other SqlNode

        throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                "SQL parsing and transformation failed！ "
                        + "SQL_ID="
                        + queryContext.getSqlJobId() + ". "
                        + "Unsupported sql syntax =" + queryContext.getSql());
    }
}
