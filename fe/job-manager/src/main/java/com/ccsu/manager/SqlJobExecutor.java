package com.ccsu.manager;

import com.ccsu.grpc.observer.JobExecutorObserver;
import com.ccsu.grpc.observer.grpc.SqlQueryGrpcObserver;
import com.ccsu.parser.sqlnode.*;
import com.ccsu.system.BackEndConfig;
import com.ccsu.client.GrpcProvider;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import context.QueryContext;
import handler.*;
import handler.Void;
import io.grpc.ManagedChannel;
import observer.SqlJobObserver;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SqlJobExecutor {

    private static final Logger LOGGER = Logger.get(SqlJobExecutor.class);
    private final QueryContext queryContext;
    private GrpcProvider grpcProvider;
    private BackEndConfig backEndConfig;
    private final SettableFuture<Void> future;

    public SqlJobExecutor(QueryContext queryContext, GrpcProvider grpcProvider, BackEndConfig backEndConfig) {
        this.queryContext = queryContext;
        this.backEndConfig = backEndConfig;
        this.grpcProvider = grpcProvider;
        this.future = SettableFuture.create();
    }

    public Void run() {
        try {
            Void aVoid = executeSql();
            LOGGER.info(String.format("SqlJob:[%s] execution successfully!", queryContext.getSqlJobId()));
            return aVoid;
        } catch (CommonException e) {
            logThrowableErrorForSqlJob(e);

            if (e.getErrorCode() != CommonErrorCode.JOB_FUTURE_GET) {
                for (SqlJobObserver observer : queryContext.getObservers()) {
                    observer.onError(e);
                }
            }
        } catch (Throwable t) {

            logThrowableErrorForSqlJob(t);

            for (SqlJobObserver observer : queryContext.getObservers()) {
                observer.onError(t);
            }
        }

        return Void.DEFAULT;
    }

    private void logThrowableErrorForSqlJob(Throwable t) {
        String errorMessage = String.format("SqlJob:[%s] execution failure. error msg:%s", queryContext.getSqlJobId(), t.getMessage());
        LOGGER.error(t, errorMessage);
    }

    private Void executeSql() {

        SqlNode sqlNode = queryContext.getSqlParser().parse(queryContext.getSql());

        if (sqlNode.isA(SqlKind.QUERY)) {

            queryContext.getObservers().add(new JobExecutorObserver(future));

            QueryPlanResult planResult =
                    new QueryPlanHandler().handle(sqlNode, queryContext);

            LOGGER.info("SQL parsed and plan successfully!");

            ManagedChannel channel = grpcProvider.getOrCreateChannel(backEndConfig);

            proto.execute.BridgeGrpc.BridgeStub stub = proto.execute.BridgeGrpc.newStub(channel);

            LOGGER.info("Getting channel successful, ready to send grpc request to be.");
            // send and receive grpc message
            stub.executeQuery(generateExecQueryRequest(planResult), new SqlQueryGrpcObserver(queryContext.getObservers()));

            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new CommonException(CommonErrorCode.JOB_FUTURE_GET,
                        String.format("Send execute query grpc error, grpc address: %s, detail :%s", backEndConfig, e.getMessage()));
            }

            return Void.DEFAULT;
        }

        // DDL Or Other SqlNode
        SqlHandler sqlHandler = HANDLER_DISPATCHER.get(sqlNode.getClass().getName());

        // TODO refactor design
        if (sqlHandler != null) {
            String result = sqlHandler.handle(sqlNode, queryContext).toString();
            for (SqlJobObserver observer : queryContext.getObservers()) {
                observer.onDataArrived(result);
                observer.onCompleted();
            }
            return Void.DEFAULT;
        }

        throw new CommonException(CommonErrorCode.SQL_PARSER_ERROR,
                "SQL parsing and transformation failed！ "
                        + "SQL_ID="
                        + queryContext.getSqlJobId() + ". "
                        + "Unsupported sql syntax =" + queryContext.getSql());
    }


    private proto.execute.ExecQueryRequest generateExecQueryRequest(QueryPlanResult planResult) {
        return proto.execute.ExecQueryRequest.newBuilder()
                .setHeader(proto.execute.RequestHeader.newBuilder()
                        .setJobId(queryContext.getSqlJobId()))
                .setNode(planResult.getResult())
                .build();
    }

    private static final Map<String, SqlHandler> HANDLER_DISPATCHER = ImmutableMap.of(
            SqlSetConfig.class.getName(), new SetOptionHandler(),
            SqlShowCatalogs.class.getName(), new ShowCatalogHandler(),
            SqlShowSchemas.class.getName(), new ShowSchemasHandler(),
            SqlShowTables.class.getName(), new ShowTablesHandler(),
            SqlShowColumns.class.getName(), new ShowColumnsHandler(),
            SqlShowCreateCatalog.class.getName(), new ShowCreateCatalogHandler(),
            SqlCreateCatalog.class.getName(), new CreateCatalogHandler(),
            SqlRefreshTable.class.getName(), new RefreshTableHandler()
    );
}
