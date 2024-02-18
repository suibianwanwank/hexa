package com.ccsu.cli;

import com.ccsu.cli.grpc.CliBridgeGrpcService;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.manager.JobManager;
import com.ccsu.system.CliServerConfig;
import com.facebook.airlift.log.Logger;
import com.google.inject.Inject;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class CliGrpcManager {

    private static final Logger LOGGER = Logger.get(CliGrpcManager.class);

    private final CliServerConfig cliServerConfig;

    private final JobManager jobManager;

    @Inject
    public CliGrpcManager(JobManager jobManager,
                          CliServerConfig cliServerConfig) {
        this.jobManager = jobManager;
        this.cliServerConfig = cliServerConfig;
        startCliGrpcServer();
    }

    private void startCliGrpcServer() {
        Server server = ServerBuilder.forPort(cliServerConfig.getPort())
                .addService(new CliBridgeGrpcService(jobManager))
                .build();
        try {
            server.start();
            LOGGER.info("Start cli grpc server successfully");
        } catch (IOException e) {
            throw new CommonException(CommonErrorCode.GRPC_ERROR, "Start grpc server error" + e.getMessage());
        }
    }
}
