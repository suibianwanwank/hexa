package com.ccsu.module;

import com.ccsu.cli.CliGrpcManager;
import com.ccsu.client.GrpcProvider;
import com.ccsu.client.GrpcProviderImpl;
import com.ccsu.manager.JobManager;
import com.ccsu.manager.SqlJobManager;
import com.ccsu.system.BackEndConfig;
import com.ccsu.system.CliServerConfig;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

/**
 * server module instance binder
 */
public class ServerModule
        extends AbstractConfigurationAwareModule {

    @Override
    protected void setup(Binder binder) {
        buildConfigObject(BackEndConfig.class);
        buildConfigObject(CliServerConfig.class);
        binder.bind(CliGrpcManager.class).in(Scopes.SINGLETON);
        binder.bind(JobManager.class).to(SqlJobManager.class).in(Scopes.SINGLETON);
        binder.bind(JobManager.class).to(SqlJobManager.class).in(Scopes.SINGLETON);
        binder.bind(GrpcProvider.class).to(GrpcProviderImpl.class).in(Scopes.SINGLETON);
    }
}

