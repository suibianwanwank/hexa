package com.ccsu.module;

import com.ccsu.manager.JobManager;
import com.ccsu.manager.SqlJobManager;
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
        binder.bind(JobManager.class).to(SqlJobManager.class).in(Scopes.SINGLETON);
    }
}

