package com.ccsu.module;

import com.ccsu.MetadataStoreHolder;
import com.ccsu.datasource.MetadataRegister;
import com.ccsu.manager.SqlJobManager;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

/**
 * metadata module instance binder
 */
public class MetadataModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(MetadataRegister.class).in(Scopes.SINGLETON);
        binder.bind(MetadataRegister.class).in(Scopes.SINGLETON);
        binder.bind(MetadataStoreHolder.class).in(Scopes.SINGLETON);
        binder.bind(SqlJobManager.class).in(Scopes.SINGLETON);
    }
}
