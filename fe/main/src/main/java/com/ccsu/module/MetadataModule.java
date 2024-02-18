package com.ccsu.module;

import com.ccsu.MetadataService;
import com.ccsu.MetadataServiceImpl;
import com.ccsu.MetadataStoreHolder;
import com.ccsu.event.EventRegistry;
import com.ccsu.event.LocalEventRegistry;
import com.ccsu.grpc.client.MetaBridgeClient;
import com.ccsu.manager.SqlJobManager;
import com.ccsu.schedule.CollectorEventListener;
import com.ccsu.schedule.MetadataScheduleService;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.checkerframework.checker.units.qual.s;

/**
 * metadata module instance binder
 */
public class MetadataModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(MetadataStoreHolder.class).in(Scopes.SINGLETON);
        binder.bind(SqlJobManager.class).in(Scopes.SINGLETON);
        binder.bind(MetadataService.class).to(MetadataServiceImpl.class).in(Scopes.SINGLETON);
        binder.bind(CollectorEventListener.class).in(Scopes.SINGLETON);
        binder.bind(MetadataScheduleService.class).in(Scopes.SINGLETON);
        binder.bind(EventRegistry.class).to(LocalEventRegistry.class).in(Scopes.SINGLETON);
        binder.bind(MetaBridgeClient.class).in(Scopes.SINGLETON);
    }
}
