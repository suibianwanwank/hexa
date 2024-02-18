package com.ccsu.module;

import com.ccsu.common.pool.CommandPool;
import com.ccsu.common.pool.CommandPoolImpl;
import com.ccsu.option.manager.SystemOptionManager;
import com.ccsu.option.OptionManager;
import com.ccsu.store.api.StoreManager;
import com.ccsu.store.mongodb.MongoDBManager;
import com.ccsu.store.mongodb.MongoDBStoreManagerConfig;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

/**
 * common component instance binder
 */
public class CommonModule extends AbstractConfigurationAwareModule {

    @Override
    protected void setup(Binder binder) {
        buildConfigObject(MongoDBStoreManagerConfig.class);
        binder.bind(StoreManager.class).to(MongoDBManager.class).in(Scopes.SINGLETON);
        binder.bind(OptionManager.class).to(SystemOptionManager.class).in(Scopes.SINGLETON);
        binder.bind(CommandPool.class).to(CommandPoolImpl.class).in(Scopes.SINGLETON);
    }
}
