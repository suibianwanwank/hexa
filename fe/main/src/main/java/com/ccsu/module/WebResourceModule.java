package com.ccsu.module;

import com.ccsu.resource.metadata.DataSourceResource;
import com.ccsu.resource.planner.SqlPlannerResource;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.json.JsonCodecFactory;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class WebResourceModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(JsonCodecFactory.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(DataSourceResource.class);
        jaxrsBinder(binder).bind(SqlPlannerResource.class);
    }
}
