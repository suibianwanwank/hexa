package com.ccsu.server;

import com.ccsu.module.CommonModule;
import com.ccsu.module.JacksonModule;
import com.ccsu.module.MetadataModule;
import com.ccsu.module.ServerModule;
import com.ccsu.module.WebResourceModule;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.discovery.client.DiscoveryModule;
import com.facebook.airlift.event.client.HttpEventModule;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;

public class Launcher {

    private static final Logger LOGGER = Logger.get(Launcher.class);

    public static void main(String[] args) {
        Launcher launcher = new Launcher();
        launcher.launch();
    }

    public void launch() {

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new JaxrsModule(),
                new CommonModule(),
                new ServerModule(),
                new HttpEventModule(),
                new HttpServerModule(),
                new MetadataModule(),
                new DiscoveryModule(),
                new NodeModule(),
                new WebResourceModule(),
                new JacksonModule()
        );

        Bootstrap application = new Bootstrap(modules.build());

        final Injector injector = application.initialize();

        LOGGER.info("=========== Hexa front end start successfully ===========");

    }
}
