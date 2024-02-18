package com.ccsu.system;

import com.facebook.airlift.configuration.Config;

/**
 * Address configuration for BE.
 *
 * <p>If you need to expand distributed execution in the future,
 * you will need to refactor this section of the design.</p>
 */
public class BackEndConfig {

    private String host;

    private int port;

    @Config("endpoint.backend.host")
    public void setHost(String host) {
        this.host = host;
    }

    @Config("endpoint.backend.port")
    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", host, port);
    }
}
