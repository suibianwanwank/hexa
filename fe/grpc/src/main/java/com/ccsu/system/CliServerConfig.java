package com.ccsu.system;

import com.facebook.airlift.configuration.Config;

public class CliServerConfig {
    private int port;

    @Config("cli.server.port")
    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }
}
