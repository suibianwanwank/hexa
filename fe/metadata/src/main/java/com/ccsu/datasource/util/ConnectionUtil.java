package com.ccsu.datasource.util;

import com.ccsu.datasource.api.pojo.DatasourceConfig;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionUtil {

    private ConnectionUtil() {

    }

    /**
     * Closes a <code>AutoCloseable</code> unconditionally.
     * <p>
     * Equivalent to {@link AutoCloseable#close()}, except any exceptions will be ignored. This is typically used in
     * finally blocks.
     * </p>
     *
     * @param closeable the objects to close, may be null or already closed
     */
    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
//            LOGGER.error("closeQuietly occur error", ioe);
        }
    }

    public static void connectClose(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
//                LOGGER.warn("close druid connect error.");
            }
        }
    }

    public static String generateConnectKey(DatasourceConfig datasourceConfig) {
        if (datasourceConfig == null) {
            throw new RuntimeException("datasourceConfig is null");
        }
        String dataSourceType = datasourceConfig.getSourceType().name();
        String host = datasourceConfig.getHost();
        String port = datasourceConfig.getPort();
        String database = datasourceConfig.getDatabase();
        String userName = datasourceConfig.getUserName();
        String password = datasourceConfig.getPassword();

        return dataSourceType + "_" + host + "_" + port + "_"
                + database + "_" + userName + "_" + password;
    }
}
