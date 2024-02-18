package com.ccsu.datasource.api;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.DatasourceType;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.datasource.api.pojo.TableInfo;
import com.ccsu.datasource.mysql.MysqlConnectorService;
import com.ccsu.datasource.util.ConnectionUtil;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.common.collect.ImmutableMap;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {

    private ConnectionManager() {

    }

    private static final Map<DatasourceType, ConnectorService> CONNECTOR_SERVICE_MAP =
            ImmutableMap.of(DatasourceType.MYSQL, MysqlConnectorService.getInstance());

    private static final Map<String, DruidDataSource> DATASOURCE_MAP = new ConcurrentHashMap<>();

    public static DruidPooledConnection getConnection(DatasourceConfig datasourceConfig) throws SQLException {
        String connectKey = ConnectionUtil.generateConnectKey(datasourceConfig);
        DruidDataSource druidDataSource = DATASOURCE_MAP.get(connectKey);
        if (druidDataSource == null) {
            ConnectorService connectorService = CONNECTOR_SERVICE_MAP.get(datasourceConfig.getSourceType());
            druidDataSource = connectorService.createDruidDataSource(datasourceConfig);
            DATASOURCE_MAP.put(connectKey, druidDataSource);
        }
        if (!druidDataSource.isEnable()) {
            druidDataSource.restart();
        }
        return druidDataSource.getConnection();
    }

    public static List<MetadataEntity> getTableNames(ConnectorRequest connectorRequest, MetadataEntity scope) {
        DatasourceType sourceType = connectorRequest.getDatasourceConfig().getSourceType();
        DruidPooledConnection connection = null;
        try {
            connection = getConnection(connectorRequest.getDatasourceConfig());
            ConnectorService connectorService = CONNECTOR_SERVICE_MAP.get(sourceType);
            return connectorService.getTableNames(connectorRequest, scope, connection);
        } catch (SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR, se.getMessage());
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    public static List<MetadataEntity> listSchemas(ConnectorRequest connectorRequest, MetadataEntity scope) {
        DatasourceType sourceType = connectorRequest.getDatasourceConfig().getSourceType();
        DruidPooledConnection connection = null;
        try {
            connection = getConnection(connectorRequest.getDatasourceConfig());
            ConnectorService connectorService = CONNECTOR_SERVICE_MAP.get(sourceType);
            return connectorService.listSchemas(connectorRequest, scope, connection);
        } catch (SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR, se.getMessage());
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    public static TableInfo extractTableInfo(ConnectorRequest connectorRequest, MetadataEntity scope) {
        DatasourceType sourceType = connectorRequest.getDatasourceConfig().getSourceType();
        DruidPooledConnection connection = null;
        try {
            connection = getConnection(connectorRequest.getDatasourceConfig());
            ConnectorService connectorService = CONNECTOR_SERVICE_MAP.get(sourceType);
            return connectorService.extractTableInfo(connectorRequest, scope, connection);
        } catch (SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR, se.getMessage());
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    public static void logDruidDataSource(DataSource dataSource) {
        if (dataSource != null) {
            return;
        }
        DATASOURCE_MAP.values().forEach(t -> {
        });
    }
}
