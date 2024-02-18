package com.ccsu.datasource.mysql;

import com.ccsu.datasource.api.ConnectorService;
import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.datasource.jdbc.JdbcConnectorService;
import com.ccsu.utils.StringUtil;

import java.util.List;

public class MysqlConnectorService extends JdbcConnectorService {

    public static final String MYSQL_JDBC_URL = "jdbc:mysql://%s:%s/";
    public static final String MYSQL_URL_SUFFIX = "?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8";
    public static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    private static final MysqlConnectorService INSTANCE = new MysqlConnectorService();

    public static ConnectorService getInstance() {
        return INSTANCE;
    }

    @Override
    public List<MetadataEntity> getTableNames(ConnectorRequest request,
                                              MetadataEntity scope,
                                              String filter,
                                              Integer limit) {
        return null;
    }

    @Override
    public String getJDBCDriverClassName() {
        return MYSQL_DRIVER_CLASS_NAME;
    }

    @Override
    public String getJDBCUrl(DatasourceConfig datasourceConfig) {
        String host = StringUtil.trim(datasourceConfig.getHost());
        String port = StringUtil.trim(datasourceConfig.getPort());
        return String.format(MYSQL_JDBC_URL, host, port) + MYSQL_URL_SUFFIX;
    }
}
