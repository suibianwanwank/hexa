package com.ccsu.datasource.api;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.datasource.api.pojo.TableInfo;

import java.util.List;

public interface ConnectorService {


    String getJDBCDriverClassName();

    String getJDBCUrl(DatasourceConfig datasourceConfig);

    List<MetadataEntity> getTableNames(ConnectorRequest request,
                                       MetadataEntity scope,
                                       String filter,
                                       Integer limit);

    DruidDataSource createDruidDataSource(DatasourceConfig datasourceConfig);

    List<MetadataEntity> getTableNames(ConnectorRequest connectorRequest,
                                       MetadataEntity scope,
                                       DruidPooledConnection connection);

    List<MetadataEntity> listSchemas(ConnectorRequest connectorRequest,
                                     MetadataEntity scope,
                                     DruidPooledConnection connection);

    TableInfo extractTableInfo(ConnectorRequest request,
                               MetadataEntity scope,
                               DruidPooledConnection connection);
}
