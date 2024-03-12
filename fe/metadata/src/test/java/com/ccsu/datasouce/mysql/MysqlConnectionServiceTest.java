package com.ccsu.datasouce.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.ccsu.datasource.api.ConnectorService;
import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.DatasourceType;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.datasource.api.pojo.TableInfo;
import com.ccsu.datasource.mysql.MysqlConnectorService;
import com.ccsu.datasource.util.ConnectionUtil;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

public class MysqlConnectionServiceTest {

    private static final String CATALOG_NAME = "mysql01";
    private static final String DATABASE_NAME = CATALOG_NAME;
    private static final String DEFAULT_CLUSTER = "suibianwanwan";

    private final ConnectorService connectorService = new MysqlConnectorService();

    @Test
    @Ignore
    public void testCreateConnection() {
        DatasourceConfig datasourceConfig =
                new DatasourceConfig(DatasourceType.MYSQL,
                        "10.5.20.26", "3306", "root", "Aloudata@12", null, null);
        DruidDataSource druidDataSource = null;
        DruidPooledConnection connection = null;
        try {
            druidDataSource = connectorService.createDruidDataSource(datasourceConfig);
            connection = druidDataSource.getConnection();
        } catch (SQLException e) {
            Assert.fail("failed to connect source");
        } finally {
            ConnectionUtil.closeQuietly(druidDataSource);
            ConnectionUtil.connectClose(connection);
        }
    }

    @Test
    @Ignore
    public void testGetSchemas() {
        DatasourceConfig datasourceConfig =
                new DatasourceConfig(DatasourceType.MYSQL,
                        "10.5.20.26", "3306", "root", "Aloudata@12", null, null);
        ConnectorRequest connectorRequest = new ConnectorRequest(datasourceConfig, false);
        MetadataEntity metadataEntity = MetadataEntity.buildCatalog(CATALOG_NAME, DATABASE_NAME);
        DruidDataSource druidDataSource = null;
        DruidPooledConnection connection = null;
        try {
            druidDataSource = connectorService.createDruidDataSource(datasourceConfig);
            connection = druidDataSource.getConnection();
            List<MetadataEntity> tableNames = connectorService.listSchemas(connectorRequest, metadataEntity, connection);
            Assert.assertNotNull(tableNames);
        } catch (SQLException e) {
            Assert.fail("failed to connect source");
        } finally {
            ConnectionUtil.closeQuietly(druidDataSource);
            ConnectionUtil.connectClose(connection);
        }
    }

    @Test
    @Ignore
    public void testListTables() {
        DatasourceConfig datasourceConfig =
                new DatasourceConfig(DatasourceType.MYSQL,
                        "10.5.20.26", "3306", "root", "Aloudata@12", null, null);
        ConnectorRequest connectorRequest = new ConnectorRequest(datasourceConfig, false);
        MetadataEntity metadataEntity = MetadataEntity.buildSchema(CATALOG_NAME, null, "tpcds");
        DruidDataSource druidDataSource = null;
        DruidPooledConnection connection = null;
        try {
            druidDataSource = connectorService.createDruidDataSource(datasourceConfig);
            connection = druidDataSource.getConnection();
            List<MetadataEntity> entityList = connectorService.getTableNames(connectorRequest, metadataEntity, connection);
            Assert.assertNotNull(entityList);
        } catch (SQLException e) {
            Assert.fail("failed to connect source");
        } finally {
            ConnectionUtil.closeQuietly(druidDataSource);
            ConnectionUtil.connectClose(connection);
        }
    }

    @Test
    @Ignore
    public void testGetTableInfo() {
        DatasourceConfig datasourceConfig =
                new DatasourceConfig(DatasourceType.MYSQL,
                        "10.5.20.26", "3306", "root", "Aloudata@12", null, null);
        ConnectorRequest connectorRequest = new ConnectorRequest(datasourceConfig, false);
        MetadataEntity metadataEntity = MetadataEntity.buildTable(CATALOG_NAME, null, "tpcds", "store_sales");
        DruidDataSource druidDataSource = null;
        DruidPooledConnection connection = null;
        try {
            druidDataSource = connectorService.createDruidDataSource(datasourceConfig);
            connection = druidDataSource.getConnection();
            TableInfo info = connectorService.extractTableInfo(connectorRequest, metadataEntity, connection);
            Assert.assertNotNull(info);
        } catch (SQLException e) {
            Assert.fail("failed to connect source");
        } finally {
            ConnectionUtil.closeQuietly(druidDataSource);
            ConnectionUtil.connectClose(connection);
        }
    }
}