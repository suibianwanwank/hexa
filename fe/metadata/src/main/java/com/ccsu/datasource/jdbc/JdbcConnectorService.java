package com.ccsu.datasource.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.StringUtils;
import com.ccsu.datasource.api.ConnectorService;
import com.ccsu.datasource.api.pojo.ConnectorRequest;
import com.ccsu.datasource.api.pojo.DatasourceConfig;
import com.ccsu.datasource.api.pojo.FieldInfo;
import com.ccsu.datasource.api.pojo.MetadataEntity;
import com.ccsu.datasource.api.pojo.ScopeType;
import com.ccsu.datasource.api.pojo.TableInfo;
import com.ccsu.datasource.util.ConnectionUtil;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.ccsu.constants.TimeConstans.MINUTE_TO_MILLS_SECOND;
import static com.ccsu.datasource.util.ConnectorConstants.MULTI_CHARACTER_SEARCH;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_BUFFER_LENGTH;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_CHAR_OCTET_LENGTH;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_COLUMN_DEF;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_COLUMN_NAME;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_COLUMN_SIZE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_DATA_TYPE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_DECIMAL_DIGITS;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_IS_AUTOINCREMENT;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_IS_GENERATEDCOLUMN;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_IS_NULLABLE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_NULLABLE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_NUM_PREC_RADIX;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_ORDINAL_POSITION;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_REF_GENERATION;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_REMARKS;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SCOPE_CATALOG;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SCOPE_SCHEMA;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SCOPE_TABLE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SELF_REFERENCING_COL_NAME;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SOURCE_DATA_TYPE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SQL_DATA_TYPE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_SQL_DATETIME_SUB;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TABLE_CAT;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TABLE_NAME;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TABLE_SCHEM;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TABLE_TYPE;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TYPE_CAT;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TYPE_NAME;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SCHEMA_KEY_TYPE_SCHEM;
import static com.ccsu.datasource.util.JdbcConnectorConstants.SYSTEM_DATABASE_NAME_INFORMATION_SCHEMA;

public abstract class JdbcConnectorService implements ConnectorService {

    private static final Long DEFAULT_MIN_EVICTABLE_IDLE = MINUTE_TO_MILLS_SECOND * 3;
    private static final Long DEFAULT_MAX_EVICTABLE_IDLE = MINUTE_TO_MILLS_SECOND * 5;
    private static final Long DEFAULT_CONNECT_ERROR = MINUTE_TO_MILLS_SECOND * 3;

    protected static final String[] TABLE_TYPES = {"TABLE", "VIEW"};


    @Override
    public List<MetadataEntity> listSchemas(final ConnectorRequest connectorRequest,
                                            final MetadataEntity scope,
                                            DruidPooledConnection connection) {
        if (scope.getScopeType() != ScopeType.CATALOG) {
            return ImmutableList.of();
        }
        try {
            String catalog = connection.getCatalog();
            final List<MetadataEntity> names = Lists.newArrayList();
            final ResultSet schemas = connection.getMetaData().getCatalogs();

            while (schemas.next()) {
                final String schemaName = schemas.getString(SCHEMA_KEY_TABLE_CAT);
                // skip system schemas
                if (!schemaName.equals(SYSTEM_DATABASE_NAME_INFORMATION_SCHEMA)) {
                    names.add(MetadataEntity.buildSchema(catalog, connectorRequest.
                            getDatasourceConfig().getDatabase(), schemaName));
                }
            }
            ConnectionUtil.closeQuietly(connection);
            return null;
        } catch (final SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR,
                    String.format("Failed to collect metadata, request:%s", connectorRequest));
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    @Override
    public List<MetadataEntity> getTableNames(final ConnectorRequest connectorRequest,
                                              final MetadataEntity scope,
                                              final DruidPooledConnection connection) {
        if (scope.getScopeType() != ScopeType.SCHEMA) {
            return ImmutableList.of();
        }
//        LOGGER.debug("Beginning to list tables names for qualified name {} for request {}", name, context);
        final String catalogName = scope.getCatalogName();
        final String schemaName = scope.getSchemaName();
        try {
            final List<MetadataEntity> names = Lists.newArrayList();
            try (ResultSet tables = getTable(connection, scope)) {
                while (tables.next()) {
                    MetadataEntity tableEntity =
                            MetadataEntity.buildTable(catalogName, schemaName, schemaName,
                                    tables.getString(SCHEMA_KEY_TABLE_NAME));
                    names.add(tableEntity);
                }
                ConnectionUtil.closeQuietly(tables);
            }

            ConnectionUtil.closeQuietly(connection);
//            LOGGER.debug("Finished listing tables names for qualified name {} for request {}", name, context);
            return names;
        } catch (final SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR,
                    String.format("Failed to collect metadata, request:%s", connectorRequest));
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    @Override
    public TableInfo extractTableInfo(ConnectorRequest request,
                                      final MetadataEntity scope,
                                      DruidPooledConnection connection) {
        if (scope.getScopeType() != ScopeType.TABLE) {
            return null;
        }
        try {
            TableInfo tableInfo = null;

            try (ResultSet tables = getTable(connection, scope)) {
                while (tables.next()) {
                    tableInfo = processTableInfo(scope, tables);
                    // Set table details
                    setTableInfoDetails(connection, tableInfo);
                }
                ConnectionUtil.closeQuietly(tables);
            }
            // If table does not exist, throw TableNotFoundException.
            if (tableInfo == null) {
                throw new CommonException(CommonErrorCode.META_COLLECT_ERROR,
                        String.format("table: %s not exist", scope.getTableName()));
            }
            List<FieldInfo> fieldInfos = new ArrayList<>();
            try (ResultSet columns = getColumns(connection, scope)) {
                while (columns.next()) {
                    final FieldInfo fieldInfo = processFieldInfo(scope, columns);
                    if (fieldInfo == null) {
                        continue;
                    }
                    fieldInfos.add(fieldInfo);
                }
                ConnectionUtil.closeQuietly(columns);
            }

            try (ResultSet indexInfo = getIndexInfo(connection, scope);) {
                while (indexInfo.next()) {
                    short ordinalPosition = indexInfo.getShort("ORDINAL_POSITION");
                    fieldInfos.get(ordinalPosition).setIsIndexKey(true);
                }
                ConnectionUtil.closeQuietly(indexInfo);
            }
            tableInfo.setFields(fieldInfos);
            ConnectionUtil.closeQuietly(connection);
            return tableInfo;
        } catch (final SQLException se) {
            throw new CommonException(CommonErrorCode.META_COLLECT_ERROR, se.getMessage());
        } finally {
            ConnectionUtil.connectClose(connection);
        }
    }

    /**
     * Get the tables. See {@link DatabaseMetaData#getTables(String, String, String, String[]) getTables} for
     * expected format of the ResultSet columns.
     */
    protected ResultSet getTable(final Connection connection, final MetadataEntity name)
            throws SQLException {
        final String jdbcCatalogName = name.getSchemaName();
        final String jdbcSchemaName = name.getDatabaseName();
        final String jdbcTableName = name.getTableName();
        final DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getTables(jdbcCatalogName, jdbcSchemaName, jdbcTableName, TABLE_TYPES);
    }

    /**
     * Get the columns for a table. See {@link DatabaseMetaData#getColumns(String, String, String, String)
     * getColumns} for format of the ResultSet columns.
     */
    protected ResultSet getColumns(final Connection connection, final MetadataEntity entity) throws SQLException {
        final String jdbcCatalogName = entity.getSchemaName();
        final String jdbcSchemaName = entity.getDatabaseName();
        final String jdbcTableName = entity.getTableName();
        final DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getColumns(jdbcCatalogName, jdbcSchemaName, jdbcTableName, MULTI_CHARACTER_SEARCH);
    }

    /**
     * Get the index column for a table. See {@link DatabaseMetaData#getColumns(String, String, String, String)
     * getColumns} for format of the ResultSet columns.
     */
    protected ResultSet getIndexInfo(final Connection connection, final MetadataEntity entity) throws SQLException {
        final String jdbcCatalogName = entity.getSchemaName();
        final String jdbcSchemaName = entity.getDatabaseName();
        final String jdbcTableName = entity.getTableName();
        final DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getIndexInfo(jdbcCatalogName, jdbcSchemaName, jdbcTableName, false, true);
    }

    /**
     * processTableInfo from ResultSet
     */
    protected TableInfo processTableInfo(MetadataEntity entity, ResultSet tables) throws SQLException {
        TableInfo tableInfo = TableInfo.builder()
                .tableCat(tables.getString(SCHEMA_KEY_TABLE_CAT))
                .tableSchema(tables.getString(SCHEMA_KEY_TABLE_SCHEM))
                .tableName(tables.getString(SCHEMA_KEY_TABLE_NAME))
                // 表类型,典型的类型是 "TABLE"、"VIEW"、"SYSTEM TABLE"、"GLOBAL TEMPORARY"、"LOCAL TEMPORARY"、"ALIAS" 和
                // "SYNONYM"。
                .tableType(tables.getString(SCHEMA_KEY_TABLE_TYPE))
                .remarks(tables.getString(SCHEMA_KEY_REMARKS))
                .typeCat(tables.getString(SCHEMA_KEY_TYPE_CAT))
                .typeSchema(tables.getString(SCHEMA_KEY_TYPE_SCHEM))
                .typeName(tables.getString(SCHEMA_KEY_TYPE_NAME))
                .selfReferencingColName(tables.getString(SCHEMA_KEY_SELF_REFERENCING_COL_NAME))
                .refGeneration(tables.getString(SCHEMA_KEY_REF_GENERATION))
                .build();
        return tableInfo;
    }

    /**
     * processFieldInfo from ResultSet
     */
    public FieldInfo processFieldInfo(MetadataEntity entity, ResultSet column) throws SQLException {
        if (!StringUtils.equalsIgnoreCase(entity.getTableName(), column.getString(SCHEMA_KEY_TABLE_NAME))) {
            return null;
        }
        FieldInfo fieldInfo = FieldInfo.builder()
                .tableCat(column.getString(SCHEMA_KEY_TABLE_CAT))
                .tableSchema(column.getString(SCHEMA_KEY_TABLE_SCHEM))
                .tableName(column.getString(SCHEMA_KEY_TABLE_NAME))
                .columnName(column.getString(SCHEMA_KEY_COLUMN_NAME))
                .dataType(column.getInt(SCHEMA_KEY_DATA_TYPE))
                .typeName(column.getString(SCHEMA_KEY_TYPE_NAME))
                .columnSize(column.getInt(SCHEMA_KEY_COLUMN_SIZE))
                .bufferLength(column.getInt(SCHEMA_KEY_BUFFER_LENGTH))
                .decimalDigits(column.getInt(SCHEMA_KEY_DECIMAL_DIGITS))
                .numPrecRadix(column.getInt(SCHEMA_KEY_NUM_PREC_RADIX))
                .nullable(column.getInt(SCHEMA_KEY_NULLABLE))
                .remarks(column.getString(SCHEMA_KEY_REMARKS))
                .columnDef(column.getString(SCHEMA_KEY_COLUMN_DEF))
                .sqlDataType(column.getInt(SCHEMA_KEY_SQL_DATA_TYPE))
                .sqlDatetimeSub(column.getInt(SCHEMA_KEY_SQL_DATETIME_SUB))
                .charOctetLength(column.getInt(SCHEMA_KEY_CHAR_OCTET_LENGTH))
                .ordinalPosition(column.getInt(SCHEMA_KEY_ORDINAL_POSITION))
                .isNullable(column.getString(SCHEMA_KEY_IS_NULLABLE))
                .scopeCatalog(column.getString(SCHEMA_KEY_SCOPE_CATALOG))
                .scopeSchema(column.getString(SCHEMA_KEY_SCOPE_SCHEMA))
                .scopeTable(column.getString(SCHEMA_KEY_SCOPE_TABLE))
                .sourceDataType(column.getShort(SCHEMA_KEY_SOURCE_DATA_TYPE))
                .isAutoincrement(column.getString(SCHEMA_KEY_IS_AUTOINCREMENT))
                .isGeneratedcolumn(column.getString(SCHEMA_KEY_IS_GENERATEDCOLUMN))
                .build();
        return fieldInfo;
    }

    @Override
    public DruidDataSource createDruidDataSource(DatasourceConfig datasourceConfig) {
        DruidDataSource dataSource = new DruidDataSource();
        // 最大空闲时间
        dataSource.setMaxEvictableIdleTimeMillis(DEFAULT_MAX_EVICTABLE_IDLE);
        dataSource.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE);
        // 最大活跃连接数
        dataSource.setMaxActive(2);
        // 重试失败后中断
        dataSource.setBreakAfterAcquireFailure(true);
        // 重试次数
        dataSource.setConnectionErrorRetryAttempts(2);
        // 重试间隔
        dataSource.setTimeBetweenConnectErrorMillis(DEFAULT_CONNECT_ERROR);
        dataSource.setDriverClassName(getJDBCDriverClassName());
        dataSource
                .setUrl(getJDBCUrl(datasourceConfig));
        dataSource.setUsername(datasourceConfig.getUserName());
        dataSource.setPassword(datasourceConfig.getPassword());
        return dataSource;
    }

    protected void setTableInfoDetails(final Connection connection, final TableInfo tableInfo) {
    }
}
