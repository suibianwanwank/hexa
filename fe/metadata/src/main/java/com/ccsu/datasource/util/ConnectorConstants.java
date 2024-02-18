/**
 * Aloudata.com Inc.
 * Copyright (c) 2021-2022 All Rights Reserved.
 */

package com.ccsu.datasource.util;


public class ConnectorConstants {

    /** string end. */

    /**
     * Standard error message for all default implementations.
     */
    public static final String GET_COUNT_SQL = "SELECT COUNT(0) FROM %s.%s";

    public static final String UNSUPPORTED_MESSAGE = "Not supported for this connector";

    /**
     * Verify the connection failed.
     */
    public static final String VERIFY_FAILED_MESSAGE = "Verify the connection of %s failed";

    /**
     * The connection not loaded.
     */
    public static final String VERIFY_NOT_LOADED_MESSAGE = "The connection of %s not loaded";

    /**
     * ConnectorContext.
     */
    public static final String CONNECTOR_CONTEXT = "ConnectorContext";

    /**
     * ConnectorInfoConverter.
     */
    public static final String CONNECTOR_INFO_CONVERTER = "ConnectorInfoConverter";

    /**
     * The string used for multi character search in SQL.
     */
    public static final String MULTI_CHARACTER_SEARCH = "%";

    /**
     * The string used for single character search in SQL.
     */
    public static final String SINGLE_CHARACTER_SEARCH = "_";

    public static final String EMPTY = "";

    public static final String COMMA_SPACE = ", ";
    public static final String COMMA = ",";

    public static final String MINUS = "-";

    public static final String UNSIGNED = "unsigned";

    public static final String ZERO = "0";

    public static final String STRING_EMPTY = "";

    public static final String POINT_SEPARATOR = ".";

    public static final String PATH_SEPARATOR = "/";

    public static final String SCHEME_SEPARATOR = "://";

    public static final String EQUALS = "=";

    public static final String BEAN_NAME_THREAD_POOL_TASK_EXECUTOR_FOR_DATASOURCE =
            "connectorDatasourceTaskExecutor";

    public static final String BEAN_NAME_THREAD_POOL_TASK_EXECUTOR_FOR_EXTRACT_TABLE_NAMES =
            "extractTableNamesTaskExecutor";
    public static final String HIVE_METASTORE_TRANSIENT_LASTDDLTIME = "transient_lastDdlTime";
    public static final String TABLE_TYPE_PROP = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    public static final String HDFS_SITE_XML_PATH = "hdfs.site.path";
    public static final String ICEBERG_TABLE_LOCATION = "table_location";
    public static final String HADOOP_USER_NAME = "hadoop.user.name";

    /** string end. */

    /**
     * char begin.
     */

    public static final char QNAME_SEP_METADATA_NAMESPACE = '@';

    public static final char LEFT_PAREN = '(';

    public static final char RIGHT_PAREN = ')';

    public static final char SPACE = ' ';

    /** char end. */

    /** number begin. */

    /**
     * time out for check the connection.
     */
    public static final long TIME_OUT = 30L;

    /**
     * batch size for partition.
     */
    public static final int PARTITION_BATCH_SIZE = 1000;

    public static final int NUMBER_0 = 0;

    public static final int NUMBER_1 = 1;

    public static final int NUMBER_2 = 2;

    public static final int NUMBER_3 = 3;

    public static final int NUMBER_4 = 4;

    public static final int NUMBER_5 = 5;

    /**
     * number end.
     */
    private ConnectorConstants() {
    }

}
