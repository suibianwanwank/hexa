package com.ccsu;

import com.beust.jcommander.internal.Lists;
import com.ccsu.meta.type.arrow.ArrowTypeEnum;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.pojo.DatasourceType;
import com.ccsu.meta.ExtendCatalogReader;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.ColumnInfo;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.option.OptionManager;
import com.ccsu.option.manager.SystemOptionManager;
import com.ccsu.profile.JobProfile;
import com.ccsu.store.api.StoreManager;
import com.ccsu.store.rocksDB.RocksDBManager;
import com.google.common.collect.ImmutableList;
import context.QueryContext;
import convert.SqlConverter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.validate.SqlValidator;
import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import validator.ValidatorProvider;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static com.ccsu.meta.utils.MetadataUtil.generateSystemPath;

public class TestFixture {


    private final static DatasourceConfig DEFAULT_SOURCE_CONFIG =
            new DatasourceConfig(DatasourceType.MYSQL, "127.0.0.1", "8080", "aaa", "bbb", null, null);

    private final static MetaPath DEFAULT_CATALOG_PATH =
            MetaPath.buildCatalogPath("default_catalog");

    private final static MetaPath DEFAULT_SCHEMA_PATH =
            MetaPath.buildSchemaPath("default_catalog", "default");

    private final static MetaPath DEFAULT_TABLE_PATH =
            MetaPath.buildTablePath("default_catalog", "default", "table1");

    private final static MetaPath DEFAULT_TABLE2_PATH =
            MetaPath.buildTablePath("default_catalog", "default", "table2");

    private final static String DEFAULT_CLUSTER = "suibianwanwan33";
    protected SqlParser sqlParser = new CalciteSqlParser();
    protected SqlConverter sqlConverter;
    protected SqlValidator sqlValidator;
    protected StoreManager storeManager;
    protected OptionManager optionManager;
    protected MetadataStoreHolder metaDataStoreHolder;
    protected MetadataService metadataService;
    protected QueryContext queryContext;

    public TestFixture() {
        init();
    }

    protected void initStoreManager() {
        File temp;
        try {
            temp = File.createTempFile("test", "");
            temp.delete();
            temp.mkdir();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storeManager = new RocksDBManager(temp);
    }

    protected void init() {
        initStoreManager();
        optionManager = new SystemOptionManager(storeManager);

        metaDataStoreHolder = new MetadataStoreHolder(storeManager);
        metadataService = new MetadataServiceImpl(metaDataStoreHolder, null, storeManager);

        registerDefaultMetadata();


        Prepare.CatalogReader catalogReader = ExtendCatalogReader.create(DEFAULT_CLUSTER,
                new JavaTypeFactoryImpl(),
                metadataService, null, Collections.emptyList(),
                false);
        sqlValidator = ValidatorProvider.create(catalogReader);
        queryContext = new QueryContext(null,
                DEFAULT_CLUSTER, null,
                sqlParser, null,
                catalogReader, optionManager, sqlValidator, Lists.newArrayList(), metadataService, storeManager, new JobProfile("111"));
        sqlConverter = SqlConverter.of(queryContext, getQueryContext().getRelOptCostFactory());
    }

    public void registerDefaultMetadata() {
        metadataService.registerCatalog(new MetaIdentifier(DEFAULT_CLUSTER, DEFAULT_CATALOG_PATH), DEFAULT_SOURCE_CONFIG);

        SchemaInfo schemaInfo = new SchemaInfo("default");

        MetaPath systemCatalog = generateSystemPath(DEFAULT_CATALOG_PATH, DEFAULT_SOURCE_CONFIG);

        ColumnInfo column1 = ColumnInfo.builder().columnName("column1").columnType(ArrowTypeEnum.UTF8).build();
        ColumnInfo column2 = ColumnInfo.builder().columnName("column2").columnType(ArrowTypeEnum.INT64).build();

        TableInfo table1 =
                new TableInfo(systemCatalog.getPath().get(0), schemaInfo.getSchemaName(), "table1", ImmutableList.of(column1, column2), 100L);
        metadataService.addOrUpdateSchema(generateSystemPath(DEFAULT_SCHEMA_PATH, DEFAULT_SOURCE_CONFIG), schemaInfo);
        metadataService.addOrUpdateTable(generateSystemPath(DEFAULT_TABLE_PATH, DEFAULT_SOURCE_CONFIG), table1);


        ColumnInfo column3 = ColumnInfo.builder().columnName("column3").columnType(ArrowTypeEnum.UTF8).build();
        ColumnInfo column4 = ColumnInfo.builder().columnName("column4").columnType(ArrowTypeEnum.INT64).build();

        TableInfo table2 =
                new TableInfo(systemCatalog.getPath().get(0), schemaInfo.getSchemaName(), "table2", ImmutableList.of(column3, column4), 100L);
        metadataService.addOrUpdateSchema(generateSystemPath(DEFAULT_SCHEMA_PATH, DEFAULT_SOURCE_CONFIG), schemaInfo);
        metadataService.addOrUpdateTable(generateSystemPath(DEFAULT_TABLE2_PATH, DEFAULT_SOURCE_CONFIG), table2);
    }

    public QueryContext getQueryContext() {
        return queryContext;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    public SqlConverter getSqlConverter() {
        return sqlConverter;
    }
}