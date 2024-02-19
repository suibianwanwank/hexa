package com.ccsu.manager;

import com.ccsu.MetadataService;
import com.ccsu.Result;
import com.ccsu.common.pool.CommandPool;
import com.ccsu.meta.ExtendCatalogReader;
import com.ccsu.option.OptionManager;
import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import com.ccsu.session.UserRequest;
import com.google.inject.Inject;
import context.QueryContext;
import convert.VolcanoRelOptCostFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlValidator;
import validator.ValidatorProvider;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;


public class SqlJobManager implements JobManager {
    private final MetadataService metadataService;
    private final OptionManager optionManager;
    private final SqlParser sqlParser;
    private final RelDataTypeFactory relDataTypeFactory;
    private final CommandPool commandPool;

    @Inject
    public SqlJobManager(MetadataService metadataService,
                         OptionManager optionManager,
                         CommandPool commandPool) {
        this(
                metadataService,
                optionManager,
                new CalciteSqlParser(),
                commandPool,
                new JavaTypeFactoryImpl());
    }

    public SqlJobManager(MetadataService metadataService,
                         OptionManager optionManager,
                         SqlParser sqlParser,
                         CommandPool commandPool,
                         RelDataTypeFactory relDataTypeFactory) {
        this.metadataService = metadataService;
        this.optionManager = optionManager;
        this.sqlParser = sqlParser;
        this.relDataTypeFactory = relDataTypeFactory;
        this.commandPool = commandPool;
    }

    @Override
    public Result cancelSqlJob() {
        return null;
    }

    @Override
    public CompletableFuture<Result> submitSqlJob(UserRequest userRequest, String sql) {
        QueryContext queryContext = createQueryContext(userRequest.getClusterId(), sql);

        return commandPool.submit(
                String.format("The job with sql job id %s has been submitted.", sql),
                () -> executeSql(queryContext),
                false);
    }

    private Result executeSql(QueryContext queryContext) {
        SqlJobExecutor sqlJobExecutor = new SqlJobExecutor(queryContext, null, null);
        return sqlJobExecutor.run();
    }

    private QueryContext createQueryContext(String clusterId, String sql) {
        Prepare.CatalogReader catalogReader =
                ExtendCatalogReader.create(clusterId, relDataTypeFactory,
                        metadataService, Collections.emptyList(), true);
        SqlValidator sqlValidator = ValidatorProvider.create(catalogReader);
        return new QueryContext(sql, clusterId, JobUtil.generateSqlJobId(),
                sqlParser, new VolcanoRelOptCostFactory(), catalogReader, sqlValidator);
    }
}
