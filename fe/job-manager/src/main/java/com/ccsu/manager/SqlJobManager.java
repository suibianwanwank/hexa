package com.ccsu.manager;

import com.ccsu.MetadataService;
import com.ccsu.client.GrpcProvider;
import com.ccsu.common.pool.CommandPool;
import com.ccsu.meta.ExtendCatalogReader;
import com.ccsu.meta.type.ArrowTypeFactory;
import com.ccsu.meta.type.ArrowTypeMapping;
import com.ccsu.observer.JobProfileObserver;
import com.ccsu.observer.LoggerRecordObserver;
import com.ccsu.profile.JobProfile;
import com.ccsu.store.api.StoreManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import observer.SqlJobObserver;
import com.ccsu.option.OptionManager;
import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import com.ccsu.session.UserRequest;
import com.ccsu.system.BackEndConfig;
import com.google.inject.Inject;
import context.QueryContext;
import convert.VolcanoRelOptCostFactory;
import handler.Void;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlValidator;
import validator.ValidatorProvider;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.ccsu.manager.JobUtil.syncCompletableFuture;


public class SqlJobManager
        implements JobManager {
    private final MetadataService metadataService;
    private final OptionManager optionManager;
    private final SqlParser sqlParser;
    private final RelDataTypeFactory relDataTypeFactory;
    private final ArrowTypeMapping arrowTypeMapping;
    private final CommandPool commandPool;
    private final GrpcProvider grpcProvider;
    private final BackEndConfig backEndConfig;
    private final StoreManager storeManager;

    @Inject
    public SqlJobManager(MetadataService metadataService,
                         StoreManager storeManager,
                         OptionManager optionManager,
                         CommandPool commandPool,
                         BackEndConfig backEndConfig,
                         GrpcProvider grpcProvider) {
        this(
                metadataService,
                storeManager,
                optionManager,
                new CalciteSqlParser(),
                commandPool,
                new ArrowTypeFactory(),
                backEndConfig,
                grpcProvider);
    }

    public SqlJobManager(MetadataService metadataService,
                         StoreManager storeManager,
                         OptionManager optionManager,
                         SqlParser sqlParser,
                         CommandPool commandPool,
                         RelDataTypeFactory relDataTypeFactory,
                         BackEndConfig backEndConfig,
                         GrpcProvider grpcProvider) {
        this.metadataService = metadataService;
        this.storeManager = storeManager;
        this.optionManager = optionManager;
        this.sqlParser = sqlParser;
        this.relDataTypeFactory = relDataTypeFactory;
        this.arrowTypeMapping = new ArrowTypeMapping((ArrowTypeFactory) relDataTypeFactory);
        this.commandPool = commandPool;
        this.backEndConfig = backEndConfig;
        this.grpcProvider = grpcProvider;
    }

    @Override
    public void cancelSqlJob() {

    }

    @Override
    public void submitSqlJob(UserRequest userRequest, String sql, List<SqlJobObserver> observers) {
        QueryContext queryContext = createQueryContext(userRequest.getClusterId(), sql, observers);

        SqlJobExecutor sqlJobExecutor = new SqlJobExecutor(queryContext, grpcProvider, backEndConfig);

        CompletableFuture<Void> future = commandPool.submit(
                String.format("The job with sql job id %s has been submitted.", sql),
                sqlJobExecutor::run,
                false);

        syncCompletableFuture(future);
    }

    private QueryContext createQueryContext(String clusterId, String sql, List<SqlJobObserver> observers) {
        Prepare.CatalogReader catalogReader =
                ExtendCatalogReader.create(clusterId, relDataTypeFactory,
                        metadataService, arrowTypeMapping, Collections.emptyList(), true);
        SqlValidator sqlValidator = ValidatorProvider.create(catalogReader);
        JobProfile jobProfile = new JobProfile(sql);
        String jobId = JobUtil.generateSqlJobId();


        List<SqlJobObserver> observerList = Lists.newArrayList();
        observerList.addAll(observers);
        observerList.add(JobProfileObserver.newJobProfileObserver(jobId, jobProfile, storeManager));
        observerList.add(LoggerRecordObserver.newLoggerObserver(jobId));

        return new QueryContext(sql, clusterId, jobId, sqlParser,
                new VolcanoRelOptCostFactory(), catalogReader, optionManager,
                sqlValidator, ImmutableList.copyOf(observerList), metadataService, storeManager, jobProfile);
    }
}
