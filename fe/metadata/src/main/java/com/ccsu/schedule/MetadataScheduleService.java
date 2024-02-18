package com.ccsu.schedule;

import com.ccsu.LeaderService;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.pojo.MetadataEntity;
import com.ccsu.event.EventRegistry;
import com.ccsu.store.api.*;
import com.ccsu.event.Event;
import com.ccsu.event.EventPublisher;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.option.OptionManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class MetadataScheduleService implements LeaderService {

    private static final String COLLECT_INFO_NAME = "Collect Metadata";

    // TODO may optimize to get option
    private static final Long DEFAULT_CIRCLE_TIME = 1000 * 60 * 15L;
    private final ScheduledExecutorService scheduleCollectExecutor;
    private final EventPublisher eventPublisher;
    private final DataIndexStore<String, MetaSchedule> metaScheduleDataStore;

    @Inject
    public MetadataScheduleService(EventRegistry eventRegistry,
                                   CollectorEventListener collectorEventListener,
                                   OptionManager optionManager,
                                   StoreManager storeManager) {
        this.eventPublisher = eventRegistry.getEventPublisher();
        this.scheduleCollectExecutor = newScheduleCollectExecutor(optionManager);
        this.metaScheduleDataStore = storeManager.getOrCreateDataIndexStore(new ScheduleStoreConfig());
        eventRegistry.addListener(COLLECT_INFO_NAME, collectorEventListener);
    }

    @Override
    @PostConstruct
    public void start() {
        scheduleCollectExecutor.scheduleWithFixedDelay(
                this::scheduleExecute, 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
    }

    private void scheduleExecute() {
        LocalDateTime currentTime = LocalDateTime.now().withSecond(0).withNano(0);
        try {
            LocalDateTime plusMinutes = currentTime.plusMinutes(1L);
            long plusMinutesTimestamp = Timestamp.valueOf(plusMinutes).getTime();
            ImmutableFindByCondition condition = ImmutableFindByCondition.builder()
                    .condition(FindByCondition.rangeLong("nextFireTime", 0, true, plusMinutesTimestamp, false))
                    .build();

            Iterable<? extends EntityWithTag<String, MetaSchedule>> iterable = metaScheduleDataStore.find(condition);
            for (EntityWithTag<String, MetaSchedule> entityWithTag : iterable) {
                MetaSchedule schedule = entityWithTag.getValue();

                if (schedule == null) {
                    continue;
                }

                eventPublisher.publish(new Event<>(COLLECT_INFO_NAME, schedule.getCollectInfo()));

                MetaSchedule next = schedule.nextSchedule(schedule.getCycleTime() + System.currentTimeMillis());
                metaScheduleDataStore.put(next.toString(), next);
            }
        } catch (Exception e) {

        }
    }

    public void registerTableInCatalogCollectEvent(MetaPath path, DatasourceConfig datasourceConfig, boolean isSync) {

        MetadataEntity entity = path.getCatalogName() != null ?
                MetadataEntity.buildCatalogWithDatabase(path.getCatalogName(), datasourceConfig.getDatabase()) :
                MetadataEntity.buildCatalogWithoutDatabase(path.getCatalogName());
        CollectInfo collectInfo = new CollectInfo(MetaScheduleType.COLLECT_TABLE_NAME_IN_CATALOG, datasourceConfig, entity);
        Event<CollectInfo> event = new Event<>(COLLECT_INFO_NAME, collectInfo);

        publishEvent(event, isSync);

        MetaSchedule next = new MetaSchedule(collectInfo, DEFAULT_CIRCLE_TIME, System.currentTimeMillis() + DEFAULT_CIRCLE_TIME);
        metaScheduleDataStore.put(next.toString(), next);
    }

    public void registerTableDetailCollectEvent(MetaPath path, DatasourceConfig datasourceConfig, boolean isSync) {

        MetadataEntity entity = path.getCatalogName() != null ?
                MetadataEntity.buildTableWithDatabase(path.getCatalogName(), datasourceConfig.getDatabase(), path.getSchemaName(), path.getTableName()) :
                MetadataEntity.buildTableWithoutDatabase(path.getCatalogName(), path.getSchemaName(), path.getTableName());
        CollectInfo collectInfo = new CollectInfo(MetaScheduleType.COLLECT_TABLE_DETAIL, datasourceConfig, entity);
        Event<CollectInfo> event = new Event<>(COLLECT_INFO_NAME, collectInfo);

        publishEvent(event, isSync);

        MetaSchedule next = new MetaSchedule(collectInfo, DEFAULT_CIRCLE_TIME, System.currentTimeMillis() + DEFAULT_CIRCLE_TIME);
        metaScheduleDataStore.put(next.toString(), next);
    }

    private void publishEvent(Event<CollectInfo> event, boolean isSync){
        if (isSync) {
            eventPublisher.publishSync(event);
        } else {
            eventPublisher.publish(event);
        }
    }

    private ScheduledExecutorService newScheduleCollectExecutor(OptionManager optionManager) {
        return new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("schedule-collect-%d")
                .build());
    }
}
