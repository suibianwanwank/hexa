package com.ccsu.schedule;

import com.ccsu.LeaderService;
import com.ccsu.MetadataService;
import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.FindByCondition;
import com.ccsu.store.api.ImmutableFindByCondition;
import com.ccsu.event.Event;
import com.ccsu.event.EventPublisher;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.option.OptionManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class MetadataScheduleService implements LeaderService {

    private final MetadataService metadataService;
    private final ScheduledExecutorService scheduleCollectExecutor;
    private final EventPublisher eventPublisher;
    private final OptionManager optionManager;
    private DataIndexStore<String, MetaSchedule> metaScheduleDataStore;

    @Inject
    public MetadataScheduleService(MetadataService metadataService,
                                   EventPublisher eventPublisher,
                                   OptionManager optionManager) {
        this.metadataService = requireNonNull(metadataService, "metadataStoreHolder is null");
        this.eventPublisher = requireNonNull(eventPublisher, "eventPublisher is null");
        this.optionManager = requireNonNull(optionManager, "optionManager is null");
        this.scheduleCollectExecutor = newScheduleCollectExecutor(optionManager);
    }

    @Override
    public void start() {
        scheduleCollectExecutor.scheduleWithFixedDelay(() -> {
            try {
                scheduleExecute();
            } catch (Throwable t) {
            }
        }, 0, 0, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {

    }

    private void scheduleExecute() {
        LocalDateTime currentTime = LocalDateTime.now().withSecond(0).withNano(0);
        int succeedCount = 0;
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

                MetaScheduleType collectEventEnum = schedule.getType();

                MetaPath path = MetaPath.buildCatalogPath(schedule.getCatalogName());
                eventPublisher.publish(convertToEvent(path));

                MetaSchedule next = schedule.nextSchedule(schedule.getCycleTime() + System.currentTimeMillis());
                metaScheduleDataStore.put(next.toString(), next);
                succeedCount++;
            }
        } catch (Exception e) {

        }
    }

    private Event<CollectInfo> convertToEvent(MetaPath path) {
        CollectInfo collectInfo = new CollectInfo();
        return new Event<>();
    }

    private ScheduledExecutorService newScheduleCollectExecutor(OptionManager optionManager) {
        return new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("schedule-collect-%d")
                .build());
    }
}
