package com.ccsu.schedule;

import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.store.api.*;
import com.ccsu.store.intern.format.PojoFormat;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

public class ScheduleStoreConfig implements StoreConfig<String, MetaSchedule> {

    public static final String CONFIG_NAME = "SCHEDULE_CONFIG";
    public static final IndexKey SCHEDULE_TIME =
            new IndexKey("SCHEDULE_TIME_INDEX", Long.class, false, SearchTypes.SearchFieldSorting.FieldType.LONG, true);

    @Override
    public String name() {
        return CONFIG_NAME;
    }

    @Override
    public Converter<String, byte[]> keyBytesConverter() {
        return Converter.STRING_UTF_8;
    }

    @Override
    public Format<MetaSchedule> valueFormat() {
        return new PojoFormat<>(MetaSchedule.class);
    }

    @Nullable
    @Override
    public IndexConverter<String, MetaSchedule> indexConverter() {

        return new IndexConverter<String, MetaSchedule>() {
            @Override
            public List<IndexKey> getIndexKeys() {
                return Lists.newArrayList(SCHEDULE_TIME);
            }

            @Override
            public void convert(IndexDocumentWriter writer, String key, MetaSchedule schedule) {
                writer.write(SCHEDULE_TIME, schedule.getCycleTime());
            }
        };
    }
}
