package com.ccsu.profile;

import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.store.api.*;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

public class ProfileStoreConfig {
    private static final String PROFILE_CONFIG_NAME = "PROFILE_CONFIG";

    private ProfileStoreConfig() {
    }

    public static final StoreConfig<String, JobProfile> JOB_PROFILE_CONFIG = new StoreConfig<String, JobProfile>() {
        @Override
        public String name() {
            return PROFILE_CONFIG_NAME ;
        }

        @Override
        public Converter<String, byte[]> keyBytesConverter() {
            return Converter.STRING_UTF_8;
        }

        @Override
        public Format<JobProfile> valueFormat() {
            return Format.ofPojo(JobProfile.class);
        }

        @Nullable
        @Override
        public IndexConverter<String, JobProfile> indexConverter() {
            return new IndexConverter<String, JobProfile>() {
                @Override
                public List<IndexKey> getIndexKeys() {
                    return Lists.newArrayList();
                }

                @Override
                public void convert(IndexDocumentWriter writer, String key, JobProfile record) {

                }
            };
        }
    };
}
