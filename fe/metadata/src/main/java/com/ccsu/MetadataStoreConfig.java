package com.ccsu;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.store.api.Converter;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.IndexConverter;
import com.ccsu.store.api.IndexDocumentWriter;
import com.ccsu.store.api.IndexKey;
import com.ccsu.store.api.StoreConfig;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

public class MetadataStoreConfig {

    private MetadataStoreConfig() {

    }

    private static final String CATALOG_CONFIG_NAME = "CATALOG_CONFIG";

    public static final IndexKey CREATE_TIME = IndexKey.newBuilder("createTime", Long.class)
            .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
            .build();

    public static final IndexKey CLUSTER_KEY = IndexKey.newBuilder("clusterId", String.class)
            .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
            .build();

    public static final StoreConfig<String, DatasourceConfig> CATALOG_CONFIG_CONFIG =
            new StoreConfig<String, DatasourceConfig>() {

                @Override
                public String name() {
                    return CATALOG_CONFIG_NAME;
                }

                @Override
                public Converter<String, byte[]> keyBytesConverter() {
                    return Converter.STRING_UTF_8;
                }

                @Override
                public Format<DatasourceConfig> valueFormat() {
                    return new Format<DatasourceConfig>() {
                        @Override
                        public Converter<DatasourceConfig, byte[]> getBytesConverter() {
                            return Converter.ofPojo(DatasourceConfig.class);
                        }

                        @Nullable
                        @Override
                        public Converter<DatasourceConfig, String> getJsonConverter() {
                            return null;
                        }

                        @Override
                        public Class<DatasourceConfig> getType() {
                            return DatasourceConfig.class;
                        }
                    };
                }

                @Nullable
                @Override
                public IndexConverter<String, DatasourceConfig> indexConverter() {
                    return new IndexConverter<String, DatasourceConfig>() {
                        @Override
                        public List<IndexKey> getIndexKeys() {
                            return Lists.newArrayList();
                        }

                        @Override
                        public void convert(IndexDocumentWriter writer,
                                            String key,
                                            DatasourceConfig record) {
                            writer.write(CREATE_TIME, System.currentTimeMillis());
                            String[] split = key.split("#");
                            writer.write(CLUSTER_KEY, split[0]);
                        }
                    };
                }

            };
}
