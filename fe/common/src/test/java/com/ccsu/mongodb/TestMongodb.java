package com.ccsu.mongodb;

import com.ccsu.store.api.Converter;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.IndexConverter;
import com.ccsu.store.api.IndexDocumentWriter;
import com.ccsu.store.api.IndexKey;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.api.StoreManager;
import com.ccsu.store.mongodb.MongoDBManager;
import com.ccsu.store.mongodb.MongoDBStoreManagerConfig;
import com.google.common.collect.ImmutableList;
import org.bson.Document;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.List;

public class TestMongodb {
    private final StoreManager mongoDBManager;

    {
        MongoDBStoreManagerConfig config = new MongoDBStoreManagerConfig();
        config.setUri("mongodb://aloudata:aloudata123@10.161.241.125:27017/?authSource=admin&");
        config.setDatabase("xiaohei_test");
        mongoDBManager = new MongoDBManager(config);
        mongoDBManager.start();
    }

    private StoreConfig<String, Document> newStoreConfig(String name) {
        return new StoreConfig<String, Document>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Converter<String, byte[]> keyBytesConverter() {
                return Converter.STRING_UTF_8;
            }

            @Override
            public Format<Document> valueFormat() {
                return Format.ofPojo(Document.class);
            }

            @Override
            public IndexConverter<String, Document> indexConverter() {
                IndexKey username = IndexKey.newBuilder("username", String.class).build();
                return new IndexConverter<String, Document>() {
                    @Override
                    public List<IndexKey> getIndexKeys() {
                        return ImmutableList.of(username);
                    }

                    @Override
                    public void convert(IndexDocumentWriter writer, String key, Document record) {
                        writer.write(username, record.getString("name"));
                    }
                };
            }
        };
    }
    @Test
    @Ignore
    public void test1() {
        DataStore<String, Document> dataStore = mongoDBManager.getOrCreateDataStore(newStoreConfig("config1"));
        dataStore.put("user", new Document().append("name", "user"));
        dataStore.put("catalog", new Document().append("name", "catalog"));
        EntityWithTag<String, Document> catalog = dataStore.get("catalog");
        System.out.println(catalog);
    }
}