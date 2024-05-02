/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ccsu.store.mongodb;

import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.api.StoreManager;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.facebook.airlift.log.Logger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.inject.Inject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

public class MongoDBManager
        implements StoreManager {
    private static final Logger LOGGER = Logger.get(MongoDBManager.class);

    private final MongoDBStoreManagerConfig config;

    private MongoDatabase mongoDatabase;

    private MongoClient mongoClient;

    private final Cache<String, DataStore> mongoDBStoreMaps = CacheBuilder.newBuilder()
            .maximumSize(10)
            .removalListener((RemovalListener<String, DataStore>) notification -> {
                try {
                    notification.getValue().close();
                } catch (Exception ex) {
                    LOGGER.error(ex, "store close error");
                }
            }).build();

    private <K, V> MongoStoreWrapper<K, V> newStore(StoreConfig<K, V> storeConfig) {
        MongoDBStore mongoDBStore = new MongoDBStore(mongoDatabase, storeConfig);
        return new MongoStoreWrapper<>(
                mongoDBStore,
                new StringConvertor<>(storeConfig.keyBytesConverter()),
                new BsonConvertor<>(mongoDatabase.getCodecRegistry(), storeConfig),
                storeConfig);
    }

    @Inject
    public MongoDBManager(MongoDBStoreManagerConfig config) {
        this.config = requireNonNull(config);
    }

    @Override
    @PostConstruct
    public void start() {
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .codecRegistry(CodecRegistryProvider.get());
        this.mongoClient = new MongoClient(new MongoClientURI(config.getUri(), optionsBuilder));
        this.mongoDatabase = this.mongoClient.getDatabase(config.getDatabase());
    }

    @Override
    public <K, V> DataStore getOrCreateDataStore(StoreConfig<K, V> storeConfig) {
        requireNonNull(storeConfig);
        requireNonNull(storeConfig.name());
        try {
            return mongoDBStoreMaps.get(storeConfig.name(), () -> newStore(storeConfig));
        } catch (ExecutionException e) {
            throw new CommonException(CommonErrorCode.INDEX_STORE_ERROR, e.getMessage());
        }
    }

    @Override
    public <K, V> DataIndexStore<K, V> getOrCreateDataIndexStore(StoreConfig<K, V> storeConfig) {
        return (DataIndexStore<K, V>) getOrCreateDataStore(storeConfig);
    }

    @Override
    public <K, V> DataStore<K, V> getDataStore(String name) {
        requireNonNull(name);
        return mongoDBStoreMaps.getIfPresent(name);
    }

    @Override
    public <K, V> DataIndexStore<K, V> getDataIndexStore(String name) {
        return (DataIndexStore<K, V>) mongoDBStoreMaps.getIfPresent(name);
    }

    @Override
    public StoreMetadataCommitter getStoreMetaCommitter() {
        return StoreMetadataCommitter.NO_OP;
    }

    @Override
    @PreDestroy
    public void close() {
        this.mongoDBStoreMaps.invalidateAll();
        this.mongoClient.close();
    }
}
