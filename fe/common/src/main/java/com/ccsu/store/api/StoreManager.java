package com.ccsu.store.api;

import com.ccsu.store.mongodb.StoreMetadataCommitter;

import javax.annotation.Nullable;


public interface StoreManager extends AutoCloseable {
    void start();

    <K, V> DataStore<K, V> getOrCreateDataStore(StoreConfig<K, V> storeConfig);

    <K, V> DataIndexStore<K, V> getOrCreateDataIndexStore(StoreConfig<K, V> storeConfig);

    /**
     * replay if crashed. must after start, getOrCreateDataStore and getOrCreateDataIndexStore
     */
    default void replayIfNecessary() {
    }

    @Nullable
    <K, V> DataStore<K, V> getDataStore(String name);

    @Nullable
    <K, V> DataIndexStore<K, V> getDataIndexStore(String name);

    StoreMetadataCommitter getStoreMetaCommitter();
}
