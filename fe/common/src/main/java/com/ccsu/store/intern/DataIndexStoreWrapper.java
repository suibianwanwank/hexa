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
package com.ccsu.store.intern;


import com.ccsu.store.api.Converter;
import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.FindByCondition;
import com.ccsu.store.api.FindByRange;
import com.ccsu.store.api.ImmutableEntityWithTag;
import com.ccsu.store.api.ImmutableFindByRange;
import com.ccsu.store.api.StoreConfig;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Datastore wrapper which convert key and value to store the underlying datastore.
 *
 * @param <K> key
 * @param <V> value
 * @param <T> converted key
 * @param <U> converted value
 */

public class DataIndexStoreWrapper<K, V, T, U>
        implements DataIndexStore<K, V> {
    private static final Logger LOGGER = Logger.get(DataIndexStoreWrapper.class);

    private final DataIndexStore<T, U> dataIndexStore;
    private final Converter<K, T> keyConverter;
    private final Converter<V, U> valueConverter;
    private final StoreConfig<K, V> storeConfig;

    public DataIndexStoreWrapper(final DataIndexStore<T, U> dataIndexStore,
                                 @Nullable final Converter<K, T> keyConverter,
                                 @Nullable final Converter<V, U> valueConverter,
                                 final StoreConfig<K, V> storeConfig) {
        this.dataIndexStore = requireNonNull(dataIndexStore);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.storeConfig = storeConfig;
    }

    @Override
    @Nullable
    public EntityWithTag<K, V> get(K key) {
        requireNonNull(key);
        T keyBytes = keyConverter.convert(key);
        EntityWithTag<T, U> entity = dataIndexStore.get(keyBytes);
        if (entity == null) {
            return null;
        }
        return convert(entity);
    }

    @Override
    public Iterable<EntityWithTag<K, V>> get(List<K> keys) {
        requireNonNull(keys);
        List<T> dataIndexStoreKeys = new ArrayList<>(keys.size());
        for (K key : keys) {
            dataIndexStoreKeys.add(keyConverter.convert(key));
        }
        Iterable<EntityWithTag<T, U>> entities = (Iterable<EntityWithTag<T, U>>) dataIndexStore.get(dataIndexStoreKeys);
        return Iterables.transform(entities, this::convert);
    }

    @Override
    public boolean contains(K key) {
        requireNonNull(key);
        T dataIndexStoreKey = keyConverter.convert(key);
        return dataIndexStore.contains(dataIndexStoreKey);
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        T dataIndexStoreKey = keyConverter.convert(key);
        U dataIndexStoreValue = valueConverter.convert(value);
        dataIndexStore.put(dataIndexStoreKey, dataIndexStoreValue);
    }

    @Override
    public void delete(K key) {
        requireNonNull(key);
        T dataIndexStoreKey = keyConverter.convert(key);
        dataIndexStore.delete(dataIndexStoreKey);
    }

    @Override
    public Iterable<EntityWithTag<K, V>> find() {
        Iterable<? extends EntityWithTag<T, U>> entities = dataIndexStore.find();
        return Iterables.transform(entities, this::convert);
    }

    @Override
    public Iterable<EntityWithTag<K, V>> find(FindByRange<K> findByRange) {
        requireNonNull(findByRange);
        ImmutableFindByRange.Builder<T> findByByteRange = ImmutableFindByRange.builder();
        K start = findByRange.getStart();
        if (start != null) {
            findByByteRange.start(keyConverter.convert(start)).isStartInclusive(findByRange.isStartInclusive());
        }
        K end = findByRange.getEnd();
        if (end != null) {
            findByByteRange.end(keyConverter.convert(end)).isEndInclusive(findByRange.isEndInclusive());
        }
        return Iterables.transform(dataIndexStore.find(findByByteRange.build()), this::convert);
    }

    @Override
    public String validateTagThenPut(K key, V value, @Nullable String currentTag) {
        requireNonNull(key);
        requireNonNull(value);
        T dataIndexStoreKey = keyConverter.convert(key);
        U dataIndexStoreValue = valueConverter.convert(value);
        return dataIndexStore.validateTagThenPut(dataIndexStoreKey, dataIndexStoreValue, currentTag);
    }

    @Override
    public void validateTagThenDelete(K key, String currentTag) {
        requireNonNull(key);
        requireNonNull(currentTag);
        T dataIndexStoreKey = keyConverter.convert(key);
        dataIndexStore.validateTagThenDelete(dataIndexStoreKey, currentTag);
    }

    @Override
    public StoreConfig<K, V> getStoreConfig() {
        return storeConfig;
    }

    public DataIndexStore<T, U> getDataIndexStore() {
        return dataIndexStore;
    }

    public Converter<K, T> getKeyConverter() {
        return keyConverter;
    }

    public Converter<V, U> getValueConverter() {
        return valueConverter;
    }

    @Override
    public Iterable<? extends EntityWithTag<K, V>> find(FindByCondition findByCondition) {
        requireNonNull(findByCondition);
        Iterable<? extends EntityWithTag<T, U>> entities = dataIndexStore.find(findByCondition);
        return Iterables.transform(entities, this::convert);
    }

    private EntityWithTag<K, V> toEntityWithTag(K key, V value, String tag) {
        return ImmutableEntityWithTag.<K, V>builder().key(key).value(value).tag(tag).build();
    }

    private EntityWithTag<K, V> convert(EntityWithTag<T, U> entity) {
        V value = valueConverter.revert(entity.getValue());
        K key = keyConverter.revert(entity.getKey());
        return toEntityWithTag(key, value, entity.getTag());
    }

    @Override
    public void close() throws Exception {
//        AutoCloseables.close(dataIndexStore);
    }
}
