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
package com.ccsu.store.api;

import javax.annotation.Nullable;
import java.util.List;

/**
 * data store
 *
 * @param <K> key
 * @param <V> value
 */
public interface DataStore<K, V>
        extends AutoCloseable {
    /**
     * get entity
     *
     * @param key key
     * @return entity, nullable
     */
    @Nullable
    EntityWithTag<K, V> get(K key);

    /**
     * multi get
     *
     * @param keys
     * @return
     */
    Iterable<? extends EntityWithTag<K, V>> get(List<K> keys);

    /**
     * Indicates whether contains the key
     *
     * @param key indexKey
     * @return true if the store contains the key, otherwise false
     */
    boolean contains(K key);

    /**
     * @param key
     * @param value
     */
    void put(K key, V value);

    /**
     * @param key the key remove from store
     */
    void delete(K key);

    /**
     * find all
     *
     * @return
     */
    Iterable<? extends EntityWithTag<K, V>> find();

    /**
     * findByRange between key range
     *
     * @param findByRange
     * @return
     */
    Iterable<? extends EntityWithTag<K, V>> find(FindByRange<K> findByRange);

    /**
     * Check tag then put. This is an atomic read-modify-write operation.
     *
     * @param key
     * @param value
     * @param currentTag The current tag of the data corresponding to the key in the store. If it is not equal, it will be thrown {@link ConcurrentModificationException}.
     *                   If it is null, it represents the creation operation. If the key already exists, it will also throw {@link ConcurrentModificationException}.
     * @return new tag
     */
    String validateTagThenPut(K key, V value, @Nullable String currentTag);

    /**
     * Check tag then delete. This is an atomic read-modify-write operation.
     */
    void validateTagThenDelete(K key, String currentTag);

    StoreConfig<K, V> getStoreConfig();
}
