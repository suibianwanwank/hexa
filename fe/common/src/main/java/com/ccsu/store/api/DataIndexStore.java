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

/**
 * the store support complex query
 *
 * @param <K>
 * @param <V>
 */
public interface DataIndexStore<K, V>
        extends DataStore<K, V> {
    /**
     * Find entity by condition. Extended the ability of secondary index on kv.
     *
     * @param findByCondition
     * @return
     */
    Iterable<? extends EntityWithTag<K, V>> find(FindByCondition findByCondition);
}
