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

import com.ccsu.store.api.DataStore;

public interface StoreMetadataCommitter {
    void commit(String storeName);

    void replay(ReplayHandler replayHandler);

    void allowCommit();

    boolean canCommit();

    /**
     * Sink implementation.
     */
    StoreMetadataCommitter NO_OP = new StoreMetadataCommitter() {
        @Override
        public void commit(String storeName) {
        }

        @Override
        public void replay(ReplayHandler replayHandler) {
        }

        @Override
        public void allowCommit() {
        }

        @Override
        public boolean canCommit() {
            return false;
        }
    };

    interface ReplayHandler {
        <K, V> void put(DataStore<K, V> dataStore, K key, V value);

        <K, V> void delete(DataStore<K, V> dataStore, K key);
    }
}
