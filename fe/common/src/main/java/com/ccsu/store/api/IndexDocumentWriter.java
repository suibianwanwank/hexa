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

public interface IndexDocumentWriter {
    void write(IndexKey key, @Nullable String values);

    /**
     * Add the provided long values to the index.
     *
     * @param key   index key.
     * @param value values to be indexed.
     */
    void write(IndexKey key, @Nullable Long value);

    /**
     * Add the provided double values to the index.
     *
     * @param key   index key.
     * @param value values to be indexed.
     */
    void write(IndexKey key, @Nullable Double value);

    /**
     * Add the provided integer values to the index.
     *
     * @param key   index key.
     * @param value values to be indexed.
     */
    void write(IndexKey key, @Nullable Integer value);

    void write(IndexKey key, @Nullable byte[] value);
}
