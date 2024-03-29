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

import org.immutables.value.Value;

/**
 * the entity about key with value
 *
 * @param <K> key
 * @param <V> value
 */
@Value.Immutable
public interface Entity<K, V> {
    /**
     * get the key corresponding to this entity.
     *
     * @return key of this entity.
     */
    K getKey();

    /**
     * Get the value corresponding to this entity.
     *
     * @return value of this entity.
     */
    V getValue();
}
