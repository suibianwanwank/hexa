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

import javax.annotation.Nullable;

@Value.Immutable
public interface EntityWithTag<K, V> extends Entity<K, V> {
    /**
     * Get the version tag of this entity.
     *
     * @return version tag of this entity.
     */
    @Value.Default
    @Nullable
    default String getTag() {
        return null;
    }

    static EntityWithTag<byte[], byte[]> toEntity(byte[] key, byte[] value, String tag) {
        return ImmutableEntityWithTag.<byte[], byte[]>builder().key(key)
                .value(value).tag(tag).build();
    }
}
