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
public interface FindByRange<K> {
    /**
     * Get the starting range.
     *
     * @return starting range of type K. Returns {@code null} if start range is null.
     */
    @Value.Default
    @Nullable
    default K getStart() {
        return null;
    }

    /**
     * Get the ending range.
     *
     * @return ending range of type K. Returns {@code null} if end range is null.
     */
    @Value.Default
    @Nullable
    default K getEnd() {
        return null;
    }

    /**
     * Indicates whether the starting range is inclusive.
     *
     * @return true if starting range is inclusive, false otherwise.
     */
    @Value.Default
    default boolean isStartInclusive() {
        return false;
    }

    /**
     * Indicates whether the ending range is inclusive.
     *
     * @return true if ending range is inclusive, false otherwise.
     */
    @Value.Default
    default boolean isEndInclusive() {
        return false;
    }
}
