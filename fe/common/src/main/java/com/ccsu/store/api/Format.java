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


import com.ccsu.store.intern.format.PojoFormat;
import com.ccsu.store.intern.format.ProtobufFormat;
import com.ccsu.store.intern.format.ProtostuffFormat;

import javax.annotation.Nullable;

public interface Format<T> {
    @Nullable
    Converter<T, byte[]> getBytesConverter();

    @Nullable
    Converter<T, String> getJsonConverter();

    Class<T> getType();

    static <E extends com.google.protobuf.Message> Format<E> ofProtobufFormat(Class<E> e) {
        return new ProtobufFormat<>(e);
    }

    static <E extends io.protostuff.Message<E>> Format<E> ofProtostuffFormat(Class<E> e) {
        return new ProtostuffFormat<>(e);
    }

    static <E> Format<E> ofPojo(Class<E> e) {
        return new PojoFormat<>(e);
    }
}
