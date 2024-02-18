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

package com.ccsu.store.intern.format;


import com.ccsu.store.api.Converter;
import com.ccsu.store.api.Format;
import com.ccsu.utils.ProtostuffUtils;
import io.protostuff.Schema;

import javax.annotation.Nullable;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class PojoFormat<T>
        implements Format<T> {
    private final Class<T> clazz;
    private final Schema<T> schema;

    public PojoFormat(Class<T> clazz) {
        this.clazz = clazz;
        this.schema = requireNonNull(ProtostuffUtils.getSchema(clazz));
    }

    @Nullable
    @Override
    public Converter<T, byte[]> getBytesConverter() {
        return Converter.ofProtostuff(schema);
    }

    @Nullable
    @Override
    public Converter<T, String> getJsonConverter() {
        return new Converter.NonNullConverter<>(new Converter<T, String>() {
            @Override
            public String convert(T t) {
                return ProtostuffUtils.toJSON(t, schema, false);
            }

            @Override
            public T revert(String s) {
                try {
                    return ProtostuffUtils.fromJSON(s, schema, false);
                } catch (IOException e) {
                    throw new IllegalArgumentException(s, e);
                }
            }
        });
    }

    @Override
    public Class<T> getType() {
        return this.clazz;
    }
}
