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
import com.ccsu.utils.ProtobufUtils;
import com.google.protobuf.Parser;

import javax.annotation.Nullable;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ProtobufFormat<T extends com.google.protobuf.Message> implements Format<T> {
    private final Class<T> clazz;
    private final Parser<T> parser;

    public ProtobufFormat(Class<T> clazz) {
        this.clazz = clazz;
        this.parser = requireNonNull(ProtobufUtils.parser(clazz));
    }

    @Override
    @Nullable
    public Converter<T, byte[]> getBytesConverter() {
        return Converter.ofProtobuf(parser);
    }

    @Override
    @Nullable
    public Converter<T, String> getJsonConverter() {
        return new Converter.NonNullConverter<>(new Converter<T, String>() {
            @Override
            public String convert(T t) {
                try {
                    return ProtobufUtils.toJSONString(t);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            @Override
            public T revert(String s) {
                try {
                    return ProtobufUtils.fromJSONString(clazz, s);
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
