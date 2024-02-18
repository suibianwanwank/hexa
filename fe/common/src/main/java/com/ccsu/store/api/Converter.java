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

import com.ccsu.utils.ProtobufUtils;
import com.ccsu.utils.ProtostuffUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import io.protostuff.Schema;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public interface Converter<T, E> {
    E convert(T t);

    T revert(E e);

    Converter<String, byte[]> STRING_UTF_8 = new NonNullConverter<>(new Converter<String, byte[]>() {
        @Override
        public byte[] convert(String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String revert(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    });

    static <T> Converter<T, byte[]> ofProtostuff(Schema<T> schema) {
        return new NonNullConverter<>(new Converter<T, byte[]>() {
            @Override
            public byte[] convert(T t) {
                return ProtostuffUtils.toByteArr(schema, t);
            }

            @Override
            public T revert(byte[] bytes) {
                return ProtostuffUtils.toMessage(schema, bytes);
            }
        });
    }

    static <T extends com.google.protobuf.Message> Converter<T, byte[]> ofProtobuf(Class<T> tClass) {
        Parser<T> parser = requireNonNull(ProtobufUtils.parser(tClass));
        return ofProtobuf(parser);
    }

    static <T extends com.google.protobuf.Message> Converter<T, byte[]> ofProtobuf(Parser<T> parser) {
        return new NonNullConverter<>(new Converter<T, byte[]>() {
            @Override
            public byte[] convert(T t) {
                return t.toByteArray();
            }

            @Override
            public T revert(byte[] bytes) {
                try {
                    return parser.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        });
    }

    static <T> Converter<T, byte[]> ofPojo(Class<T> tClass) {
        final Schema<T> schema = ProtostuffUtils.getSchema(tClass);
        return ofProtostuff(schema);
    }

    class NonNullConverter<T, E>
            implements Converter<T, E> {
        private Converter<T, E> converter;

        public NonNullConverter(Converter<T, E> converter) {
            this.converter = converter;
        }

        @Override
        public E convert(T t) {
            requireNonNull(t);
            return converter.convert(t);
        }

        @Override
        public T revert(E e) {
            requireNonNull(e);
            return converter.revert(e);
        }
    }
}
