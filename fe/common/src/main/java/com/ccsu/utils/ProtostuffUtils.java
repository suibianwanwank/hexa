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

package com.ccsu.utils;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ProtostuffUtils {

    private static final int DEFAULT_ALLOCATE_SIZE = 256;
    private static final ThreadLocal<LinkedBuffer> TL =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(DEFAULT_ALLOCATE_SIZE));
    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();

    private ProtostuffUtils() {
    }


    public static <T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>) SCHEMA_CACHE.get(clazz);
        if (schema == null) {
            schema = RuntimeSchema.getSchema(clazz);
            if (schema != null) {
                SCHEMA_CACHE.put(clazz, schema);
            }
        }
        return schema;
    }

    public static <T> byte[] toByteArr(Schema<T> schema, T message) {
        final LinkedBuffer buffer = TL.get();
        try {
            return ProtostuffIOUtil.toByteArray(message, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    public static <T> T toMessage(Schema<T> schema, byte[] bytes) {
        T t = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, t, schema);
        return t;
    }

    public static <T> void toJSON(OutputStream out, T message, Schema<T> schema,
                                  boolean numeric) throws IOException {
        try (JsonGenerator jsonGenerator =
                     JsonIOUtils.DEFAULT_JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)
                             .disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS)) {
            JsonIOUtils.writeTo(jsonGenerator, message, schema, numeric);
        }
    }

    public static <T> String toJSON(T message, Schema<T> schema, boolean numeric) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            toJSON(baos, message, schema, numeric);
            return new String(baos.toByteArray(), UTF_8);
        } catch (IOException e) {
            throw new AssertionError("IOException not expected with ByteArrayOutputStream", e);
        }
    }

    public static <T> T fromJSON(String data, Schema<T> schema, boolean numeric) throws IOException {
        T message = schema.newMessage();
        fromJSON(data.getBytes(UTF_8), message, schema, numeric);

        return message;
    }

    public static <T> void fromJSON(byte[] data, T message, Schema<T> schema, boolean numeric)
            throws IOException {
        // Configure a parser to intepret non-numeric numbers like NaN correctly
        // although non-standard JSON.
        try (JsonParser parser = JsonIOUtils
                .newJsonParser(null, data, 0, data.length)
                .enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)) {
            JsonIOUtils.mergeFrom(parser, message, schema, numeric);
        }
    }
}
