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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Ascii;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ProtobufUtils {
    private ProtobufUtils() {
    }

    private static final ObjectMapper MAPPER = newMapper()
            .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

    public static <T> Parser<T> parser(Class<T> clazz) {
        try {
            Method defaultInstanceGetter = clazz.getDeclaredMethod("getDefaultInstance");
            com.google.protobuf.Message defaultInst =
                    (com.google.protobuf.Message) defaultInstanceGetter.invoke(null);
            return (Parser<T>) defaultInst.getParserForType();
        } catch (NoSuchMethodException
                 | IllegalAccessException
                 | InvocationTargetException e) {
            throw new IllegalArgumentException("Unable to get the parser for " + clazz.getName(), e);
        }
    }

    public static final ObjectMapper newMapper() {
        return new ObjectMapper()
                // Reproduce Protostuff configuration
                .enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
                .enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)
                .disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS)
                .setPropertyNamingStrategy(PROTOBUF_TO_CAMEL_STRATEGY)
                .registerModule(new ProtobufModule());
    }

    @SuppressWarnings("serial")
    public static final PropertyNamingStrategy PROTOBUF_TO_CAMEL_STRATEGY =
            new PropertyNamingStrategy.PropertyNamingStrategyBase() {
                @Override
                public String translate(String propertyName) {
                    if (propertyName == null || propertyName.isEmpty()) {
                        return propertyName;
                    }

                    // Follow protobuf algorithm described at
                    // https://developers.google.com/protocol-buffers/docs/reference/java-generated#fields
                    final StringBuilder buffer = new StringBuilder(propertyName.length());
                    buffer.append(Ascii.toLowerCase(propertyName.charAt(0)));
                    boolean toCapitalize = false;
                    for (int i = 1; i < propertyName.length(); i++) {
                        char c = propertyName.charAt(i);
                        if (c == '_') {
                            toCapitalize = true;
                            continue;
                        }

                        if (toCapitalize) {
                            buffer.append(Ascii.toUpperCase(c));
                            toCapitalize = false;
                        } else {
                            buffer.append(c);
                        }
                    }
                    return buffer.toString();
                }
            };

    public static final <M extends MessageOrBuilder> String toJSONString(
            M value) throws IOException {
        return MAPPER.writeValueAsString(value);
    }

    public static final <M extends MessageOrBuilder> Map<String, Object> toMap(M value) {
        return MAPPER.convertValue(value, new TypeReference<Map<String, Object>>() {
        });
    }

    public static final <M> M fromMap(Class<M> clazz, Map value) {
        return MAPPER.convertValue(value, clazz);
    }

    public static final <M extends MessageOrBuilder> M fromJSONString(
            Class<M> clazz, String json) throws IOException {
        return MAPPER.readValue(json, clazz);
    }
}
