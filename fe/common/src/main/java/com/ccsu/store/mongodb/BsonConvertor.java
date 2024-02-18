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
package com.ccsu.store.mongodb;

import com.ccsu.store.api.Converter;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.utils.ProtobufUtils;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonReader;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.Map;

public class BsonConvertor<T>
        implements Converter<T, Bson> {
    private final CodecRegistry codecRegistry;

    private final StoreConfig<?, T> storeConfig;

    public BsonConvertor(CodecRegistry codecRegistry, StoreConfig<?, T> storeConfig) {
        this.codecRegistry = codecRegistry;
        this.storeConfig = storeConfig;
    }

    @Override
    public Bson convert(T value) {
        if (value == null) {
            return null;
        }
        Class<T> type = storeConfig.valueFormat().getType();
        if (value instanceof MessageOrBuilder) {
            Map<String, Object> map = ProtobufUtils.toMap((MessageOrBuilder) value);
            return new BsonDocumentWrapper<>(map, codecRegistry.get(Map.class));
        }
        if (value instanceof Message) {
            String json = storeConfig.valueFormat().getJsonConverter().convert(value);
            return BsonDocument.parse(json);
        }
        return new BsonDocumentWrapper<>(value, codecRegistry.get(type));
    }

    @Override
    public T revert(Bson document) {
        if (document == null) {
            return null;
        }
        Class<T> type = storeConfig.valueFormat().getType();
        if (MessageOrBuilder.class.isAssignableFrom(type)) {
            BsonReader bsonReader = document.toBsonDocument(Map.class, codecRegistry).asBsonReader();
            Map decode = codecRegistry.get(Map.class).decode(bsonReader, DecoderContext.builder().build());
            return ProtobufUtils.fromMap(type, decode);
        }
        if (Message.class.isAssignableFrom(type)) {
            BsonDocument bsonDocument = document.toBsonDocument(Map.class, codecRegistry);
            return storeConfig.valueFormat().getJsonConverter().revert(bsonDocument.toJson(JsonWriterSettings.builder()
                    .outputMode(JsonMode.RELAXED)
                    .build()));
        }
        return toEntity(document);
    }

    private T toEntity(Bson document) {
        Class<T> type = storeConfig.valueFormat().getType();
        BsonReader bsonReader = document.toBsonDocument(type, codecRegistry).asBsonReader();
        return codecRegistry.get(type).decode(bsonReader, DecoderContext.builder().build());
    }
}
