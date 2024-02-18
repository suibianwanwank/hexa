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

import com.ccsu.store.api.IndexConverter;
import com.ccsu.store.api.IndexDocumentWriter;
import com.ccsu.store.api.IndexKey;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Preconditions;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoIndexConvertUtil {
    private static final Logger LOGGER = Logger.get(MongoDBStore.class);

    private static final String MONGO_INDEX_FIELD_PREFIX = "_idx_%s";

    private MongoIndexConvertUtil() {
    }

    public static String formatIndexFieldName(String fieldName) {
        return String.format(MONGO_INDEX_FIELD_PREFIX, fieldName);
    }

    public static <K, V> void writeIndexToDocument(K key,
                                                   V value,
                                                   BsonDocument document,
                                                   IndexConverter<K, V> indexConverter) {
        if (indexConverter == null) {
            return;
        }
        indexConverter.convert(new BsonIndexDocumentWriter(document), key, value);
    }

    public static <T> void initIndex(MongoCollection<Document> collection, IndexConverter<?, T> indexConverter) {
        // build index if necessary
        if (null == indexConverter) {
            return;
        }
        List<IndexKey> indexKeys = indexConverter.getIndexKeys();
        ListIndexesIterable<Document> documents = collection.listIndexes();
        Set<String> existIndexes = new HashSet<>();
        for (Document document : documents) {
            String key = document.get("key", Document.class).keySet().iterator().next();
            existIndexes.add(key);
        }
        List<String> newIndex = new ArrayList<>();
        for (IndexKey indexKey : indexKeys) {
            String indexedField = formatIndexFieldName(indexKey.getIndexFieldName());
            if (!existIndexes.contains(indexedField)) {
                IndexOptions indexOptions = new IndexOptions();
                collection.createIndex(Indexes.ascending(indexedField), indexOptions);
                newIndex.add(indexedField);
            }
        }
        LOGGER.info("build index for collection: %s , old index: %s , new index: %s",
                collection.getNamespace(), existIndexes, newIndex);
    }

    public static class BsonIndexDocumentWriter
            implements IndexDocumentWriter {
        private final BsonDocument document;

        public BsonIndexDocumentWriter(BsonDocument document) {
            this.document = document;
        }

        @Override
        public void write(IndexKey key, @Nullable String value) {
            if (value == null || "".equals(value)) {
                return;
            }
            Preconditions.checkArgument(key.getValueType() == String.class);
            String fieldKey = formatIndexFieldName(key.getIndexFieldName());
            if (key.canContainMultipleValues()) {
                appendMultiValue(fieldKey, new BsonString(value));
            } else {
                Preconditions.checkState(!document.containsKey(fieldKey),
                        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
                document.append(fieldKey, new BsonString(value));
            }
        }

        @Override
        public void write(IndexKey key, @Nullable Long value) {
            if (value == null) {
                return;
            }
            Preconditions.checkArgument(key.getValueType() == Long.class);
            String fieldKey = formatIndexFieldName(key.getIndexFieldName());
            if (key.canContainMultipleValues()) {
                appendMultiValue(fieldKey, new BsonInt64(value));
            } else {
                Preconditions.checkState(!document.containsKey(fieldKey),
                        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
                document.append(fieldKey, new BsonInt64(value));
            }
        }

        @Override
        public void write(IndexKey key, @Nullable Double value) {
            if (value == null) {
                return;
            }
            Preconditions.checkArgument(key.getValueType() == Double.class);
            String fieldKey = formatIndexFieldName(key.getIndexFieldName());
            if (key.canContainMultipleValues()) {
                appendMultiValue(fieldKey, new BsonDouble(value));
            } else {
                Preconditions.checkState(!document.containsKey(fieldKey),
                        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
                document.append(fieldKey, new BsonDouble(value));
            }
        }

        @Override
        public void write(IndexKey key, @Nullable Integer value) {
            if (value == null) {
                return;
            }
            Preconditions.checkArgument(key.getValueType() == Integer.class);
            String fieldKey = formatIndexFieldName(key.getIndexFieldName());
            if (key.canContainMultipleValues()) {
                appendMultiValue(fieldKey, new BsonInt32(value));
            } else {
                Preconditions.checkState(!document.containsKey(fieldKey),
                        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
                document.append(fieldKey, new BsonInt32(value));
            }
        }

        @Override
        public void write(IndexKey key, @Nullable byte[] value) {
            if (value == null) {
                return;
            }
            Preconditions.checkArgument(key.getValueType() == String.class);
            String fieldKey = formatIndexFieldName(key.getIndexFieldName());
            if (key.canContainMultipleValues()) {
                appendMultiValue(fieldKey, new BsonString(new String(value)));
            } else {
                Preconditions.checkState(!document.containsKey(fieldKey),
                        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
                document.append(fieldKey, new BsonString(new String(value)));
            }
        }

        private void appendMultiValue(String fieldKey, BsonValue bsonValue) {
            BsonArray array = document.getArray(fieldKey, new BsonArray());
            array.add(bsonValue);
            document.put(fieldKey, array);
        }
    }
}
