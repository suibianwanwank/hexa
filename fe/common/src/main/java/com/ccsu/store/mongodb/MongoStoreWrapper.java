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
import com.ccsu.store.intern.DataIndexStoreWrapper;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class MongoStoreWrapper<K, V> extends DataIndexStoreWrapper<K, V, String, Bson> {
    private final CodecRegistry codecRegistry;

    public MongoStoreWrapper(MongoDBStore dataIndexStore,
                             @Nullable Converter<K, String> keyConverter,
                             @Nullable Converter<V, Bson> valueConverter,
                             StoreConfig<K, V> storeConfig) {
        super(dataIndexStore, keyConverter, valueConverter, storeConfig);
        this.codecRegistry = dataIndexStore.getCollection().getCodecRegistry();
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        String mKey = this.getKeyConverter().convert(key);
        Bson mValue = this.getValueConverter().convert(value);
        MongoIndexConvertUtil.writeIndexToDocument(key, value,
                mValue.toBsonDocument(BsonDocument.class, this.codecRegistry),
                this.getStoreConfig().indexConverter());
        this.getDataIndexStore().put(mKey, mValue);
    }

    @Override
    public String validateTagThenPut(K key, V value, @Nullable String currentTag) {
        requireNonNull(key);
        requireNonNull(value);
        String mKey = this.getKeyConverter().convert(key);
        Bson mValue = this.getValueConverter().convert(value);
        MongoIndexConvertUtil.writeIndexToDocument(key, value,
                mValue.toBsonDocument(BsonDocument.class, this.codecRegistry),
                this.getStoreConfig().indexConverter());
        return this.getDataIndexStore().validateTagThenPut(mKey, mValue, currentTag);
    }
}
