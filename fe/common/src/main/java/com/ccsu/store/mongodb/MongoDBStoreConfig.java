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

public class MongoDBStoreConfig<K, V> {
    private final String name;

    private final Converter<K, byte[]> keyConverter;

    private final Converter<V, String> valueConverter;

    public MongoDBStoreConfig(String name,
                              Converter<K, byte[]> keyConverter,
                              Converter<V, String> valueConverter) {
        this.name = name;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    public String getName() {
        return name;
    }

    public Converter<K, byte[]> getKeyConverter() {
        return keyConverter;
    }

    public Converter<V, String> getValueConverter() {
        return valueConverter;
    }
}
