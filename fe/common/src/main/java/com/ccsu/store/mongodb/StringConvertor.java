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

public class StringConvertor<K>
        implements Converter<K, String> {
    private Converter<K, byte[]> kConverter;

    public StringConvertor(Converter<K, byte[]> kConverter) {
        this.kConverter = kConverter;
    }

    @Override
    public String convert(K k) {
        return Converter.STRING_UTF_8.revert(kConverter.convert(k));
    }

    @Override
    public K revert(String s) {
        return kConverter.revert(Converter.STRING_UTF_8.convert(s));
    }
}
