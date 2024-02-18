package com.ccsu.store.api;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class StoreConfigBuilder<K, V> {
    private final String name;
    private final Converter<K, byte[]> keyBytesConverter;
    private final Format<V> valueFormat;
    private IndexConverter<K, V> indexConverter;

    public StoreConfigBuilder(String name,
                              Converter<K, byte[]> keyBytesConverter,
                              Format<V> valueFormat) {
        this.name = requireNonNull(name);
        this.keyBytesConverter = requireNonNull(keyBytesConverter);
        this.valueFormat = requireNonNull(valueFormat);
    }

    public StoreConfigBuilder<K, V> indexConverter(IndexConverter<K, V> indexConverter) {
        this.indexConverter = indexConverter;
        return this;
    }

    public StoreConfig<K, V> build() {
        return new StoreConfigImpl<>(name, keyBytesConverter, valueFormat, indexConverter);
    }

    private static class StoreConfigImpl<K, V>
            implements StoreConfig<K, V> {
        private final String name;
        private final Converter<K, byte[]> keyBytesConverter;
        private final Format<V> valueFormat;
        private final IndexConverter<K, V> indexConverter;

        StoreConfigImpl(String name,
                               Converter<K, byte[]> keyBytesConverter,
                               Format<V> valueFormat,
                               @Nullable IndexConverter<K, V> indexConverter) {
            this.name = requireNonNull(name);
            this.keyBytesConverter = requireNonNull(keyBytesConverter);
            this.valueFormat = requireNonNull(valueFormat);
            this.indexConverter = indexConverter;
        }

        @Override
        public String name() {
            return name;
        }

        @Nullable
        @Override
        public Converter<K, byte[]> keyBytesConverter() {
            return keyBytesConverter;
        }

        @Nullable
        @Override
        public Format<V> valueFormat() {
            return valueFormat;
        }

        @Nullable
        @Override
        public IndexConverter<K, V> indexConverter() {
            return indexConverter;
        }
    }
}
