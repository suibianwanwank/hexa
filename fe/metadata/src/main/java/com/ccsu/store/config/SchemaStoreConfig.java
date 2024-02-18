package com.ccsu.store.config;

import com.ccsu.store.api.Converter;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.IndexConverter;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.converter.MetadataPathConverter;
import com.ccsu.store.intern.format.PojoFormat;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.SchemaInfo;

import javax.annotation.Nullable;

public class SchemaStoreConfig implements StoreConfig<MetaPath, SchemaInfo> {
    public SchemaStoreConfig() {
    }

    @Override
    public String name() {
        return "schema";
    }

    @Override
    public Converter<MetaPath, byte[]> keyBytesConverter() {
        return MetadataPathConverter.INSTANCE;
    }

    @Override
    public Format<SchemaInfo> valueFormat() {
        return new PojoFormat<>(SchemaInfo.class);
    }

    @Nullable
    @Override
    public IndexConverter<MetaPath, SchemaInfo> indexConverter() {
        return null;
    }
}
