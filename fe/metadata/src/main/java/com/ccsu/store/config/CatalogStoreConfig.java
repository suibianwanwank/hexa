package com.ccsu.store.config;

import com.ccsu.store.api.Converter;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.IndexConverter;
import com.ccsu.store.api.IndexDocumentWriter;
import com.ccsu.store.api.IndexKey;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.converter.MetadataPathConverter;
import com.ccsu.store.intern.format.PojoFormat;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.CatalogInfo;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

public class CatalogStoreConfig implements StoreConfig<MetaPath, CatalogInfo> {

    public CatalogStoreConfig() {
    }

    @Override
    public String name() {
        return "catalog";
    }

    @Override
    public Converter<MetaPath, byte[]> keyBytesConverter() {
        return MetadataPathConverter.INSTANCE;
    }

    @Override
    public Format<CatalogInfo> valueFormat() {
        return new PojoFormat<>(CatalogInfo.class);
    }

    @Nullable
    @Override
    public IndexConverter<MetaPath, CatalogInfo> indexConverter() {
        return new IndexConverter<MetaPath, CatalogInfo>() {
            @Override
            public List<IndexKey> getIndexKeys() {
                return Lists.newArrayList();
            }

            @Override
            public void convert(IndexDocumentWriter writer, MetaPath key, CatalogInfo record) {
            }
        };
    }
}
