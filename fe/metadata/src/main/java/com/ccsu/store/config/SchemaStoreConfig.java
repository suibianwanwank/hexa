package com.ccsu.store.config;

import com.ccsu.datastore.api.SearchTypes;
import com.ccsu.store.api.*;
import com.ccsu.store.converter.MetadataPathConverter;
import com.ccsu.store.intern.format.PojoFormat;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.SchemaInfo;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;

public class SchemaStoreConfig implements StoreConfig<MetaPath, SchemaInfo> {

    public static final IndexKey CATALOG_INDEX =
            new IndexKey("CATALOG_NAME_INDEX", String.class, false, SearchTypes.SearchFieldSorting.FieldType.STRING, true);

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
        return new IndexConverter<MetaPath, SchemaInfo>() {
            @Override
            public List<IndexKey> getIndexKeys() {
                return ImmutableList.of(CATALOG_INDEX);
            }

            @Override
            public void convert(IndexDocumentWriter writer, MetaPath key, SchemaInfo record) {
                writer.write(CATALOG_INDEX, key.getCatalogName());
            }
        };
    }
}
