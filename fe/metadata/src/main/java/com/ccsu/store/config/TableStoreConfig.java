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
import com.ccsu.meta.data.TableInfo;

import javax.annotation.Nullable;
import java.util.List;

public class TableStoreConfig implements StoreConfig<MetaPath, TableInfo> {

    public static final String CATALOG_INDEX = "catalog_index";

    public TableStoreConfig() {
    }

    @Override
    public String name() {
        return "table";
    }

    @Override
    public Converter<MetaPath, byte[]> keyBytesConverter() {
        return MetadataPathConverter.INSTANCE;
    }

    @Override
    public Format<TableInfo> valueFormat() {
        return new PojoFormat<>(TableInfo.class);
    }

    @Nullable
    @Override
    public IndexConverter<MetaPath, TableInfo> indexConverter() {
        return new IndexConverter<MetaPath, TableInfo>() {
            @Override
            public List<IndexKey> getIndexKeys() {
                return null;
            }

            @Override
            public void convert(IndexDocumentWriter writer, MetaPath key, TableInfo record) {
                writer.write(null, record.getCatalogName());
            }
        };
    }
}
