package com.ccsu.store.config;

import com.ccsu.datastore.api.SearchTypes;
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
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.K;

import javax.annotation.Nullable;
import java.util.List;

public class TableStoreConfig implements StoreConfig<MetaPath, TableInfo> {

    public static final IndexKey CATALOG_INDEX =
            new IndexKey("CATALOG_NAME_INDEX", String.class, false, SearchTypes.SearchFieldSorting.FieldType.STRING, true);

    public static final IndexKey CATALOG_AND_SCHEMA_INDEX =
            new IndexKey("CATALOG_AND_SCHEMA_INDEX", String.class, false, SearchTypes.SearchFieldSorting.FieldType.STRING, true);

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
                return Lists.newArrayList();
            }

            @Override
            public void convert(IndexDocumentWriter writer, MetaPath key, TableInfo record) {
                writer.write(CATALOG_INDEX, key.getCatalogName());
                writer.write(CATALOG_AND_SCHEMA_INDEX, String.format("%s.%s", key.getCatalogName(), key.getSchemaName()));
            }
        };
    }
}
