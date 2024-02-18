package com.ccsu.store.converter;

import com.ccsu.store.api.Converter;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.serialize.FormatUtil;
import com.google.common.base.Splitter;

public class MetadataPathConverter implements Converter<MetaPath, byte[]> {

    public static final MetadataPathConverter INSTANCE = new MetadataPathConverter();

    private static final Splitter TYPE_SPLITTER = Splitter.on('#');

    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('.').trimResults();

    public MetadataPathConverter() {
    }


    @Override
    public byte[] convert(MetaPath metaPath) {
        return FormatUtil.toJsonString(metaPath).getBytes();
    }

    @Override
    public MetaPath revert(byte[] bytes) {
        return FormatUtil.fromJson(bytes, MetaPath.class);
    }
}
