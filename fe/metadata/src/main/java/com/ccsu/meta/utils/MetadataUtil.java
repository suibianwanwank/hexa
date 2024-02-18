package com.ccsu.meta.utils;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaPath;

import java.util.ArrayList;
import java.util.List;

public class MetadataUtil {
    private MetadataUtil() {
    }

    public static MetaPath generateSystemPath(MetaPath identifierPath, DatasourceConfig config) {
        List<String> relPath = new ArrayList<>(identifierPath.getPath());
        relPath.set(0, config.getConfigUniqueKey());
        return new MetaPath(relPath, identifierPath.getPathType());
    }
}
