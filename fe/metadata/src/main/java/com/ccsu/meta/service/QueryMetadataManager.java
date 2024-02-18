package com.ccsu.meta.service;

import com.ccsu.meta.ExtendTranslateTable;

import java.util.List;

public interface QueryMetadataManager {
    boolean existsCatalog(String catalogName);

    ExtendTranslateTable getTable(List<String> path, boolean caseSensitive);

    boolean existSchema(List<String> path, boolean caseSensitive);
}
