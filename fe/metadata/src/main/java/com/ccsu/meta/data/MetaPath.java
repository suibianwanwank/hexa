package com.ccsu.meta.data;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class MetaPath {

    private final List<String> path;

    private final PathType pathType;

    public static MetaPath buildCatalogPath(String catalogName) {
        return new MetaPath(Collections.singletonList(catalogName), PathType.CATALOG);
    }

    public static MetaPath buildSchemaPath(String catalogName, String schemaName) {
        return new MetaPath(Arrays.asList(catalogName, schemaName), PathType.SCHEMA);
    }

    public static MetaPath buildTablePath(String catalogName, String schemaName, String tableName) {
        return new MetaPath(Arrays.asList(catalogName, schemaName, tableName), PathType.TABLE);
    }

    public MetaPath(List<String> path, PathType pathType) {
        this.path = path;
        this.pathType = pathType;
    }

    public String getCatalogName() {
        if (path.isEmpty()) {
            return null;
        }
        return path.get(0);
    }

    public String getSchemaName() {
        if (path == null || path.size() <= 1) {
            return null;
        }
        return path.get(1);
    }

    public String getTableName() {
        if (path == null || path.size() <= 2) {
            return null;
        }
        return path.get(2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetaPath oPath = (MetaPath) o;
        return Objects.equals(path, getPath())
                && pathType == oPath.getPathType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, pathType);
    }

    @Override
    public String toString() {
        return String.join(".", path);
    }

    public enum PathType {
        SCHEMA, TABLE, CATALOG,
    }
}
