package com.ccsu.meta;

import com.ccsu.meta.service.QueryMetadataManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class ChainCalciteSchema extends CalciteSchema {
    private final QueryMetadataManager metadata;
    private final List<String> schemaPath;

    public ChainCalciteSchema(QueryMetadataManager metadata, List<String> schemaPath) {
        super(
                null,
                null,
                schemaPath.isEmpty() ? "" : schemaPath.get(schemaPath.size() - 1),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        this.metadata = metadata;
        this.schemaPath = schemaPath;
    }


    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        List<String> path = ImmutableList.copyOf(Iterables.concat(schemaPath, ImmutableList.of(schemaName)));
        if (metadata.existSchema(path, caseSensitive)) {
            return new ChainCalciteSchema(metadata, path);
        }
        return null;
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        List<String> tablePath = ImmutableList.copyOf(Iterables.concat(schemaPath, ImmutableList.of(tableName)));
        ExtendTranslateTable table = metadata.getTable(tablePath, caseSensitive);
        if (table == null) {
            return null;
        }
        return new TableEntryImpl(this, tableName, table, ImmutableList.of());
    }

    @Override
    protected @Nullable TypeEntry getImplicitType(String s, boolean b) {
        return null;
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(String s, boolean b) {
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {

    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {

    }

    @Override
    protected void addImplicitFunctionsToBuilder(ImmutableList.Builder<Function> builder, String s, boolean b) {

    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {

    }

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {

    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
            ImmutableSortedMap.Builder<String, Table> builder) {

    }

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema calciteSchema, SchemaVersion schemaVersion) {
        return null;
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

    @Override
    public void setCache(boolean b) {

    }

    @Override
    public CalciteSchema add(String s, Schema schema) {
        return null;
    }

    @Override
    public List<String> path(@Nullable String name) {
        final List<String> list = new ArrayList<>();
        if (name != null) {
            list.add(name);
        }
        if (schemaPath != null && !schemaPath.isEmpty()) {
            for (int i = schemaPath.size() - 1; i >= 0; i--) {
                list.add(schemaPath.get(i));
            }
        }
        return ImmutableList.copyOf(Lists.reverse(list));
    }
}
