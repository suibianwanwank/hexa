package com.ccsu.meta;

import com.ccsu.MetadataService;
import com.ccsu.meta.service.QueryMetadataManager;
import com.ccsu.meta.service.QueryMetadataManagerImpl;
import com.ccsu.meta.type.ArrowTypeMapping;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;


public class ExtendCatalogReader implements Prepare.CatalogReader {
    protected final CalciteSchema rootSchema;

    protected final RelDataTypeFactory relDataTypeFactory;

    private final QueryMetadataManager metadata;

    private final ArrowTypeMapping arrowTypeMapping;

    private final SqlNameMatcher sqlNameMatcher;

    private final List<List<String>> schemaPaths;


    public static ExtendCatalogReader create(String clusterId,
                                             RelDataTypeFactory relDataTypeFactory,
                                             MetadataService metadataService,
                                             ArrowTypeMapping arrowTypeMapping,
                                             List<String> rootPath,
                                             boolean isCaseSensitive) {
        List<List<String>> schemaPaths = Lists.newArrayList();
        if (rootPath != null && !rootPath.isEmpty()) {
            schemaPaths.add(rootPath);
        }
        schemaPaths.add(Lists.newArrayList());
        SqlNameMatcher sqlNameMatcher = SqlNameMatchers.withCaseSensitive(isCaseSensitive);
        QueryMetadataManager queryMetadataManager =
                new QueryMetadataManagerImpl(metadataService, arrowTypeMapping, clusterId);
        CalciteSchema calciteSchema = new ChainCalciteSchema(queryMetadataManager, Collections.emptyList());

        return new ExtendCatalogReader(schemaPaths,
                calciteSchema, sqlNameMatcher, queryMetadataManager, arrowTypeMapping, relDataTypeFactory);
    }

    public ExtendCatalogReader(
            List<List<String>> schemaPaths,
            CalciteSchema rootSchema,
            SqlNameMatcher sqlNameMatcher,
            QueryMetadataManager metadata,
            ArrowTypeMapping arrowTypeMapping,
            RelDataTypeFactory relDataTypeFactory) {
        this.schemaPaths = schemaPaths;
        this.rootSchema = rootSchema;
        this.metadata = metadata;
        this.sqlNameMatcher = sqlNameMatcher;
        this.arrowTypeMapping = arrowTypeMapping;
        this.relDataTypeFactory = relDataTypeFactory;
    }

    @Override
    public Prepare.@Nullable PreparingTable getTableForMember(List<String> list) {
        return this.getTable(list);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return relDataTypeFactory;
    }

    @Override
    public void registerRules(RelOptPlanner relOptPlanner) throws Exception {

    }

    @Override
    public Prepare.CatalogReader withSchemaPath(List<String> list) {
        return new ExtendCatalogReader(schemaPaths, rootSchema, sqlNameMatcher, metadata, arrowTypeMapping, relDataTypeFactory);
    }

    @Override
    public Prepare.@Nullable PreparingTable getTable(List<String> path) {
        ExtendTranslateTable table = metadata.getTable(path, sqlNameMatcher.isCaseSensitive());
        if (table == null) {
            return null;
        }
        RelDataType rowType = table.getRowType(relDataTypeFactory);
        return new ExtendPreparingTable(this, table, rowType);
    }

    @Override
    public @Nullable RelDataType getNamedType(SqlIdentifier sqlIdentifier) {
        return null;
    }

    @Override
    public List<SqlMoniker> getAllSchemaObjectNames(List<String> path) {
        final CalciteSchema schema =
                SqlValidatorUtil.getSchema(rootSchema, path, sqlNameMatcher);
        if (schema == null) {
            return ImmutableList.of();
        }

        final ImmutableList.Builder<SqlMoniker> result = new ImmutableList.Builder<>();
        if (!"".equals(schema.name)) {
            result.add(moniker(schema, null, SqlMonikerType.SCHEMA));
        }

        final Map<String, CalciteSchema> schemaMap = schema.getSubSchemaMap();
        for (String subSchema : schemaMap.keySet()) {
            result.add(moniker(schema, subSchema, SqlMonikerType.SCHEMA));
        }
        for (String table : schema.getTableNames()) {
            result.add(moniker(schema, table, SqlMonikerType.TABLE));
        }

        final NavigableSet<String> functions = schema.getFunctionNames();
        for (String function : functions) {
            result.add(moniker(schema, function, SqlMonikerType.FUNCTION));
        }
        return result.build();
    }

    private static SqlMonikerImpl moniker(
            CalciteSchema schema, @Nullable String name, SqlMonikerType type) {
        final List<String> path = schema.path(name);
        if (path.size() == 1
                && !"".equals(schema.root().name)
                && type == SqlMonikerType.SCHEMA) {
            type = SqlMonikerType.CATALOG;
        }
        return new SqlMonikerImpl(path, type);
    }

    @Override
    public List<List<String>> getSchemaPaths() {
        return schemaPaths;
    }

    @Override
    public @Nullable RelDataTypeField field(RelDataType relDataType, String s) {
        return null;
    }

    @Override
    public SqlNameMatcher nameMatcher() {
        return sqlNameMatcher;
    }

    @Override
    public boolean matches(String string, String name) {
        return sqlNameMatcher.isCaseSensitive() ? string.equals(name) : string.equalsIgnoreCase(name);
    }

    @Override
    public RelDataType createTypeFromProjection(RelDataType relDataType, List<String> list) {
        return null;
    }

    @Override
    public boolean isCaseSensitive() {
        return sqlNameMatcher.isCaseSensitive();
    }

    @Override
    public CalciteSchema getRootSchema() {
        return rootSchema;
    }

    @Override
    public CalciteConnectionConfig getConfig() {
        return null;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier sqlIdentifier,
            @Nullable SqlFunctionCategory sqlFunctionCategory,
            SqlSyntax sqlSyntax,
            List<SqlOperator> list,
            SqlNameMatcher sqlNameMatcher) {

    }

    @Override
    public List<SqlOperator> getOperatorList() {
        return ImmutableList.of();
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        return aClass.isInstance(this) ? aClass.cast(this) : null;
    }
}
