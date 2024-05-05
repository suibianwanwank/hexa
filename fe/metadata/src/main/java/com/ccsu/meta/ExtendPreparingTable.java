package com.ccsu.meta;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class ExtendPreparingTable implements Prepare.PreparingTable {
    private final ExtendCatalogReader catalogReader;
    private final RelDataType rowType;
    private final ExtendTranslateTable table;

    public ExtendPreparingTable(ExtendCatalogReader catalogReader,
                                ExtendTranslateTable table,
                                RelDataType rowType) {
        this.catalogReader = catalogReader;
        this.rowType = rowType;
        this.table = table;
    }

    @Override
    public List<String> getQualifiedName() {
        List<String> path = new ArrayList<>();
        path.add(table.getTableInfo().getSchemaName());
        path.add(table.getTableInfo().getTableName());
        return path;
    }

    @Override
    public SqlMonotonicity getMonotonicity(String columnName) {
        return null;
    }

    @Override
    public SqlAccessType getAllowedAccess() {
        return null;
    }

    @Override
    public boolean supportsModality(SqlModality modality) {
        return false;
    }

    @Override
    public boolean isTemporal() {
        return false;
    }

    @Override
    public boolean columnHasDefaultValue(RelDataType rowType, int ordinal, InitializerContext initializerContext) {
        return false;
    }

    @Override
    public double getRowCount() {
        return 0;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return catalogReader;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return new LogicalTableScan(context.getCluster(), this);
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        return null;
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return null;
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return null;
    }

    @Override
    public @Nullable Expression getExpression(Class clazz) {
        return null;
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return null;
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return null;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        if (aClass == CommonTranslateTable.class) {
            return aClass.cast(table);
        }
        return null;
    }
}
