package com.ccsu.meta;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.meta.type.ArrowTypeMapping;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;


public class CommonTranslateTable implements ExtendTranslateTable {
    private TableInfo tableInfo;
    private DatasourceConfig config;
    private ArrowTypeMapping arrowTypeMapping;


    public CommonTranslateTable(TableInfo tableInfo, DatasourceConfig config, ArrowTypeMapping arrowTypeMapping) {
        this.tableInfo = tableInfo;
        this.config = config;
        this.arrowTypeMapping = arrowTypeMapping;
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public DatasourceConfig getSourceConfig() {
        return config;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return arrowTypeMapping.convertToArrowDataType(config.getSourceType(), tableInfo.getColumns());
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            public Double getRowCount() {
                return getRowCount();
            }

            public List<RelReferentialConstraint> getReferentialConstraints() {
                return ImmutableList.of();
            }

            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }
        };
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return null;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column,
            SqlCall call,
            @Nullable SqlNode parent,
            @Nullable CalciteConnectionConfig config) {
        return false;
    }
}
