package convert;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

public class ExtendSqlToRelConverter extends SqlToRelConverter {

    public ExtendSqlToRelConverter(RelOptTable.ViewExpander viewExpander,
                                   SqlValidator validator,
                                   Prepare.CatalogReader catalogReader,
                                   RelOptCluster cluster,
                                   SqlRexConvertletTable convertletTable,
                                   Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    public RelNode toRel(RelOptTable table, List<RelHint> hints) {
        return table.toRel(new RelOptTable.ToRelContext(){
            @Override
            public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, @Nullable List<String> viewPath) {
                return null;
            }

            @Override
            public RelOptCluster getCluster() {
                return cluster;
            }

            @Override
            public List<RelHint> getTableHints() {
                return Collections.emptyList();
            }
        });
    }
}
