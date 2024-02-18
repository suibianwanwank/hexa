package convert;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class ExtendSqlToRelConverter extends SqlToRelConverter {
    SqlConverter sqlConverter;

    public ExtendSqlToRelConverter(RelOptTable.ViewExpander viewExpander,
                                   SqlValidator validator,
                                   Prepare.CatalogReader catalogReader,
                                   RelOptCluster cluster,
                                   SqlRexConvertletTable convertletTable,
                                   Config config,
                                   SqlConverter sqlConverter) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
        this.sqlConverter = sqlConverter;
    }

    @Override
    public RelNode toRel(RelOptTable table, List<RelHint> hints) {
        return table.toRel(new SqlToRelContext(sqlConverter));
    }

    @Override
    protected void convertFrom(Blackboard bb, @Nullable SqlNode from) {
        if (from == null) {
            bb.setRoot(LogicalValues.createOneRow(cluster), false);
            return;
        }
//        if (from.getKind() == SqlKind.WITH_ITEM) {
//            // TODO add consume mark
//        }
        super.convertFrom(bb, from);
    }
}
