package program.physical.rel;

import arrow.datafusion.PhysicalExprNode;
import arrow.datafusion.PhysicalExtensionNode;
import arrow.datafusion.PhysicalPlanNode;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;

public class EnumerableSqlScan
        extends EnumerableTableScan
        implements PhysicalPlan {

    private final String sql;


    public static EnumerableSqlScan create(String sql, RelOptCluster cluster, RelOptTable table, Class elementType) {
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getCollationList();
                            }
                            return ImmutableList.of();
                        });
        return new EnumerableSqlScan(sql, cluster, traitSet, table, elementType);
    }

    /**
     * Creates an EnumerableTableScan.
     *
     * <p>Use {@link #create} unless you know what you are doing.
     *
     * @param cluster
     * @param traitSet
     * @param table
     * @param elementType
     */
    public EnumerableSqlScan(String sql, RelOptCluster cluster, RelTraitSet traitSet,
                             RelOptTable table, Class elementType) {
        super(cluster, traitSet, table, elementType);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public PhysicalPlanNode transformToPP() {
        PhysicalExtensionNode.Builder builder = PhysicalExtensionNode.newBuilder()
                .setNode(null);
        return PhysicalPlanNode.newBuilder()
                .setExtension(builder)
                .build();
    }
}
