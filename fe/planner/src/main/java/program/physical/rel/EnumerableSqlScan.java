package program.physical.rel;

import arrow.datafusion.PhysicalExtensionNode;
import arrow.datafusion.PhysicalPlanNode;
import arrow.datafusion.SourceType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class EnumerableSqlScan
        extends EnumerableTableScan
        implements PhysicalPlan {

    SourceType sourceType;
    private final String sql;


    public static EnumerableSqlScan create(SourceType sourceType, String sql,
                                           RelOptCluster cluster, RelOptTable table, Class elementType) {
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getCollationList();
                            }
                            return ImmutableList.of();
                        });
        return new EnumerableSqlScan(sourceType, sql, cluster, traitSet, table, elementType);
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
    public EnumerableSqlScan(SourceType sourceType, String sql, RelOptCluster cluster, RelTraitSet traitSet,
                             RelOptTable table, Class elementType) {
        super(cluster, traitSet, table, elementType);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        PhysicalExtensionNode.Builder builder = PhysicalExtensionNode.newBuilder();
        return PhysicalPlanNode.newBuilder()
                .setExtension(builder)
                .build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new EnumerableSqlScan(null, getSql(), getCluster(), traitSet, table, Object.class);
    }
}
