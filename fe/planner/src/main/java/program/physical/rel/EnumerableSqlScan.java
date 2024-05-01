package program.physical.rel;

import com.ccsu.pojo.DatasourceConfig;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;

import java.util.List;

import static com.ccsu.pojo.DatasourceType.transformToProtoSourceType;
import static program.physical.rel.PhysicalPlanTransformUtil.buildRelNodeSchema;

public class EnumerableSqlScan
        extends EnumerableTableScan
        implements PhysicalPlan {

    private DatasourceConfig config;
    private final String sql;


    public static EnumerableSqlScan create(DatasourceConfig config, String sql,
                                           RelOptCluster cluster, RelOptTable table, Class elementType) {
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getCollationList();
                            }
                            return ImmutableList.of();
                        });
        return new EnumerableSqlScan(config, sql, cluster, traitSet, table, elementType);
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
    public EnumerableSqlScan(DatasourceConfig config, String sql, RelOptCluster cluster, RelTraitSet traitSet,
                             RelOptTable table, Class elementType) {
        super(cluster, traitSet, table, elementType);
        this.config = config;
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public proto.datafusion.PhysicalPlanNode transformToDataFusionNode() {

        proto.datafusion.SourceScanConfig.Builder scanConfig = proto.datafusion.SourceScanConfig.newBuilder()
                .setHost(config.getHost())
                .setPort(Integer.parseInt(config.getPort()))
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSourceType(transformToProtoSourceType(config.getSourceType()));


        if (config.getDatabase() != null) {
            scanConfig.setDatabase(config.getDatabase());
        }


        proto.datafusion.Schema.Builder schema = buildRelNodeSchema(getTable().getRowType().getFieldList());
        proto.datafusion.SqlScanExecNode.Builder builder = proto.datafusion.SqlScanExecNode.newBuilder()
                .setConfig(scanConfig)
                .setSchema(schema)
                .addSql(sql);

        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setSqlScan(builder)
                .build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new EnumerableSqlScan(null, getSql(), getCluster(), traitSet, table, Object.class);
    }
}
