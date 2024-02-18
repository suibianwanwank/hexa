package convert;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;

public class RelOptClusterProvider {

    private RelOptClusterProvider() {
    }

    public static final RelMetadataProvider DEFAULT_INSTANCE = ChainedRelMetadataProvider.of(
            ImmutableList.of(
//                    TardisRelMdRowCount.SOURCE,
//                    TardisRelMdCumulativeCost.SOURCE,
//                    TardisRelMdSize.SOURCE,
//                    TardisRelMdNonCumulativeCost.SOURCE,
//                    TardisRelMdSelectivity.SOURCE,
//                    TardisRelMdDistinctRowCount.SOURCE,
                    DefaultRelMetadataProvider.INSTANCE));

    public static RelOptCluster createRelOptCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DEFAULT_INSTANCE);
        cluster.setMetadataQuerySupplier(ExtendRelMetadataQuery::new);
        return cluster;
    }
}
