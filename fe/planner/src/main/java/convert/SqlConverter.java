package convert;

import com.ccsu.profile.JobProfile;
import com.ccsu.profile.Phrase;
import com.ccsu.profile.ProfileUtil;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Stopwatch;
import config.SqlToRelNodeConfig;
import context.QueryContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import program.logical.LogicalVolcanoPlanner;

import java.util.concurrent.TimeUnit;

public class SqlConverter {
    private static final Logger LOGGER = Logger.get(SqlConverter.class);

    private final SqlValidator sqlValidator;

    private final Prepare.CatalogReader catalogReader;

    private final RelOptCostFactory relOptCostFactory;

    private final RelOptPlanner planner;

    private final RelOptCluster relOptCluster;

    private final JobProfile jobProfile;

    public static SqlConverter of(QueryContext queryContext, RelOptCostFactory relOptCostFactory) {
        Prepare.CatalogReader catalogReader = queryContext.getCatalogReader();

        RexBuilder rexBuilder = new RexBuilder(catalogReader.getTypeFactory());
        VolcanoPlanner planner = LogicalVolcanoPlanner.of(queryContext.getOptionManager());

        RelOptCluster relOptCluster = RelOptClusterProvider.createRelOptCluster(planner, rexBuilder);
        return new SqlConverter(queryContext.getSqlValidator(),
                catalogReader, relOptCostFactory, planner, relOptCluster, queryContext.getJobProfile());
    }

    public SqlConverter(SqlValidator sqlValidator, Prepare.CatalogReader catalogReader, RelOptCostFactory relOptCostFactory,
                        RelOptPlanner planner, RelOptCluster relOptCluster, JobProfile jobProfile) {
        this.sqlValidator = sqlValidator;
        this.catalogReader = catalogReader;
        this.relOptCostFactory = relOptCostFactory;
        this.planner = planner;
        this.relOptCluster = relOptCluster;
        this.jobProfile = jobProfile;
    }


    public RelNode convertQuery(SqlNode sqlNode, boolean expand) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        SqlToRelConverter.Config config = new SqlToRelNodeConfig()
                .withTrimUnusedFields(true)
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withRelBuilderConfigTransform(relBuilderConfig -> relBuilderConfig
                        .withSimplify(false))
                .withInSubQueryThreshold(25)
                .withHintStrategyTable(HintStrategyTable.EMPTY);

        SqlToRelConverter sqlToRelConverter = new ExtendSqlToRelConverter(new SqlToRelContext(this),
                sqlValidator,
                catalogReader,
                relOptCluster,
                StandardConvertletTable.INSTANCE,
                config);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, false);

        RelNode relNode = RelRoot.of(root.rel, root.validatedRowType, root.kind).rel;

        ProfileUtil.addPlanPhraseJobProfile(jobProfile,
                Phrase.AST_TO_LOGICAL_PLAN, stopwatch.elapsed(TimeUnit.NANOSECONDS), relNode);

        return relNode;
    }

    public RelOptCostFactory getRelOptCostFactory() {
        return relOptCostFactory;
    }

    public RelOptPlanner getPlanner() {
        return planner;
    }

    public RelOptCluster getRelOptCluster() {
        return relOptCluster;
    }
}
