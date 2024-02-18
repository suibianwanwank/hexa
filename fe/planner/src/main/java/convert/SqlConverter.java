package convert;

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

public class SqlConverter {
    private final SqlValidator sqlValidator;

    private final Prepare.CatalogReader catalogReader;

    private final RelOptCostFactory relOptCostFactory;

    private final RelOptPlanner planner;

    private final RelOptCluster relOptCluster;

    public static SqlConverter of(QueryContext queryContext, RelOptCostFactory relOptCostFactory) {
        Prepare.CatalogReader catalogReader = queryContext.getCatalogReader();

        RexBuilder rexBuilder = new RexBuilder(catalogReader.getTypeFactory());
        VolcanoPlanner planner = LogicalVolcanoPlanner.of(queryContext.getOptionManager());

        RelOptCluster relOptCluster = RelOptClusterProvider.createRelOptCluster(planner, rexBuilder);
        return new SqlConverter(queryContext.getSqlValidator(),
                catalogReader, relOptCostFactory, planner, relOptCluster);
    }

    public SqlConverter(SqlValidator sqlValidator, Prepare.CatalogReader catalogReader,
                        RelOptCostFactory relOptCostFactory, RelOptPlanner planner, RelOptCluster relOptCluster) {
        this.sqlValidator = sqlValidator;
        this.catalogReader = catalogReader;
        this.relOptCostFactory = relOptCostFactory;
        this.planner = planner;
        this.relOptCluster = relOptCluster;
    }


    public RelNode convertQuery(SqlNode sqlNode, boolean expand) {
        SqlToRelConverter.Config config = new SqlToRelNodeConfig()
                .withTrimUnusedFields(true)
                .withExpand(expand)
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withRelBuilderConfigTransform(relBuilderConfig -> relBuilderConfig.withSimplify(false))
                .withHintStrategyTable(HintStrategyTable.EMPTY)
                .withCreateValuesRel(true);

        SqlToRelConverter sqlToRelConverter = new ExtendSqlToRelConverter(new SqlToRelContext(this),
                sqlValidator,
                catalogReader,
                relOptCluster,
                StandardConvertletTable.INSTANCE,
                config,
                this);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, false);


        return RelRoot.of(root.rel, root.validatedRowType, root.kind).rel;
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
