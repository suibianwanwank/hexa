package handler;

import com.facebook.airlift.log.Logger;
import context.QueryContext;
import convert.SqlConverter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import program.QueryPlannerProgram;
import program.physical.rel.PhysicalPlan;

public class QueryPlanHandler
        implements SqlHandler<QueryPlanResult, SqlNode, QueryContext> {

    private static final Logger LOGGER = Logger.get(QueryPlanHandler.class);

    @Override
    public QueryPlanResult handle(SqlNode sqlNode, QueryContext context) {

        SqlNode validate = context.getSqlValidator().validate(sqlNode);

        RelNode originRelNode =
                SqlConverter.of(context, context.getRelOptCostFactory()).convertQuery(validate, false);

        context.setOriginRelNode(originRelNode);

        RelNode preLogical = QueryPlannerProgram.PRE_SIMPLIFY_PROGRAM.optimize(originRelNode, context);

        RelNode physicalPlan = QueryPlannerProgram.LOGICAL_OPTIMIZE_PROGRAM.optimize(preLogical, context);

        RelNode lastPlan = QueryPlannerProgram.PHYSICAL_OPTIMIZE_PROGRAM.optimize(physicalPlan, context);

        LOGGER.info("Optimised physical execution plan:\n %s", RelOptUtil.toString(lastPlan));

        proto.datafusion.PhysicalPlanNode dataFusionPlan = ((PhysicalPlan) lastPlan).transformToDataFusionNode();

        return QueryPlanResult.success(dataFusionPlan);
    }
}
