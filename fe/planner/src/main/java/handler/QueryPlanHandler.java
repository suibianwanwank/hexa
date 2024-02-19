package handler;

import arrow.datafusion.PhysicalPlanNode;
import context.QueryContext;
import convert.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import program.QueryPlannerProgram;
import program.physical.rel.PhysicalPlan;

public class QueryPlanHandler
        implements SqlHandler<QueryPlanResult, SqlNode, QueryContext> {
    @Override
    public QueryPlanResult handle(SqlNode sqlNode, QueryContext context) {

        SqlNode validate = context.getSqlValidator().validate(sqlNode);

        RelNode originRelNode =
                SqlConverter.of(context, context.getRelOptCostFactory()).convertQuery(validate, false);

        RelNode preLogical = QueryPlannerProgram.PRE_SIMPLIFY_PROGRAM.optimize(originRelNode, context);

        RelNode physicalPlan = QueryPlannerProgram.LOGICAL_OPTIMIZE_PROGRAM.optimize(preLogical, context);

        RelNode lastPlan =
                QueryPlannerProgram.PHYSICAL_OPTIMIZE_PROGRAM.optimize(physicalPlan, context);

        PhysicalPlanNode dataFusionPlan = ((PhysicalPlan) lastPlan).transformToDataFusionNode();

        return new QueryPlanResult(dataFusionPlan);
    }
}
