package handler;

import context.QueryContext;
import convert.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import program.QueryPlannerProgram;

public class QueryPlanHandler
        implements SqlHandler<QueryPlanResult, SqlNode, QueryContext> {
    @Override
    public QueryPlanResult handle(SqlNode sqlNode, QueryContext context) {

        SqlNode validate = context.getSqlValidator().validate(sqlNode);

        RelNode originRelNode =
                SqlConverter.of(context, context.getRelOptCostFactory()).convertQuery(validate, false);

        RelNode preLogical = QueryPlannerProgram.PRE_SIMPLIFY_PROGRAM.optimize(originRelNode, context);

        RelNode physicalPlan = QueryPlannerProgram.LOGICAL_OPTIMIZE_PROGRAM.optimize(preLogical, context);

//        PhysicalNode physicalNode =
//                (PhysicalNode) QueryPlannerProgram.LOGICAL_TO_PHYSICAL_PROGRAM.optimize(logical, context);

        // TODO optimize physical plan

        return new QueryPlanResult(null);
    }
}
