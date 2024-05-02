package handler;

import com.ccsu.parser.sqlnode.SqlProfileExplain;
import context.QueryContext;

public class ExplainHandler
        implements SqlHandler<String, SqlProfileExplain, QueryContext> {
    @Override
    public String handle(SqlProfileExplain sqlProfileExplain, QueryContext context) {
        new QueryPlanHandler()
                .handle(sqlProfileExplain.getSqlStatement(), context);

        return context.getJobProfile().formatExplain();
    }
}
