package com.ccsu.planner;

import com.ccsu.TestFixture;
import context.QueryContext;
import handler.QueryPlanHandler;
import handler.QueryPlanResult;
import handler.SqlHandler;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class SqlQueryHandlerTest {
    SqlHandler<QueryPlanResult, SqlNode, QueryContext> handler = new QueryPlanHandler();

    private final TestFixture fixture = new TestFixture();

    @Test
    public void testPlanHandler() {
        String sql = "select * from \"default_catalog\".\"default\".\"table1\" as \"a\" ,"
                + "\"default_catalog\".\"default\".\"table2\" as \"b\" "
                + "where \"a\".\"column1\" = \"b\".\"column3\" ";
        SqlNode sqlNode = fixture.getSqlParser().parse(sql);
        QueryPlanResult handle = handler.handle(sqlNode, fixture.getQueryContext());
    }
}