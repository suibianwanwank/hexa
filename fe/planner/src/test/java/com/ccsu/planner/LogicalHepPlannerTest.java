package com.ccsu.planner;

import com.ccsu.TestFixture;
import com.ccsu.parser.SqlParser;
import convert.SqlConverter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import program.logical.LogicalHepProgram;


public class LogicalHepPlannerTest {

    private final TestFixture fixture = new TestFixture();
    private SqlParser sqlParser;
    private SqlValidator sqlValidator;
    private SqlConverter sqlConverter;

    @Before
    public void init() {
        sqlParser = fixture.getSqlParser();
        sqlValidator = fixture.getQueryContext().getSqlValidator();
        sqlConverter = fixture.getSqlConverter();
    }

    @Test
    public void test1() {
        String sql = "select * from \"default_catalog\".\"default\".\"table1\" as \"a\" ,"
                + "\"default_catalog\".\"default\".\"table2\" as \"b\" "
                + "where \"a\".\"column1\" = \"b\".\"column3\" ";
        SqlNode sqlNode = sqlParser.parse(sql);
        SqlNode validate = sqlValidator.validate(sqlNode);
        RelNode relNode = sqlConverter.convertQuery(validate, false);
        LogicalHepProgram hepProgram = new LogicalHepProgram.LogicalHepProgramBuilder()
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(CoreRules.FILTER_INTO_JOIN)
                .build();
        RelNode optimize = hepProgram.optimize(relNode, fixture.getQueryContext());
        String expected = "LogicalProject(column1=[$0], column2=[$1], column3=[$2], column4=[$3])\n"
                + "  LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
                + "    LogicalTableScan(table=[[default_catalog, default, table1]])\n"
                + "    LogicalTableScan(table=[[default_catalog, default, table2]])\n";
        Assert.assertEquals(expected, RelOptUtil.toString(optimize));
    }
}