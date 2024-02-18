package com.ccsu.validate;

import com.ccsu.TestFixture;
import com.ccsu.parser.SqlParser;
import convert.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.Before;
import org.junit.Test;

public class SqlValidateTest {

    private final TestFixture fixture = new TestFixture();

    private SqlParser sqlParser;
    private SqlValidator sqlValidator;
    private SqlConverter sqlConverter;

    @Before
    public void test1(){
        sqlParser = fixture.getSqlParser();
        sqlValidator = fixture.getQueryContext().getSqlValidator();;
        sqlConverter = fixture.getSqlConverter();
    }

    @Test
    public void testValidate01() {
        String sql = "select * from \"default_catalog\".\"default\".\"table1\" ";
        SqlNode parse = sqlParser.parse(sql);
        SqlNode validate = sqlValidator.validate(parse);
        RelNode relNode = sqlConverter.convertQuery(validate, true);
        System.out.println(relNode);
    }

    @Test
    public void testValidate02() {
        String sql = "select * from \"default_catalog\".\"default\".\"table1\" where sda > 10 ";
        SqlNode parse = sqlParser.parse(sql);
        SqlNode validate = sqlValidator.validate(parse);
        RelNode relNode = sqlConverter.convertQuery(validate, true);
        System.out.println(relNode);
    }
}