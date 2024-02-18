package parser;

import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class TestCalciteDDLParse {

    private final SqlParser sqlParser = new CalciteSqlParser();

    @Test
    public void testDDL3() {
        String sql = "show catalogs";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL4() {
        String sql = "show schemas from asda";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL5() {
        String sql = "show catalogs";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL6() {
        String sql = "show create catalog pg";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL7() {
        String sql = "show create catalog \"pg\"";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL8() {
        String sql = "SHOW COLUMNS FROM pg.a.B";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL9() {
        String sql = "show tables from pg.a";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL10() {
        String sql = "create catalog POSTGRESQL as \"pg\" with { "
                + "\"sourceType\": \"POSTGRESQL\",\n"
                + "\"host\" :  \"127.0.0.1\",\n"
                + " \"port\": \"5432\",\n"
                + " \"username\": \"root\",\n"
                + " \"password\": \"suibianwanwan\",\n"
                + " \"database\": \"postgres\" "
                + "}";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }
}