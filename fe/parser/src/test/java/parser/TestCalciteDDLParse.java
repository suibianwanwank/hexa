package parser;

import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class TestCalciteDDLParse {

    SqlParser sqlParser = new CalciteSqlParser();

    @Test
    public void testDDL(){
        String sql = "create view \"view1\" as select * from table1";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL2(){
        String sql = "create catalog \"catalog1\".\"schema1\" ";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL3(){
        String sql = "show catalogs ";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testDDL5(){
        String sql = "show catalogs from name";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }
}