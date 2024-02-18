package parser;

import com.ccsu.parser.CalciteSqlParser;
import com.ccsu.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class CalciteParserTest {
    SqlParser sqlParser = new CalciteSqlParser();
    @Test
    public void testSqlParser1() {
        String sql = "select * from \"default_catalog\".\"default\".\"tpcds_q6_view\" limit 10 ";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }

    @Test
    public void testSqlParser2() {
        String sql = "select * from a where a.id > 10 or b.age < 8 limit 10";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }
    @Test
    public void testSqlParser3() {
        String sql = "select\n" +
                "\tcount_137 \"__Row Count\",\n" +
                "\tprovince \"Province\",\n" +
                "\tsum_35 \"Sum of Price\",\n" +
                "\tcity \"City\",\n" +
                "\tsum_22 \"Sum of Quantity\",\n" +
                "\tcategory_name \"Category Name\",\n" +
                "\tif_41 price_level,\n" +
                "\tsum_82 \"Sum of Discount\",\n" +
                "\tcategory_id \"Category Id\",\n" +
                "\tproduct_name \"Product Name\",\n" +
                "\tquantity \"Quantity\",\n" +
                "\tprice \"Price\",\n" +
                "\tdiscount \"Discount\",\n" +
                "\tcountry \"Country\",\n" +
                "\tv_10 \"Target Const\"\n" +
                "from\n" +
                "\t(\n" +
                "\tselect\n" +
                "\t\tq42.country country,\n" +
                "\t\tq42.province province,\n" +
                "\t\tq42.city city,\n" +
                "\t\tq42.category_id category_id,\n" +
                "\t\tq42.category_name category_name,\n" +
                "\t\tq42.product_name product_name,\n" +
                "\t\tq42.quantity quantity,\n" +
                "\t\tq42.price price,\n" +
                "\t\tq42.discount discount,\n" +
                "\t\tq42.v_10 v_10,\n" +
                "\t\tq42.sum_35 sum_35,\n" +
                "\t\tq42.if_41 if_41,\n" +
                "\t\tq85.count_137 count_137,\n" +
                "\t\tq133.sum_22 sum_22,\n" +
                "\t\tq133.province_18 province_18,\n" +
                "\t\tq133.city_19 city_19,\n" +
                "\t\tq167.sum_82 sum_82,\n" +
                "\t\tq167.province_75 province_75,\n" +
                "\t\tq167.city_76 city_76,\n" +
                "\t\tq167.category_name_77 category_name_77,\n" +
                "\t\tq167.if_43 if_43\n" +
                "\tfrom\n" +
                "\t\t(\n" +
                "\t\tselect\n" +
                "\t\t\t*\n" +
                "\t\tfrom\n" +
                "\t\t\t(\n" +
                "\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom\n" +
                "\t\t\t\t(\n" +
                "\t\t\t\tselect\n" +
                "\t\t\t\t\t*\n" +
                "\t\t\t\tfrom\n" +
                "\t\t\t\t\t(\n" +
                "\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\tq1.country country,\n" +
                "\t\t\t\t\t\tq1.province province,\n" +
                "\t\t\t\t\t\tq1.city city,\n" +
                "\t\t\t\t\t\tq1.category_id category_id,\n" +
                "\t\t\t\t\t\tq1.category_name category_name,\n" +
                "\t\t\t\t\t\tq1.product_name product_name,\n" +
                "\t\t\t\t\t\tq1.quantity quantity,\n" +
                "\t\t\t\t\t\tq1.price price,\n" +
                "\t\t\t\t\t\tq1.discount discount,\n" +
                "\t\t\t\t\t\tq1.v_10 v_10,\n" +
                "\t\t\t\t\t\tq4.sum_35 sum_35,\n" +
                "\t\t\t\t\t\tcase\n" +
                "\t\t\t\t\t\t\twhen (q4.sum_35 > 809621) then '高'\n" +
                "\t\t\t\t\t\t\telse '低'\n" +
                "\t\t\t\t\t\tend if_41\n" +
                "\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\tcountry,\n" +
                "\t\t\t\t\t\t\tprovince,\n" +
                "\t\t\t\t\t\t\tcity,\n" +
                "\t\t\t\t\t\t\tcategory_id,\n" +
                "\t\t\t\t\t\t\tcategory_name,\n" +
                "\t\t\t\t\t\t\tproduct_name,\n" +
                "\t\t\t\t\t\t\tquantity,\n" +
                "\t\t\t\t\t\t\tprice,\n" +
                "\t\t\t\t\t\t\tdiscount,\n" +
                "\t\t\t\t\t\t\t1 v_10\n" +
                "\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q1\n" +
                "\t\t\t\t\tinner join (\n" +
                "\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\tsum_34 sum_35,\n" +
                "\t\t\t\t\t\t\tprovince_30 province_31\n" +
                "\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\tprovince province_30,\n" +
                "\t\t\t\t\t\t\t\tsum(price) sum_34\n" +
                "\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q2\n" +
                "\t\t\t\t\t\t\tgroup by\n" +
                "\t\t\t\t\t\t\t\tprovince ) q3 ) q4 on\n" +
                "\t\t\t\t\t\t( ( coalesce(q1.province, '') = coalesce(q4.province_31, '') )\n" +
                "\t\t\t\t\t\t\tand ((q1.province is not null) = (q4.province_31 is not null)) ) ) q6\n" +
                "\t\t\t\twhere\n" +
                "\t\t\t\t\t(if_41 = '低') ) q7 ) q18\n" +
                "\t\torder by\n" +
                "\t\t\tprovince asc,\n" +
                "\t\t\tcity asc,\n" +
                "\t\t\tcategory_name asc,\n" +
                "\t\t\tif_41 asc\n" +
                "\t\tlimit 10001 ) q42\n" +
                "\tcross join (\n" +
                "\t\tselect\n" +
                "\t\t\tcount(1) count_137\n" +
                "\t\tfrom\n" +
                "\t\t\t(\n" +
                "\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom\n" +
                "\t\t\t\t(\n" +
                "\t\t\t\tselect\n" +
                "\t\t\t\t\t*\n" +
                "\t\t\t\tfrom\n" +
                "\t\t\t\t\t(\n" +
                "\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t*\n" +
                "\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\tq43.province province,\n" +
                "\t\t\t\t\t\t\tq43.city city,\n" +
                "\t\t\t\t\t\t\tq43.category_name category_name,\n" +
                "\t\t\t\t\t\t\tcase\n" +
                "\t\t\t\t\t\t\t\twhen (q46.sum_35 > 809621) then '高'\n" +
                "\t\t\t\t\t\t\t\telse '低'\n" +
                "\t\t\t\t\t\t\tend if_41\n" +
                "\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q43\n" +
                "\t\t\t\t\t\tinner join (\n" +
                "\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\tsum_34 sum_35,\n" +
                "\t\t\t\t\t\t\t\tprovince_30 province_31\n" +
                "\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t\tprovince province_30,\n" +
                "\t\t\t\t\t\t\t\t\tsum(price) sum_34\n" +
                "\t\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q44\n" +
                "\t\t\t\t\t\t\t\tgroup by\n" +
                "\t\t\t\t\t\t\t\t\tprovince ) q45 ) q46 on\n" +
                "\t\t\t\t\t\t\t( ( coalesce(q43.province, '') = coalesce(q46.province_31, '') )\n" +
                "\t\t\t\t\t\t\t\tand ( (q43.province is not null) = (q46.province_31 is not null) ) ) ) q48\n" +
                "\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t(if_41 = '低') ) q49\n" +
                "\t\t\t\t\t) q60 ) q84 ) q85\n" +
                "\tinner join (\n" +
                "\t\tselect\n" +
                "\t\t\tsum_21 sum_22,\n" +
                "\t\t\tprovince_17 province_18,\n" +
                "\t\t\tcity_18 city_19\n" +
                "\t\tfrom\n" +
                "\t\t\t(\n" +
                "\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom\n" +
                "\t\t\t\t(\n" +
                "\t\t\t\tselect\n" +
                "\t\t\t\t\t*\n" +
                "\t\t\t\tfrom\n" +
                "\t\t\t\t\t(\n" +
                "\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\tprovince province_17,\n" +
                "\t\t\t\t\t\tcity city_18,\n" +
                "\t\t\t\t\t\tsum(quantity) sum_21\n" +
                "\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q88\n" +
                "\t\t\t\t\tgroup by\n" +
                "\t\t\t\t\t\tprovince,\n" +
                "\t\t\t\t\t\tcity ) q89 ) q98 ) q109 ) q133 on\n" +
                "\t\t( ( ( coalesce(q42.province, '') = coalesce(q133.province_18, '') )\n" +
                "\t\t\tand ( (q42.province is not null) = (q133.province_18 is not null) ) )\n" +
                "\t\t\tand ( (coalesce(q42.city, '') = coalesce(q133.city_19, ''))\n" +
                "\t\t\t\tand ((q42.city is not null) = (q133.city_19 is not null)) ) )\n" +
                "\tinner join (\n" +
                "\t\tselect\n" +
                "\t\t\tsum_81 sum_82,\n" +
                "\t\t\tprovince_74 province_75,\n" +
                "\t\t\tcity_75 city_76,\n" +
                "\t\t\tcategory_name_76 category_name_77,\n" +
                "\t\t\tif_42 if_43\n" +
                "\t\tfrom\n" +
                "\t\t\t(\n" +
                "\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom\n" +
                "\t\t\t\t(\n" +
                "\t\t\t\tselect\n" +
                "\t\t\t\t\tprovince province_74,\n" +
                "\t\t\t\t\tcity city_75,\n" +
                "\t\t\t\t\tcategory_name category_name_76,\n" +
                "\t\t\t\t\tif_41 if_42,\n" +
                "\t\t\t\t\tsum(discount) sum_81\n" +
                "\t\t\t\tfrom\n" +
                "\t\t\t\t\t(\n" +
                "\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t*\n" +
                "\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\tq135.province province,\n" +
                "\t\t\t\t\t\t\tq135.city city,\n" +
                "\t\t\t\t\t\t\tq135.category_name category_name,\n" +
                "\t\t\t\t\t\t\tq135.discount discount,\n" +
                "\t\t\t\t\t\t\tcase\n" +
                "\t\t\t\t\t\t\t\twhen (q138.sum_35 > 809621) then '高'\n" +
                "\t\t\t\t\t\t\t\telse '低'\n" +
                "\t\t\t\t\t\t\tend if_41\n" +
                "\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q135\n" +
                "\t\t\t\t\t\tinner join (\n" +
                "\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\tsum_34 sum_35,\n" +
                "\t\t\t\t\t\t\t\tprovince_30 province_31\n" +
                "\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t\tprovince province_30,\n" +
                "\t\t\t\t\t\t\t\t\tsum(price) sum_34\n" +
                "\t\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t\t(\n" +
                "\t\t\t\t\t\t\t\t\tselect\n" +
                "\t\t\t\t\t\t\t\t\t\t*\n" +
                "\t\t\t\t\t\t\t\t\tfrom\n" +
                "\t\t\t\t\t\t\t\t\t\tsigma.tb_order tb_order\n" +
                "\t\t\t\t\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t\t\t\t\t( city in ( '七台河市', '三沙市', '三门峡市', '佛山市', '北区', '厦门市', '嘉兴市', '宝鸡市', '武汉市' )\n" +
                "\t\t\t\t\t\t\t\t\t\t\tand ( (29 <= price)\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\tor (price is null) ) ) ) q136\n" +
                "\t\t\t\t\t\t\t\tgroup by\n" +
                "\t\t\t\t\t\t\t\t\tprovince ) q137 ) q138 on\n" +
                "\t\t\t\t\t\t\t( ( coalesce(q135.province, '') = coalesce(q138.province_31, '') )\n" +
                "\t\t\t\t\t\t\t\tand ( (q135.province is not null) = (q138.province_31 is not null) ) ) ) q140\n" +
                "\t\t\t\t\twhere\n" +
                "\t\t\t\t\t\t(if_41 = '低') ) q141\n" +
                "\t\t\t\tgroup by\n" +
                "\t\t\t\t\tprovince,\n" +
                "\t\t\t\t\tcity,\n" +
                "\t\t\t\t\tcategory_name,\n" +
                "\t\t\t\t\tif_41 ) q142\n" +
                "\t\t\twhere\n" +
                "\t\t\t\t( (6 <= sum_81)\n" +
                "\t\t\t\t\tor (sum_81 is null) ) ) q143 ) q167 on\n" +
                "\t\t( ( ( ( ( coalesce(q42.province, '') = coalesce(q167.province_75, '') )\n" +
                "\t\t\tand ( (q42.province is not null) = (q167.province_75 is not null) ) )\n" +
                "\t\t\tand ( (coalesce(q42.city, '') = coalesce(q167.city_76, ''))\n" +
                "\t\t\t\tand ((q42.city is not null) = (q167.city_76 is not null)) ) )\n" +
                "\t\t\tand ( ( coalesce(q42.category_name, '') = coalesce(q167.category_name_77, '') )\n" +
                "\t\t\t\tand ( (q42.category_name is not null) = (q167.category_name_77 is not null) ) ) )\n" +
                "\t\t\tand ( (coalesce(q42.if_41, '') = coalesce(q167.if_43, ''))\n" +
                "\t\t\t\tand ((q42.if_41 is not null) = (q167.if_43 is not null)) ) )\n" +
                "\torder by\n" +
                "\t\tq42.province asc,\n" +
                "\t\tq42.city asc,\n" +
                "\t\tq42.category_name asc,\n" +
                "\t\tq42.if_41 asc\n" +
                "\tlimit 10001 ) q169";
        SqlNode parse = sqlParser.parse(sql);
        System.out.println(parse);
    }
}