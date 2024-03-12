package com.ccsu.datasouce.mysql;


import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.*;

public class JdbcTest {
    @Test
    @Ignore
    public void testJdbc() throws SQLException, ClassNotFoundException {
        //1、导入驱动jar包
        //2、注册驱动
        Class.forName("com.mysql.jdbc.Driver");

        //3、获取数据库的连接对象
        Connection con = DriverManager.getConnection("jdbc:mysql://10.5.20.26:3306/xiaohei", "root", "Aloudata@12");
        System.out.println(con.getCatalog());
        System.out.println(con.getSchema());
        DatabaseMetaData metaData = con.getMetaData();

        ResultSet tables = metaData.getTables(con.getCatalog(), null, "%", null);
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            ResultSet columns = metaData.getColumns(con.getCatalog(), null, tableName, null);
            while (columns.next()){
                String columnName = columns.getString("COLUMN_NAME");
                System.out.println(columnName);
            }
            System.out.println(tableName);
        }
//        metaData.getColumns()
    }
}