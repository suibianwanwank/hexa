package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowTables extends SqlCall {
    public static final SqlSpecialOperator SHOW_TABLES = new SqlSpecialOperator("SHOW_TABLES",
            SqlKind.OTHER_DDL);

    private SqlIdentifier sourcePath;

    public SqlShowTables(SqlParserPos pos, SqlIdentifier sourcePath) {
        super(pos);
        this.sourcePath = sourcePath;
    }

    public SqlShowTables(SqlParserPos pos) {
        super(pos);
    }


    @Override
    public SqlOperator getOperator() {
        return SHOW_TABLES;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW TABLES");
        writer.keyword("FROM");
        sourcePath.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getSourcePath() {
        return sourcePath;
    }
}
