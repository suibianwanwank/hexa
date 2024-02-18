package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlRefreshTable extends SqlCall {
    public static final SqlSpecialOperator REFRESH_TABLE = new SqlSpecialOperator("REFRESH_TABLES",
            SqlKind.OTHER_DDL);

    private SqlIdentifier sourcePath;

    public SqlRefreshTable(SqlParserPos pos, SqlIdentifier sourcePath) {
        super(pos);
        this.sourcePath = sourcePath;
    }

    public SqlRefreshTable(SqlParserPos pos) {
        super(pos);
    }


    @Override
    public SqlOperator getOperator() {
        return REFRESH_TABLE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REFRESH TABLE");
        sourcePath.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getSourcePath() {
        return sourcePath;
    }
}
