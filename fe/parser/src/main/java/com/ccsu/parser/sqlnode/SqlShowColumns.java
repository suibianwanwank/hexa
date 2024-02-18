package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowColumns extends SqlCall {
    public static final SqlSpecialOperator SHOW_COLUMNS = new SqlSpecialOperator("SHOW_COLUMNS",
            SqlKind.OTHER_DDL);

    private SqlIdentifier schemaPath;

    public SqlShowColumns(SqlParserPos pos, SqlIdentifier schemaPath) {
        super(pos);
        this.schemaPath = schemaPath;
    }

    public SqlShowColumns(SqlParserPos pos) {
        super(pos);
    }


    @Override
    public SqlOperator getOperator() {
        return SHOW_COLUMNS;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW SCHEMA");
        writer.keyword("FROM");
        schemaPath.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getSchemaPath() {
        return schemaPath;
    }
}
