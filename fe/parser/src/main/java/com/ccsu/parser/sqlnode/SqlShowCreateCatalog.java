package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowCreateCatalog extends SqlCall {
    public static final SqlSpecialOperator SHOW_CREATE_CATALOG = new SqlSpecialOperator("SHOW_CREATE_CATALOG",
            SqlKind.OTHER_DDL);

    private SqlIdentifier catalogName;

    public SqlShowCreateCatalog(SqlParserPos pos, SqlIdentifier catalogName) {
        super(pos);
        this.catalogName = catalogName;
    }

    public SqlShowCreateCatalog(SqlParserPos pos) {
        super(pos);
    }


    @Override
    public SqlOperator getOperator() {
        return SHOW_CREATE_CATALOG;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW CREATE CATALOG");
        writer.keyword("FROM");
        catalogName.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getCatalogName() {
        return catalogName;
    }
}
