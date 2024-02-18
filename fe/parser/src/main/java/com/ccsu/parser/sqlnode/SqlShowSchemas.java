package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowSchemas extends SqlCall {

    public static final SqlSpecialOperator SHOW_DATASOURCE_INFO = new SqlSpecialOperator("SHOW_SCHEMAS_INFO",
            SqlKind.OTHER_DDL);

    private final SqlIdentifier catalogName;

    public SqlShowSchemas(SqlParserPos pos, SqlIdentifier catalogName) {
        super(pos);
        this.catalogName = catalogName;
    }

    @Override
    public SqlOperator getOperator() {
        return SHOW_DATASOURCE_INFO;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW SCHEMAS");
        if (catalogName != null) {
            writer.keyword("FROM");
            catalogName.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getCatalogName() {
        return catalogName;
    }
}
