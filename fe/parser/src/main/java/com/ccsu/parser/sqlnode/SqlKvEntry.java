package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlKvEntry extends SqlCall {

    public static final SqlSpecialOperator PROPERTIES_MAPPING =
            new SqlSpecialOperator("KV_ENTRY", SqlKind.OTHER_DDL);

    private final SqlNode key;

    private final SqlNode value;

    public SqlKvEntry(SqlParserPos pos, SqlNode key, SqlNode value) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    @Override
    public SqlOperator getOperator() {
        return PROPERTIES_MAPPING;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getKey()
    {
        return key;
    }

    public SqlNode getValue()
    {
        return value;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword(" : ");
        value.unparse(writer, leftPrec, rightPrec);
    }
}
