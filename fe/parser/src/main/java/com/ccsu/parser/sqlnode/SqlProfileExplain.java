package com.ccsu.parser.sqlnode;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlProfileExplain extends SqlCall {

    private final SqlNode sqlStatement;
    public static final SqlSpecialOperator EXPLAIN_SQL = new SqlSpecialOperator("EXPLAIN_SQL", SqlKind.EXPLAIN);

    public SqlProfileExplain(SqlParserPos pos, SqlNode sqlStatement) {
        super(pos);
        this.sqlStatement = sqlStatement;
    }

    @Override
    public SqlOperator getOperator() {
        return EXPLAIN_SQL;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(sqlStatement);
    }

    public SqlNode getSqlStatement()
    {
        return sqlStatement;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
    {
        writer.keyword("EXPLAIN");
        sqlStatement.unparse(writer, leftPrec, rightPrec);
    }
}
