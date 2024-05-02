package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowProfile extends SqlCall {
    public static final SqlSpecialOperator SHOW_PROFILE = new SqlSpecialOperator("SHOW_PROFILE",
            SqlKind.OTHER_DDL);

    private final SqlIdentifier jobId;

    public SqlShowProfile(SqlParserPos pos, SqlIdentifier jobId) {
        super(pos);
        this.jobId = jobId;
    }

    @Override
    public SqlOperator getOperator() {
        return SHOW_PROFILE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW PROFILE");
        jobId.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getJobId() {
        return jobId;
    }
}
