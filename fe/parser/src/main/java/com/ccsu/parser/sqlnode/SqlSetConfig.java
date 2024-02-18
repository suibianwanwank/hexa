package com.ccsu.parser.sqlnode;

import com.beust.jcommander.internal.Nullable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.testng.collections.Lists;

import java.util.List;

public class SqlSetConfig extends SqlCall {

    private static final SqlSpecialOperator SET_CONFIG_OP = new SqlSpecialOperator("SET_CONFIG_OP", SqlKind.OTHER_DDL);
    private final SqlIdentifier scope;
    private final SqlKvEntry entry;

    public SqlSetConfig(SqlParserPos pos,
                        @Nullable SqlIdentifier scope,
                        SqlKvEntry entry) {
        super(pos);
        this.scope = scope;
        this.entry = entry;
    }

    @Override
    public SqlOperator getOperator() {
        return SET_CONFIG_OP;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> sqlNodeList = Lists.newArrayList();
        if (scope == null) {
            sqlNodeList.add(scope);
        }
        sqlNodeList.add(entry);
        return sqlNodeList;
    }

    public SqlIdentifier getScope() {
        return scope;
    }

    public SqlKvEntry getEntry() {
        return entry;
    }
}
