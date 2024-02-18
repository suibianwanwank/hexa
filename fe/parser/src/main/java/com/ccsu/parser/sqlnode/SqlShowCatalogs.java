package com.ccsu.parser.sqlnode;

import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
public class SqlShowCatalogs extends SqlCall {

    public static final SqlSpecialOperator SHOW_DATASOURCE_INFO = new SqlSpecialOperator("SHOW_CATALOG_INFO",
            SqlKind.OTHER_DDL);

    private SqlIdentifier clusterName;

    public SqlShowCatalogs(SqlParserPos pos, SqlIdentifier clusterName) {
        super(pos);
        this.clusterName = clusterName;
    }

    public SqlShowCatalogs(SqlParserPos pos) {
        super(pos);
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
        writer.keyword("SHOW CATALOGS");
        if (clusterName != null) {
            writer.keyword("FROM");
            clusterName.unparse(writer, leftPrec, rightPrec);
        }
    }
}
