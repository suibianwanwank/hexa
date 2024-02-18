package com.ccsu.parser.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlCreateCatalog extends SqlCreate {
    public static final SqlSpecialOperator CREATE_CATALOG = new SqlSpecialOperator("CREATE_CATALOG",
            SqlKind.OTHER_DDL);

    private final SqlIdentifier datasourceType;

    private final SqlIdentifier catalogName;

    private final SqlNodeList properties;

    public SqlCreateCatalog(SqlParserPos pos,
                            boolean ifNotExists,
                            SqlIdentifier datasourceType,
                            SqlIdentifier catalogName,
                            SqlNodeList properties) {
        super(CREATE_CATALOG, pos, false, ifNotExists);
        this.datasourceType = requireNonNull(datasourceType, "datasourceType is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.properties = properties;
    }

    public String getCatalogName()
    {
        return catalogName.getSimple();
    }

    @Override
    public SqlOperator getOperator() {
        return CREATE_CATALOG;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                datasourceType,
                properties);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");

        writer.keyword("CATALOG");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        this.datasourceType.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        catalogName.unparse(writer, leftPrec, rightPrec);
        if (properties != null && !properties.isEmpty()) {
            writer.newlineAndIndent();
            writer.keyword("WITH");
            writer.print("{");
            properties.unparse(writer, leftPrec, rightPrec);
            writer.print("}");
        }
    }

    public SqlIdentifier getDatasourceType() {
        return datasourceType;
    }

    public SqlNodeList getProperties() {
        return properties;
    }
}
