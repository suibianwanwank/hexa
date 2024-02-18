package com.ccsu.parser;

import com.ccsu.error.CommonException;

import com.ccsu.parser.ExtendSqlParserImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import static com.ccsu.error.CommonErrorCode.SQL_PARSER_ERROR;

public class CalciteSqlParser implements SqlParser {

    private static final org.apache.calcite.sql.parser.SqlParser.Config CONFIG
            = org.apache.calcite.sql.parser.SqlParser.Config.DEFAULT
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withConformance(SqlConformanceEnum.MYSQL_5)
            .withQuotedCasing(Casing.UNCHANGED)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withParserFactory(ExtendSqlParserImpl.FACTORY);

    @Override
    public SqlNode parse(String sql) {
        org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql, CONFIG);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw new CommonException(SQL_PARSER_ERROR, e.getCause().getMessage());
        }
        return sqlNode;
    }
}
