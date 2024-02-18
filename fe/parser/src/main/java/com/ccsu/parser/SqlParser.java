package com.ccsu.parser;

import org.apache.calcite.sql.SqlNode;

public interface SqlParser {
    SqlNode parse(String sql);
}
