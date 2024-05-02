package com.ccsu.parser;

import com.ccsu.profile.JobProfile;
import org.apache.calcite.sql.SqlNode;

public interface SqlParser {
    SqlNode parse(String sql, JobProfile profile);

    SqlNode parse(String sql);
}
