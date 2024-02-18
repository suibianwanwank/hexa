package validator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class UDFOperatorTable implements SqlOperatorTable {

    private static final SqlOperatorTable STD_OPERATOR_TABLE = SqlStdOperatorTable.instance();

    private final ListMultimap<String, SqlOperator> opMap = ArrayListMultimap.create();

    private final List<SqlOperator> operators = new ArrayList<>();

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        @Nullable SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList,
                                        SqlNameMatcher nameMatcher) {

    }

    @Override
    public List<SqlOperator> getOperatorList() {
        return operators;
    }

    public void add(String name, SqlOperator sqlOperator) {
        opMap.put(name, sqlOperator);
        operators.add(sqlOperator);
    }
}
