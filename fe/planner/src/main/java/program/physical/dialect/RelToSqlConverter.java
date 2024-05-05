package program.physical.dialect;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public class RelToSqlConverter extends org.apache.calcite.rel.rel2sql.RelToSqlConverter {

    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
    public RelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    protected Result dispatch(RelNode e) {
        return super.dispatch(e);
    }

    public SqlNode convertRelToSqlNode(RelNode e) {
        return visitRoot(e).asStatement();
    }
}
