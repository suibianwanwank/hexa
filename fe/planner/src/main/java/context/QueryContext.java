package context;

import com.ccsu.option.OptionManager;
import com.ccsu.parser.SqlParser;
import com.google.common.base.Stopwatch;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.validate.SqlValidator;


public class QueryContext {
    private final String sql;
    private final String clusterId;
    private final String sqlJobId;
    private final RelOptCostFactory relOptCostFactory;
    private final SqlParser sqlParser;
    private final SqlValidator sqlValidator;
    private final Prepare.CatalogReader catalogReader;
    private OptionManager optionManager;
    private Stopwatch stopwatch;

    public QueryContext(String sql,
                        String clusterId,
                        String sqlJobId,
                        SqlParser sqlParser,
                        RelOptCostFactory relOptCostFactory,
                        Prepare.CatalogReader catalogReader,
                        SqlValidator sqlValidator) {
        this.sql = sql;
        this.clusterId = clusterId;
        this.sqlJobId = sqlJobId;
        this.sqlParser = sqlParser;
        this.relOptCostFactory = relOptCostFactory;
        this.catalogReader = catalogReader;
        this.sqlValidator = sqlValidator;
    }


    public RelOptCostFactory getRelOptCostFactory() {
        return relOptCostFactory;
    }

    public SqlValidator getSqlValidator() {
        return sqlValidator;
    }

    public Prepare.CatalogReader getCatalogReader() {
        return this.catalogReader;
    }

    public String getSql() {
        return sql;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getSqlJobId() {
        return sqlJobId;
    }

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    public OptionManager getOptionManager() {
        return optionManager;
    }
}
