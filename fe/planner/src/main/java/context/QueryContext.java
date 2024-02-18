package context;

import com.ccsu.MetadataService;
import com.ccsu.option.OptionManager;
import com.ccsu.parser.SqlParser;
import com.google.common.base.Stopwatch;
import observer.SqlJobObserver;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

/**
 * Context of sql query handler.
 */
public class QueryContext {
    private final String sql;
    private final String clusterId;
    private final String sqlJobId;
    private final RelOptCostFactory relOptCostFactory;
    private final SqlParser sqlParser;
    private final SqlValidator sqlValidator;
    private final Prepare.CatalogReader catalogReader;
    private final OptionManager optionManager;
    private Stopwatch stopwatch;
    private final List<SqlJobObserver> observers;
    private final MetadataService metadataService;
    private RelNode originRelNode;

    public QueryContext(String sql,
                        String clusterId,
                        String sqlJobId,
                        SqlParser sqlParser,
                        RelOptCostFactory relOptCostFactory,
                        Prepare.CatalogReader catalogReader,
                        OptionManager optionManager,
                        SqlValidator sqlValidator,
                        List<SqlJobObserver> observers,
                        MetadataService metadataService) {
        this.sql = sql;
        this.clusterId = clusterId;
        this.sqlJobId = sqlJobId;
        this.sqlParser = sqlParser;
        this.relOptCostFactory = relOptCostFactory;
        this.catalogReader = catalogReader;
        this.sqlValidator = sqlValidator;
        this.optionManager = optionManager;
        this.metadataService = metadataService;
        this.stopwatch = Stopwatch.createUnstarted();
        this.observers = observers;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
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

    public List<SqlJobObserver> getObservers() {
        return observers;
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public RelNode getOriginRelNode() {
        return originRelNode;
    }

    public void setOriginRelNode(RelNode originRelNode) {
        this.originRelNode = originRelNode;
    }
}
