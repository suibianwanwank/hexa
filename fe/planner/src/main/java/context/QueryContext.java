package context;

import com.ccsu.MetadataService;
import com.ccsu.option.OptionManager;
import com.ccsu.parser.SqlParser;
import com.ccsu.profile.JobProfile;
import com.ccsu.store.api.StoreManager;
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
    private final StoreManager storeManager;
    private final OptionManager optionManager;
    private final List<SqlJobObserver> observers;
    private final MetadataService metadataService;
    private RelNode originRelNode;
    private final JobProfile jobProfile;

    public QueryContext(String sql,
                        String clusterId,
                        String sqlJobId,
                        SqlParser sqlParser,
                        RelOptCostFactory relOptCostFactory,
                        Prepare.CatalogReader catalogReader,
                        OptionManager optionManager,
                        SqlValidator sqlValidator,
                        List<SqlJobObserver> observers,
                        MetadataService metadataService,
                        StoreManager storeManager,
                        JobProfile jobProfile) {
        this.sql = sql;
        this.clusterId = clusterId;
        this.sqlJobId = sqlJobId;
        this.sqlParser = sqlParser;
        this.relOptCostFactory = relOptCostFactory;
        this.catalogReader = catalogReader;
        this.sqlValidator = sqlValidator;
        this.optionManager = optionManager;
        this.metadataService = metadataService;
        this.storeManager = storeManager;
        this.observers = observers;
        this.jobProfile = jobProfile;
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

    public JobProfile getJobProfile() {
        return jobProfile;
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }
}
