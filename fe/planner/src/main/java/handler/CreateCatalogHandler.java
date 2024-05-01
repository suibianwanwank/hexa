package handler;

import com.ccsu.MetadataService;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.parser.sqlnode.SqlCreateCatalog;
import com.ccsu.parser.sqlnode.SqlKvEntry;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.pojo.DatasourceType;
import context.QueryContext;
import org.apache.calcite.sql.SqlNode;

public class CreateCatalogHandler implements SqlHandler<Void, SqlCreateCatalog, QueryContext> {

    @Override
    public Void handle(SqlCreateCatalog sqlNode, QueryContext context) {
        String catalogName = sqlNode.getCatalogName();

        MetaIdentifier identifier =
                new MetaIdentifier(context.getClusterId(), MetaPath.buildCatalogPath(catalogName));

        MetadataService metadataService = context.getMetadataService();

        CatalogInfo catalog = metadataService.getCatalog(identifier);

        if (catalog != null && sqlNode.ifNotExists) {
            return Void.DEFAULT;
        }

        // TODO add config message check
        DatasourceConfig datasourceConfig = new DatasourceConfig();
        for (SqlNode node : sqlNode.getProperties()) {
            SqlKvEntry kvEntry = (SqlKvEntry) node;
            String key = kvEntry.getKey().toString();
            String value = kvEntry.getValue().toString();
            fillDatasourceConfig(key, value, datasourceConfig);
        }

        datasourceConfig.setSourceType(DatasourceType.valueOf(sqlNode.getDatasourceType().toString()));
        metadataService.registerCatalog(identifier, datasourceConfig);
        return Void.DEFAULT;
    }

    private void fillDatasourceConfig(String key, String value, DatasourceConfig datasourceConfig) {
        switch (key.toUpperCase()) {
            case "HOST":
                datasourceConfig.setHost(value);
                break;
            case "PORT":
                datasourceConfig.setPort(value);
                break;
            case "USERNAME":
                datasourceConfig.setUsername(value);
                break;
            case "PASSWORD":
                datasourceConfig.setPassword(value);
                break;
            case "DATABASE":
                datasourceConfig.setDatabase(value);
                break;
        }
    }
}
