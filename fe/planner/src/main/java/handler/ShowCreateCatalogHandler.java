package handler;

import com.ccsu.error.CommonException;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.parser.sqlnode.SqlShowCreateCatalog;
import com.ccsu.pojo.DatasourceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import context.QueryContext;

import static com.ccsu.error.CommonErrorCode.JSON_ERROR;

public class ShowCreateCatalogHandler
        implements SqlHandler<String, SqlShowCreateCatalog, QueryContext> {

    @Override
    public String handle(SqlShowCreateCatalog sqlNode, QueryContext context) {
        String clusterId = context.getClusterId();
        MetaPath path = MetaPath.buildCatalogPath(sqlNode.getCatalogName().getSimple());

        DatasourceConfig config = context.getMetadataService()
                .getSourceConfigByCatalogName(new MetaIdentifier(clusterId, path));

        return generateCreateCatalogJson(sqlNode.getCatalogName().getSimple(), config);
    }

    private String generateCreateCatalogJson(String catalogName, DatasourceConfig config) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return "CREATE CATALOG "
                    + config.getSourceType()
                    + " AS \""
                    + catalogName
                    + "\" WITH "
                    + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
        } catch (Exception e) {
            throw new CommonException(JSON_ERROR, "Error to serialize datasource config");
        }
    }
}
