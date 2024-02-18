package handler;

import com.ccsu.MetadataService;
import com.ccsu.meta.data.CatalogInfo;
import com.ccsu.parser.sqlnode.SqlShowCatalogs;
import context.QueryContext;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;

import static handler.HandlerUtil.generateCenterTable;

public class ShowCatalogHandler
        implements SqlHandler<String, SqlShowCatalogs, QueryContext> {

    private static final String[] RETURN_COLUMNS = {"CatalogName", "SourceType"};

    @Override
    public String handle(SqlShowCatalogs showCatalogs, QueryContext context) {
        SqlIdentifier clusterIdentifier = showCatalogs.getClusterName();

        String clusterId = clusterIdentifier == null ? context.getClusterId() : clusterIdentifier.getSimple();

        MetadataService metadataService = context.getMetadataService();

        List<CatalogInfo> catalogInfos = metadataService.getAllCatalog(clusterId);


        return generateResult(catalogInfos);
    }

    private String generateResult(List<CatalogInfo> catalogInfos) {
        if (catalogInfos == null
                || catalogInfos.isEmpty()) {
            return "";
        }

        String[][] data = new String[catalogInfos.size()][2];
        for (int i = 0; i < catalogInfos.size(); i++) {
            data[i][0] = catalogInfos.get(i).getCatalogName();
            data[i][1] = catalogInfos.get(i).getDatasourceType().name();
        }

        return generateCenterTable(RETURN_COLUMNS, data);
    }
}
