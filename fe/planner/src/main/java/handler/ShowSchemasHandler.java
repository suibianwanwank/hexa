package handler;

import com.ccsu.MetadataService;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.SchemaInfo;
import com.ccsu.parser.sqlnode.SqlShowSchemas;
import context.QueryContext;

import java.util.List;

import static com.ccsu.utils.TableFormatUtil.generateCenterTable;

public class ShowSchemasHandler
        implements SqlHandler<String, SqlShowSchemas, QueryContext> {

    private static final String[] RETURN_COLUMNS = {"SchemaName"};

    @Override
    public String handle(SqlShowSchemas showSchemas, QueryContext context) {
        String catalogName = showSchemas.getCatalogName().getSimple();

        String clusterId = context.getClusterId();

        MetadataService metadataService = context.getMetadataService();

        List<SchemaInfo> schemaInfos = metadataService.getAllSchemas(new MetaIdentifier(clusterId, MetaPath.buildCatalogPath(catalogName)));

        return generateResult(schemaInfos);
    }

    private String generateResult(List<SchemaInfo> schemaInfos) {
        if (schemaInfos == null
                || schemaInfos.isEmpty()) {
            return "";
        }

        String[][] data = new String[schemaInfos.size()][1];
        for (int i = 0; i < schemaInfos.size(); i++) {
            data[i][0] = schemaInfos.get(i).getSchemaName();
        }

        return generateCenterTable(RETURN_COLUMNS, data);
    }
}
