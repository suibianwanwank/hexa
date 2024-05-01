package handler;

import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.parser.sqlnode.SqlShowTables;
import context.QueryContext;

import java.util.ArrayList;
import java.util.List;

public class ShowTablesHandler
        implements SqlHandler<String, SqlShowTables, QueryContext> {

    private static final String[] RETURN_COLUMNS = new String[]{"TableName"};

    @Override
    public String handle(SqlShowTables sqlNode, QueryContext context) {
        String clusterId = context.getClusterId();

        List<String> paths = new ArrayList<>(sqlNode.getSourcePath().names);
        MetaPath path = new MetaPath(paths, MetaPath.PathType.SCHEMA);

        List<TableInfo> tableInfos = context.getMetadataService().getAllTable(new MetaIdentifier(clusterId, path));

        String[][] data = new String[tableInfos.size()][1];
        for (int i = 0; i < tableInfos.size(); i++) {
            TableInfo tableInfo = tableInfos.get(i);
            data[i][0] = tableInfo.getTableName();
        }

        return HandlerUtil.generateCenterTable(RETURN_COLUMNS, data);
    }
}
