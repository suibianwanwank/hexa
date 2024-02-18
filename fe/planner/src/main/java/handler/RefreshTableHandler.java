package handler;

import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.parser.sqlnode.SqlRefreshTable;
import com.ccsu.parser.sqlnode.SqlShowTables;
import context.QueryContext;

import java.util.ArrayList;
import java.util.List;

public class RefreshTableHandler implements SqlHandler<String, SqlRefreshTable, QueryContext> {

    @Override
    public String handle(SqlRefreshTable sqlNode, QueryContext context) {
        String clusterId = context.getClusterId();

        List<String> paths = new ArrayList<>(sqlNode.getSourcePath().names);
        MetaPath path = new MetaPath(paths, MetaPath.PathType.TABLE);

        TableInfo table = context.getMetadataService().getAndRefreshTable(new MetaIdentifier(clusterId, path));

        if (table == null) {
            return String.format("Table: %s not exist", paths);
        }

        return Void.DEFAULT.toString();
    }
}
