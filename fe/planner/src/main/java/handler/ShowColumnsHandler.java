package handler;

import com.ccsu.meta.data.ColumnInfo;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import com.ccsu.meta.data.TableInfo;
import com.ccsu.parser.sqlnode.SqlShowColumns;
import com.ccsu.utils.TableFormatUtil;
import context.QueryContext;

import java.util.ArrayList;
import java.util.List;

public class ShowColumnsHandler
        implements SqlHandler<String, SqlShowColumns, QueryContext> {

    private static final String[] RETURN_COLUMNS = {"ColumnName", "ColumnType", "isNullable"};

    @Override
    public String handle(SqlShowColumns sqlNode, QueryContext context) {
        String clusterId = context.getClusterId();

        List<String> paths = new ArrayList<>(sqlNode.getSchemaPath().names);
        MetaPath path = new MetaPath(paths, MetaPath.PathType.TABLE);

        TableInfo table = context.getMetadataService().getTable(new MetaIdentifier(clusterId, path));

        if (table == null) {
            return String.format("Table path: %s not exist", path);
        }
        String[][] data = new String[table.getColumns().size()][3];
        for (int i = 0; i < table.getColumns().size(); i++) {
            ColumnInfo columnInfo = table.getColumns().get(i);
            data[i][0] = columnInfo.getColumnName();
            data[i][1] = columnInfo.showDataType();
            data[i][2] = String.valueOf(columnInfo.isNullable());
        }

        return TableFormatUtil.generateCenterTable(RETURN_COLUMNS, data);
    }
}
