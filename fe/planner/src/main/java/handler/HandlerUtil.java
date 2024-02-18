package handler;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;

import java.util.List;

public class HandlerUtil {

    private HandlerUtil() {
    }

    public static String generateCenterTable(String[] columnNames, String[][] data) {
        Column[] columns = new Column[columnNames.length];

        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = new Column().header(columnNames[i]).headerAlign(HorizontalAlign.CENTER).dataAlign(HorizontalAlign.CENTER);
        }

        return AsciiTable.getTable(columns, data);
    }
}
