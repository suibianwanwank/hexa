package com.ccsu.utils;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;

public class TableFormatUtil {

    private TableFormatUtil() {
    }

    public static String generateCenterTable(String[] columnNames, String[][] data) {
        return generateCenterTable(columnNames, data, HorizontalAlign.CENTER, HorizontalAlign.CENTER);
    }

    public static String generateCenterTable(String[] columnNames, String[][] data,
                                             HorizontalAlign headerAlign, HorizontalAlign dataAlign) {
        Column[] columns = new Column[columnNames.length];

        for (int i = 0; i < columnNames.length; i++) {
            columns[i] = new Column().header(columnNames[i])
                    .headerAlign(headerAlign)
                    .dataAlign(dataAlign);
        }

        return AsciiTable.getTable(columns, data);
    }
}
