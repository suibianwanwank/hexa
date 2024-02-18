package com.ccsu.meta;

import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.TableInfo;
import org.apache.calcite.schema.TranslatableTable;

public interface ExtendTranslateTable extends TranslatableTable {
    TableInfo getTableInfo();

    DatasourceConfig getSourceConfig();
}
