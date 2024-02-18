package com.ccsu.meta.type;

import com.ccsu.meta.data.ColumnInfo;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public interface TypeConverter {
    RelDataType convertType(List<ColumnInfo> columns);
}

