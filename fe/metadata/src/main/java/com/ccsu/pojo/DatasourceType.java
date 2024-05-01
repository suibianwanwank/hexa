package com.ccsu.pojo;

import proto.datafusion.SourceType;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;

public enum DatasourceType {
    MYSQL,
    HIVE2,
    POSTGRESQL,
    ORACLE,
    SQLSERVER;

    public static SourceType transformToProtoSourceType(DatasourceType datasourceType){
        switch (datasourceType) {
            case MYSQL:
                return SourceType.MYSQL;
            case HIVE2:
                return SourceType.HIVE2;
            case ORACLE:
                return SourceType.ORACLE;
            case SQLSERVER:
                return SourceType.SQLSERVER;
            case POSTGRESQL:
                return SourceType.POSTGRESQL;
        }
        throw new CommonException(CommonErrorCode.PLAN_TRANSFORM_ERROR, "Not support source type:" + datasourceType);
    }
}
