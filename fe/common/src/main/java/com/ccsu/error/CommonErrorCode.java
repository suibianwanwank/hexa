package com.ccsu.error;

import lombok.Getter;

@Getter
public enum CommonErrorCode {

    SQL_PARSER_ERROR(0x0000_0000, CommonErrorType.USER_ERROR),

    METADATA_ERROR(0x0000_0001, CommonErrorType.USER_ERROR),

    META_COLLECT_ERROR(0x0000_0002, CommonErrorType.UNKNOWN_ERROR),

    JDBC_CONNECT_ERROR(0x0000_0003, CommonErrorType.EXTERNAL_ERROR),

    FILE_ERROR(0x0000_0004, CommonErrorType.EXTERNAL_ERROR),

    INDEX_STORE_ERROR(0x0000_0005, CommonErrorType.SYSTEM_ERROR),

    SYSTEM_OPTION_ERROR(0x0000_0006, CommonErrorType.SYSTEM_ERROR),

    PLANNER_CANCEL_ERROR(0x0000_0007, CommonErrorType.SYSTEM_ERROR),

    PLAN_TRANSFORM_ERROR(0x0000_0008, CommonErrorType.SYSTEM_ERROR),

    UNKNOWN_ERROR(0x9999_9999, CommonErrorType.UNKNOWN_ERROR);
    private final int code;
    private final CommonErrorType type;

    CommonErrorCode(int code, CommonErrorType type) {
        this.code = code;
        this.type = type;
    }


    @Override
    public String toString() {
        return "CodeType : " + name() + "  "
                + "code : " + code + "  "
                + "errorType : " + type.toString();
    }
}
