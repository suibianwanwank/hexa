package com.ccsu.error;

/**
 * Class for all hexa fe exception.
 */
public class CommonException extends RuntimeException {
    private final CommonErrorCode errorCode;
    private final String message;

    public CommonException(CommonErrorCode errorCode, String message) {
        this.message = message;
        this.errorCode = errorCode;
    }

    @Override
    public String getMessage() {
        return errorCode.name() + ", detailMessage: " + message;
    }

    public CommonErrorCode getErrorCode() {
        return errorCode;
    }
}
