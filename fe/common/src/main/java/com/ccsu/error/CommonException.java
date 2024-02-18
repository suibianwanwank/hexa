package com.ccsu.error;


public class CommonException extends RuntimeException {
    private final CommonErrorCode errorCode;
    private final String message;

    public CommonException(CommonErrorCode errorCode, String message) {
        this.message = message;
        this.errorCode = errorCode;
    }

    @Override
    public String getMessage() {
        return errorCode + "\n" + "detailMessage : " + message;
    }
}
