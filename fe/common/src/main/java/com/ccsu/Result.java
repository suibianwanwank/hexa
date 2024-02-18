package com.ccsu;

public interface Result<T> {
    boolean isSuccess();

    T getResult();

    String getErrorMsg();
}
