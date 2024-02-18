package com.ccsu.utils;

public class StringUtil {
    private StringUtil() {
    }

    public static String trim(String str) {
        if (str == null) {
            return null;
        }
        return str.trim();
    }
}
