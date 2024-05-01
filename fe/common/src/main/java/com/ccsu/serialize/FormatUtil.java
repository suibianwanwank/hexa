package com.ccsu.serialize;

import com.google.gson.Gson;

/**
 * Gson based json serialization deserialization utility class.
 */
public final class FormatUtil {
    private static final Gson GSON = new Gson();

    private FormatUtil() {
    }

    public static <T> String toJsonString(T pojo) {
        return GSON.toJson(pojo);
    }

    public static <T> T fromJson(String jsonString, Class<T> tClass) {
        return GSON.fromJson(jsonString, tClass);
    }

    public static <T> T fromJson(byte[] jsonBytes, Class<T> tClass) {
        return GSON.fromJson(new String(jsonBytes), tClass);
    }
}
