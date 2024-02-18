package com.ccsu.serialize;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
