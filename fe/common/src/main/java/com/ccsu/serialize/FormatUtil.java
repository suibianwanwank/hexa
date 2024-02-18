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

    public static <T> T fromJsonFile(String url, Class<T> tClass) {
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(url));
        } catch (FileNotFoundException e) {
            throw new CommonException(CommonErrorCode.FILE_ERROR, e.getMessage());
        }
        return GSON.fromJson(reader, tClass);
    }

    public static byte[] toByte(Object obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
        } catch (IOException e) {
            return null;
        }
        return baos.toByteArray();
    }

    public static Object fromBytes(byte[] bytes) {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException | ClassNotFoundException ex) {
            //TODO error
        }
        return obj;
    }
}
