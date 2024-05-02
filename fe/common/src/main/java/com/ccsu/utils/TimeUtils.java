package com.ccsu.utils;

public class TimeUtils {
    private TimeUtils() {
    }

    public static String formatNanosToMsString(long nanoTime) {
        double v = nanoTime / 1000000.00;
        return String.format("%fms", v);
    }

    public static String formatMsTimeToMsString(long millTime) {
        return String.format("%dms", millTime);
    }
}
