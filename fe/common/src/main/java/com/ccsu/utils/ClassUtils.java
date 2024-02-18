package com.ccsu.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ClassUtils {
    private ClassUtils() {
    }

    //
    public static List<Class<?>> getClassesByPackage(String packageName, ClassLoader classLoader) {
        try {
            ImmutableSet<ClassPath.ClassInfo> classes = ClassPath.from(classLoader)
                    .getTopLevelClassesRecursive(packageName);
            return classes.stream().map(ClassPath.ClassInfo::load).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to get classes from package: " + packageName, e);
        }
    }
}
