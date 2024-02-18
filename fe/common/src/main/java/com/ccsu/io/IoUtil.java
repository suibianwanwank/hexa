package com.ccsu.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class IoUtil {
    private IoUtil() {
    }

    public static boolean deleteFileIfExist(String path) {
        File file = new File(path);
        if (file.exists()) {
            try {
                Files.walkFileTree(Paths.get(path), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(
                            Path file, BasicFileAttributes attributes) throws IOException {
                        Files.delete(file); // 有效，因为它始终是一个文件
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(
                            Path dir, IOException exc) throws IOException {
                        Files.delete(dir); //这将起作用，因为目录中的文件已被删除
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                return false;
            }
        }
        return true;
    }
}
