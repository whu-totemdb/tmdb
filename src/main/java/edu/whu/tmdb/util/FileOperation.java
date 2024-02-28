package edu.whu.tmdb.util;/*
 * className:FileOperation
 * Package:edu.whu.tmdb.util
 * Description:
 * @Author: xyl
 * @Create:2023/8/6 - 14:58
 * @Version:v1
 */

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileOperation {
    public static void createNewFile(File file) {
        try {
            if (!file.exists()) {
                // Create parent directories if they don't exist
                file.getParentFile().mkdirs();
                // Create the file
                file.createNewFile();
                System.out.println(file+" created successfully!");
            }
//            else {
//                System.out.println(file+" already exists.");
//            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error creating file: " + e.getMessage());
        }
    }

    public static String getFileNameWithoutExtension(String srcPath) {
        Path path = Paths.get(srcPath);
        String fileName = path.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        if(dotIndex > 0) {
            return fileName.substring(0, dotIndex);
        }
        return fileName;
    }
}
