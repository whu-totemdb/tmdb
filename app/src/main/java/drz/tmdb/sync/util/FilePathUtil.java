package drz.tmdb.sync.util;

import android.content.Context;

public class FilePathUtil {

    //应用的文件存储目录
    public static String getFileDir(Context context){
        return context.getFilesDir().getAbsolutePath();
    }

    //应用的文件缓存目录
    public static String getCacheDir(Context context){
        return context.getCacheDir().getAbsolutePath();
    }

    public static String getDatabasePath(Context context, String s){
        return context.getDatabasePath(s).getAbsolutePath();
    }

    public static String getExternalFileDir(Context context, String s){
        return context.getExternalFilesDir(s).getAbsolutePath();
    }
}
