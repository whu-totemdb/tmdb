package edu.whu.tmdb.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Constants {

    // 日志文件目录
    public static final String LOG_BASE_DIR = "data/log/";

    // 从文件的offset偏移处读取长度为length的字节流
    public static byte[] readBytesFromFile( long offset, int length, String fileName) {
        byte[] ret = new byte[length];
        try {
            FileInputStream input = new FileInputStream(new File(fileName));
            // 移动到指定偏移并读取相应长度
            input.skip(offset);
            input.read(ret, 0, length);
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // 将字节流data，以追加的形式，写到文件中
    public static void writeBytesToFile(byte[] data){
        try{
            File fileTree = new File(LOG_BASE_DIR + "log_btree");
            // 写data
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(fileTree, true));
            output.write(data,0,data.length);
            output.flush();
            output.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


}
