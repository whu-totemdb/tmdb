package edu.whu.tmdb.storage.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

// 定义一些常量和静态方法
public class Constant {

    // key 作为String允许占用的最大长度16B
    public static final int MAX_KEY_LENGTH = 16;

    // memTable最大大小为4MB=4*1024*1024B，超过就会触发compact到外存
    public static final long MAX_MEM_SIZE = 4L * 1024 * 1024;

    // LSM-Tree文件目录
    public static final String DATABASE_DIR = "data/level/";

    // 系统表文件目录
    public static final String SYSTEM_TABLE_DIR = "data/sys/";

    // 最大level数
    public static final int MAX_LEVEL = 6;

    // level0 允许的最大SSTable数量
    public static final int MAX_LEVEL0_FILE_COUNT = 4;

    // data block大小限制 4KB
    public static final int MAX_DATA_BLOCK_SIZE = 4 * 1024;

    // 允许各level的总大小 8MB 10MB 100MB 1000MB
    public static final long MAX_LEVEL0_SIZE = 8L * 1024 * 1024;
    public static final long MAX_LEVEL1_SIZE = 10L * 1024 * 1024;
    public static final long MAX_LEVEL2_SIZE = 100L * 1024 * 1024;
    public static final long MAX_LEVEL3_SIZE = 1000L * 1024 * 1024;
    public static final long MAX_LEVEL4_SIZE = 10000L * 1024 * 1024;
    public static final long MAX_LEVEL5_SIZE = 100000L * 1024 * 1024;
    public static final long MAX_LEVEL6_SIZE = 1000000L * 1024 * 1024;
    public static final long[] MAX_LEVEL_SIZE = {MAX_LEVEL0_SIZE, MAX_LEVEL1_SIZE, MAX_LEVEL2_SIZE, MAX_LEVEL3_SIZE, MAX_LEVEL4_SIZE, MAX_LEVEL5_SIZE, MAX_LEVEL6_SIZE};


    // 编码int为byte
    public static byte[] INT_TO_BYTES(int value){
        int len = 4;
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[len - i - 1] = (byte)(value >> 8 * i);
        }
        return b;
    }

    // 解码byte为int
    public static int BYTES_TO_INT(byte[] b, int start, int len) {
        int sum = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            int n = b[i]& 0xff;
            n <<= (--len) * 8;
            sum += n;
        }
        return sum;
    }

    // 编码long为byte[]
    public static byte[] LONG_TO_BYTES(long x){
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, x);
        return buffer.array();
    }

    // 解码byte[]为long
    public static long BYTES_TO_LONG(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }


    // 解码byte[]为key，长度不足的补0
    public static final String BYTES_TO_KEY(byte[] b){
        String s;
        int k=0;
        for(int i=0;i<Constant.MAX_KEY_LENGTH;i++){
            if(b[i]!=32){
                k++;
            }else{
                break;
            }
        }
        s=new String(b,0,k);
        return s;
    }

    // 判断区间[a, b]  [c, d]是否有重叠
    // 如果b<c或者d<a则没有重叠
    public static boolean hasOverlap(long a, long b, long c, long d){
        if(b <= c || d <= a)
            return false;
        else
            return true;
    }


    // 将字节流data，以追加的形式，写到文件fileName中
    public static void writeBytesToFile(byte[] data, String fileName){
        try{
            File file = new File(DATABASE_DIR + fileName);
            // 写data
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file, true));
            output.write(data,0,data.length);
            output.flush();
            output.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 从文件fileName的offset偏移处读取长度为length的字节流
    public static byte[] readBytesFromFile(RandomAccessFile raf, long offset, int length) {
        byte[] ret = new byte[length];
        try {
            // 移动到指定偏移并读取相应长度
            raf.seek(offset);
            raf.read(ret);
            //raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }


    // 比较array1从array1Start开始，长度为length的数组，是否等于，array2从array2Start开始，长度为length的数组
    public static boolean compareArray(byte[] array1, int array1Start, byte[] array2, int array2Start, int length) {
        if (array1Start < 0 || array2Start < 0 || (array1Start + length) > array1.length || (array2Start + length) > array2.length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (array1[array1Start + i] != array2[array2Start + i]) {
                return false;
            }
        }
        return true;
    }
}
