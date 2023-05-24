package drz.tmdb.level;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

// 定义一些常量和静态方法
public class Constant {

    // LSM-Tree文件目录
    public static final String DATABASE_DIR = "/data/data/drz.tmdb/level/";

    // 最大level数
    public static final int MAX_LEVEL = 6;

    // level0 允许的最大SSTable数量
    public static final int MAX_LEVEL0_FILE_COUNT = 4;

    // data block大小限制 4KB
    public static final long MAX_DATA_BLOCK_SIZE = 4 * 1024;

    // 允许各level的总大小 8MB 10MB 100MB 1000MB
    public static final long MAX_LEVEL0_SIZE = 8L * 1024 * 1024;
    public static final long MAX_LEVEL1_SIZE = 10L * 1024 * 1024;
    public static final long MAX_LEVEL2_SIZE = 100L * 1024 * 1024;
    public static final long MAX_LEVEL3_SIZE = 1000L * 1024 * 1024;
    public static final long MAX_LEVEL4_SIZE = 10000L * 1024 * 1024;
    public static final long MAX_LEVEL5_SIZE = 100000L * 1024 * 1024;
    public static final long MAX_LEVEL6_SIZE = 1000000L * 1024 * 1024;
    public static final long[] MAX_LEVEL_SIZE = {MAX_LEVEL0_SIZE, MAX_LEVEL1_SIZE, MAX_LEVEL2_SIZE, MAX_LEVEL3_SIZE, MAX_LEVEL4_SIZE, MAX_LEVEL5_SIZE, MAX_LEVEL6_SIZE};

    // key 作为String允许占用的最大长度
    public static final int MAX_KEY_LENGTH = 16;

//    /**
//     * BTree的阶<br>
//     * BTree中关键字个数为[ceil(m/2)-1,m-1]    <br>
//     * BTree中子树个数为[ceil(m/2),m]
//     */
//    public static final int BTREE_ORDER = 200;
//
//    /**
//     * 非根节点中最小的关键字个数
//     */
//    public static final int MIN_KEY_SIZE = (int) (Math.ceil(BTREE_ORDER / 2.0) - 1);
//
//    /**
//     * 非根节点中最大的关键字个数
//     */
//    public static final int MAX_KEY_SIZE = BTREE_ORDER - 1;
//
//    /**
//     * 每个结点中最小的孩子个数
//     */
//    public static final int MIN_CHILDREN_SIZE = (int) (Math.ceil(BTREE_ORDER / 2.0));
//
//    /**
//     * 每个结点中最大的孩子个数
//     */
//    public static final int MAX_CHILDREN_SIZE = BTREE_ORDER ;
//

    // 编码key字符串为byte[]
    public static final byte[] KEY_TO_BYTES(String key){
        byte[] ret = new byte[Constant.MAX_KEY_LENGTH];
        byte[] temp = key.getBytes();
        if(temp.length <= Constant.MAX_KEY_LENGTH){
            for(int i=0;i<temp.length;i++){
                ret[i]=temp[i];
            }
            for(int i=temp.length;i<Constant.MAX_KEY_LENGTH;i++){
                // 不足的地方补全0
                ret[i]=(byte)32;
            }
        }
        return ret;
    }

    // 解码byte[]为key
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


    // 判断区间[a, b]  [c, d]是否有重叠
    // 如果b<c或者d<a则没有重叠
    public static boolean hasOverlap(String a, String b, String c, String d){
        if(b.compareTo(c)<= 0 || d.compareTo(a)<=0)
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
    public static byte[] readBytesFromFile(String fileName, long offset, int length) {
        byte[] ret = new byte[length];
        try {
            // 打开文件
            RandomAccessFile raf = new RandomAccessFile(DATABASE_DIR + fileName, "r");
            // 移动到指定偏移并读取相应长度
            raf.seek(offset);
            raf.read(ret);
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }



}
