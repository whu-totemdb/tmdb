package drz.oddb.Level;

import static drz.oddb.Level.Constant.BYTES_TO_INT;
import static drz.oddb.Level.Constant.DATABASE_DIR;

import com.alibaba.fastjson.JSON;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class BloomFilter {

    /**
     * 数组长度，要求必须为8的整数倍
     */
    private int arraySize;

    /**
     * 数组
     * 使用字节数组代替bit数组，
     * 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
     */
    private byte[] array;


    // constructor 1 通过bit数组位数初始化
    public BloomFilter(int arraySize) {
        this.arraySize = arraySize >> 3;
        array = new byte[this.arraySize];
    }

    // constructor 2 通过文件名读文件进行初始化
    public BloomFilter(String fileName, int offset){
        this.arraySize = Constant.BLOOM_FILTER_ARRAY_LENGTH >> 3;
        this.array = new byte[this.arraySize];
        try{
            FileInputStream input = new FileInputStream(new File(DATABASE_DIR + fileName));
            input.skip(offset);
            input.read(this.array, 0, this.arraySize);
            input.close();
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 写入数据
     * @param key
     */
    public void add(String key) {
        int first = hashcode_1(key) % arraySize;
        int second = hashcode_2(key) % arraySize;
        int third = hashcode_3(key) % arraySize;

        // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
        int i = first >> 3; // n/8
        int j = first % 8;
        // 通过|操作将byte数组第i个元素的第j位置1
        array[i] = (byte) (array[i] | (1 << j));

        i = second >> 3;
        j = second % 8;
        array[i] = (byte) (array[i] | (1 << j));

        i = third >> 3; // n/8
        j = third % 8;
        array[i] = (byte) (array[i] | (1 << j));
    }


    /**
     * 判断数据是否存在
     * @param key
     * @return
     */
    public boolean check(String key) {
        int first = hashcode_1(key) % arraySize;
        int second = hashcode_2(key) % arraySize;
        int third = hashcode_3(key) % arraySize;

        // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
        int i = first >> 3; // n/8
        int j = first % 8;
        // 通过&操作取到byte数组第i个元素的第j位
        int firstIndex = (array[i] >> j) & 1;
        if (firstIndex == 0) {
            return false;
        }

        i = second >> 3;
        j = second % 8;
        int secondIndex = (array[i] >> j) & 1;
        if (secondIndex == 0) {
            return false;
        }

        i = third >> 3;
        j = third % 8;
        int thirdIndex = (array[i] >> j) & 1;
        if (thirdIndex == 0) {
            return false;
        }
        return true;
    }


    /**
     * hash 算法1
     * @param key
     * @return
     */
    private int hashcode_1(String key) {
        int hash = 0;
        int i;
        for (i = 0; i < key.length(); ++i) {
            hash = 33 * hash + key.charAt(i);
        }
        return Math.abs(hash);
    }


    /**
     * hash 算法2
     * @param data
     * @return
     */
    private int hashcode_2(String data) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < data.length(); i++) {
            hash = (hash ^ data.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash);
    }


    /**
     *  hash 算法3
     * @param key
     * @return
     */
    private int hashcode_3(String key) {
        int hash, i;
        for (hash = 0, i = 0; i < key.length(); ++i) {
            hash += key.charAt(i);
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return Math.abs(hash);
    }


    // 将BloomFilter记录到文件中
    public void writeToFile(String fileName){
        try{
            File f = new File(Constant.DATABASE_DIR + fileName);

            // 追加写
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(f, true));

            output.write(this.array,0,this.array.length);
            output.flush();
            output.close();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

}
