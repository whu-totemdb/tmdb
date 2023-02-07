package drz.tmdb.Level;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class BloomFilter {

    // 数据元素的个数
    private int itemCount;

    // 所需bit位数，要求必须为8的整数倍
    private int bitCount;

    // 所需byte位数
    private int byteCount;

    // 使用字节数组代替bit数组,转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
    private byte[] byteArray;


    // constructor 1 通过bit数组位数初始化
    public BloomFilter(int itemCount) {
        this.itemCount = itemCount;
        this.bitCount = 20 * itemCount;
        this.byteCount = bitCount / 8 + 1;
        this.byteArray = new byte[byteCount];
    }

    // constructor 2 通过文件名读文件进行初始化
    // 前4字节记录itemCount，剩余部分记录byteArray
    public BloomFilter(String fileName, long offset, int length){
        try{
            // 打开SSTable
            File f = new File(Constant.DATABASE_DIR + fileName);
            FileInputStream input = new FileInputStream(f);
            // 移动到指定偏移
            input.skip(offset);
            // 先读itemCount
            byte[] buffer = new byte[4];
            input.read(buffer, 0, 4);
            this.itemCount = Constant.BYTES_TO_INT(buffer, 0 , 4);
            this.bitCount = 20 * itemCount;
            this.byteCount = bitCount / 8 + 1;
            // 再读byteArray
            buffer = new byte[length - 4];
            input.read(buffer, 0, length - 4);
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
        int first = hashcode_1(key) % bitCount;
        int second = hashcode_2(key) % bitCount;
        int third = hashcode_3(key) % bitCount;

        // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
        int i = first >> 3; // n/8
        int j = first % 8;
        // 通过|操作将byte数组第i个元素的第j位置1
        byteArray[i] = (byte) (byteArray[i] | (1 << j));

        i = second >> 3;
        j = second % 8;
        byteArray[i] = (byte) (byteArray[i] | (1 << j));

        i = third >> 3; // n/8
        j = third % 8;
        byteArray[i] = (byte) (byteArray[i] | (1 << j));
    }


    /**
     * 判断数据是否存在
     * @param key
     * @return
     */
    public boolean check(String key) {
        int first = hashcode_1(key) % bitCount;
        int second = hashcode_2(key) % bitCount;
        int third = hashcode_3(key) % bitCount;

        // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
        int i = first >> 3; // n/8
        int j = first % 8;
        // 通过&操作取到byte数组第i个元素的第j位
        int firstIndex = (byteArray[i] >> j) & 1;
        if (firstIndex == 0) {
            return false;
        }

        i = second >> 3;
        j = second % 8;
        int secondIndex = (byteArray[i] >> j) & 1;
        if (secondIndex == 0) {
            return false;
        }

        i = third >> 3;
        j = third % 8;
        int thirdIndex = (byteArray[i] >> j) & 1;
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
        // todo 需要重写
        try{
            File f = new File(Constant.DATABASE_DIR + fileName);

            // 追加写
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(f, true));

            output.write(this.byteArray,0,this.byteArray.length);
            output.flush();
            output.close();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

}
