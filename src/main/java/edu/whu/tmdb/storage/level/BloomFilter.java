package edu.whu.tmdb.storage.level;


import edu.whu.tmdb.storage.utils.Constant;
import edu.whu.tmdb.storage.utils.K;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BloomFilter {

    // 数据元素的个数
    private int itemCount;

    // 所需bit位数，要求必须为8的整数倍
    private int bitCount;

    // 所需byte位数
    private int byteCount;

    // 使用字节数组代替bit数组,转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
    private byte[] byteArray;

    public int getByteCount() {
        return byteCount;
    }

    public int getItemCount() {
        return itemCount;
    }


    // constructor 1 通过bit数组位数初始化
    public BloomFilter(int itemCount) {
        // 限制最多允许1亿条数据
        if(itemCount > 10000000)
            itemCount = 10000000;
        this.itemCount = itemCount;
        this.bitCount = 20 * itemCount;
        this.byteCount = bitCount / 8 + 1;
        this.byteArray = new byte[byteCount];
        return;
    }

    // constructor 2 通过文件名读文件进行初始化
    // 前4字节记录itemCount，剩余部分记录byteArray
    public BloomFilter(RandomAccessFile raf, long offset, int length){
        byte[] buffer = Constant.readBytesFromFile(raf, offset, length);
        // 前4字节记录itemCount
        this.itemCount = Constant.BYTES_TO_INT(buffer, 0 , 4);
        this.bitCount = 20 * itemCount;
        this.byteCount = bitCount / 8 + 1;
        // 再读byteArray
        this.byteArray = new byte[byteCount];
        System.arraycopy(buffer, 4, this.byteArray, 0, this.byteCount);
    }


    /**
     * 写入数据
     * @param key
     */
    public void add(K key) {
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
    public boolean check(K key) {
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
    private int hashcode_1(K key) {
        int hash = 0;
        int i;
        byte[] bytes = key.serialize();
        for (i = 0; i < bytes.length; ++i) {
            hash = 33 * hash + bytes[i];
        }
        return Math.abs(hash);
    }


    /**
     * hash 算法2  Long原生hash
     * @param key
     * @return
     */
    private int hashcode_2(K key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        byte[] bytes = key.serialize();
        for (int i = 0; i < bytes.length; i++) {
            hash = (hash ^ bytes[i]) * p;
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
    private int hashcode_3(K key) {
        int hash, i;
        byte[] bytes = key.serialize();
        for (hash = 0, i = 0; i < bytes.length; ++i) {
            hash += bytes[i];
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return Math.abs(hash);
    }


    // 将BloomFilter记录到文件中
    // 先4B写itemCount，再写bytesArray
    public void writeToFile(BufferedOutputStream outputStream){
        byte[] buffer = new byte[4 + this.byteCount];
        // 4字节int记录itemCount
        System.arraycopy(Constant.INT_TO_BYTES(this.itemCount), 0, buffer, 0, Integer.BYTES);
        System.arraycopy(this.byteArray, 0, buffer, Integer.BYTES, this.byteCount);
        try{
            outputStream.write(buffer, 0, buffer.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
