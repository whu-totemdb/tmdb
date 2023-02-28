package drz.tmdb.Level;


import com.alibaba.fastjson.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Random;

import drz.tmdb.Memory.MemManager;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;

public class Test {

    // RamUsageEstimator 计算对象占用大小测试
    public static void test1(){
        Random random = new Random();
        long[] sizes = new long[100];
        for(int i=0; i<100; i++){
            ObjectTableItem o = new ObjectTableItem(random.nextInt(100), i,random.nextInt(100),random.nextInt(100));
            sizes[i] = RamUsageEstimator.sizeOf(o);
        }
        return;
    }

    // add 测试
    public static void test2() throws IOException {
        MemManager memManager = new MemManager();
        Random random = new Random();
        int i = 0;
        while(true){
            ObjectTableItem o = new ObjectTableItem(random.nextInt(100), i,random.nextInt(100),random.nextInt(100));
            memManager.add(o);
            ClassTableItem c = new ClassTableItem();
            memManager.add(c);
            SwitchingTableItem s = new SwitchingTableItem("attr", "" + i, "00");
            memManager.add(s);
            i++;
        }
    }

    // MemTable写入SSTable
    public static void test3() throws IOException {
        MemManager memManager = new MemManager();
        ObjectTableItem o = new ObjectTableItem(10, 1, 10, 10);
        memManager.add(o);
        o = new ObjectTableItem(10, 2, 10, 10);
        memManager.add(o);
        o = new ObjectTableItem(10, 3, 10, 10);
        memManager.add(o);
        o = new ObjectTableItem(10, 4, 10, 10);
        memManager.add(o);
        memManager.saveMemTableToFile();
        SSTable sst = new SSTable("SSTable1", 1);
        return;
    }

    // bit数组与byte数组转化测试
    public static void test4(){
        int arraySize = 32;
        byte[] array = new byte[arraySize>>3];
        int[] nums = {10, 20, 16};
        // 将第n位置为1
        for (int n : nums){
            // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
            int i = n >> 3; // n/8
            int j = n % 8;
            array[i] = (byte) (array[i] | (1 << j));
        }
        // 取第n位
        for (int n : nums){
            // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
            int i = n >> 3; // n/8
            int j = n % 8;
            // 通过移位操作取到byte数组第i个元素的第j位
            int firstIndex = (array[i] >> j) & 1;
            System.out.println(firstIndex);
        }

    }

    // Bloom Filter 读写测试
    public static void test5(){
        BloomFilter bf = new BloomFilter(3);
        bf.add("a1");
        bf.add("a2");
        bf.add("a3");
        try{
            File f = new File(Constant.DATABASE_DIR + "data1");
            f.createNewFile();
            //bf.writeToFile("data1");
            BloomFilter bf2 = new BloomFilter(Constant.DATABASE_DIR + "data1", 0L, (int) f.length());
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    // long的编码与解码
    public static void test6(){
        long x = 123456L;
        byte[] b = Constant.LONG_TO_BYTES(x);
        long y = Constant.BYTES_TO_LONG(b);
    }

    // B树性能测试
    public static void test7(){
        long t1 = System.currentTimeMillis();
        BTree<String, String> btree = new BTree<String, String>(5);
        for (int i = 0; i < 10000; ++i) {
            String k = "k" + i;
            String v = "v" + i;
            btree.insert(k, v);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("存储1w个键值对，耗时" + (t2 - t1) + "ms");
    }

    // B数读写磁盘测试
    public static void test8(){
        BTree<String, Long> btree = new BTree<String, Long>(5);
        for (int i = 0; i < 1000; ++i) {
            String k = "k" + i;
            long v = 123L * i;
            btree.insert(k, v);
        }
        try{
            File f = new File(Constant.DATABASE_DIR + "test");
            f.createNewFile();
        }catch (Exception e){
            e.printStackTrace();
        }
        long t1 = System.currentTimeMillis();
        //long rootOffset = btree.write(Constant.DATABASE_DIR + "test", 0)[1];
        long t2 = System.currentTimeMillis();
        System.out.println("BTNode存储1000个键值对，耗时" + (t2 - t1) + "ms");
        //BTree<String, String> btree2 = new BTree<>(Constant.DATABASE_DIR + "test", rootOffset);
        long t3 = System.currentTimeMillis();
        System.out.println("BTNode读取1000个键值对，耗时" + (t3 - t2) + "ms");
        return;
    }

    // SSTable读写测试
    public static void test9() throws IOException {
        SSTable sst = new SSTable("SSTable1", 1);
        for(int i=1; i<50000; i++){
            sst.data.put("k" + i, "v" + i);
        }
        // 写
        long time1 = System.currentTimeMillis();
        sst.writeSSTable();
        long time2 = System.currentTimeMillis();
        System.out.println("写文件用时" + (time2 - time1) + "ms");
        // 读
        time1 = System.currentTimeMillis();
        SSTable f = new SSTable("SSTable1", 2);
        time2 = System.currentTimeMillis();
        System.out.println("读文件用时" + (time2 - time1) + "ms");
        return;
    }

    // 使用 Apache common io 来替代原来的java.io，提升读写文件的性能 测试
    // 结论：频繁OutputStream的创建、flush、close占用大量时间
    public static void test10(){
        long time1 = System.currentTimeMillis();
        try{
            File file = new File("/data/data/drz.tmdb/level/test1");
            //file.createNewFile();
            // 写data
            byte[] data = new byte[]{1, 2, 3, 5};
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file, true));
            for(int i=0; i<10000; i++){
                output.write(data,0,data.length);
            }
            output.flush();
            output.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
        long time2 = System.currentTimeMillis();
        System.out.println("原-写文件用时" + (time2 - time1) + "ms");  // 23ms

        time1 = System.currentTimeMillis();
        try{
            File file = FileUtils.getFile("/data/data/drz.tmdb/level/test2");
            //file.createNewFile();
            byte[] data = new byte[]{1, 2, 3, 5};
            for(int i=0; i<10000; i++){
                FileUtils.writeByteArrayToFile(file, data,true);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        time2 = System.currentTimeMillis();
        System.out.println("新-写文件用时" + (time2 - time1) + "ms"); // 830ms

        time1 = System.currentTimeMillis();
        try{
            RandomAccessFile raf = new RandomAccessFile("/data/data/drz.tmdb/level/test2", "rw");
            long fileLength = raf.length();
            raf.seek(fileLength);
            byte[] data = new byte[]{1, 2, 3, 5};
            for(int i=0; i<10000; i++){
                raf.write(data,0, data.length);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        time2 = System.currentTimeMillis();
        System.out.println("新-写文件用时" + (time2 - time1) + "ms"); // 106ms
    }

    // 读取耗时测试
    // 结论： 使用RandomAccessFile实现随机位置的读取快得多
    public static void test11() throws IOException {
        long time1 = System.currentTimeMillis();
        for(int i=0; i<10000; i++){
            byte[] n = Constant.readBytesFromFile("SSTable1", i * 5 , 10);
        }
        long time2 = System.currentTimeMillis();
        System.out.println("旧版read  用时" + (time2 - time1) + "ms"); // 687ms

        time1 = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("/data/data/drz.tmdb/level/SSTable1", "rw");
        for(int i=0; i<10000; i++){
            raf.seek(i * 5);
            byte[] b = new byte[10];
            raf.read(b, 0, 10);
        }
        time2 = System.currentTimeMillis();
        System.out.println("新版read  用时" + (time2 - time1) + "ms");   // 178ms
        return;
    }

    // compaction 选择SSTable 算法测试
    public static void test12(){
        // test1 : compaction in level 0
        LevelManager levelManager = new LevelManager(1);
        levelManager.levelInfo = new HashMap<>();
        levelManager.levelInfo.put("1", "0-1231-a1-a3");
        levelManager.levelInfo.put("2", "0-10086-a3-a5");
        levelManager.levelInfo.put("3", "0-7879-a1-a7");
        levelManager.levelInfo.put("4", "0-5454-a1-a8");
        levelManager.level_0.add(1);
        levelManager.level_0.add(2);
        levelManager.level_0.add(3);
        levelManager.level_0.add(4);
        levelManager.manualCompaction(0);


        // compaction 选择SSTable 算法测试
        // test2 : compaction in level >= 1
        levelManager = new LevelManager(1);
        levelManager.levelInfo = new HashMap<>();
        levelManager.levelInfo.put("2", "1-1231-b-e");
        levelManager.levelInfo.put("1", "1-10086-f-g");
        levelManager.levelInfo.put("3", "1-7879-h-i");
        levelManager.levelInfo.put("4", "1-5454-j-m");
        levelManager.levelInfo.put("5", "2-5454-a-c");
        levelManager.levelInfo.put("6", "2-5454-d-k");
        levelManager.levelInfo.put("7", "2-5454-l-o");
        levelManager.level_1.add(1);
        levelManager.level_1.add(2);
        levelManager.level_1.add(3);
        levelManager.level_1.add(4);
        levelManager.level_2.add(5);
        levelManager.level_2.add(6);
        levelManager.level_2.add(7);
        levelManager.manualCompaction(1); // expected 136

    }

    // 频繁 Object - String - byte[] 转化的耗时测试
    public static void test13(){
        long time1 = System.currentTimeMillis();
        for(int i=0; i<10000; i++){
            byte[] b = JSONObject.toJSONString(new Object()).getBytes();
        }
        long time2 = System.currentTimeMillis();
        System.out.println("用时" + (time2 - time1) + "ms");

    }


    public static void test14(){



    }


}
