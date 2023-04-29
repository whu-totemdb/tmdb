package drz.tmdb.level;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import drz.tmdb.memory.MemManager;
import drz.tmdb.memory.SystemTable.BiPointerTableItem;
import drz.tmdb.memory.SystemTable.ClassTableItem;
import drz.tmdb.memory.SystemTable.DeputyTableItem;
import drz.tmdb.memory.SystemTable.ObjectTableItem;
import drz.tmdb.memory.Tuple;
import drz.tmdb.map.TrajectoryPoint;
import drz.tmdb.map.TrajectoryUtils;

public class Test {

    // RamUsageEstimator 计算对象占用大小测试
    public static void test1(){
        Random random = new Random();
        long[] sizes = new long[100];
        for(int i=0; i<100; i++){
            ObjectTableItem o = new ObjectTableItem();
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
            ObjectTableItem o = new ObjectTableItem();
            memManager.add(o);
            ClassTableItem c = new ClassTableItem();
            memManager.add(c);
            //SwitchingTableItem s = new SwitchingTableItem("attr", "" + i, "00");
            //memManager.add(s);
            i++;
        }
    }

    // MemTable写入SSTable
    public static void test3() throws IOException {
        MemManager memManager = new MemManager();
        ObjectTableItem o = new ObjectTableItem();
        memManager.add(o);
        o = new ObjectTableItem();
        memManager.add(o);
        o = new ObjectTableItem();
        memManager.add(o);
        o = new ObjectTableItem();
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
    public static void test12() throws IOException {
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

    // 测试B-Tree search
    public static void test14(){
        BTree<String, Long> btree = new BTree<String, Long>(3);
        for (int i = 0; i < 20; ++i) {
            String k = "k" + i;
            long v = 123L * i;
            btree.insert(k, v);
        }
        try{
            File f = new File(Constant.DATABASE_DIR + "test");
            f.createNewFile();
            BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f, true));
            long rootOffset = btree.write(writeAccess, 0)[1];
            writeAccess.flush();
            writeAccess.close();
            BTree<String, Long> btree2 = new BTree<>("test", rootOffset);

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 10; ++i) {
                int random = (new Random()).nextInt(30);
                System.out.println(btree2.search("k" + random));
            }
            long t2 = System.currentTimeMillis();
            System.out.println("查询b树1k次耗时" + (t2 - t1) + "ms");

        }catch (Exception e){
            e.printStackTrace();
        }
        return;


    }

    // compaction测试
    public static void test15() throws IOException {
        SSTable sst1 = new SSTable("SSTable1", 1);
        for(int i=0; i<1000; i++){
            sst1.data.put("k" + 2 * i, "v1");
        }
        long t1 = System.currentTimeMillis();
        sst1.writeSSTable();
        long t2 = System.currentTimeMillis();
        System.out.println("写一个SSTable耗时" + (t2 - t1) + "ms");
        SSTable sst2 = new SSTable("SSTable2", 1);
        for(int i=0; i<1000; i++){
            sst2.data.put("k" + 3 * i, "v2");
        }
        sst2.writeSSTable();
        SSTable sst3 = new SSTable("SSTable3", 1);
        for(int i=0; i<1000; i++){
            sst3.data.put("k" + 4 * i, "v3");
        }
        sst3.writeSSTable();
        LevelManager levelManager = new LevelManager();
        levelManager.level_1.add(1);
        levelManager.level_1.add(2);
        levelManager.level_2.add(3);
        levelManager.levelInfo.put("1", "1-200-k0-k999");
        levelManager.levelInfo.put("2", "1-200-k0-k999");
        levelManager.levelInfo.put("3", "2-200-k0-k999");
        levelManager.levelInfo.put("maxDataFileSuffix", "3");
        System.out.println("开始compaction");
        t1 = System.currentTimeMillis();
        levelManager.manualCompaction(1);
        t2 = System.currentTimeMillis();
        System.out.println("执行compaction耗时" + (t2 - t1) + "ms");
        SSTable sst4 = new SSTable("SSTable4", 2);
        return;
    }

    // bloom filter 命中率检测 -> 假阳性概率约为0.5%
    public static void test16(){
        int hit = 0;
        int in = 0;
        int falsePositive = 0;
        SSTable sst1 = new SSTable("test_3", 1);
        for(int i=0; i<100000; i++){
            sst1.data.put("k" + i, "v1");
        }
        sst1.writeSSTable();
        Random random = new Random();
        for(int i=0; i<100000; i++){
            int randomInt = random.nextInt(200000);
            String searchKey = "k" + randomInt;
            if(randomInt < 100000){
                in++;
                if(sst1.bloomFilter.check(searchKey))
                    hit++;
            }else if(sst1.bloomFilter.check(searchKey)){
                falsePositive++;
            }
        }
        System.out.println("执行1w次，其中应在SSTable中" + in + "次，检测到" + hit + "次，假阳性" + falsePositive + "次");
        return;

    }

    // 测试B树 right search
    // re-test: 2023/4/27
    public static void test18(){
        BTree bTree = new BTree<>(3);
        for(int i=0; i<10; i++){
            bTree.insert("k" + i, (long)i);
        }
        System.out.println(bTree.getMaxKey());
        System.out.println(bTree.leftSearch("k"+0));//0
        System.out.println(bTree.leftSearch("k05"));//1
        System.out.println(bTree.leftSearch("k"+15));//2
        System.out.println(bTree.leftSearch("k"+25));//3
        System.out.println(bTree.leftSearch("k"+35));//4
        System.out.println(bTree.leftSearch("k"+45));//5
        System.out.println(bTree.leftSearch("k"+55));//6
        System.out.println(bTree.leftSearch("k"+65));//7
        System.out.println(bTree.leftSearch("k"+75));//8
        System.out.println(bTree.leftSearch("k"+85));//9
        System.out.println(bTree.leftSearch("k"+95));//null

        return;
    }

    // 测试search
    public static void test17() throws IOException {
//        MemManager memManager = new MemManager();
//        for(int i=0; i<1000; i++){
//            Tuple t = new Tuple();
//            t.tupleId = i;
//            memManager.add(t);
//        }
//        memManager.saveMemTableToFile();
//        System.out.println("开始search");
//        long t1 = System.currentTimeMillis();
//        int findCount = 0;
//        for(int i=0; i<2000; i++){
//            Tuple t = memManager.search("" + i);
//            if(t != null)
//                findCount++;
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println("执行1000次search耗时" + (t2 - t1) + "ms"); // 2.8s
//        return;

    }

    // 测试系统表的读写
    public static void test19() throws IOException {
        MemManager memManager1 = new MemManager();
        memManager1.add(new BiPointerTableItem(1,2,3,4));
        memManager1.add(new BiPointerTableItem(100,200,300,400));
        memManager1.add(new ClassTableItem("1", 1, 2, 3, "4", "5", "6", "7"));
        memManager1.add(new ClassTableItem("zvfz", 1, 2, 3, "zfwzf", "zfwsz", "zfws", "fdzg"));
        //memManager1.add(new SwitchingTableItem("zdfa", "dawd", "dasd"));
        //memManager1.add(new SwitchingTableItem("zzvc", "zsdsz", "dwww"));
        memManager1.add(new DeputyTableItem(0, 1, new String[]{"120", "arwar", "fafaf"}));
        memManager1.add(new DeputyTableItem(100, 555, new String[]{"122220", "arwagsr", "faf358af", "444fww"}));
        memManager1.add(new ObjectTableItem(10,20));
        memManager1.add(new ObjectTableItem(50,70));
        memManager1.saveAll();
        MemManager memManager2 = new MemManager();
        return;
    }

    // 测试Tuple的序列化与反序列化
    public static void test20(){
        Tuple t1 = new Tuple();
        t1.tupleHeader = 5;
        t1.tuple = new Object[t1.tupleHeader];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleHeader = 5;
        t2.tuple = new Object[t2.tupleHeader];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        String str1 = JSONObject.toJSONString(t1);
        System.out.println(str1);
        String str2 = JSONObject.toJSONString(t1);
        System.out.println(str2);

        Tuple t3 = JSON.parseObject(str1, Tuple.class);
        Tuple t4 = JSON.parseObject(str2, Tuple.class);

        long time1 = System.currentTimeMillis();
        for(int i=1; i<1000; i++){
            String str = JSONObject.toJSONString(t1);
        }
        long time2 = System.currentTimeMillis();
        System.out.println("序列化1k次" + (time2 - time1) + "ms");

        time1 = System.currentTimeMillis();
        for(int i=1; i<1000; i++){
            Tuple t5 = JSON.parseObject(str1, Tuple.class);
        }
        time2 = System.currentTimeMillis();
        System.out.println("反序列化1k次" + (time2 - time1) + "ms");

        return ;

    }

    // 测试update
    public static void test21() throws IOException {

        SSTable s = new SSTable("test",1);
        s.data.put("0", "e");
        s.data.put("1", "e");
        s.writeSSTable();
        SSTable ss = new SSTable("test", 2);
        String str = ss.search("1");
        return;
    }

    // 测试轨迹序列化与反序列化
    public static void test22() throws IOException {

        ArrayList<TrajectoryPoint> trajectory = new ArrayList<>();
        trajectory.add(new TrajectoryPoint(23.55532231, 420.1563456465));
        trajectory.add(new TrajectoryPoint(44.43454832, 420.1544338874));
        String str = TrajectoryUtils.serialize(trajectory);
        ArrayList<TrajectoryPoint> trajectory2 = TrajectoryUtils.deserialize(str);
        return;
    }


    // 有缓存下的search测试
    public static void test23() throws IOException {
        MemManager memManager = new MemManager();
        for(int i=0; i<1000; i++){
            Tuple t = new Tuple();
            t.tupleId = i;
            memManager.add(t);
        }
        memManager.saveMemTableToFile();
        System.out.println("开始search");
        long t1 = System.currentTimeMillis();
        int findCount = 0;
        for(int i=0; i<1000; i++){
            Object t = memManager.search("t" + i);
            if(t != null)
                findCount++;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("meta block cache提速后执行1000次search耗时" + (t2 - t1) + "ms"); // 1030ms

        long t3 = System.currentTimeMillis();
        for(int i=0; i<1000; i++){
            Object t = memManager.search("t" + i);
        }
        long t4 = System.currentTimeMillis();
        System.out.println("data cache提速后执行1000次searchsearch耗时" + (t4 - t3) + "ms"); // 17ms


        return;

    }

    public static void test24() throws IOException {

    }



}