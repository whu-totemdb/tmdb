package drz.tmdb.Level;


public class Test {
    public static void test(){

//        // 关于FileInfo的测试
//        FileInfo f1 = new FileInfo(1, 300, "a1", "a10");
//        FileInfo f2 = new FileInfo(2,400, "a1", "a11");
//        FileInfo f3 = new FileInfo(3,300,"a0", "b10");
//        SortedSet<FileInfo> s = new TreeSet<>();
//        s.add(f1);
//        s.add(f2);
//        s.add(f3);
//        return;


//        // RamUsageEstimator 计算对象占用大小测试
//        Random random = new Random();
//        long[] sizes = new long[100];
//        for(int i=0; i<100; i++){
//            ObjectTableItem o = new ObjectTableItem(random.nextInt(100), i,random.nextInt(100),random.nextInt(100));
//            this.objectTable.objectTable.add(o);
//            sizes[i] = RamUsageEstimator.sizeOf(o);
//        }
//        return;


//        // add 测试
//        Random random = new Random();
//        int i = 0;
//        while(true){
//            ObjectTableItem o = new ObjectTableItem(random.nextInt(100), i,random.nextInt(100),random.nextInt(100));
//            add(o);
//            ClassTableItem c = new ClassTableItem("name", i, i, i, "attrname", "int", "ori");
//            add(c);
//            SwitchingTableItem s = new SwitchingTableItem("attr", "" + i, "00");
//            add(s);
//            i++;
//        }

////        // FileData 测试
//        ObjectTableItem o = new ObjectTableItem(10, 1, 10, 10);
//        add(o);
//        o = new ObjectTableItem(10, 2, 10, 10);
//        add(o);
//        o = new ObjectTableItem(10, 3, 10, 10);
//        add(o);
//        o = new ObjectTableItem(10, 4, 10, 10);
//        add(o);
//        saveMemTableToFile();
//        FileData f1 = new FileData("data1", 1);
//        return;


//        // bit数组与byte数组转化测试
//        int arraySize = 32;
//        byte[] array = new byte[arraySize>>3];
//        int[] nums = {10, 20, 16};
//        // 将第n位置为1
//        for (int n : nums){
//            // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
//            int i = n >> 3; // n/8
//            int j = n % 8;
//            array[i] = (byte) (array[i] | (1 << j));
//        }
//        // 取第n位
//        for (int n : nums){
//            // 转化关系：bit数组第n位 = byte数组第(n/8)个元素的第(n%8)位
//            int i = n >> 3; // n/8
//            int j = n % 8;
//            // 通过移位操作取到byte数组第i个元素的第j位
//            int firstIndex = (array[i] >> j) & 1;
//            System.out.println(firstIndex);
//        }

//        // Bloom Filter 读写测试
//        BloomFilter bf = new BloomFilter(Constant.BLOOM_FILTER_ARRAY_LENGTH);
//        bf.add("a1");
//        bf.add("a2");
//        bf.add("a3");
//        try{
//            File f = new File(Constant.DATABASE_DIR + "data1");
//            f.createNewFile();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        bf.writeToFile("data1");
//        BloomFilter bf2 = new BloomFilter("data1", 0);

//        // long的编码与解码
//        long x = 123456L;
//        byte[] b = Constant.LONG_TO_BYTES(x);
//        long y = Constant.BYTES_TO_LONG(b);


//        // B树测试
        long t1 = System.currentTimeMillis();
        BTree<String, String> btree = new BTree<String, String>(5);
        for (int i = 0; i < 100; ++i) {
            String k = "k" + i;
            String v = "v" + i;
            btree.insert(k, v);
        }
        long t2 = System.currentTimeMillis();
        //System.out.println("存储1w个键值对，耗时" + (t2 - t1) + "ms");


        // B数读写磁盘测试



        return;

    }
}
