package edu.whu.tmdb.storage.memory;

import com.alibaba.fastjson2.JSONObject;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.storage.cache.CacheManager;
import edu.whu.tmdb.storage.level.LevelManager;
import edu.whu.tmdb.storage.level.SSTable;
import edu.whu.tmdb.storage.memory.Flush;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.utils.Constant;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import edu.whu.tmdb.util.FileOperation;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;


public class MemManager {
    // 数据表
    public TreeMap<K, V> memTable = new TreeMap<>();

    // 当前数据表占用内存大小
    private int currentMemSize = 0;

    // 系统表
    public static ObjectTable objectTable = new ObjectTable();
    public static ClassTable classTable = new ClassTable();
    public static DeputyTable deputyTable = new DeputyTable();
    public static BiPointerTable biPointerTable = new BiPointerTable();
    public static SwitchingTable switchingTable = new SwitchingTable();

    // 日志管理
    public LogManager logManager = new LogManager(this);

    // 缓存管理
    public static CacheManager cacheManager = new CacheManager();

    // LSM-Tree层级管理
    public static LevelManager levelManager = new LevelManager();


    // 1. 私有静态变量，用于保存MemManager的单一实例
    private static volatile MemManager instance = null;

    // 3. 提供一个全局访问点
    public static MemManager getInstance(){
        // 双重检查锁定模式
        if (instance == null) { // 第一次检查
            synchronized (MemManager.class) {
                if (instance == null) { // 第二次检查
                    instance = new MemManager();
                    levelManager.cacheManager = cacheManager;
                }
            }
        }
        return instance;
    }

    // 2. 私有构造函数，确保不能从类外部实例化
    /**构造函数负责从文件中读取历史数据，并将系统表加载到内存中*/
    public MemManager(){
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }

        File f = new File(Constant.SYSTEM_TABLE_DIR);
        if(!f.exists()){
            f.mkdirs();
            return;
        }

        try {
            loadDeputyTable();
            loadSwitchingTable();
            loadClassTable();
            loadBiPointerTable();
            loadObjectTable();
        }catch (Exception e){
            e.printStackTrace();
        }


        levelManager.cacheManager = this.cacheManager;
        // 将level-0和level-1和level-2的SSTable的meta block存进缓存中
        for(Integer fileSuffix : this.levelManager.level_0){
            this.cacheManager.metaCache.add(new SSTable("SSTable" + fileSuffix, 3));
        }
        for(Integer fileSuffix : this.levelManager.level_1){
            this.cacheManager.metaCache.add(new SSTable("SSTable" + fileSuffix, 3));
        }
        for(Integer fileSuffix : this.levelManager.level_2){
            this.cacheManager.metaCache.add(new SSTable("SSTable" + fileSuffix, 3));
        }
    }

    // 持久化保存所有数据
    public void saveAll(){
        try{
            saveSwitchingTable();
            saveDeputyTable();
            saveClassTable();
            saveBiPointerTable();
            saveObjectTable();
        }catch (Exception e){
            e.printStackTrace();
        }

        if(this.memTable.size() != 0)
            saveMemTableToFile();
        this.levelManager.saveMetaToFile();
    }


    // 往MemManager中添加对象
    public void add(Object o){
        if(o instanceof ObjectTableItem){
            objectTable.objectTableList.add((ObjectTableItem) o);
        }else if(o instanceof BiPointerTableItem){
            biPointerTable.biPointerTableList.add((BiPointerTableItem) o);
        }else if(o instanceof ClassTableItem){
            classTable.classTableList.add((ClassTableItem) o);
        }else if(o instanceof DeputyTableItem){
            deputyTable.deputyTableList.add((DeputyTableItem) o);
        }else if(o instanceof SwitchingTableItem){
            switchingTable.switchingTableList.add((SwitchingTableItem) o);
        }else if(o instanceof Tuple){
            //先写日志
            K k = new K("t" + ((Tuple) o).tupleId);
            V v = new V(JSONObject.toJSONString((Tuple) o));
            logManager.WriteLog(k.key, (byte) 0, v.valueString);
            this.memTable.put(k, v);
            this.currentMemSize += k.key.length() + v.valueString.length();

            // 加入缓存
            cacheManager.dataCache.put(k, v);

            // 如果内存数据大小超过限制则开始compaction
            if(this.currentMemSize > Constant.MAX_MEM_SIZE){
                try{
                    saveMemTableToFile();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


    // 清空内存中的数据
    void clearMem(){
        this.currentMemSize = 0;
        this.memTable.clear();
    }


    // 将内存中的数据持久化保存
    public int saveMemTableToFile(){

        if (this.memTable.size() == 0) {
            return -1;
        }

        // 获取最新dataFileSuffix并+1
        int dataFileSuffix = levelManager.addFileSuffix();

        // 开启flush
        Flush flush = new Flush(dataFileSuffix, this);
        flush.run();

        return dataFileSuffix;
    }


    // 查询指定key，返回value
    public V search(K key){

        // 先查cache
        V cacheResult = this.cacheManager.dataCache.get(key);
        if(cacheResult != null)
            return cacheResult;

        // 查MEMTable
        V memResult = this.memTable.get(key);
        if(memResult != null)
            return memResult;

        // 从level-0 依次往底层查找直到找到
        try{
            for(int i = 0; i<=Constant.MAX_LEVEL; i++){
                ArrayList<Integer> arrayList = new ArrayList<>(levelManager.levels[i]);
                for(int j=arrayList.size()-1; j>=0; j--){
                    Integer suffix = arrayList.get(j);
                    // i ：level
                    // j : fileSuffix

                    // 从缓存中获取SSTable，获取不到再去读磁盘
                    SSTable sst = this.cacheManager.metaCache.get(suffix);

                    // 查询一个SSTable
                    V diskResult = sst.search(key);

                    // search的结果为null表示key不在zone map的范围内，则跳过该SSTable，查询该层的下一个
                    if(diskResult == null)
                        continue;

                    // search的结果为new V()表示在此SSTable中没有找到对应key
                    if(diskResult.equals(new V())){
                        if(i == 0)
                            // level 0 由于存在overlap，即使查到new V()也要继续遍历该层所有SSTable
                            continue;
                        else
                            // 其他层不同SSTable之间不存在overlap，找到new V()，该层就可以直接跳过
                            break;
                    }

                    // 成功找到的情况
                    // 将此k-v加入缓存
//                    this.cacheManager.dataCache.put(key, diskResult);
                    return diskResult;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        // 如果所有SSTable中都没有，则返回null
        return null;
    }


    // 范围查询，两个参数分别表示 开始key 和 结束key
    public Map<K, V> rangeQuery(K startKey, K endKey){
        Map<K, V> result = new TreeMap<>();

        // 输入参数有问题，返回空
        if(startKey == null || endKey == null || startKey.compareTo(endKey) >= 0)
            return result;

//        // 1. 查缓存
//        Map<K, V> m1 = this.cacheManager.dataCache.cachedData.subMap(startKey, endKey);
//        result.putAll(m1);

//        // 2. 查内存表
//        Map<K, V> m2 = this.memTable.subMap(startKey, endKey);
//        for(Map.Entry<K, V> entry : m2.entrySet()){
//            // 内存表中数据的优先程度不如缓存，缓存中一定是最新的，因此有重叠的部分保留缓存中的抛弃内存中的
//            if(!result.containsKey(entry.getKey()))
//                result.put(entry.getKey(), entry.getValue());
//        }

        // 3. 查SSTable
        // 从level-0 依次往底层查找直到找到
        try{
            for(int i = 0; i<=Constant.MAX_LEVEL; i++){
                ArrayList<Integer> arrayList = new ArrayList<>(levelManager.levels[i]);
                for(int j=arrayList.size()-1; j>=0; j--){
                    Integer suffix = arrayList.get(j);
                    // i ：level
                    // j : fileSuffix
                    SSTable sst = this.cacheManager.metaCache.get(suffix);

                    // 查询一个SSTable
                    Map<K, V> m3 = sst.rangeQuery(startKey, endKey);

                    // 由于遍历SSTable时是按照新到旧的顺序，因此此处采用如下的淘汰策略
                    for(Map.Entry<K, V> entry : m3.entrySet()){
                        if(!result.containsKey(entry.getKey()))
                            result.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

//        // 更新缓存
//        for(Map.Entry<K, V> entry : result.entrySet()){
//            this.cacheManager.dataCache.put(entry.getKey(), entry.getValue());
//        }

        return result;
    }


    // BiPointerTableItem 有四个int属性
    // classid  objectid deputyid  deputyobjectid
    public void saveBiPointerTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "bpt");
        if (!f.exists()) {
            f.createNewFile();
        }
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(BiPointerTableItem item : this.biPointerTable.biPointerTableList){
            // 存classid
            writeAccess.write(Constant.INT_TO_BYTES(item.classid));
            // 存objectid
            writeAccess.write(Constant.INT_TO_BYTES(item.objectid));
            // 存deputyid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyid));
            // 存deputyobjectid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyobjectid));
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadBiPointerTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "bpt");
        if(!f.exists()){
            return;
        }
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long fileSize = raf.length();
        long cur = 0L;
        while(cur < fileSize){
            // 读取4个int构造BiPointerTableItem
            BiPointerTableItem item = new BiPointerTableItem(raf.readInt(), raf.readInt(), raf.readInt(), raf.readInt());
            this.biPointerTable.biPointerTableList.add(item);
            cur += Integer.BYTES * 4;
        }
    }


    // 先用一个int存maxClassId
    // ClassTableItem有以下属性
    // int      classid
    // int      attrnum
    // int      attrid
    // String   classname
    // String   attrname
    // String   attrtype
    // String   classtype
    // String   alias
    public void saveClassTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "ct");
        if (!f.exists()) {
            f.createNewFile();
        }
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));

        // 存maxClassId
        writeAccess.write(Constant.INT_TO_BYTES(this.classTable.maxid));

        // 存各个ClassTableItem
        for(ClassTableItem item : this.classTable.classTableList){
            // 存classid
            writeAccess.write(Constant.INT_TO_BYTES(item.classid));
            // 存attrnum
            writeAccess.write(Constant.INT_TO_BYTES(item.attrnum));
            // 存attrid
            writeAccess.write(Constant.INT_TO_BYTES(item.attrid));
            // 存classname，String类型需要先存一个4字节int作为长度，再存储数据
            writeAccess.write(Constant.INT_TO_BYTES(item.classname.length()));
            writeAccess.write(item.classname.getBytes());
            // 存attrname，String类型需要先存一个4字节int作为长度
            writeAccess.write(Constant.INT_TO_BYTES(item.attrname.length()));
            writeAccess.write(item.attrname.getBytes());
            // 存attrtype，String类型需要先存一个4字节int作为长度
            writeAccess.write(Constant.INT_TO_BYTES(item.attrtype.length()));
            writeAccess.write(item.attrtype.getBytes());
            // 存classtype，String类型需要先存一个4字节int作为长度
            writeAccess.write(Constant.INT_TO_BYTES(item.classtype.length()));
            writeAccess.write(item.classtype.getBytes());
            // 存alias，String类型需要先存一个4字节int作为长度
            writeAccess.write(Constant.INT_TO_BYTES(item.alias.length()));
            writeAccess.write(item.alias.getBytes());
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadClassTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "ct");
        if (!f.exists()) {
            return;
        }
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long fileSize = raf.length();
        long cur = 0L;
        // 先读maxClassId
        this.classTable.maxid = raf.readInt();
        cur += Integer.BYTES;
        while(cur < fileSize){
            // 读各个ClassTableItem
            ClassTableItem item = new ClassTableItem();
            // 读classid
            item.classid = raf.readInt();
            cur += Integer.BYTES;
            // 读attrnum
            item.attrnum = raf.readInt();
            cur += Integer.BYTES;
            // 读attrid
            item.attrid = raf.readInt();
            cur += Integer.BYTES;
            // 读classname，String类型需要先读一个4字节int作为长度
            int len = raf.readInt();
            //System.out.println(raf.getFilePointer());
            byte[] buffer = new byte[len];
            raf.read(buffer);
            item.classname = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读attrname，String类型需要先读一个4字节int作为长度
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.attrname = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读attrtype，String类型需要先读一个4字节int作为长度
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.attrtype = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读classtype，String类型需要先读一个4字节int作为长度
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.classtype = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读alias，String类型需要先读一个4字节int作为长度
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.alias = new String(buffer);
            cur += (Integer.BYTES + len);

            this.classTable.classTableList.add(item);
        }
    }

    // DeputyTableItem 有以下属性
    // int originid
    // int deputyid
    // String[] deputyrule
    public void saveDeputyTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "dt");
        if(!f.exists()) {
            f.createNewFile();
        }
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(DeputyTableItem item : this.deputyTable.deputyTableList){
            // 存originid
            writeAccess.write(Constant.INT_TO_BYTES(item.originid));
            // 存deputyid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyid));
            // 存deputyrule，String[]类型，先用int存有多少个String，每个String前也需要一个int存该String的长度
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyrule.length));   // deputyrule的数量
            for(String str : item.deputyrule){
                writeAccess.write(Constant.INT_TO_BYTES(str.length()));
                writeAccess.write(str.getBytes());
            }
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadDeputyTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "dt");
        if(!f.exists()) { return; }

        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long fileSize = raf.length();
        long cur = 0L;
        while(cur < fileSize){
            DeputyTableItem item = new DeputyTableItem();

            // 读originid
            item.originid = raf.readInt();
            cur += Integer.BYTES;

            // 读deputyid
            item.deputyid = raf.readInt();
            cur += Integer.BYTES;

            // 读deputyrule，参考存的逻辑，取出deputyrule的数量、每个deputyrule的长度和数据
            int deputyruleAmount = raf.readInt();
            cur += Integer.BYTES;
            item.deputyrule = new String[deputyruleAmount];
            for (int i = 0; i < deputyruleAmount; i++) {
                int strLength = raf.readInt();      // 读deputyrule的长度
                cur += Integer.BYTES;
                byte[] buffer = new byte[strLength];// 读deputyrule的数据
                raf.read(buffer);
                cur += strLength;
                item.deputyrule[i] = new String(buffer);
            }
            this.deputyTable.deputyTableList.add(item);
        }
    }

    // SwitchingTableItem的属性：
    // int oriId
    // int oriAttrid
    // int deputyId
    // int deputyAttrId
    // String rule = ""
    public void saveSwitchingTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "st");
        FileOperation.createNewFile(f);
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(SwitchingTableItem item: this.switchingTable.switchingTableList){
            // 存oriclassid
            writeAccess.write(Constant.INT_TO_BYTES(item.oriId));
            // writeAccess.write(item.oriId);
            // 存oriattrid
            writeAccess.write(Constant.INT_TO_BYTES(item.oriAttrid));
            // writeAccess.write(item.deputy.getBytes());
            // 存deputyclassid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyId));
            // writeAccess.write(item.attr.getBytes());
            // 存deputyattrid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyAttrId));
            // writeAccess.write(item.deputy.getBytes());
            // 存rule，先存储长度，再存储数据
            writeAccess.write(Constant.INT_TO_BYTES(item.rule.length()));
            writeAccess.write(item.rule.getBytes());
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadSwitchingTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "st");
        if(!f.exists()) {
            return;
        }
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long fileSize = raf.length();
        long cur = 0L;
        while(cur < fileSize){
            SwitchingTableItem item = new SwitchingTableItem();

            // 读取oriId
            item.oriId = raf.readInt();
            cur += Integer.BYTES;

            // 读取oriAttrid
            item.oriAttrid = raf.readInt();
            cur += Integer.BYTES;

            // 读取deputyId
            item.deputyId = raf.readInt();
            cur += Integer.BYTES;

            // 读取deputyAttrId
            item.deputyAttrId = raf.readInt();
            cur += Integer.BYTES;

            // 读rule
            int ruleLength = raf.readInt();
            cur += Integer.BYTES;
            byte[] buffer = new byte[ruleLength];
            raf.read(buffer);
            cur += ruleLength;
            item.rule = new String(buffer);

            this.switchingTable.switchingTableList.add(item);
        }
    }

    // 先用int记录maxTupleId
    // ObjectTableItem的属性：
    // int classid
    // int tupleid
    // int sstSuffix
    public void saveObjectTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "ot");
        FileOperation.createNewFile(f);
        RandomAccessFile raf = new RandomAccessFile(f, "rw");

        // 用int记录maxTupleId
        raf.writeInt(this.objectTable.maxTupleId);

        // 依次存每个ObjectTableItem
        for(ObjectTableItem item : this.objectTable.objectTableList){
            // 存classid
            raf.writeInt(item.classid);
            // 存tupleid
            raf.writeInt(item.tupleid);
            // 存sstSuffix
            raf.writeInt(item.sstSuffix);
        }
        raf.close();
    }

    public void loadObjectTable() throws IOException {
        File f = new File(Constant.SYSTEM_TABLE_DIR + "ot");
        if(!f.exists()){
            return;
        }
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long fileSize = raf.length();
        long cur = 0L;

        // 读 maxTupleId
        objectTable.maxTupleId = raf.readInt();
        cur += Integer.BYTES;

        while(cur < fileSize){
            ObjectTableItem item = new ObjectTableItem();
            // 读classid
            item.classid = raf.readInt();
            cur += Integer.BYTES;
            // 读tupleid
            item.tupleid = raf.readInt();
            cur += Integer.BYTES;
            // 读sstSuffix
            item.sstSuffix = raf.readInt();
            cur += Integer.BYTES;

            objectTable.objectTableList.add(item);
        }
    }

}
