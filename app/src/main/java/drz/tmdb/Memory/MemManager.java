package drz.tmdb.Memory;

import static drz.tmdb.Level.Test.*;


import org.apache.lucene.util.RamUsageEstimator;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Level.Constant;
import drz.tmdb.Level.LevelManager;
import drz.tmdb.Level.SSTable;
import drz.tmdb.Log.LogManager;
import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;

public class MemManager {

    public List<ObjectTableItem> objectTable = new ArrayList<>();
    private List<ClassTableItem> classTable = new ArrayList<>();
    private List<DeputyTableItem> deputyTable = new ArrayList<>();
    private List<BiPointerTableItem> biPointerTable = new ArrayList<>();
    private List<SwitchingTableItem> switchingTable = new ArrayList<>();


    private int currentMemSize = 0; // 当前数据占用内存大小

    public LevelManager levelManager = new LevelManager();

    public LogManager logManager = new LogManager();

    public MemManager() throws IOException {

    }

    public MemManager(List<ObjectTableItem> o, List<ClassTableItem> c, List<DeputyTableItem> d, List<BiPointerTableItem> b, List<SwitchingTableItem> s) throws IOException {
        this.objectTable = o;
        this.classTable = c;
        this.deputyTable = d;
        this.biPointerTable = b;
        this.switchingTable = s;

        test17();
    }


    // 往MemManager中添加对象
    public void add(Object o) throws IOException {
        //先写日志
        String k = Constant.calculateKey(o);
        String v = JSONObject.toJSONString(o);
        logManager.WriteLog(k, (byte) 0,v);

        if(o instanceof ObjectTableItem){
            this.objectTable.add((ObjectTableItem) o);
        }else if(o instanceof BiPointerTableItem){
            this.biPointerTable.add((BiPointerTableItem) o);
        }else if(o instanceof ClassTableItem){
            this.classTable.add((ClassTableItem) o);
        }else if(o instanceof DeputyTableItem){
            this.deputyTable.add((DeputyTableItem) o);
        }else if(o instanceof SwitchingTableItem){
            this.switchingTable.add((SwitchingTableItem) o);
        }

        this.currentMemSize += RamUsageEstimator.sizeOf(o);
        // 如果内存数据大小超过限制则开始compaction
        if(this.currentMemSize > Constant.MAX_MEM_SIZE){
            System.out.println("内存已满，开始写入外存--------");
            long t1 = System.currentTimeMillis();
            saveMemTableToFile();
            long t2 = System.currentTimeMillis();
            System.out.println("将写满的MemTable写到SSTable耗时" + (t2 - t1) + "ms");
            clearMem();

            // 同时触发levelManager的autoCompaction
            this.levelManager.autoCompaction();
        }

    }

    // 清空内存中的数据
    private void clearMem(){
        this.currentMemSize = 0;
        this.biPointerTable.clear();
        this.classTable.clear();
        this.deputyTable.clear();
        this.objectTable.clear();
        this.switchingTable.clear();
    }


    // 将内存中的数据持久化保存
    public void saveMemTableToFile(){
        // 获取最新dataFileSuffix并+1
        int dataFileSuffix = Integer.parseInt(levelManager.levelInfo.get("maxDataFileSuffix")) + 1;
        levelManager.levelInfo.put("maxDataFileSuffix", "" + dataFileSuffix);

        // 生成SSTable对象，将内存中的对象以k-v的形式转移到FileData中
        SSTable sst= new SSTable("SSTable" + dataFileSuffix, 1);
        for(Object o : this.biPointerTable){
            String k = Constant.calculateKey(o);
            sst.data.put(k, JSONObject.toJSONString(o));

        }
        for(Object o : this.classTable){
            String k = Constant.calculateKey(o);
            sst.data.put(k, JSONObject.toJSONString(o));

        }
        for(Object o : this.deputyTable){
            String k = Constant.calculateKey(o);
            sst.data.put(k, JSONObject.toJSONString(o));

        }
        for(Object o : this.objectTable){
            String k = Constant.calculateKey(o);
            sst.data.put(k, JSONObject.toJSONString(o));

        }
        for(Object o : this.switchingTable){
            String k = Constant.calculateKey(o);
            sst.data.put(k, JSONObject.toJSONString(o));

        }

        // 写SSTable
        long SSTableTotalSize = sst.writeSSTable();

        // 将该文件添加到对应level中
        levelManager.level_0.add(dataFileSuffix);
        // levelInfo 的结构  dataFileSuffix : level-length-minKey-maxKey
        levelManager.levelInfo.put("" + dataFileSuffix, "0" + "-" + SSTableTotalSize + "-" + sst.getMinKey() + "-" + sst.getMaxKey());

        // 内存清空
        clearMem();
        //设置检查点
        logManager.setCheckpoint();
    }


    // 查询key对应的value
    // todo: write your code here
    public String search(String key) throws IOException {
        return null;
    }


    public void saveSystemTable(){
        saveBiPointerTable();
        saveClassTable();
        saveDeputyTable();
        saveSwitchingTable();
    }

    public void loadSystemTable(){
        loadBiPointerTable();
        loadClassTable();
        loadDeputyTable();
        loadSwitchingTable();
    }

    // BiPointerTableItem 有四个int属性
    //  classid  objectid deputyid  deputyobjectid
    private void saveBiPointerTable(){
        File f = new File(Constant.DATABASE_DIR + "bpt");

    }

    private void loadBiPointerTable(){

    }

    private void saveClassTable(){

    }

    private void loadClassTable(){

    }

    private void saveDeputyTable(){

    }

    private void loadDeputyTable(){

    }

    private void saveSwitchingTable(){

    }

    private void loadSwitchingTable(){

    }

}
