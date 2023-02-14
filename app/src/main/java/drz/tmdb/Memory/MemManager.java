package drz.tmdb.Memory;

import static drz.tmdb.Level.Test.test;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Level.FileData;
import drz.tmdb.Level.LevelManager;
import drz.tmdb.Level.Constant;
import drz.tmdb.Transaction.SystemTable.BiPointerTableItem;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.DeputyTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;

public class MemManager {

    public List<ObjectTableItem> objectTable = new ArrayList<>();
    private List<ClassTableItem> classTable = new ArrayList<>();
    private List<DeputyTableItem> deputyTable = new ArrayList<>();
    private List<BiPointerTableItem> biPointerTable = new ArrayList<>();
    private List<SwitchingTableItem> switchingTable = new ArrayList<>();


    private int currentMemSize = 0; // 当前数据占用内存大小

    public LevelManager levelManager = new LevelManager();

    public MemManager(){

    }

    public MemManager(List<ObjectTableItem> o, List<ClassTableItem> c, List<DeputyTableItem> d, List<BiPointerTableItem> b, List<SwitchingTableItem> s){
        this.objectTable = o;
        this.classTable = c;
        this.deputyTable = d;
        this.biPointerTable = b;
        this.switchingTable = s;

        test();
    }


    // 往MemManager中添加对象
    public void add(Object o){
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

        // 生成FileData对象，将内存中的对象以k-v的形式转移到FileData中
        FileData f = new FileData("SSTable" + dataFileSuffix, 1);
        for(Object o : this.biPointerTable){
            String k = Constant.calculateKey(o);
            f.data.put(k, o);

        }
        for(Object o : this.classTable){
            String k = Constant.calculateKey(o);
            f.data.put(k, o);

        }
        for(Object o : this.deputyTable){
            String k = Constant.calculateKey(o);
            f.data.put(k, o);

        }
        for(Object o : this.objectTable){
            String k = Constant.calculateKey(o);
            f.data.put(k, o);

        }
        for(Object o : this.switchingTable){
            String k = Constant.calculateKey(o);
            f.data.put(k, o);

        }

        // 写SSTable
        long SSTableTotalSize = f.writeSSTable();

        // 将该文件添加到对应level中
        levelManager.level_0.add(dataFileSuffix);
        // totalIndex 的结构  dataFileSuffix : level-length-minKey-maxKey
        levelManager.levelInfo.put("" + dataFileSuffix, "0" + "-" + SSTableTotalSize + "-" + f.getMinKey() + "-" + f.getMaxKey());

        // 内存清空
        clearMem();
    }


}
