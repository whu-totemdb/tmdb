package drz.oddb.Memory;

import static drz.oddb.Level.Test.test;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.List;

import drz.oddb.Level.FileData;
import drz.oddb.Level.LevelManager;
import drz.oddb.Level.Constant;
import drz.oddb.Transaction.SystemTable.BiPointerTableItem;
import drz.oddb.Transaction.SystemTable.ClassTableItem;
import drz.oddb.Transaction.SystemTable.DeputyTableItem;
import drz.oddb.Transaction.SystemTable.ObjectTableItem;
import drz.oddb.Transaction.SystemTable.SwitchingTableItem;

public class MemManager {

    public final List<ObjectTableItem> objectTable;
    private final List<ClassTableItem> classTable;
    private final List<DeputyTableItem> deputyTable;
    private final List<BiPointerTableItem> biPointerTable;
    private final List<SwitchingTableItem> switchingTable;

    private int currentMemSize = 0; // 当前数据占用内存大小

    public LevelManager levelManager = new LevelManager();

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
        int dataFileSuffix = Integer.parseInt(levelManager.totalIndex.get("maxDataFileSuffix")) + 1;
        levelManager.totalIndex.put("maxDataFileSuffix", "" + dataFileSuffix);

        // 生成FileData对象，将内存中的数据转移到FileData中
        FileData f = new FileData("data" + dataFileSuffix, 3);
        f.biPointerTable.addAll(this.biPointerTable);
        f.classTable.addAll(this.classTable);
        f.deputyTable.addAll(this.deputyTable);
        f.objectTable.addAll(this.objectTable);
        f.switchingTable.addAll(this.switchingTable);

        // 写SSTable
        List<String> info = f.writeSSTable();
        String length = info.get(0);
        String minKey = info.get(1);
        String maxKey = info.get(2);

        // 将该文件添加到对应level中
        levelManager.level_0.add(dataFileSuffix);
        // totalIndex 的结构  dataFileSuffix : level-length-minKey-maxKey
        levelManager.totalIndex.put("" + dataFileSuffix, "0" + "-" + length + "-" + minKey + "-" + maxKey);

        // 内存清空
        this.biPointerTable.clear();
        this.classTable.clear();
        this.deputyTable.clear();
        this.objectTable.clear();
        this.switchingTable.clear();

    }


}
