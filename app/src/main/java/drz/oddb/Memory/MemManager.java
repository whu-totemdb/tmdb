package drz.oddb.Memory;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.List;
import java.util.Random;

import drz.oddb.Level.FileData;
import drz.oddb.Level.LevelManager;
import drz.oddb.Transaction.Constant;
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

        //test();
    }

    private void test(){

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


        // add 测试
        Random random = new Random();
        int i = 0;
        while(true){
            ObjectTableItem o = new ObjectTableItem(random.nextInt(100), i,random.nextInt(100),random.nextInt(100));
            add(o);
            ClassTableItem c = new ClassTableItem("name", i, i, i, "attrname", "int", "ori");
            add(c);
            SwitchingTableItem s = new SwitchingTableItem("attr", "" + i, "00");
            add(s);
            i++;
        }

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
