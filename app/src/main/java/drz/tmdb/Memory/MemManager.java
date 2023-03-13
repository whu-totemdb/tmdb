package drz.tmdb.Memory;

import static drz.tmdb.Level.Test.*;


import org.apache.lucene.util.RamUsageEstimator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

import drz.tmdb.Level.Constant;
import drz.tmdb.Level.LevelManager;
import drz.tmdb.Level.SSTable;
import drz.tmdb.Log.LogManager;
import drz.tmdb.Memory.SystemTable.BiPointerTable;
import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTable;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTable;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTable;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTable;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;

public class MemManager {

    // 系统表
    public static ObjectTable objectTable = new ObjectTable();
    public static ClassTable classTable = new ClassTable();
    public static DeputyTable deputyTable = new DeputyTable();
    public static BiPointerTable biPointerTable = new BiPointerTable();
    public static SwitchingTable switchingTable = new SwitchingTable();

    // 数据表
    public TupleList tupleList = new TupleList();

    // 当前数据表占用内存大小
    private int currentMemSize = 0;

    public LevelManager levelManager = new LevelManager();

    public LogManager logManager = new LogManager();

    // 构造函数
    // 从文件中读取历史数据，将系统表加载到内存中
    public MemManager() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR);
        if(!f.exists()){
            f.mkdirs();
            return;
        }
        loadDeputyTable();
        loadSwitchingTable();
        loadClassTable();
        loadBiPointerTable();
        loadObjectTable();
    }

    // 持久化保存所有数据
    public void saveAll() throws IOException {
        saveSwitchingTable();
        saveDeputyTable();
        saveClassTable();
        saveBiPointerTable();
        saveObjectTable();
        //saveMemTableToFile();
        //this.levelManager.saveMetaToFile();
    }


    // 往MemManager中添加对象
    public void add(Object o){
        //先写日志
        //String k = Constant.calculateKey(o);
        //String v = JSONObject.toJSONString(o);
        //logManager.WriteLog(k, (byte) 0,v);

        if(o instanceof ObjectTableItem){
            this.objectTable.objectTable.add((ObjectTableItem) o);
        }else if(o instanceof BiPointerTableItem){
            this.biPointerTable.biPointerTable.add((BiPointerTableItem) o);
        }else if(o instanceof ClassTableItem){
            this.classTable.classTable.add((ClassTableItem) o);
        }else if(o instanceof DeputyTableItem){
            this.deputyTable.deputyTable.add((DeputyTableItem) o);
        }else if(o instanceof SwitchingTableItem){
            this.switchingTable.switchingTable.add((SwitchingTableItem) o);
        }else if(o instanceof Tuple){
            this.tupleList.addTuple((Tuple) o);
            this.currentMemSize += RamUsageEstimator.sizeOf(o);
        }


        // 如果内存数据大小超过限制则开始compaction
        if(this.currentMemSize > drz.tmdb.Memory.Constant.MAX_MEM_SIZE){
            try{
                System.out.println("内存已满，开始写入外存--------");
                long t1 = System.currentTimeMillis();
                saveMemTableToFile();
                long t2 = System.currentTimeMillis();
                System.out.println("将写满的MemTable写到SSTable耗时" + (t2 - t1) + "ms");
                clearMem();

                // 同时触发levelManager的autoCompaction
                this.levelManager.autoCompaction();
            }catch (Exception e){
                e.printStackTrace();
            }

        }

    }

    // 清空内存中的数据
    private void clearMem(){
        this.currentMemSize = 0;
        this.tupleList.tuplelist.clear();
    }


    // 将内存中的数据持久化保存
    public void saveMemTableToFile() throws IOException {
        // 获取最新dataFileSuffix并+1
        int dataFileSuffix = Integer.parseInt(levelManager.levelInfo.get("maxDataFileSuffix")) + 1;
        levelManager.levelInfo.put("maxDataFileSuffix", "" + dataFileSuffix);

        // 生成SSTable对象，将内存中的对象以k-v的形式转移到FileData中
        SSTable sst= new SSTable("SSTable" + dataFileSuffix, 1);

        for(Tuple t : this.tupleList.tuplelist){
            String k = "" + t.tupleId;
            sst.data.put(k, JSONObject.toJSONString(t));
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
    public Tuple search(String key){
        return null;
    }


    // BiPointerTableItem 有四个int属性
    // classid  objectid deputyid  deputyobjectid
    public void saveBiPointerTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "bpt");
        if(!f.exists())
            f.createNewFile();
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(BiPointerTableItem item : this.biPointerTable.biPointerTable){
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
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "bpt");
        if(!f.exists())
            return;
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long l = raf.length();
        long cur = 0L;
        while(cur < l){
            // 读取4个int构造BiPointerTableItem
            BiPointerTableItem item = new BiPointerTableItem(raf.readInt(), raf.readInt(), raf.readInt(), raf.readInt());
            this.biPointerTable.biPointerTable.add(item);
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
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "ct");
        if(!f.exists())
            f.createNewFile();
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));

        // 存maxClassId
        writeAccess.write(Constant.INT_TO_BYTES(this.classTable.maxid));

        // 存各个ClassTableItem
        for(ClassTableItem item: this.classTable.classTable){
            // 存classid
            writeAccess.write(Constant.INT_TO_BYTES(item.classid));
            // 存attrnum
            writeAccess.write(Constant.INT_TO_BYTES(item.attrnum));
            // 存attrid
            writeAccess.write(Constant.INT_TO_BYTES(item.attrid));
            // 存classname，String类型需要先存一个4字节int作为长度
            writeAccess.write(Constant.INT_TO_BYTES(item.classname.length()));
//            writeAccess.flush();
//            System.out.println(f.length());
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
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "ct");
        if(!f.exists())
            return;
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long l = raf.length();
        long cur = 0L;
        // 先读maxClassId
        this.classTable.maxid = raf.readInt();
        cur += Integer.BYTES;
        while(cur < l){
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

            this.classTable.classTable.add(item);
        }
    }

    // DeputyTableItem 有以下属性
    // int originid
    // int deputyid
    // String[] deputyrule
    public void saveDeputyTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "dt");
        if(!f.exists())
            f.createNewFile();
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(DeputyTableItem item: this.deputyTable.deputyTable){
            // 存originid
            writeAccess.write(Constant.INT_TO_BYTES(item.originid));
            // 存deputyid
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyid));
            // 存deputyrule，String[]类型，先用int存有多少个String，每个String前也需要一个int存该String的长度
            writeAccess.write(Constant.INT_TO_BYTES(item.deputyrule.length));
            for(String str : item.deputyrule){
                writeAccess.write(Constant.INT_TO_BYTES(str.length()));
                writeAccess.write(str.getBytes());
            }
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadDeputyTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "dt");
        if(!f.exists())
            return;
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long l = raf.length();
        long cur = 0L;
        while(cur < l){
            DeputyTableItem item = new DeputyTableItem();
            // 读originid
            item.originid = raf.readInt();
            cur += Integer.BYTES;
            // 读deputyid
            item.deputyid = raf.readInt();
            cur += Integer.BYTES;
            // 读deputyrule
            int strCount = raf.readInt();
            cur += Integer.BYTES;
            item.deputyrule = new String[strCount];
            for(int i=0; i<strCount; i++){
                int strLength = raf.readInt();
                cur += Integer.BYTES;
                byte[] buffer = new byte[strLength];
                raf.read(buffer);
                cur += strLength;
                item.deputyrule[i] = new String(buffer);
            }
            this.deputyTable.deputyTable.add(item);
        }
    }

    // SwitchingTableItem的属性：
    // String attr
    // String deputy
    // String rule
    public void saveSwitchingTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "st");
        if(!f.exists())
            f.createNewFile();
        BufferedOutputStream writeAccess = new BufferedOutputStream(new FileOutputStream(f));
        for(SwitchingTableItem item: this.switchingTable.switchingTable){
            // 存attr
            writeAccess.write(Constant.INT_TO_BYTES(item.attr.length()));
            writeAccess.write(item.attr.getBytes());
            // 存deputy
            writeAccess.write(Constant.INT_TO_BYTES(item.deputy.length()));
            writeAccess.write(item.deputy.getBytes());
            // 存rule
            writeAccess.write(Constant.INT_TO_BYTES(item.rule.length()));
            writeAccess.write(item.rule.getBytes());
        }
        writeAccess.flush();
        writeAccess.close();
    }

    public void loadSwitchingTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "st");
        if(!f.exists())
            return;
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long l = raf.length();
        long cur = 0L;
        while(cur < l){
            SwitchingTableItem item = new SwitchingTableItem();
            // 读attr
            int len = raf.readInt();
            byte[] buffer = new byte[len];
            raf.read(buffer);
            item.attr = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读deputy
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.deputy = new String(buffer);
            cur += (Integer.BYTES + len);
            // 读rule
            len = raf.readInt();
            buffer = new byte[len];
            raf.read(buffer);
            item.rule = new String(buffer);
            cur += (Integer.BYTES + len);

            this.switchingTable.switchingTable.add(item);
        }
    }

    // 先用int记录maxTupleId
    // ObjectTableItem的属性：
    // int classid
    // int tupleid
    // int sstSuffix
    public void saveObjectTable() throws IOException {
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "ot");
        if(!f.exists())
            f.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");

        // 用int记录maxTupleId
        raf.writeInt(this.objectTable.maxTupleId);

        // 依次存每个ObjectTableItem
        for(ObjectTableItem item: this.objectTable.objectTable){
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
        File f = new File(drz.tmdb.Memory.Constant.SYSTEM_TABLE_DIR + "ot");
        if(!f.exists())
            return;
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long l = raf.length();
        long cur = 0L;

        // 读 maxTupleId
        this.objectTable.maxTupleId = raf.readInt();
        cur += Integer.BYTES;

        while(cur < l){
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

            this.objectTable.objectTable.add(item);
        }
    }

}
