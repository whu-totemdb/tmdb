package drz.tmdb.Level;

import static drz.tmdb.Level.Constant.DATABASE_DIR;

import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import drz.tmdb.Transaction.SystemTable.BiPointerTableItem;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.DeputyTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;

// 充当MemTable和SSTable的中间媒介
// 需要从SSTable中读取数据时（例如compaction），先读到一个FileData对象中
// 需要从内存写到SSTable时，也生成一个FileData对象，再调用writeSSTable()方法
public class FileData {

    public List<ObjectTableItem> objectTable = new ArrayList<ObjectTableItem>();
    public List<ClassTableItem> classTable = new ArrayList<ClassTableItem>();
    public List<DeputyTableItem> deputyTable = new ArrayList<DeputyTableItem>();
    public List<BiPointerTableItem> biPointerTable = new ArrayList<BiPointerTableItem>();
    public List<SwitchingTableItem> switchingTable = new ArrayList<SwitchingTableItem>();


    // SSTable的文件名
    private String fileName;

    // 最大key与最小key
    private String maxKey = "";
    private String minKey = "";

    // BloomFilter
    public BloomFilter bloomFilter;

    // 索引-B树的根节点
    // key , value为"offset-length"
    public BTree<String, String> root = new BTree<>();

    // constructor
    // 将文件读到内存中
    // mode = 1 构造空的FileData对象，用于写文件
    // mode = 2 从SSTable读meta数据
    public FileData(String fileName, int mode){
        if(mode == 1){
            this.fileName = fileName;
        }
        if(mode == 2){
            // 读Footer
            long[] info = readFooter();
            long zoneMapOffset = info[0];
            long zoneMapLength = info[1];
            long bloomFilterOffset = info[2];
            long bloomFilterLength = info[3];
            long indexBlockOffset = info[4];
            long indexBlockLength = info[5];
            // 读zone map
            readZoneMap(zoneMapOffset, zoneMapLength);
            // 初始化BloomFilter
            readBloomFilter(bloomFilterOffset, bloomFilterLength);
            // 初始化index block
            readIndexBlock(indexBlockOffset, indexBlockLength);
        }
    }

    // 读Footer，返回的数据解析为6个long，分别对应zone map、bloom filter、index block的偏移和长度
    private long[] readFooter(){
        long[] ret = new long[6];
        try{
            // 打开SSTable
            File f = new File(Constant.DATABASE_DIR + this.fileName);
            FileInputStream input = new FileInputStream(f);
            // 移到文件末尾
            long fileLength = f.length();
            long startIndex = fileLength - 1 - 48;
            input.skip(startIndex);
            // 读取48B
            byte[] buffer = new byte[48];
            input.read(buffer, 0, 48);
            input.close();
            // 依次解析这6个long
            for(int i=0; i<6; i++){
                byte[] b = new byte[8];
                System.arraycopy(buffer, 8 * i, b, 0, 8);
                ret[i] = Constant.BYTES_TO_LONG(b);
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // 读zone map, zone map的格式：16字节先存minKey，16字节存maxKey
    private void readZoneMap(long offset, long length){
        try{
            // 打开SSTable
            File f = new File(Constant.DATABASE_DIR + this.fileName);
            FileInputStream input = new FileInputStream(f);
            // 移动到指定偏移并读取相应长度
            input.skip(offset);
            byte[] buffer = new byte[(int) length];
            input.read(buffer, 0, (int) length);
            input.close();
            // 解析数据
            byte[] b1 = new byte[Constant.MAX_KEY_LENGTH];
            byte[] b2 = new byte[Constant.MAX_KEY_LENGTH];
            System.arraycopy(buffer, 0, b1, 0, Constant.MAX_KEY_LENGTH);
            System.arraycopy(buffer, Constant.MAX_KEY_LENGTH, b2, 0, Constant.MAX_KEY_LENGTH);
            this.minKey = Constant.BYTES_TO_KEY(b1);
            this.maxKey = Constant.BYTES_TO_KEY(b2);
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }

    // 读BloomFilter, 前4字节记录Bloom Filter的itemCount
    private void readBloomFilter(long offset, long length){
        // 通过BloomFilter的构造函数初始化
        this.bloomFilter = new BloomFilter(this.fileName, offset, (int) length);
    }

    // 读index block，并将各BTNode重新建成树
    private void readIndexBlock(long offset, long length){
        // todo
    }




    // 写到level层的SSTable中
    // 返回["length", "minKey", "maxKey"]
    public List<String> writeSSTable(){

        List<Object> allObject = new ArrayList<>();
        allObject.addAll(this.biPointerTable);
        allObject.addAll(this.classTable);
        allObject.addAll(this.deputyTable);
        allObject.addAll(this.objectTable);
        allObject.addAll(this.switchingTable);

        // 记录最大最小key以及当前偏移
        String maxKey = "";
        String minKey = "";
        int offset = 0;

        for(Object o : allObject){
            // 写data
            String k = Constant.calculateKey(o);
            String v = JSONObject.toJSONString(o);
            byte[] data = v.getBytes(); // 将value值转化为字节流
            writeSingleToSSTable(data, this.dateFileName);
            this.indexMap.put(k, "" + offset + "-"+ data.length); // 更新索引， key : "开始下标-长度"
            offset += data.length; // 更新offset

            // 更新max和min
            if(maxKey.length() == 0){
                maxKey = k;
                minKey = k;
            }else{
                if(k.compareTo(maxKey)>0)
                    maxKey = k;
                if(k.compareTo(minKey)<0)
                    minKey = k;
            }
        }

        // minKey和maxKey写入索引文件
//        this.indexMap.put("minKey", minKey);
//        this.indexMap.put("maxKey", maxKey);
        byte[] min = Constant.KEY_TO_BYTES(minKey);
        writeSingleToSSTable(min, this.indexFileName);
        byte[] max = Constant.KEY_TO_BYTES(maxKey);
        writeSingleToSSTable(max, this.indexFileName);

        // 将索引转换成字节流
        byte[] index_data = JSONObject.toJSONString(this.indexMap).getBytes();
        byte[] meta = Constant.INT_TO_BYTES(index_data.length); // 使用4字节记录长度，读取时需要用
        byte[] in = ArrayUtils.addAll(meta, index_data);
        // 写indexFile
        writeSingleToSSTable(in, this.indexFileName);

        System.out.println("成功将内存中的数据写到文件data" + this.dateFileName + "中，大小：" + offset + "B");

        ArrayList<String> ret = new ArrayList<>();
        ret.add("" + offset);
        ret.add(minKey);
        ret.add(maxKey);

        return ret;
    }


    // 将字节流data，以追加的形式，写到文件dateFileName中
    private void writeSingleToSSTable(byte[] data, String dateFileName){
        try{
            File dataFile = new File(Constant.DATABASE_DIR + dateFileName);
            dataFile.createNewFile();

            // 写data
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(dataFile, true));
            output.write(data,0,data.length);
            output.flush();
            output.close();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }



    // 读data数据文件
    private void readDataFile(){
        try{
            // 遍历indexMap，根据索引中给出的偏移量去读数据
            for(Map.Entry<String, String> entry : this.indexMap.entrySet()){
                String k = entry.getKey();
                String tmp = entry.getValue();
                if(!tmp.contains("-"))
                    continue;
                // 解析indexMap的格式  key : startOffset-length
                int startOffset = Integer.parseInt(tmp.split("-")[0]);
                int lengthToRead = Integer.parseInt(tmp.split("-")[1]);
                byte[] buff = new byte[lengthToRead];
                FileInputStream input = new FileInputStream(new File(DATABASE_DIR + this.dateFileName));
                //指定偏移量开始读文件
                input.skip(startOffset);
                input.read(buff, 0, lengthToRead);
                if(k.startsWith("b"))
                    this.biPointerTable.add(JSONObject.parseObject(new String(buff), BiPointerTableItem.class));
                else if(k.startsWith("c"))
                    this.classTable.add(JSONObject.parseObject(new String(buff), ClassTableItem.class));
                else if(k.startsWith("d"))
                    this.deputyTable.add(JSONObject.parseObject(new String(buff), DeputyTableItem.class));
                else if(k.startsWith("o"))
                    this.objectTable.add(JSONObject.parseObject(new String(buff), ObjectTableItem.class));
                else if(k.startsWith("s"))
                    this.switchingTable.add(JSONObject.parseObject(new String(buff), SwitchingTableItem.class));
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }



}
