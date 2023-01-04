package drz.oddb.Level;

import static drz.oddb.Level.Constant.BYTES_TO_INT;
import static drz.oddb.Level.Constant.DATABASE_DIR;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import drz.oddb.Transaction.SystemTable.BiPointerTableItem;
import drz.oddb.Transaction.SystemTable.ClassTableItem;
import drz.oddb.Transaction.SystemTable.DeputyTableItem;
import drz.oddb.Transaction.SystemTable.ObjectTableItem;
import drz.oddb.Transaction.SystemTable.SwitchingTableItem;

// 充当MemTable和SSTable的中间媒介
// 需要从SSTable中读取数据时（例如compaction），先读到一个FileData对象中
// 需要从内存写到SSTable时，也生成一个FileData对象，再调用writeSSTable()方法
public class FileData {

    public final ArrayList<ObjectTableItem> objectTable = new ArrayList<ObjectTableItem>();
    public final ArrayList<ClassTableItem> classTable = new ArrayList<ClassTableItem>();
    public final ArrayList<DeputyTableItem> deputyTable = new ArrayList<DeputyTableItem>();
    public final ArrayList<BiPointerTableItem> biPointerTable = new ArrayList<BiPointerTableItem>();
    public final ArrayList<SwitchingTableItem> switchingTable = new ArrayList<SwitchingTableItem>();

    // data文件和index文件的文件名
    private String dateFileName;
    private String indexFileName;

    // 索引
    public Map<String, String> indexMap = new TreeMap<>();


    // constructor
    // 将文件读到内存中
    // mode = 1 从SSTable读数据
    // mode = 2 从SSTable读数据且只读索引
    // mode = 3 构造空的FileData对象
    public FileData(String fileName, int mode){
        if(mode == 1){
            this.dateFileName = fileName;
            this.indexFileName = fileName.replace("data","index");

            // 先读索引再读数据
            readIndexFile();
            readDataFile();
        }
        if(mode == 2){
            this.dateFileName = fileName;
            this.indexFileName = fileName.replace("data","index");

            // 读index索引文件
            readIndexFile();
        }
        if(mode == 3){
            this.dateFileName = fileName;
            this.indexFileName = fileName.replace("data","index");
            this.indexMap = new HashMap<>();
        }
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


    // 读index索引文件
    private void readIndexFile(){
        try{
            // 读索引文件
            FileInputStream input = new FileInputStream(new File(DATABASE_DIR + this.indexFileName));

            // 先读maxKey和minKey
            byte[] max = new byte[Constant.MAX_KEY_LENGTH];
            input.read(max, 0, Constant.MAX_KEY_LENGTH);
            String maxKey = Constant.BYTES_TO_KEY(max);
            byte[] min = new byte[Constant.MAX_KEY_LENGTH];
            input.read(min, 0, Constant.MAX_KEY_LENGTH);
            String minKey = Constant.BYTES_TO_KEY(min);

            //读取长度
            byte[] x=new byte[4];
            input.read(x,0,4);
            int lengthToRead = BYTES_TO_INT(x, 0, 4);

            // 读取正文
            byte[] buff = new byte[lengthToRead];
            input.read(buff, 0, lengthToRead);
            this.indexMap = (Map<String, String>) JSON.parse(new String(buff));

            this.indexMap.put("maxKey", maxKey);
            this.indexMap.put("minKey", minKey);
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
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


//    // 控制台打印信息（用来观察执行compaction前后的效果）
//    public void printFileDataInfo(){
//        System.out.println("数据文件:" + this.dateFileName + ", 索引文件:" + this.indexFileName +
//                ", minKey:" + this.indexMap.get("minKey") + ", maxKey:" + this.indexMap.get("maxKey"));
//        System.out.print("包含的key值：");
//        for(String k : this.indexMap.keySet()){
//            if(k.equals("minKey") || k.equals("maxKey"))
//                continue;
//            System.out.print(k + " ");
//        }
//
//    }

}
