package drz.tmdb.Level;

import static drz.tmdb.Level.Constant.DATABASE_DIR;
import static drz.tmdb.Level.Constant.INT_TO_BYTES;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import drz.tmdb.Transaction.SystemTable.BiPointerTableItem;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.DeputyTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;


public class LevelManager {

    // 记录层级信息，比如file0属于哪个level
    // 格式为：
    // "dataFileSuffix" : "level-size-minKey-maxKey"
    // "maxDataFileSuffix" : "131"  // 自增的文件下标
    // 此外，ClassTable中的maxid和ObjectTable中的maxTupleId也需要记录在此处，每次跟随其他属性一起进行保存或初始化
    // "maxClassId" : "45"
    // "maxTupleId" : "522"
    public Map<String, String> levelInfo = new HashMap<String, String>();

    // 记录各level包含哪些data文件(使用sortedset因为，suffix大的一定是最新版本的数据)
    public final Set<Integer> level_0 = new TreeSet<Integer>();
    public final Set<Integer> level_1 = new TreeSet<Integer>();
    public final Set<Integer> level_2 = new TreeSet<Integer>();
    public final Set<Integer> level_3 = new TreeSet<Integer>();
    public final Set<Integer> level_4 = new TreeSet<Integer>();
    public final Set<Integer> level_5 = new TreeSet<Integer>();
    public final Set<Integer> level_6 = new TreeSet<Integer>();
    public final Set[] levels = {level_0, level_1, level_2, level_3, level_4, level_5, level_6};

    // constructor
    public LevelManager(){
        // 每次初始化时加载总索引表
        try{
            File dir = new File(Constant.DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File metaFile = new File(DATABASE_DIR + "meta");
            if(!metaFile.exists()){
                // 如果初始化时没有历史数据，则给maxDataFileSuffix, maxClassId, maxTupleId一个初始值0
                this.levelInfo.put("maxDataFileSuffix","0");
                this.levelInfo.put("maxClassId","0");
                this.levelInfo.put("maxTupleId","0");
            } else{
                FileInputStream input = new FileInputStream(metaFile);

                //读取meta(获取长度)
                byte[] x=new byte[4];
                input.read(x,0,4);
                int lengthToRead = Constant.BYTES_TO_INT(x, 0, 4);

                // 读取正文
                byte[] buff = new byte[lengthToRead];
                input.read(buff, 0, lengthToRead);
                this.levelInfo = (Map<String, String>) JSON.parse(new String(buff));
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }

        // 使用总索引表去初始化各个level
        for(Entry<String, String> entry : this.levelInfo.entrySet()){
            if(entry.getValue().contains("-")){
                int level = Integer.parseInt(entry.getValue().split("-")[0]);
                int suffix = Integer.parseInt(entry.getKey());
                levels[level].add(suffix);
            }
        }

    }


    // 退出时将索引表持久化保存
    public void saveMetaToFile(){
        try{
            File dir = new File(DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File metaFile = new File(DATABASE_DIR + "meta");
            if(!metaFile.exists()){
                metaFile.createNewFile();
            }
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(metaFile));
            String s_in = JSONObject.toJSONString(this.levelInfo);
            byte[] in = s_in.getBytes();
            //System.out.println(in.length);
            byte[] meta = INT_TO_BYTES(in.length);
            output.write(meta,0,meta.length);
            output.write(in,0,in.length);

            output.flush();
            output.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 手动调用的compaction，指定需要进行compaction的level
    public void manualCompaction(int level){
        if(level < 0 || level >= Constant.MAX_LEVEL)
            return;
        // 若i=0，则将level-0所有SSTable进行compaction成新SSTable并加入level-1，并删除level-0中所有旧SSTable
        else if(level == 0){
            List<String> filesToCompact = new ArrayList<>(); // 记录需要进行compaction的文件名
            for(Integer i : this.level_0){
                filesToCompact.add("SSTable" + i);
            }
            // todo
        }
        // 设level-i中需要进行compaction的文件集合files=[ ]
        // 1.选择level-i中文件名后缀最小的SSTable x加入files中，files=[x]；
        // 2.在level-i中寻找所有与x有重叠的SSTable，假设为y与z，将其加入files中，files=[x, y, z]；
        // 设level-(i+1)中需要进行compaction的文件集合files_2=[ ]
        // 3.选择level-(i+1)中与files存在重叠的SSTable，假设为a、b，加入files_2中，files_2=[a, b]
        // 4.再次遍历level-i中的SSTable，寻找是否有这样的SSTable t，满足t加入files中后，重复步骤3没有新的SSTable加入files_2。如果有这样的SSTable t，则将t加入files，files=[x, y, z, t1, t2, …];
        // files与files_2中所有的SSTable就是此次compaction需要合并的所有SSTable
        else{
//            List<String> filesToCompact = new ArrayList<>(); // 记录需要进行compaction的文件名
//            Integer i = this.levels[level]
//            filesToCompact.add()

        }
    }


    // 自动调用的compaction，根据score选择最需要执行的一个就行
    public void autoCompaction(){
        List<Float> scores = calScore();
        // 遍历一遍找最大score的level
        float maxScore = -1;
        int maxScoreLevel = -1;
        for(int i = 0; i < 4 ; i++){
            if(scores.get(i) > maxScore){
                maxScore = scores.get(i);
                maxScoreLevel = i;
            }
        }

        if(maxScore > 1){
            // 对该level执行compaction
            manualCompaction(maxScoreLevel);
        }
    }


    // 计算每个level当前的score = 该层总大小 / 该层大小上限
    public List<Float> calScore(){
        // 各层score
        List<Float> scores = new ArrayList<Float>(Constant.MAX_LEVEL + 1);

        // level 0 层使用单独的计算策略，原因可参考设计文档
        int level0FileCount = 0;
        // 各层大小
        List<Integer> sizes = new ArrayList<Integer>(Constant.MAX_LEVEL + 1);

        for(String v: this.levelInfo.values()){
            if(v.contains("-")){
                String[] t = v.split("-");
                int level = Integer.parseInt(t[0]);
                int size = Integer.parseInt(t[1]);
                sizes.set(level, sizes.get(level) + size);
                if(level == 0)
                    level0FileCount++;
            }
        }

        scores.set(0, (float)(level0FileCount / Constant.MAX_LEVEL0_FILE_COUNT));
        for(int i=1; i<= Constant.MAX_LEVEL; i++){
            scores.set(i, (float)(sizes.get(i)) / Constant.MAX_LEVEL_SIZE[i]);
        }

        return scores;
    }
}
