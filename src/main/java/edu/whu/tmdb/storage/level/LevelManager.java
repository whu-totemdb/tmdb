package edu.whu.tmdb.storage.level;



import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import edu.whu.tmdb.storage.cache.CacheManager;
import edu.whu.tmdb.storage.utils.Constant;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class LevelManager {

    // 记录层级信息，比如file0属于哪个level
    // 格式为：
    // "dataFileSuffix" : "level-size-minKey-maxKey"
    // "maxDataFileSuffix" : "131"  // 自增的文件下标
    public Map<String, String> levelInfo = new HashMap<String, String>();

    // 记录各level包含哪些data文件(使用sortedset因为，suffix大的一定是最新版本的数据)
    public final TreeSet<Integer> level_0 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_1 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_2 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_3 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_4 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_5 = new TreeSet<Integer>();
    public final TreeSet<Integer> level_6 = new TreeSet<Integer>();
    public final TreeSet[] levels = {level_0, level_1, level_2, level_3, level_4, level_5, level_6};


    public CacheManager cacheManager;

    // constructor
    public LevelManager(){
        // 每次初始化时加载总索引表
        try{
            File dir = new File(Constant.DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File metaFile = new File(Constant.DATABASE_DIR + "meta");
            if(!metaFile.exists()){
                // 如果初始化时没有历史数据，则给maxDataFileSuffix一个初始值0
                this.levelInfo.put("maxDataFileSuffix","0");
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

    // 用于test
    public LevelManager(int mode){

    }

    // 退出时将索引表持久化保存
    public void saveMetaToFile(){
        try{
            File dir = new File(Constant.DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File metaFile = new File(Constant.DATABASE_DIR + "meta");
            if(!metaFile.exists()){
                metaFile.createNewFile();
            }
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(metaFile));
            String s_in = JSONObject.toJSONString(this.levelInfo);
            byte[] in = s_in.getBytes();
            //System.out.println(in.length);
            byte[] meta = Constant.INT_TO_BYTES(in.length);
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

    // 返回下一个新文件的后缀int
    public int addFileSuffix(){
        int dataFileSuffix = Integer.parseInt(this.levelInfo.get("maxDataFileSuffix")) + 1;
        this.levelInfo.put("maxDataFileSuffix", "" + dataFileSuffix);
        return dataFileSuffix;
    }


    // 手动调用的compaction，指定需要进行compaction的level
    public void manualCompaction(int level) throws IOException {
        if(level < 0 || level >= Constant.MAX_LEVEL)
            return;

        //System.out.println("开始compaction");

        Set<Integer> filesToCompact = new HashSet<>(); // 记录需要进行compaction的文件名后缀

        // 若i=0，则将level-0所有SSTable进行compaction成新SSTable并加入level-1，并删除level-0中所有旧SSTable
        if(level == 0){
            filesToCompact.addAll(this.level_0);
        }
        // https://github.com/facebook/rocksdb/wiki/Choose-Level-Compaction-Files
        // 设level-i中需要进行compaction的文件集合files=[ ]
        // 1.选择level-i中文件名后缀最小的SSTable x加入files中，files=[x]；
        // 2.在level-i中寻找所有与x有重叠的SSTable，假设为y与z，将其加入files中，files=[x, y, z]；
        // 设level-(i+1)中需要进行compaction的文件集合files_2=[ ]
        // 3.选择level-(i+1)中与files存在重叠的SSTable，假设为a、b，加入files_2中，files_2=[a, b]
        // 4.再次遍历level-i中的SSTable，寻找是否有这样的SSTable t，满足t加入files中后，重复步骤3没有新的SSTable加入files_2。如果有这样的SSTable t，则将t加入files，files=[x, y, z, t1, t2, …];
        // files与files_2中所有的SSTable就是此次compaction需要合并的所有SSTable
        else{
            // 1. 选择level i中文件名后缀最小的SSTable
            int i = (int) this.levels[level].first(); // 由于是SortedSet，第一个元素就是最小
            filesToCompact.add(i);

            // 2. 在level i中找有重叠的SSTable
            filesToCompact.addAll(findOverlapSSTable(i, level));

            // 3. 在level-(i+1)中找与filesToCompact存在重叠的SSTable，加入file2
            Set<Integer> files2 = new HashSet<>();
            for(Integer fileSuffix : filesToCompact){
                files2.addAll(findOverlapSSTable(fileSuffix, level + 1));
            }

            // 4. 再次遍历level-i中的SSTable，寻找是否有这样的SSTable t，满足t加入files中后，重复步骤3没有新的SSTable加入files_2
            // 意思是，检查level-i 中的SSTable，如果与（level-(i+1) 与 file2 的差集s） 无交集，则加入合并列表
            // 先构造  level-(i+1) 与 file2 的差集s
            Set<Integer> s = new HashSet<>(this.levels[level + 1]);
            for(Integer integer : files2)
                s.remove(integer);
            // 在level i 中找与s无交集的SSTable
            filesToCompact.addAll(findNotOverLapSSTable(s, level));

            filesToCompact.addAll(files2);
        }

        // 执行compaction
        Compaction compaction = new Compaction(this, filesToCompact, level + 1);
        compaction.run();

    }


    // 遍历level层中所有文件，找出与s中所有SSTable均无重叠的文件名后缀
    private Set<Integer> findNotOverLapSSTable(Set<Integer> set, int level){
        Set<Integer> ret = new HashSet<>();
        for(Object o : this.levels[level]) {
            Integer suffix1 = (Integer) o;

            boolean flag = true;
            for (Integer suffix2 : set) {
                if (hasOverlap(suffix1, suffix2)){
                    flag = false;
                    break;
                }
            }
            if(flag)
                ret.add(suffix1);
        }
        return ret;
    }

    // 遍历level层中所有文件，找出与SSTable i有重叠的文件名后缀
    private Set<Integer> findOverlapSSTable(int i, int level){
        Set<Integer> ret = new HashSet<>();
        for(Object j : this.levels[level]){
            int fileSuffix2 =(Integer) j;
            if(hasOverlap(i, fileSuffix2)){
                ret.add(fileSuffix2);
            }
        }
        return  ret;
    }

    // 判断文件后缀为i1和i2的两个SSTable是否有重叠
    private boolean hasOverlap(int i1, int i2){
        long min1 = Long.parseLong(this.levelInfo.get("" + i1).split("-")[2]);
        long max1 = Long.parseLong(this.levelInfo.get("" + i1).split("-")[3]);
        long min2 = Long.parseLong(this.levelInfo.get("" + i2).split("-")[2]);
        long max2 = Long.parseLong(this.levelInfo.get("" + i2).split("-")[3]);
        return Constant.hasOverlap(min1, max1, min2, max2);
    }


    // 自动调用的compaction，根据score选择最需要执行的一个就行
    public void autoCompaction() throws IOException {
        List<Float> scores = calScore();
        // 遍历一遍找最大score的level
        float maxScore = -1;
        int maxScoreLevel = -1;
        for(int i = 0; i < Constant.MAX_LEVEL ; i++){
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
        for(int i=0; i<Constant.MAX_LEVEL + 1; i++)
            scores.add(0f);

        // level 0 层使用单独的计算策略，原因可参考设计文档
        int level0FileCount = 0;
        // 各层大小
        List<Integer> sizes = new ArrayList<Integer>(Constant.MAX_LEVEL + 1);
        for(int i=0; i<Constant.MAX_LEVEL + 1; i++)
            sizes.add(0);

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

        scores.set(0, ((float) level0FileCount) / Constant.MAX_LEVEL0_FILE_COUNT);
        for(int i=1; i<= Constant.MAX_LEVEL; i++){
            scores.set(i, (float)(sizes.get(i)) / Constant.MAX_LEVEL_SIZE[i]);
        }

        return scores;
    }


    // 打印展示此时的level状态
    public void printLevels(){
        for(int i=0; i<=6; i++){
            System.out.print("level " + i + " - ");
            for(Object j : this.levels[i])
                System.out.print(" " + j);
            System.out.println();
        }
    }

}
