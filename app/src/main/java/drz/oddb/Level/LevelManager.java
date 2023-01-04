package drz.oddb.Level;

import static drz.oddb.Level.Constant.DATABASE_DIR;
import static drz.oddb.Level.Constant.INT_TO_BYTES;

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

import drz.oddb.Transaction.SystemTable.BiPointerTableItem;
import drz.oddb.Transaction.SystemTable.ClassTableItem;
import drz.oddb.Transaction.SystemTable.DeputyTableItem;
import drz.oddb.Transaction.SystemTable.ObjectTableItem;
import drz.oddb.Transaction.SystemTable.SwitchingTableItem;


public class LevelManager {

    // 总索引表，格式为  "dataFileSuffix" : "level-size-minKey-maxKey"
    //                "maxDataFileSuffix" : "131"
    // 此外，ClassTable中的maxid和ObjectTable中的maxTupleId也需要记录在此处，每次跟随其他属性一起进行保存或初始化
    //                "maxClassId" : "45"
    //                "maxTupleId" : "522"
    public Map<String, String> totalIndex = new HashMap<String, String>();

    // 记录各level包含哪些data文件(使用sortedset因为，suffix大的一定是最新版本的数据)
    public final Set<Integer> level_0 = new TreeSet<Integer>();
    public final Set<Integer> level_1 = new TreeSet<Integer>();
    public final Set<Integer> level_2 = new TreeSet<Integer>();
    public final Set<Integer> level_3 = new TreeSet<Integer>();
    public final Set<Integer> level_4 = new TreeSet<Integer>();
    public final Set[] levels = {level_0, level_1, level_2, level_3, level_4};


    // constructor
    public LevelManager(){
        // 每次初始化时加载总索引表
        try{
            File dir = new File(Constant.DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File indexFile = new File(DATABASE_DIR + "index");
            if(!indexFile.exists()){
                // 如果初始化时没有历史数据，则给maxDataFileSuffix, maxClassId, maxTupleId一个初始值0
                this.totalIndex.put("maxDataFileSuffix","0");
                this.totalIndex.put("maxClassId","0");
                this.totalIndex.put("maxTupleId","0");
            } else{
                FileInputStream input = new FileInputStream(indexFile);

                //读取meta(获取长度)
                byte[] x=new byte[4];
                input.read(x,0,4);
                int lengthToRead = Constant.BYTES_TO_INT(x, 0, 4);

                // 读取正文
                byte[] buff = new byte[lengthToRead];
                input.read(buff, 0, lengthToRead);
                this.totalIndex = (Map<String, String>) JSON.parse(new String(buff));
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }

        // 使用总索引表去初始化各个level
        for(Entry<String, String> entry : this.totalIndex.entrySet()){
            if(entry.getValue().contains("-")){
                int level = Integer.parseInt(entry.getValue().split("-")[0]);
                int suffix = Integer.parseInt(entry.getKey());
                levels[level].add(suffix);
            }
        }

    }


    // 退出时将索引表持久化保存
    // todo: what if unexpected exit
    public void saveIndexToFile(){
        try{
            File dir = new File(Constant.DATABASE_DIR);
            if(!dir.exists()){
                dir.mkdirs();
            }
            File indexFile = new File(DATABASE_DIR + "index");
            if(!indexFile.exists()){
                indexFile.createNewFile();
            }
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(indexFile));
            String s_in = JSONObject.toJSONString(this.totalIndex);
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
        if(level == 0){
            // 由于key值构造的原因，总是由 b c d o s 这五个字符开头，可以视为level0所有文件都是有重叠的
            // 1. 将level0所有文件归并排序后再根绝key不同前缀分别写到4个文件并放到level1
            // 2. level1根据前缀不同分别进行归并排序
            // 即，在level1及以上，同一个文件存储数据的key的前缀只有一种

            // 1. 首先按照文件后缀的升序依次读取level0的index文件
            //   (起到删除过期数据的作用：
            //   若不同文件中有key值相同value不同的数据，则文件后缀大的一定为最新数据)
            // 同时进行排序分发数据
            //   (由于使用的是SortedMap，排序已经实现，只需要进行分发
            //   规则：五种前缀不同的文件分别放到5个sortedmap中)
            SortedMap<String, String> b = new TreeMap<>();
            SortedMap<String, String> c = new TreeMap<>();
            SortedMap<String, String> d = new TreeMap<>();
            SortedMap<String, String> o = new TreeMap<>();
            SortedMap<String, String> s = new TreeMap<>();

            // 遍历level-0的索引文件，将k及对应v的位置保存到SortedMap中
            for(Integer fileSuffix : this.level_0){
                FileData f = new FileData("data" + fileSuffix, 2); // 只读索引文件
                for(Entry<String, String> entry : f.indexMap.entrySet()){
                    if(entry.getKey().startsWith("b"))
                        b.put(entry.getKey(), "" + fileSuffix + "-" + entry.getValue());
                    else if(entry.getKey().startsWith("c"))
                        c.put(entry.getKey(), "" + fileSuffix + "-" + entry.getValue());
                    else if(entry.getKey().startsWith("d"))
                        d.put(entry.getKey(), "" + fileSuffix + "-" + entry.getValue());
                    else if(entry.getKey().startsWith("o"))
                        o.put(entry.getKey(), "" + fileSuffix + "-" + entry.getValue());
                    else if(entry.getKey().startsWith("s"))
                        s.put(entry.getKey(), "" + fileSuffix + "-" + entry.getValue());
                }
                this.totalIndex.remove("" + fileSuffix); // 更新索引
            }
            level_0.clear(); // 更新索引

            // 到level1中找存在重叠的文件并加到sortedmap中
            Set<Integer> findFiles = new HashSet<>(); // 记录level1中参与本次compaction的文件，便于更新索引
            // 遍历level1
            for(Integer fileSuffix : this.level_1){
                String info = this.totalIndex.get("" + fileSuffix);  // meta信息
                String[] t = info.split("-");
                String min = t[2];
                String max = t[3];
                SortedMap<String, String> target = null; // 如果fileSuffix与上述某SortedMap存在重叠，则用该变量记录
                if(Constant.hasOverlap(min, max, b.firstKey(), b.lastKey()))
                    target = b;
                else if(Constant.hasOverlap(min, max, c.firstKey(), c.lastKey()))
                    target = c;
                else if(Constant.hasOverlap(min, max, d.firstKey(), d.lastKey()))
                    target = d;
                else if(Constant.hasOverlap(min, max, o.firstKey(), o.lastKey()))
                    target = o;
                else if(Constant.hasOverlap(min, max, s.firstKey(), s.lastKey()))
                    target = s;
                // 如果没有重叠，即target == null，则跳过（说明这个fileSuffix不需要参与本次compaction）
                if(target == null)
                    continue;
                // 否则，读取fileSuffix的索引文件，然后加入target中
                findFiles.add(fileSuffix);
                FileData f = new FileData("data" + fileSuffix, 2); // 只读索引文件
                for(Entry<String, String> entry : f.indexMap.entrySet()){
                    // 此处需要注意，如果有key相同的不同版本，level大小并不能决定新旧，
                    // 因为从memTable compact到SSTable时不总是compact到level0，还可以直接到level1或者level2
                    // 不过fileSuffix仍然可以作为评判标准，fileSuffix大的一定是最新版
                    if(target.containsKey(entry.getKey())) {
                        int suffix1 = fileSuffix; // level1 中的fileSuffix
                        int suffix2 = Integer.parseInt(target.get(entry.getKey()).split("-")[0]); // level0中的fileSuffix
                        if (suffix1 > suffix2)
                            // suffix1 为最新值
                            target.put(entry.getKey(), "" + fileSuffix + entry.getValue());
                    }
                    else
                        target.put(entry.getKey(), "" + fileSuffix + entry.getValue());
                }
                this.totalIndex.remove("" + fileSuffix); // 更新索引
            }
            this.level_2.removeAll(findFiles); // 更新索引



            // 3. 对于分发好的每个文件
            // · 根据索引信息去把具体数据读出来（这一步操作放在排序分发之后，且每个文件单独完成，否则会占用太大内存空间）
            // · 写入新文件
            if(b.size() != 0)
                readAndWrite(b, 1);
            if(c.size() != 0)
                readAndWrite(c, 1);
            if(d.size() != 0)
                readAndWrite(d, 1);
            if(o.size() != 0)
                readAndWrite(o, 1);
            if(s.size() != 0)
                readAndWrite(s, 1);


        }else{
            // 从 level i (i>=1) compact 到 level i+1 的方法
            // 由于在level1及以上，同一个文件存储数据的key的前缀只有一种
            // 进行归并排序就简单了不少
            // 1. 首先通过修改索引表，将 level i 的每个文件都往下传递至 level i+1
            // 2. 对 level i+1 中的文件，按照minKey升序排序[file1, file2, ...]
            // 3. 双指针从头扫描到尾遍历一遍


            // 1. 修改索引表，将 level i 的每个文件都往下传递至 level i+1
            // 1.1 遍历level i，获取fileSuffix，将totalIndex中对应的level修改
            for(Object o : this.levels[level]){
                int fileSuffix = (int) o;
                // 去totalIndex获取info
                String info = this.totalIndex.get("" + fileSuffix);
                String[] t = info.split("-");
                int pre_level = Integer.parseInt(t[0]);
                int cur_level = pre_level + 1;
                info = info.replace("" + pre_level, "" + cur_level);
                this.totalIndex.put("" + fileSuffix, info);
            }
            // 1.2
            this.levels[level + 1].addAll(this.levels[level]);
            this.levels[level].clear();


            // 2 对 level i+1 中的文件，按照minKey升序排序
            SortedSet<FileInfo> allFiles = new TreeSet<>();
            for(Object o : this.levels[level + 1]){
                int fileSuffix = (int) o;
                allFiles.add(new FileInfo(fileSuffix, this.totalIndex));
            }

            // 3 依次顺序处理allFiles
            // 如果只有1个文件, do nothing
            if(allFiles.size()<=1){
                return;
            }
            // 两个指针f1 f2, 往后扫描，直到整个allFiles都处理一遍
            FileInfo f1 = allFiles.first();
            allFiles.remove(f1);
            FileInfo f2;
            while(allFiles.size() > 0){
                f2 = allFiles.first();
                allFiles.remove(f2);

                // 如果f1 f2 key的前缀不同，do nothing，开启下一次循环
                if(f1.minKey.charAt(0) != f2.minKey.charAt(0)){
                    f1 = f2;
                    continue;
                }
                // 如果f1 f2没有重叠
                else if(!f1.hasOverlap(f2)){
                    // 如果f1.size + f2.size < maxSize ，我们考虑合并以减少文件数
                    if(f1.size + f2.size < Constant.MAX_FILE_SIZE){
                        appendFile1IntoFile2(f1, f2);
                        f1 = f2;
                        continue;
                    }
                    else{
                        f1 = f2;
                        continue;
                    }
                }
                // 如果f1 f2有重叠
                else{
                    mergeFile1IntoFile2(f1, f2);
                    f1 = f2;
                    continue;
                }
            }

        }

    }


    // 将f2中的数据合并到f1中，同时注意大小限制，可能合并成一个文件，也可能合并后还是两个文件
    private void mergeFile1IntoFile2(FileInfo f1, FileInfo f2){
        // todo
    }

    // 将f2中的数据追加到f1中
    private void appendFile1IntoFile2(FileInfo f1, FileInfo f2){
        FileData file1 = new FileData("data" + f1.fileSuffix, 1);
        FileData file2 = new FileData("data" + f2.fileSuffix, 1);

        // 更新index文件，讲f2的索引表的每一项加入f1的索引表中
        file1.indexMap.putAll(file2.indexMap);

        // 将f2的数据项也加入f1中


        // todo

    }


    // 根据索引去读data文件，然后保存到level层中
    // 需要注意单个文件不能太大，超过一定值就需要拆成若干个文件
    private void readAndWrite(SortedMap<String, String> indexMap, int level){
        int currentSize = 0;
        List<Object> currentList = new ArrayList<>();
        while(indexMap.size() != 0){
            while(indexMap.size() != 0 && currentSize < Constant.MAX_FILE_SIZE){
                String info = indexMap.get(indexMap.firstKey());
                String[] t = info.split("-");
                int suffix = Integer.parseInt(t[0]);
                int offset = Integer.parseInt(t[1]);
                int length = Integer.parseInt(t[2]);
                currentList.add(readObjectFromFile(suffix, offset, length, indexMap.firstKey()));
                currentSize += length;
                indexMap.remove(indexMap.firstKey());
            }
            // 获取dataFileSuffix
            int dataFileSuffix = Integer.parseInt(this.totalIndex.get("maxDataFileSuffix")) + 1;
            this.totalIndex.put("maxDataFileSuffix", "" + dataFileSuffix);
            writeToLevel(currentList, level, dataFileSuffix);
            currentSize = 0;
            currentList.clear();
        }
    }




    // 从后缀为suffix的文件、开始偏移为offset、长度为length 读取数据，需要给定key用于确定解析对象所属类型
    private Object readObjectFromFile(int suffix, int offset, int length, String k){
        try{
            FileInputStream input = new FileInputStream(new File(DATABASE_DIR + "data" + suffix));
            byte[] buff = new byte[length - 4];
            //指定偏移量开始读文件
            input.skip(offset + 4); // 前4字节存的meta信息，所以要+4
            input.read(buff, 0, length - 4);
            if(k.startsWith("b"))
                return JSONObject.parseObject(new String(buff), BiPointerTableItem.class);
            else if(k.startsWith("c"))
                return JSONObject.parseObject(new String(buff), ClassTableItem.class);
            else if(k.startsWith("d"))
                return JSONObject.parseObject(new String(buff), DeputyTableItem.class);
            else if(k.startsWith("o"))
                return JSONObject.parseObject(new String(buff), ObjectTableItem.class);
            else if(k.startsWith("s"))
                return JSONObject.parseObject(new String(buff), SwitchingTableItem.class);
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    // 将k-v写到level层的新文件
    public void writeToLevel(List<Object> data, int level, int dataFileSuffix){

        // 索引信息
        Map<String, String> indexMap = new HashMap<>();
        String maxKey = "";
        String minKey = "";

        // 将需要写入的数据转换成字节流
        byte[] in_data = new byte[0];  // data数据
        byte[] in_index;  // index数据
        byte[] meta_index;  // index数据的长度
        int offset = 0; // 记录每个对象存储的开始偏移
        for(Object o : data){
            String k = Constant.calculateKey(o);
            String v = JSONObject.toJSONString(o);
            byte[] b = v.getBytes();
            byte[] meta = Constant.INT_TO_BYTES(b.length);
            in_data = ArrayUtils.addAll(in_data, meta); // 前4字节先存大小
            in_data = ArrayUtils.addAll(in_data, b); // 再存具体数据
            indexMap.put(k, "" + offset + "-"+ (b.length + meta.length)); // 更新索引， key : "开始下标-长度"
            offset += meta.length + b.length;
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
        indexMap.put("minKey", minKey);
        indexMap.put("maxKey", maxKey);
        in_index = JSONObject.toJSONString(indexMap).getBytes();
        meta_index = Constant.INT_TO_BYTES(in_index.length);

        // 写data和index文件
        try{
            File dataFile = new File(Constant.DATABASE_DIR + "data" + dataFileSuffix);
            File indexFile = new File(Constant.DATABASE_DIR + "index" + dataFileSuffix);
            dataFile.createNewFile();
            indexFile.createNewFile();

            // 写data
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(dataFile));
            output.write(in_data,0,in_data.length);
            output.flush();
            output.close();

            // 写index
            output = new BufferedOutputStream(new FileOutputStream(indexFile));
            output.write(meta_index,0,meta_index.length);
            output.write(in_index,0,in_index.length);
            output.flush();
            output.close();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }

        // 更新索引
        this.levels[level].add(dataFileSuffix);
        this.totalIndex.put("" + dataFileSuffix, "" + level + "-" + offset + "-" + minKey + "-" + maxKey);

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
        // 各层大小
        List<Integer> sizes = new ArrayList<Integer>(4);
        // 各层文件数量
        List<Integer> fileCount = new ArrayList<Integer>(4);

        for(String v: this.totalIndex.values()){
            if(v.contains("-")){
                String[] t = v.split("-");
                int level = Integer.parseInt(t[0]);
                int size = Integer.parseInt(t[1]);
                sizes.set(level, sizes.get(level) + size);
                fileCount.set(level, fileCount.get(level) + 1);
            }
        }

        // 各层score
        // level 0 层使用单独的计算策略，原因可参考文档
        List<Float> scores = new ArrayList<Float>(4);
        scores.set(0, (float)(fileCount.get(0)) / 4);
        scores.set(1, (float)(sizes.get(0)) / Constant.MAX_LEVEL1_SIZE);
        scores.set(2, (float)(sizes.get(0)) / Constant.MAX_LEVEL2_SIZE);
        scores.set(3, (float)(sizes.get(0)) / Constant.MAX_LEVEL3_SIZE);

        return scores;
    }
}
