package drz.tmdb.level;

import static drz.tmdb.level.Constant.DATABASE_DIR;
import static drz.tmdb.level.Constant.INT_TO_BYTES;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

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

import drz.tmdb.cache.CacheManager;


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
            File metaFile = new File(DATABASE_DIR + "meta");
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
    public void manualCompaction(int level) throws IOException {
        if(level < 0 || level >= Constant.MAX_LEVEL)
            return;

        System.out.println("开始compaction");

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

        compact(filesToCompact, level + 1);
    }

    // 对集合set中所有SSTable执行compaction,新SSTable置于level层
    // target         = []   目标合并的SSTables（int型的后缀）
    // pointers       = []   指针，记录每个SSTable当前处理的进度（long型的offset）
    // ceilings       = []   记录每个SSTable的最大offset（通过读取Footer中zone map的偏移-1 得到）
    // currentKeys    = []   记录当前pointer对应的key
    // currentLength  = []   记录当前pointer对应的k-v的总length
    // 每次keys中的最小值，若pointer超出ceiling则对应的SSTable扫描完毕，若所有pointer都扫描完毕则结束
    // 执行思路：
    // 1.各种初始化
    // 2.while(pointers未达到ceilings){
    //     找出currentKeys中最小的Key（可能为多个）
    //     将最小key和它对应的value写入新SSTable
    //     更新pointers：最小key对应的SSTable的pointer往后移动（移动距离根据currentLength）
    //     更新currentKeys
    //     更新currentKeys
    // 3.新SSTable的meta data写入，并flush写通道
    private void compact(Set<Integer> set, int level) throws IOException {
        if(level <= 0)
            return;

        // 获取最新dataFileSuffix并+1
        int dataFileSuffix = Integer.parseInt(this.levelInfo.get("maxDataFileSuffix")) + 1;
        this.levelInfo.put("maxDataFileSuffix", "" + dataFileSuffix);

        // 打开新SSTable的写通道
        SSTable newSST = new SSTable("SSTable" + dataFileSuffix, 1);

        // 初始化compaction需要的参数
        List<Integer> targetSSTable = new ArrayList<>(set);
        int size = targetSSTable.size();
        List<Long> pointers = new ArrayList<>(size);
        List<Long> ceilings = new ArrayList<>(size);
        List<RandomAccessFile> readAccesses = new ArrayList<>(size); // 对应每个SSTable的读通道
        List<String> currentKeys = new ArrayList<>(size);
        List<Integer> currentLength = new ArrayList<>(size);

        // 初始化readAccesses和ceilings
        // 读取各个SSTable的bloom filter部分，估计总元素个数
        int estimateItemCount = 0; // 总元素个数
        for(int i=0; i<size; i++){
            pointers.add(0L);
            // 初始化readAccesses
            RandomAccessFile raf = new RandomAccessFile(DATABASE_DIR + "SSTable" + targetSSTable.get(i), "r");
            readAccesses.add(raf);
            // 初始化ceilings
            long len = raf.length() - 6 * Long.BYTES; // Footer最后48字节，
            byte[] buffer = new byte[Long.BYTES];
            raf.seek(len);
            raf.read(buffer);
            ceilings.add(Constant.BYTES_TO_LONG(buffer) - 1);
            // 初始化estimateItemCount
            len = raf.length() - 4 * Long.BYTES; // bloom filter对应的start offset
            raf.seek(len);
            long offset = raf.readLong();
            raf.seek(offset);
            estimateItemCount += raf.readInt();
            // 将读文件的指针重新指回0
            raf.seek(0);
        }

        // 读各个SSTable的第一个K-V，初始化currentKeys和currentLength
        for(int i=0; i<size; i++){
            if(pointers.get(i) > ceilings.get(i))
                continue;
            readAccesses.get(i).seek(pointers.get(i));
            currentLength.add(readAccesses.get(i).readInt());
            byte[] keyBuffer = new byte[Constant.MAX_KEY_LENGTH];
            readAccesses.get(i).read(keyBuffer, 0, Constant.MAX_KEY_LENGTH);
            currentKeys.add(Constant.BYTES_TO_KEY(keyBuffer));
        }

        // 初始化new SSTable 的 meta data
        newSST.bloomFilter = new BloomFilter(estimateItemCount); // BloomFilter

        // 写SSTable需要的一些参数，用来进行data block划分
        long dataBlockStartOffset = 0; // 此data block的开始偏移
        long totalOffset = 0; // 总偏移

        // 开始扫描各个SSTable
        while(true){

            // 找到currentkeys中最小的（可能不止一个）
            List<Integer> minKeyIndex = findMinKeyIndex(pointers, ceilings, currentKeys);
            if(minKeyIndex.size() == 0)
                break;

            // 最小key对应的在targetSSTable中的下标
            int targetIndex;
            // 如果最小key不止一个，则根据SSTable后缀的数字大小选择保留最大的后缀对应的value
            if(minKeyIndex.size() > 1){
                List<Integer> list = new ArrayList<>();
                for(Integer x : minKeyIndex)
                    list.add(targetSSTable.get(x));
                int m = Collections.max(list); // SSTable的下标最大的为最新值，其他为过期数据
                targetIndex = targetSSTable.indexOf(m);
            }else{
                targetIndex = minKeyIndex.get(0);
            }

            // 需要写入的值
            readAccesses.get(targetIndex).seek(pointers.get(targetIndex));
            byte[] data = new byte[currentLength.get(targetIndex) + Integer.BYTES];
            readAccesses.get(targetIndex).read(data);

            // 写入新SSTable
            newSST.appendToFile(data);
            totalOffset += data.length;

            // 更新minKey和maxKey，由于是归并合并，第一次一定是minKey，最后一次的一定是maxKey
            if(newSST.minKey.equals(""))
                newSST.minKey = currentKeys.get(targetIndex);
            newSST.maxKey = currentKeys.get(targetIndex);

            // 如果data block写满，则开启新data block，将旧data block的最大key和起始偏移记录到B树中
            if(totalOffset - dataBlockStartOffset > Constant.MAX_DATA_BLOCK_SIZE){
                // (max key in this data block -> data block start offset)插入B-Tree
                newSST.bTree.insert(currentKeys.get(targetIndex), dataBlockStartOffset);
                // 更新dataBlockStartOffset
                dataBlockStartOffset = totalOffset;
            }

            // 更新Bloom Filter
            newSST.bloomFilter.add(currentKeys.get(targetIndex));

            // 更新pointers、currentKeys、currentLength
            for(Integer x : minKeyIndex){
                pointers.set(x, pointers.get(x) + currentLength.get(x) + Integer.BYTES);
                if(pointers.get(x) > ceilings.get(x))
                    continue;
                readAccesses.get(x).seek(pointers.get(x));
                currentLength.set(x,readAccesses.get(x).readInt());
                byte[] keyBuffer = new byte[Constant.MAX_KEY_LENGTH];
                readAccesses.get(x).read(keyBuffer, 0, Constant.MAX_KEY_LENGTH);
                currentKeys.set(x, Constant.BYTES_TO_KEY(keyBuffer));
            }
        }
        //  遍历结束时，未满的data block信息也写入B-Tree
        if(dataBlockStartOffset != totalOffset){
            newSST.bTree.insert(newSST.maxKey, dataBlockStartOffset);
        }

        // 新SSTable的meta data写入
        // 写zone map
        long zoneMapStartOffset = totalOffset; // zone map的开始偏移
        long zoneMapLength = Constant.MAX_KEY_LENGTH * 2; // zone map的长度
        newSST.appendToFile(Constant.KEY_TO_BYTES(newSST.minKey));
        newSST.appendToFile(Constant.KEY_TO_BYTES(newSST.maxKey));
        // 写Bloom filter
        long bloomFilterStartOffset = zoneMapStartOffset + zoneMapLength;
        long bloomFilterLength = 4 + newSST.bloomFilter.getByteCount(); // +4 的原因见BloomFilter写文件的格式
        newSST.bloomFilter.writeToFile(newSST.outputStream);
        // 写index block
        long indexBlockStartOffset = bloomFilterStartOffset + bloomFilterLength;
        long[] info = newSST.bTree.write(newSST.outputStream, indexBlockStartOffset);
        long indexBlockLength = info[0];
        long bTreeRootOffset = info[1];
        // 写Footer
        long footerStartOffset = indexBlockStartOffset + indexBlockLength;
        long footerLength = Long.BYTES * 6;
        newSST.appendToFile(Constant.LONG_TO_BYTES(zoneMapStartOffset));
        newSST.appendToFile(Constant.LONG_TO_BYTES(zoneMapLength));
        newSST.appendToFile(Constant.LONG_TO_BYTES(bloomFilterStartOffset));
        newSST.appendToFile(Constant.LONG_TO_BYTES(bloomFilterLength));
        newSST.appendToFile(Constant.LONG_TO_BYTES(bTreeRootOffset));
        newSST.appendToFile(Constant.LONG_TO_BYTES(indexBlockLength));

        // flush close 各种写通道和读通道
        newSST.outputStream.flush();
        newSST.outputStream.close();
        for(RandomAccessFile ra : readAccesses){
            ra.close();
        }

        // 将该SSTable添加到对应level中
        this.levels[level].add(dataFileSuffix);
        // levelInfo 的结构  dataFileSuffix : level-length-minKey-maxKey
        this.levelInfo.put("" + dataFileSuffix, level + "-" + (footerLength + footerStartOffset) + "-" + newSST.minKey + "-" + newSST.maxKey);
        // 更新缓存
        if(level <= 2)
            this.cacheManager.metaCache.add(newSST);

        // 旧SSTable从level中删除
        for(Integer i : set){
            this.levels[level - 1].remove(i);
            this.levels[level].remove(i);
            this.levelInfo.remove("" + i);

            // 更新缓存
            this.cacheManager.metaCache.remove(i);
        }
    }

    // 在currentKeys中找到最小的key，返回对应的下标（可能不止一个）
    // 如果pointer > ceiling，则不在考虑范围内
    private List<Integer> findMinKeyIndex(List<Long> pointers, List<Long> ceilings, List<String> currentKeys){
        List<String> scope = new ArrayList<>();// 考虑范围内的key
        // 如果pointer > ceiling，则不在考虑范围内
        for(int i=0; i<pointers.size(); i++){
            if(pointers.get(i) < ceilings.get(i)){
                scope.add(currentKeys.get(i));
            }
        }
        List<Integer> ret = new ArrayList<>();
        if(scope.size() == 0)
            return ret;
        String minKey = Collections.min(scope);
        for(int i=0; i<currentKeys.size(); i++){
            if(currentKeys.get(i).equals(minKey)){
                ret.add(i);
            }
        }
        return ret;
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
        String min1 = this.levelInfo.get("" + i1).split("-")[2];
        String max1 = this.levelInfo.get("" + i1).split("-")[3];
        String min2 = this.levelInfo.get("" + i2).split("-")[2];
        String max2 = this.levelInfo.get("" + i2).split("-")[3];
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

        scores.set(0, (float)(level0FileCount / Constant.MAX_LEVEL0_FILE_COUNT));
        for(int i=1; i<= Constant.MAX_LEVEL; i++){
            scores.set(i, (float)(sizes.get(i)) / Constant.MAX_LEVEL_SIZE[i]);
        }

        return scores;
    }
}
