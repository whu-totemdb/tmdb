package edu.whu.tmdb.storage.level;


import edu.whu.tmdb.storage.utils.Constant;
import edu.whu.tmdb.storage.utils.K;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Compaction{

    LevelManager levelManager;

    // 执行compaction需要用到的信息
    Set<Integer> filesToCompact;
    int level;


    public Compaction(LevelManager levelManager, Set<Integer> filesToCompact, int level){
        this.levelManager = levelManager;
        this.filesToCompact = filesToCompact;
        this.level = level;
    }

    public void run() {

        String str1 = "";
        for(Integer i : filesToCompact)
            str1 += (i + " ");

        long t1 = System.currentTimeMillis();

        // 执行compaction
        try {
            compact(filesToCompact, level);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 统计、打印信息
        long t2 = System.currentTimeMillis();
        //Log.addLog("compact SSTables(" + str1 + "),开始时间：" + new Timestamp(t1) + ",完成时间：" + new Timestamp(t2) + ",耗时：" + (t2 - t1) + "ms");
        //Statistics.info.add("compact SSTables(" + str1 + "),开始时间：" + new Timestamp(t1) +  ",完成时间：" + new Timestamp(t2) + ",耗时：" + (t2 - t1) + "ms");
    }


    // 对集合set中所有SSTable执行compaction,新SSTable置于level层
    // targetSSTable     = []   参与进行合并的SSTables（int型的后缀）
    // dataBlockCount    = []   每个SSTable的data block的总数量
    // curDataBlockIndex = []   当前正在扫描的data block的下标（当大于等于dataBlockCount时，此SSTable扫描完毕）
    // curDataBlock      = [[]] 当前正在扫描的data block
    // pointers          = []   指针，记录每个data block处理的进度（当等于最大块大小时，read下一个块）
    // currentKeys       = []   记录当前pointer对应的key
    // currentLength     = []   记录当前pointer对应的k-v的总length
    // 执行思路：
    // 1.各种初始化
    // 2.while(true){
    //     找出currentKeys中最小的Key（可能为多个）
    //     找到对应的最新版本的最小key
    //     将最小key和它对应的value写入新SSTable
    //     更新pointers：最小key对应的SSTable的pointer往后移动（移动距离根据currentLength）
    //     更新currentKeys
    //     更新currentLength
    // 3.新SSTable的meta data写入，并flush写通道
    private void compact(Set<Integer> set, int level) throws IOException {
        if(level <= 0)
            return;

        // 如果只有一个SSTable参与compaction，一定是由于单个SSTable超过容量限制导致的compaction，直接将其移动到level层即可
        if(set.size()==1){
            int fileSuffix = (new ArrayList<>(set)).get(0); // 文件后缀
            this.levelManager.levels[level].add(fileSuffix); // 加入新level
            this.levelManager.levels[level - 1].remove(fileSuffix); // 从旧level中删除

            // 更新levelInfo，结构  dataFileSuffix : level-length-minKey-maxKey
            String info = this.levelManager.levelInfo.get("" + fileSuffix);
            String[] infos = info.split("-");
            this.levelManager.levelInfo.put("" + fileSuffix, level + "-" + infos[1] + "-" + infos[2] + "-" + infos[3]);
            return;
        }

        // 获取最新dataFileSuffix并+1
        int dataFileSuffix = Integer.parseInt(this.levelManager.levelInfo.get("maxDataFileSuffix")) + 1;
        this.levelManager.levelInfo.put("maxDataFileSuffix", "" + dataFileSuffix);

        // 打开新SSTable的写通道
        SSTable newSST = new SSTable("SSTable" + dataFileSuffix, 1);

        // 初始化compaction需要的参数
        List<Integer> targetSSTable = new ArrayList<>(set);
        int size = targetSSTable.size(); // 参与进行compaction的SSTable的数量
        int[] dataBlockCount = new int[size];
        int[] curDataBlockIndex = new int[size];
        int[] pointers = new int[size];
        byte[][] curDataBlock = new byte[size][Constant.MAX_DATA_BLOCK_SIZE];
        RandomAccessFile[] readAccesses = new RandomAccessFile[size];; // 对应每个SSTable的读通道
        K[] currentKeys = new K[size];;
        int[] currentLength = new int[size];;

        // 初始化
        int estimateItemCount = 0; // 估计总元素个数
        for(int i=0; i<size; i++){

            // 初始化readAccesses
            RandomAccessFile raf = levelManager.cacheManager.metaCache.get(targetSSTable.get(i)).raf;
            readAccesses[i] = raf;

            // 初始化dataBlockCount
            long len = raf.length() - 6 * Long.BYTES; // Footer最后48字节，
            byte[] buffer = new byte[Long.BYTES];
            raf.seek(len);
            raf.read(buffer);
            dataBlockCount[i] = (int) ((Constant.BYTES_TO_LONG(buffer) / Constant.MAX_DATA_BLOCK_SIZE));

            // 读取各个SSTable的bloom filter部分，估计总元素个数
            len = raf.length() - 4 * Long.BYTES; // bloom filter对应的start offset
            raf.seek(len);
            long offset = raf.readLong();
            raf.seek(offset);
            estimateItemCount += raf.readInt();

            // 读各个SSTable的第一个data block，装载入curDataBlock
            raf.seek(0);
            readAccesses[i].read(curDataBlock[i]);
        }

        // 初始化currentKeys和currentLength
        byte[] keyBuffer = new byte[Constant.MAX_KEY_LENGTH];
        for(int i=0; i<size; i++){
            currentLength[i] = Constant.BYTES_TO_INT(curDataBlock[i], 0, Integer.BYTES); // 提取length
            System.arraycopy(curDataBlock[i], 4, keyBuffer, 0, Constant.MAX_KEY_LENGTH); // 提起key
            currentKeys[i] = new K(keyBuffer);
        }

        // 初始化new SSTable 的 meta data
        newSST.bloomFilter = new BloomFilter(estimateItemCount); // BloomFilter

        // 写SSTable需要的一些参数，用来进行data block划分
        long dataBlockStartOffset = 0; // 此data block的开始偏移
        long totalOffset = 0; // 总偏移

        K lastKey = new K(); // 记录上一个key(写入新SSTable时使用)
        // 开始扫描各个SSTable
        while(true){

            // 找到currentkeys中最小的（可能不止一个）
            List<Integer> minKeyIndex = findMinKeyIndex(pointers, currentKeys, curDataBlockIndex, dataBlockCount);
            if(minKeyIndex.size() == 0) // 找不到最小key，说明已经结束
                break;

            int targetIndex;  // 最小key对应的在targetSSTable中的下标

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
            byte[] data = new byte[currentLength[targetIndex] + Integer.BYTES]; // k + v + length
            System.arraycopy(curDataBlock[targetIndex], pointers[targetIndex], data, 0, data.length);

            // 如果加上此kv则data block写满，则开启新data block，将旧data block的最大key和起始偏移记录到B树中
            if(totalOffset - dataBlockStartOffset + data.length > Constant.MAX_DATA_BLOCK_SIZE){
                // (max key in this data block -> data block start offset)插入B-Tree
                newSST.bTree.insert(lastKey, dataBlockStartOffset);
                // 此data block剩余部分使用0填满
                int vacant = (int) (Constant.MAX_DATA_BLOCK_SIZE + dataBlockStartOffset - totalOffset);
                byte[] buffer0 = new byte[vacant];
                newSST.appendToFile(buffer0);
                totalOffset += vacant;
                dataBlockStartOffset = totalOffset;
            }

            // 更新Bloom Filter
            newSST.bloomFilter.add(currentKeys[targetIndex]);

            // 写入新SSTable
            newSST.appendToFile(data);
            totalOffset += data.length;

            // 更新lastKey
            lastKey = currentKeys[targetIndex];

            // 更新minKey和maxKey，由于是归并合并，第一次一定是minKey，最后一次的一定是maxKey
            if(newSST.minKey.equals(new K()))
                newSST.minKey = currentKeys[targetIndex];
            newSST.maxKey = currentKeys[targetIndex];

            // 更新pointers、currentKeys、currentLength
            for(Integer x : minKeyIndex){
                // 更新pointers
                pointers[x] += currentLength[x] + Integer.BYTES;

                // 尝试读取length，如果超出了data block的最大大小，则读取下一个data block
                if(pointers[x] + Integer.BYTES > Constant.MAX_DATA_BLOCK_SIZE){
                    curDataBlockIndex[x] += 1;
                    pointers[x] = 0;
                    // 如果data block已经读完，则该SSTable结束
                    if(curDataBlockIndex[x] > dataBlockCount[x])
                        continue;
                    readAccesses[x].read(curDataBlock[x]);
                }

                // 更新currentLength
                currentLength[x] = Constant.BYTES_TO_INT(curDataBlock[x], pointers[x], Integer.BYTES); // 提取length

                // 如果length = 0，说明该data block剩余部分都是补位0，该data block结束
                if(currentLength[x] == 0){
                    curDataBlockIndex[x] += 1;
                    pointers[x] = 0;
                    // 如果data block已经读完，则该SSTable结束
                    if(curDataBlockIndex[x] > dataBlockCount[x])
                        continue;
                    long i = readAccesses[x].getFilePointer();
                    readAccesses[x].read(curDataBlock[x]);
                    // 重新更新currentLength
                    currentLength[x] = Constant.BYTES_TO_INT(curDataBlock[x], pointers[x], Integer.BYTES); // 提取length
                }

                // 更新currentKeys
                System.arraycopy(curDataBlock[x], pointers[x] + 4, keyBuffer, 0, Constant.MAX_KEY_LENGTH); // 提取key
                currentKeys[x] = new K(keyBuffer);
            }
        }
        //  遍历结束时，未满的data block信息也写入B-Tree
        if(dataBlockStartOffset != totalOffset){
            newSST.bTree.insert(newSST.maxKey, dataBlockStartOffset);
            // 此data block剩余部分使用0填满
            int vacant = (int) (Constant.MAX_DATA_BLOCK_SIZE + dataBlockStartOffset - totalOffset);
            byte[] buffer0 = new byte[vacant];
            newSST.appendToFile(buffer0);
            totalOffset += vacant;
            dataBlockStartOffset = totalOffset;
        }

        // 新SSTable的meta data写入
        // 写zone map
        long zoneMapStartOffset = totalOffset; // zone map的开始偏移
        long zoneMapLength = Constant.MAX_KEY_LENGTH * 2; // zone map的长度
        newSST.zoneMapOffset = zoneMapStartOffset;
        newSST.appendToFile(newSST.minKey.serialize());
        newSST.appendToFile(newSST.maxKey.serialize());
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

        // 收尾工作1. flush close 各种写通道和读通道
        newSST.outputStream.flush();
        newSST.outputStream.close();
        for(RandomAccessFile ra : readAccesses){
            ra.close();
        }

        // 收尾工作2. 删除对应的文件
        for(Integer fileSuffix : this.filesToCompact){
            File f = new File(Constant.DATABASE_DIR + "SSTable" + fileSuffix);
            if(f.exists())
                f.delete();
        }

        // 收尾工作4. 更新level，加新，删旧，将该SSTable添加到对应level中
        this.levelManager.levels[level].add(dataFileSuffix);
        // 旧SSTable从level中删除
        for(Integer i : set){
            this.levelManager.levels[level - 1].remove(i);
            this.levelManager.levels[level].remove(i);
            this.levelManager.levelInfo.remove("" + i);

            // 更新缓存
            this.levelManager.cacheManager.metaCache.remove(i);
        }

        // 收尾工作5. 更新levelInfo，结构  dataFileSuffix : level-length-minKey-maxKey
        this.levelManager.levelInfo.put("" + dataFileSuffix, level + "-" + (footerLength + footerStartOffset) + "-" + newSST.minKey + "-" + newSST.maxKey);

        // 收尾工作6. 更新缓存
        this.levelManager.cacheManager.metaCache.add(newSST);

        // 统计写放大
        //File f = new File(Constant.DATABASE_DIR + "SSTable" + dataFileSuffix);
        //Statistics.actualWriteSize += f.length();

    }


    // 在currentKeys中找到最小的key，返回对应的下标（可能不止一个）
    // 如果pointer > ceiling，则不在考虑范围内
    private List<Integer> findMinKeyIndex(int[] pointers, K[] currentKeys, int[] curDataBlockIndex, int[] dataBlockCount){
        List<K> scope = new ArrayList<>();// 考虑范围内的key
        // 如果curDataBlockIndex > dataBlockCount，则不在考虑范围内
        for(int i=0; i<pointers.length; i++){
            if(curDataBlockIndex[i] < dataBlockCount[i]){
                scope.add(currentKeys[i]);
            }
        }
        List<Integer> ret = new ArrayList<>();
        if(scope.size() == 0)
            return ret;

        // 找最小key
        K minKey = Collections.min(scope);

        // 最小key可能有多个
        for(int i=0; i<currentKeys.length; i++){
            if(currentKeys[i].equals(minKey)){
                ret.add(i);
            }
        }
        return ret;
    }


}
